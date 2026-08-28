package httpapi

// Benchmarks for view index build throughput and reduce query latency.
// Run with:
//
//	go test ./internal/httpapi -bench BenchmarkView -benchtime 5x -run xxx
//
// Needs the same local Postgres the integration tests use.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// BenchmarkRequestBodyAdmission measures the byte-weighted fast and queued
// paths for the body sizes called out by PERF2-04, from one principal and from
// many. It is database-independent so admission changes remain cheap to track.
func BenchmarkRequestBodyAdmission(b *testing.B) {
	sizes := []struct {
		name  string
		bytes int64
	}{
		{name: "1KiB", bytes: 1 << 10},
		{name: "64KiB", bytes: 64 << 10},
		{name: "1MiB", bytes: 1 << 20},
		{name: "maximum", bytes: defaultMaxHTTPRequestSize},
	}
	for _, principalCount := range []int{1, 16} {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("principals=%d/size=%s", principalCount, size.name), func(b *testing.B) {
				if principalCount > 1 {
					b.SetParallelism(max(1,
						(principalCount+runtime.GOMAXPROCS(0)-1)/runtime.GOMAXPROCS(0)))
				}
				limiter := newRequestBodyLimiter()
				principals := make([]string, principalCount)
				for i := range principals {
					principals[i] = fmt.Sprintf("benchmark-%d", i)
				}
				weight := requestBodyReservation(
					&http.Request{ContentLength: size.bytes}, defaultMaxHTTPRequestSize)
				var worker atomic.Uint64
				b.ReportMetric(float64(weight), "reserved_bytes/op")
				b.RunParallel(func(pb *testing.PB) {
					principal := principals[int(worker.Add(1)-1)%len(principals)]
					for pb.Next() {
						release, err := limiter.acquire(b.Context(), principal, weight)
						if err != nil {
							b.Errorf("acquire: %v", err)
							return
						}
						release()
					}
				})
			})
		}
	}
}

// benchDocs bulk-inserts n small docs shaped for the benchmark views.
func benchDocs(b *testing.B, h http.Handler, db string, n int) {
	b.Helper()
	const batch = 2000
	for start := 0; start < n; start += batch {
		docs := []any{}
		for i := start; i < min(start+batch, n); i++ {
			docs = append(docs, map[string]any{
				"_id":   fmt.Sprintf("d%07d", i),
				"group": i % 100,
				"n":     i,
			})
		}
		resp := send(b, h, "POST", "/"+db+"/_bulk_docs",
			map[string]any{"docs": docs}, testAdminAuth, adminAuth())
		if resp.status != 201 {
			b.Fatalf("bulk seed: %+v", resp)
		}
	}
}

// BenchmarkBulkDocsVDUSnapshot tracks the VDU-heavy bulk path from HTTP JSON
// encoding through its set-based write. Each 500-document request should use
// one design probe, one old-winner batch, and one security read.
func BenchmarkBulkDocsVDUSnapshot(b *testing.B) {
	s := testServer(b, testHTTPStore(b))
	const db = "bench_bulk_vdu_snapshot"
	_ = s.store.DeleteDatabase(b.Context(), db)
	if err := s.store.CreateDatabase(b.Context(), db, false); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.store.DeleteDatabase(context.Background(), db) })
	ddoc := map[string]any{
		"validate_doc_update": "function(newDoc){ if (!newDoc.valid) throw({forbidden: 'invalid'}); }",
	}
	if resp := send(b, s, "PUT", "/"+db+"/_design/rules", ddoc,
		testAdminAuth, adminAuth()); resp.status != 201 {
		b.Fatalf("install VDU: %+v", resp)
	}

	const batchSize = 500
	before := s.vdu.stats()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		docs := make([]any, batchSize)
		for i := range docs {
			docs[i] = map[string]any{
				"_id":   fmt.Sprintf("doc-%08d", iteration*batchSize+i),
				"valid": true,
				"value": i,
			}
		}
		resp := send(b, s, "POST", "/"+db+"/_bulk_docs",
			map[string]any{"docs": docs}, testAdminAuth, adminAuth())
		if resp.status != 201 || len(resp.array) != batchSize {
			b.Fatalf("bulk response: status=%d rows=%d", resp.status, len(resp.array))
		}
	}
	b.StopTimer()
	after := s.vdu.stats()
	b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "docs/sec")
	b.ReportMetric(float64(after.DesignProbes-before.DesignProbes)/float64(b.N),
		"design_probes/op")
	b.ReportMetric(float64(after.OldDocBatches-before.OldDocBatches)/float64(b.N),
		"old_winner_batches/op")
	b.ReportMetric(float64(after.SecurityReads-before.SecurityReads)/float64(b.N),
		"security_reads/op")
}

// BenchmarkVDUValidationBatching isolates the PERF2-05 change from storage.
// individual_validation models the old bulk loop; batched_snapshot models the
// request-level snapshot while invoking the same JavaScript for every doc.
func BenchmarkVDUValidationBatching(b *testing.B) {
	s := testServer(b, testHTTPStore(b))
	const dbName = "bench_vdu_validation_batching"
	_ = s.store.DeleteDatabase(b.Context(), dbName)
	if err := s.store.CreateDatabase(b.Context(), dbName, false); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.store.DeleteDatabase(context.Background(), dbName) })
	ddoc := map[string]any{
		"validate_doc_update": "function(newDoc){ if (!newDoc.valid) throw({forbidden: 'invalid'}); }",
	}
	if resp := send(b, s, "PUT", "/"+dbName+"/_design/rules", ddoc,
		testAdminAuth, adminAuth()); resp.status != 201 {
		b.Fatalf("install VDU: %+v", resp)
	}
	db, err := s.store.GetDB(b.Context(), dbName)
	if err != nil {
		b.Fatal(err)
	}
	const batchSize = 500
	ids := make([]string, batchSize)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%03d", i)
	}
	body := map[string]any{"valid": true, "value": 1}
	req := httptest.NewRequest(http.MethodPost, "/"+dbName+"/_bulk_docs", nil)
	req = req.WithContext(context.WithValue(b.Context(), userCtxKey{}, &couch.UserCtx{
		Name: "admin", Roles: []string{"_admin"}, Authenticated: "default",
	}))
	// Warm the design-function cache so both cases measure the steady state.
	if _, err := s.newVDURequestSnapshot(req, db, ids, nil, false); err != nil {
		b.Fatal(err)
	}

	run := func(b *testing.B, batched bool) {
		before := s.vdu.stats()
		b.ResetTimer()
		for range b.N {
			if batched {
				snapshot, err := s.newVDURequestSnapshot(req, db, ids, nil, false)
				if err != nil {
					b.Fatal(err)
				}
				for _, id := range ids {
					if err := s.validateDocUpdateWithSnapshot(
						req.Context(), snapshot, id, body, nil, false, nil); err != nil {
						b.Fatal(err)
					}
				}
				continue
			}
			for _, id := range ids {
				if err := s.validateDocUpdate(req, db, id, body, nil, false, nil); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.StopTimer()
		after := s.vdu.stats()
		b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "docs/sec")
		b.ReportMetric(float64(after.DesignProbes-before.DesignProbes)/float64(b.N),
			"design_probes/op")
		b.ReportMetric(float64(after.OldDocBatches-before.OldDocBatches)/float64(b.N),
			"old_winner_batches/op")
		b.ReportMetric(float64(after.SecurityReads-before.SecurityReads)/float64(b.N),
			"security_reads/op")
	}
	b.Run("individual_validation", func(b *testing.B) { run(b, false) })
	b.Run("batched_snapshot", func(b *testing.B) { run(b, true) })
}

// BenchmarkPartitionPutAtScale measures the partition-limit path with enough
// existing documents to expose accidental partition scans.
func BenchmarkPartitionPutAtScale(b *testing.B) {
	st := testHTTPStore(b)
	h := testServer(b, st)
	const dbName = "bench_partition_put"
	const seedDocs = 20_000
	_ = st.DeleteDatabase(b.Context(), dbName)
	if err := st.CreateDatabase(b.Context(), dbName, true); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.DeleteDatabase(context.Background(), dbName) })
	db, err := st.GetDB(b.Context(), dbName)
	if err != nil {
		b.Fatal(err)
	}
	for start := 0; start < seedDocs; start += 1000 {
		writes := make([]store.BulkWrite, 0, 1000)
		for i := start; i < start+1000; i++ {
			writes = append(writes, store.BulkWrite{
				ID: fmt.Sprintf("p:seed%08d", i), Body: map[string]any{"value": i},
			})
		}
		results, err := st.BulkPutDocs(b.Context(), db, writes)
		if err != nil {
			b.Fatal(err)
		}
		for i, result := range results {
			if result.Err != nil {
				b.Fatalf("seed write %d: %v", start+i, result.Err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := send(b, h, "PUT", fmt.Sprintf("/%s/p:write%08d", dbName, i),
			map[string]any{"value": i}, testAdminAuth, adminAuth())
		if resp.status != 201 {
			b.Fatalf("partition write: %+v", resp)
		}
	}
	b.StopTimer()
	b.ReportMetric(seedDocs, "seed_docs")
}

// BenchmarkViewBuild measures JS map indexing throughput. Each iteration
// installs a fresh design doc (new signature, empty index) and queries it,
// forcing a full build over the seeded docs. Reports docs/sec.
func BenchmarkViewBuild(b *testing.B) {
	h := testHandler(b)
	const db = "bench_view_build"
	const nDocs = 2000
	send(b, h, "DELETE", "/"+db, nil, testAdminAuth, adminAuth())
	if resp := send(b, h, "PUT", "/"+db, nil, testAdminAuth, adminAuth()); resp.status != 201 {
		b.Fatalf("create db: %+v", resp)
	}
	defer send(b, h, "DELETE", "/"+db, nil, testAdminAuth, adminAuth())
	benchDocs(b, h, db, nDocs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ddoc := map[string]any{
			"views": map[string]any{
				"byn": map[string]any{
					// The comment varies the signature so every iteration
					// builds from scratch.
					"map": fmt.Sprintf("function(doc){ /* %d */ emit([doc.group, doc.n], doc.n); }", i),
				},
			},
		}
		resp := send(b, h, "PUT", fmt.Sprintf("/%s/_design/bench%d", db, i),
			ddoc, testAdminAuth, adminAuth())
		if resp.status != 201 {
			b.Fatalf("ddoc: %+v", resp)
		}
		q := send(b, h, "GET", fmt.Sprintf("/%s/_design/bench%d/_view/byn?limit=1", db, i),
			nil, testAdminAuth, adminAuth())
		if q.status != 200 {
			b.Fatalf("view build: %+v", q)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(nDocs*b.N)/b.Elapsed().Seconds(), "docs/sec")
}

// benchReduceHandler seeds one db + built view shared by the reduce query
// benchmarks and returns a handler for it.
func benchReduceHandler(b *testing.B, reduce string) (http.Handler, string) {
	h := testHandler(b)
	const db = "bench_view_reduce"
	const nDocs = 20000
	info := send(b, h, "GET", "/"+db+"/_design/bench/_view/byn?limit=0&reduce=false",
		nil, testAdminAuth, adminAuth())
	if info.status != 200 || info.body["total_rows"] != float64(nDocs) {
		// Build the fixture once. Later runs reuse it.
		send(b, h, "DELETE", "/"+db, nil, testAdminAuth, adminAuth())
		if resp := send(b, h, "PUT", "/"+db, nil, testAdminAuth, adminAuth()); resp.status != 201 {
			b.Fatalf("create db: %+v", resp)
		}
		benchDocs(b, h, db, nDocs)
		ddoc := map[string]any{
			"views": map[string]any{
				"byn": map[string]any{
					"map":    "function(doc){ emit([doc.group, doc.n], doc.n); }",
					"reduce": reduce,
				},
			},
		}
		if resp := send(b, h, "PUT", "/"+db+"/_design/bench", ddoc,
			testAdminAuth, adminAuth()); resp.status != 201 {
			b.Fatalf("ddoc: %+v", resp)
		}
		if resp := send(b, h, "GET", "/"+db+"/_design/bench/_view/byn?limit=1",
			nil, testAdminAuth, adminAuth()); resp.status != 200 {
			b.Fatalf("view build: %+v", resp)
		}
	}
	return h, db
}

func benchReduceQuery(b *testing.B, query string) {
	h, db := benchReduceHandler(b, "_sum")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := send(b, h, "GET", "/"+db+"/_design/bench/_view/byn"+query,
			nil, testAdminAuth, adminAuth())
		if resp.status != 200 {
			b.Fatalf("reduce query: %+v", resp)
		}
	}
}

// BenchmarkViewMapQuery is the plain map-view read path. It uses a keyed range
// with reduce=false, the per-query overhead floor for view reads.
func BenchmarkViewMapQuery(b *testing.B) {
	benchReduceQuery(b, "?startkey=[10]&endkey=[11]&reduce=false&limit=20")
}

func BenchmarkViewIncludeDocs(b *testing.B) {
	h, db := benchReduceHandler(b, "_sum")
	path := "/" + db + "/_design/bench/_view/byn?startkey=[10]&endkey=[11]&reduce=false" +
		"&include_docs=true&update=false&limit=20"
	if resp := send(b, h, "GET", path, nil, testAdminAuth, adminAuth()); resp.status != 200 || len(resp.body["rows"].([]any)) != 20 {
		b.Fatalf("warm include_docs view: %+v", resp)
	}
	b.ResetTimer()
	for range b.N {
		resp := send(b, h, "GET", path, nil, testAdminAuth, adminAuth())
		if resp.status != 200 || len(resp.body["rows"].([]any)) != 20 {
			b.Fatalf("include_docs view: %+v", resp)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(20*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkChangesIncludeDocs(b *testing.B) {
	h, db := benchReduceHandler(b, "_sum")
	path := "/" + db + "/_changes?include_docs=true&limit=20"
	if resp := send(b, h, "GET", path, nil, testAdminAuth, adminAuth()); resp.status != 200 || len(resp.body["results"].([]any)) != 20 {
		b.Fatalf("warm include_docs changes: %+v", resp)
	}
	b.ResetTimer()
	for range b.N {
		resp := send(b, h, "GET", path, nil, testAdminAuth, adminAuth())
		if resp.status != 200 || len(resp.body["results"].([]any)) != 20 {
			b.Fatalf("include_docs changes: %+v", resp)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(20*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkViewReduceNoGroup(b *testing.B)    { benchReduceQuery(b, "") }
func BenchmarkViewReduceGroupLevel(b *testing.B) { benchReduceQuery(b, "?group_level=1") }
func BenchmarkViewReduceGroupTrue(b *testing.B)  { benchReduceQuery(b, "?group=true&limit=100") }
func BenchmarkViewReduceRange(b *testing.B) {
	benchReduceQuery(b, `?startkey=[10]&endkey=[60]`)
}
