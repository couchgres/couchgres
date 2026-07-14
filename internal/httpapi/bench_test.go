package httpapi

// Benchmarks for view index build throughput and reduce query latency.
// Run with:
//
//	go test ./internal/httpapi -bench BenchmarkView -benchtime 5x -run xxx
//
// Needs the same local Postgres the integration tests use.

import (
	"fmt"
	"net/http"
	"testing"
)

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

func BenchmarkViewReduceNoGroup(b *testing.B)    { benchReduceQuery(b, "") }
func BenchmarkViewReduceGroupLevel(b *testing.B) { benchReduceQuery(b, "?group_level=1") }
func BenchmarkViewReduceGroupTrue(b *testing.B)  { benchReduceQuery(b, "?group=true&limit=100") }
func BenchmarkViewReduceRange(b *testing.B) {
	benchReduceQuery(b, `?startkey=[10]&endkey=[60]`)
}
