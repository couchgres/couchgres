package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
)

func freshPartitionDB(t testing.TB, s *Store, name string) *DB {
	t.Helper()
	ctx := t.Context()
	_ = s.DeleteDatabase(ctx, name)
	if err := s.CreateDatabase(ctx, name, true); err != nil {
		t.Fatalf("create partitioned db %s: %v", name, err)
	}
	t.Cleanup(func() { _ = s.DeleteDatabase(context.Background(), name) })
	db, err := s.GetDB(ctx, name)
	if err != nil {
		t.Fatalf("get partitioned db %s: %v", name, err)
	}
	return db
}

func BenchmarkPartitionInfoMetadata(b *testing.B) {
	s := testStore(b)
	ctx := b.Context()
	db := freshPartitionDB(b, s, "bench_partition_metadata")
	const docCount = 20_000
	for start := 0; start < docCount; start += 1000 {
		writes := make([]BulkWrite, 0, 1000)
		for i := start; i < start+1000; i++ {
			writes = append(writes, BulkWrite{
				ID: fmt.Sprintf("p:%08d", i), Body: map[string]any{"value": i},
			})
		}
		results, err := s.BulkPutDocs(ctx, db, writes)
		if err != nil {
			b.Fatal(err)
		}
		for i, result := range results {
			if result.Err != nil {
				b.Fatalf("seed write %d: %v", start+i, result.Err)
			}
		}
	}

	b.Run("striped_metadata", func(b *testing.B) {
		for b.Loop() {
			if _, err := s.GetPartitionStats(ctx, db, "p"); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(docCount, "docs")
	})
	b.Run("legacy_scans", func(b *testing.B) {
		lo, hi := PartitionRange("p")
		for b.Loop() {
			var live, deleted, bodies, attachments int64
			if err := s.pool.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s.docs WHERE NOT deleted AND id >= $1 AND id < $2",
				db.Schema), lo, hi).Scan(&live); err != nil {
				b.Fatal(err)
			}
			if err := s.pool.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s.docs WHERE deleted AND id >= $1 AND id < $2",
				db.Schema), lo, hi).Scan(&deleted); err != nil {
				b.Fatal(err)
			}
			if err := s.pool.QueryRow(ctx, fmt.Sprintf(
				`SELECT coalesce(sum(length(%s::text)), 0)
				 FROM %s.docs d %s
				 WHERE NOT d.deleted AND d.id >= $1 AND d.id < $2`,
				winnerBody, db.Schema, winnerJoin(db.Schema)), lo, hi).Scan(&bodies); err != nil {
				b.Fatal(err)
			}
			if err := s.pool.QueryRow(ctx, fmt.Sprintf(
				`SELECT coalesce(sum(a.length), 0)
				 FROM %[1]s.attachments a
				 JOIN %[1]s.docs d ON a.doc_id = d.id
				   AND a.rev_num = d.rev_num AND a.rev_hash = d.rev_hash
				 WHERE NOT d.deleted AND d.id >= $1 AND d.id < $2`, db.Schema),
				lo, hi).Scan(&attachments); err != nil {
				b.Fatal(err)
			}
			_ = live + deleted + bodies + attachments
		}
		b.ReportMetric(docCount, "docs")
	})
}

func TestPartitionStatsTrackWinnerTransitions(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshPartitionDB(t, s, "it_partition_stats")

	bodyA := body(t, `{"long":"value"}`)
	revA, _, err := s.PutDocWithLimit(ctx, db, "p:a", bodyA, nil, nil, false,
		[]AttachmentWrite{{Name: "a", ContentType: "text/plain", Data: []byte("hello")}}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(len(couch.CanonicalBody(bodyA)) + len("hello"))
	assertPartitionStats(t, s, db, "p", 1, 0, wantSize)

	// A stub carries the decoded attachment size while the body shrinks.
	bodyA = map[string]any{}
	revA, _, err = s.PutDocWithLimit(ctx, db, "p:a", bodyA, nil, &revA, false,
		[]AttachmentWrite{{Name: "a"}}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	assertPartitionStats(t, s, db, "p", 1, 0,
		int64(len(couch.CanonicalBody(bodyA))+len("hello")))

	revA, _, err = s.PutDocWithLimit(ctx, db, "p:a", map[string]any{}, nil, &revA, true, nil, 1)
	if err != nil {
		t.Fatalf("delete while over limit: %v", err)
	}
	assertPartitionStats(t, s, db, "p", 0, 1, 0)

	// Replication bypasses admission but still updates the same exact metadata.
	bodyB := body(t, `{"replicated":true}`)
	revB := couch.Rev{Num: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := s.ForceRev(ctx, db, "p:b", bodyB, []couch.Rev{revB}, false,
		[]AttachmentWrite{{Name: "b", ContentType: "application/octet-stream", Data: []byte("xyz")}}); err != nil {
		t.Fatal(err)
	}
	wantSize = int64(len(couch.CanonicalBody(bodyB)) + len("xyz"))
	assertPartitionStats(t, s, db, "p", 1, 1, wantSize)

	if _, err := s.Purge(ctx, db, map[string][]couch.Rev{"p:b": {revB}}); err != nil {
		t.Fatal(err)
	}
	assertPartitionStats(t, s, db, "p", 0, 1, 0)

	results, err := s.BulkPutDocsWithLimit(ctx, db, []BulkWrite{
		{ID: "p:c", Body: map[string]any{}},
		{ID: "p:d", Body: map[string]any{}, Deleted: true},
	}, 1<<30)
	if err != nil || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("bulk write: %+v, %v", results, err)
	}
	assertPartitionStats(t, s, db, "p", 1, 2, int64(len(couch.CanonicalBody(map[string]any{}))))
	assertPartitionStats(t, s, db, "missing", 0, 0, 0)
}

func TestBulkPartitionLimitAppliesInRequestOrder(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshPartitionDB(t, s, "it_partition_bulk_limit")
	empty := map[string]any{}

	// Each body is two bytes. The second write is allowed to cross three bytes;
	// the third sees the batch-local total at four and is rejected.
	results, err := s.BulkPutDocsWithLimit(ctx, db, []BulkWrite{
		{ID: "p:a", Body: empty},
		{ID: "p:b", Body: empty},
		{ID: "p:c", Body: empty},
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("crossing writes rejected: %+v", results)
	}
	assertPartitionOverflow(t, results[2].Err)
	assertPartitionStats(t, s, db, "p", 2, 0, 4)

	// Request order matters: shrinking first returns below the limit, allowing
	// the following growth to cross it again in the same transaction.
	revA := results[0].Rev
	results, err = s.BulkPutDocsWithLimit(ctx, db, []BulkWrite{
		{ID: "p:a", Body: empty, Expected: &revA, Deleted: true},
		{ID: "p:c", Body: empty},
	}, 3)
	if err != nil || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("shrink then grow: %+v, %v", results, err)
	}
	assertPartitionStats(t, s, db, "p", 2, 1, 4)
}

func TestPartitionLimitIncludesAttachments(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshPartitionDB(t, s, "it_partition_attachment_limit")
	empty := map[string]any{}

	rev, _, err := s.PutDocWithLimit(ctx, db, "p:a", empty, nil, nil, false, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.PutDocWithLimit(ctx, db, "p:a", empty, nil, &rev, false,
		[]AttachmentWrite{{Name: "a", ContentType: "text/plain", Data: []byte("x")}}, 2)
	assertPartitionOverflow(t, err)
	assertPartitionStats(t, s, db, "p", 1, 0, 2)
}

func TestConcurrentPartitionGrowthSerializesAtLimit(t *testing.T) {
	s := testStore(t)
	db := freshPartitionDB(t, s, "it_partition_concurrent_limit")
	const writers = 8
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := s.PutDocWithLimit(context.Background(), db,
				fmt.Sprintf("p:%d", i), map[string]any{}, nil, nil, false, nil, 1)
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)

	accepted, rejected := 0, 0
	for err := range errs {
		if err == nil {
			accepted++
			continue
		}
		if ce, ok := err.(*couch.Error); ok && ce.Err == "partition_overflow" {
			rejected++
			continue
		}
		t.Fatalf("unexpected concurrent write error: %v", err)
	}
	if accepted != 1 || rejected != writers-1 {
		t.Fatalf("accepted=%d rejected=%d, want 1/%d", accepted, rejected, writers-1)
	}
	assertPartitionStats(t, s, db, "p", 1, 0, 2)
}

func TestPartitionedDatabaseStatsExcludeDesignDocBytes(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshPartitionDB(t, s, "it_partition_database_stats")
	partitionBody := body(t, `{"value":"partition"}`)
	if _, _, err := s.PutDoc(ctx, db, "p:doc", partitionBody,
		nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutDoc(ctx, db, "_design/rules",
		body(t, `{"views":{"all":{"map":"function(d){emit(d._id)}"}}}`),
		nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}

	info, err := s.DBInfo(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	wantExternal := int64(len(couch.CanonicalBody(partitionBody)))
	if info.DocCount != 2 || info.DocDelCount != 0 || info.ExternalSize != wantExternal {
		t.Fatalf("partitioned database stats = %+v, want 2/0/%d", info, wantExternal)
	}
}

func assertPartitionStats(
	t *testing.T,
	s *Store,
	db *DB,
	partition string,
	docCount, docDelCount, externalSize int64,
) {
	t.Helper()
	stats, err := s.GetPartitionStats(t.Context(), db, partition)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DocCount != docCount || stats.DocDelCount != docDelCount || stats.ExternalSize != externalSize {
		t.Fatalf("partition %q stats = %+v, want docs=%d deleted=%d size=%d",
			partition, stats, docCount, docDelCount, externalSize)
	}
}

func assertPartitionOverflow(t *testing.T, err error) {
	t.Helper()
	ce, ok := err.(*couch.Error)
	if !ok || ce.Status != 403 || ce.Err != "partition_overflow" {
		t.Fatalf("partition error = %v, want 403 partition_overflow", err)
	}
}
