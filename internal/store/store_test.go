package store

// Integration tests require a reachable Postgres 17. They are skipped when the
// database is unavailable. Override with COUCHGRES_TEST_PG_URL.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

func testStoreURL() string {
	url := os.Getenv("COUCHGRES_TEST_PG_URL")
	if url == "" {
		url = "postgres://localhost/couchgres_test"
	}
	return url
}

func testStore(t *testing.T) *Store {
	t.Helper()
	url := testStoreURL()
	s, err := New(t.Context(), url, 4)
	if err != nil {
		t.Skipf("Postgres unavailable at %s: %v", url, err)
	}
	t.Cleanup(s.Close)
	if _, err := s.Bootstrap(t.Context()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return s
}

func TestReplicatorAdvisoryLockSingleton(t *testing.T) {
	s1 := testStore(t)
	s2, err := New(t.Context(), testStoreURL(), 4)
	if err != nil {
		t.Fatalf("second store: %v", err)
	}
	t.Cleanup(s2.Close)

	holderCtx, cancelHolder := context.WithCancel(context.Background())
	holderStarted := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		acquired, err := s1.RunWithReplicatorLock(holderCtx, func(ctx context.Context) {
			close(holderStarted)
			<-ctx.Done()
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			holderDone <- err
			return
		}
		if !acquired {
			holderDone <- errors.New("holder did not acquire replicator lock")
			return
		}
		holderDone <- nil
	}()
	select {
	case <-holderStarted:
	case <-time.After(5 * time.Second):
		cancelHolder()
		t.Fatal("timed out waiting for holder to acquire lock")
	}

	ran := false
	acquired, err := s2.RunWithReplicatorLock(t.Context(), func(context.Context) {
		ran = true
	})
	if err != nil {
		t.Fatalf("contender lock attempt: %v", err)
	}
	if acquired || ran {
		t.Fatalf("contender acquired lock while holder was active: acquired=%v ran=%v", acquired, ran)
	}

	cancelHolder()
	select {
	case err := <-holderDone:
		if err != nil {
			t.Fatalf("holder: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for holder release")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		ran = false
		acquired, err = s2.RunWithReplicatorLock(t.Context(), func(context.Context) {
			ran = true
		})
		if err != nil {
			t.Fatalf("post-release lock attempt: %v", err)
		}
		if acquired {
			if !ran {
				t.Fatal("lock holder callback did not run")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for lock takeover")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func freshDB(t *testing.T, s *Store, name string) *DB {
	t.Helper()
	ctx := t.Context()
	_ = s.DeleteDatabase(ctx, name)
	if err := s.CreateDatabase(ctx, name, false); err != nil {
		t.Fatalf("create db %s: %v", name, err)
	}
	t.Cleanup(func() { _ = s.DeleteDatabase(context.Background(), name) })
	db, err := s.GetDB(ctx, name)
	if err != nil {
		t.Fatalf("get db %s: %v", name, err)
	}
	return db
}

func body(t *testing.T, src string) map[string]any {
	t.Helper()
	v, err := couch.DecodeJSON([]byte(src))
	if err != nil {
		t.Fatalf("decode %s: %v", src, err)
	}
	return v.(map[string]any)
}

func couchErr(t *testing.T, err error) *couch.Error {
	t.Helper()
	ce, ok := err.(*couch.Error)
	if !ok {
		t.Fatalf("expected couch error, got %v", err)
	}
	return ce
}

func TestBootstrapSystemDBsAndStableUUID(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	uuid1, err := s.Bootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	uuid2, err := s.Bootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if uuid1 != uuid2 {
		t.Fatal("server uuid must be stable across restarts")
	}
	names, err := s.ListDatabases(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, sys := range []string{"_users", "_replicator"} {
		if !slices.Contains(names, sys) {
			t.Errorf("system db %s missing from %v", sys, names)
		}
	}
	var listIndexExists bool
	if err := s.pool.QueryRow(ctx,
		"SELECT to_regclass('couchgres.databases_name_c_idx') IS NOT NULL",
	).Scan(&listIndexExists); err != nil {
		t.Fatal(err)
	}
	if !listIndexExists {
		t.Fatal("byte-ordered database-name index was not installed")
	}
}

func TestListDatabasesAppliesQueryOptions(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	names := []string{
		"it_list_dbs_a", "it_list_dbs_b", "it_list_dbs_c", "it_list_dbs_d",
	}
	for _, name := range names {
		freshDB(t, s, name)
	}

	a, b, c, d := names[0], names[1], names[2], names[3]
	bNUL, cNUL := b+"\x00", c+"\x00"
	zero, one, two, ten := int64(0), int64(1), int64(2), int64(10)
	tests := []struct {
		name   string
		params ListDatabasesParams
		want   []string
	}{
		{
			name:   "inclusive bounds",
			params: ListDatabasesParams{StartKey: &b, EndKey: &d},
			want:   []string{b, c, d},
		},
		{
			name: "descending bounds",
			params: ListDatabasesParams{
				Descending: true, StartKey: &d, EndKey: &b,
			},
			want: []string{d, c, b},
		},
		{
			name: "skip and limit",
			params: ListDatabasesParams{
				StartKey: &a, EndKey: &d, Skip: &one, Limit: &two,
			},
			want: []string{b, c},
		},
		{
			name: "descending skip and limit",
			params: ListDatabasesParams{
				Descending: true, StartKey: &d, EndKey: &a,
				Skip: &one, Limit: &two,
			},
			want: []string{c, b},
		},
		{
			name: "zero limit",
			params: ListDatabasesParams{
				StartKey: &a, EndKey: &d, Limit: &zero,
			},
			want: []string{},
		},
		{
			name: "skip past end",
			params: ListDatabasesParams{
				StartKey: &a, EndKey: &d, Skip: &ten,
			},
			want: []string{},
		},
		{
			name:   "NUL start bound",
			params: ListDatabasesParams{StartKey: &bNUL, EndKey: &d},
			want:   []string{c, d},
		},
		{
			name:   "NUL end bound",
			params: ListDatabasesParams{StartKey: &a, EndKey: &cNUL},
			want:   []string{a, b, c},
		},
		{
			name:   "empty range",
			params: ListDatabasesParams{StartKey: &d, EndKey: &b},
			want:   []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.ListDatabases(ctx, &tt.params)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ListDatabases() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatabaseLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_lifecycle")

	err := s.CreateDatabase(ctx, "it_lifecycle", false)
	if ce := couchErr(t, err); ce.Err != "file_exists" || ce.Status != 412 {
		t.Fatalf("duplicate create: %v", ce)
	}

	info, err := s.DBInfo(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if info.DocCount != 0 || info.UpdateSeq != 0 {
		t.Fatalf("fresh db info: %+v", info)
	}

	if err := s.DeleteDatabase(ctx, "it_lifecycle"); err != nil {
		t.Fatal(err)
	}
	if ce := couchErr(t, s.DeleteDatabase(ctx, "it_lifecycle")); ce.Status != 404 {
		t.Fatalf("double delete: %v", ce)
	}
	if _, err := s.GetDB(ctx, "it_lifecycle"); err == nil {
		t.Fatal("deleted db still resolvable")
	}
}

func TestDatabaseInfosBatch(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	a := freshDB(t, s, "it_info_a")
	b := freshDB(t, s, "it_info_b")

	if _, _, err := s.PutDoc(ctx, a, "live", body(t, `{"a":1}`), nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	purgeRev, _, err := s.PutDoc(ctx, a, "purge-me", body(t, `{"gone":true}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Purge(ctx, a, map[string][]couch.Rev{"purge-me": {purgeRev}}); err != nil {
		t.Fatal(err)
	}
	bRev, _, err := s.PutDoc(ctx, b, "deleted", map[string]any{}, nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutDoc(ctx, b, "deleted", map[string]any{}, nil, &bRev, true, nil); err != nil {
		t.Fatal(err)
	}

	infos, err := s.DatabaseInfos(ctx,
		[]string{"it_info_b", "missing", "Bad", "bad\x00name", "it_info_a", "it_info_b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 || infos[0].DB.Name != "it_info_a" || infos[1].DB.Name != "it_info_b" {
		t.Fatalf("batched database order: %+v", infos)
	}
	if got := infos[0].Info; got.DocCount != 1 || got.DocDelCount != 0 ||
		got.UpdateSeq != 2 || got.PurgeSeq != 1 || got.SizeBytes <= 0 || got.ExternalSize <= 0 {
		t.Fatalf("database a info: %+v", got)
	}
	if got := infos[1].Info; got.DocCount != 0 || got.DocDelCount != 1 ||
		got.UpdateSeq != 2 || got.PurgeSeq != 0 || got.SizeBytes <= 0 || got.ExternalSize != 0 {
		t.Fatalf("database b info: %+v", got)
	}
	external, err := s.SizeDocsAll(ctx, a)
	if err != nil || external != infos[0].Info.ExternalSize {
		t.Fatalf("batched external size %d, direct size %d: %v",
			infos[0].Info.ExternalSize, external, err)
	}
	purgeSeq, err := s.PurgeSeq(ctx, a)
	if err != nil || purgeSeq != infos[0].Info.PurgeSeq {
		t.Fatalf("batched purge seq %d, direct seq %d: %v",
			infos[0].Info.PurgeSeq, purgeSeq, err)
	}

	single, err := s.DBInfo(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if *single != infos[0].Info {
		t.Fatalf("single info %+v differs from batch %+v", *single, infos[0].Info)
	}
	empty, err := s.DatabaseInfos(ctx, []string{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty database batch: %+v, %v", empty, err)
	}
}

func TestDocCRUDAndRevChain(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_docs")

	rev1, seq1, err := s.PutDoc(ctx, db, "doc1", body(t, `{"a":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev1.Num != 1 {
		t.Fatalf("first rev: %v", rev1)
	}

	// Writes without a revision or with a stale revision conflict.
	_, _, err = s.PutDoc(ctx, db, "doc1", body(t, `{"a":2}`), nil, nil, false, nil)
	if couchErr(t, err).Err != "conflict" {
		t.Fatal("missing rev accepted")
	}
	stale := couch.Rev{Num: 1, Hash: "00000000000000000000000000000000"}
	_, _, err = s.PutDoc(ctx, db, "doc1", body(t, `{"a":2}`), nil, &stale, false, nil)
	if couchErr(t, err).Err != "conflict" {
		t.Fatal("stale rev accepted")
	}

	rev2, seq2, err := s.PutDoc(ctx, db, "doc1", body(t, `{"a":2}`), nil, &rev1, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev2.Num != 2 || seq2 <= seq1 {
		t.Fatalf("second write: rev=%v seq=%d", rev2, seq2)
	}

	row, err := s.GetDoc(ctx, db, "doc1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if row.Rev != rev2 || row.Body["a"].(json.Number).String() != "2" {
		t.Fatalf("head read: %+v", row)
	}

	// Old revisions are unreadable with linear-history, compacted semantics.
	_, err = s.GetDoc(ctx, db, "doc1", &rev1)
	if couchErr(t, err).Reason != "missing" {
		t.Fatalf("old rev read: %v", err)
	}

	// Delete returns 404 "deleted". GetDocAny exposes the tombstone.
	rev3, _, err := s.PutDoc(ctx, db, "doc1", map[string]any{}, nil, &rev2, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rev3.Num != 3 {
		t.Fatalf("tombstone rev: %v", rev3)
	}
	_, err = s.GetDoc(ctx, db, "doc1", nil)
	if couchErr(t, err).Reason != "deleted" {
		t.Fatalf("deleted read: %v", err)
	}
	tombstone, err := s.GetDocAny(ctx, db, "doc1")
	if err != nil || !tombstone.Deleted {
		t.Fatalf("tombstone read: %+v %v", tombstone, err)
	}

	// Recreate over the tombstone without a rev.
	rev4, _, err := s.PutDoc(ctx, db, "doc1", body(t, `{"back":true}`), nil, nil, false, nil)
	if err != nil || rev4.Num != 4 {
		t.Fatalf("recreate: %v %v", rev4, err)
	}

	history, err := s.AllRevs(ctx, db, "doc1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 4 || history[0].Num != 4 || history[3].Num != 1 {
		t.Fatalf("history: %+v", history)
	}

	info, err := s.DBInfo(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if info.DocCount != 1 || info.DocDelCount != 0 {
		t.Fatalf("info after recreate: %+v", info)
	}
}

func TestDocumentCountsTrackEveryWinnerTransition(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_doc_counts")
	assertDocumentCounts(t, s, db, 0, 0)

	rev, _, err := s.PutDoc(ctx, db, "interactive", body(t, `{"v":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 1, 0)

	rev, _, err = s.PutDoc(ctx, db, "interactive", body(t, `{"v":2}`), nil, &rev, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 1, 0)

	rev, _, err = s.PutDoc(ctx, db, "interactive", map[string]any{}, nil, &rev, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 0, 1)

	if _, _, err = s.PutDoc(ctx, db, "interactive", body(t, `{"back":true}`), nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 1, 0)

	results, err := s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "bulk-live", Body: body(t, `{"live":true}`)},
		{ID: "bulk-deleted", Body: map[string]any{}, Deleted: true},
	})
	if err != nil || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("bulk write: %+v, %v", results, err)
	}
	assertDocumentCounts(t, s, db, 2, 1)

	deletedRev := couch.Rev{Num: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := s.ForceRev(ctx, db, "replicated", map[string]any{},
		[]couch.Rev{deletedRev}, true, nil); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 2, 2)

	liveRev := couch.Rev{Num: 2, Hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	if err := s.ForceRev(ctx, db, "replicated", body(t, `{"live":true}`),
		[]couch.Rev{liveRev, deletedRev}, false, nil); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 3, 1)

	live, err := s.GetDocAny(ctx, db, "bulk-live")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Purge(ctx, db, map[string][]couch.Rev{"bulk-live": {live.Rev}}); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 2, 1)

	deleted, err := s.GetDocAny(ctx, db, "bulk-deleted")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Purge(ctx, db, map[string][]couch.Rev{"bulk-deleted": {deleted.Rev}}); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 2, 0)
}

func TestBootstrapBackfillsDocumentCountsOnce(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_doc_count_migration")

	if _, _, err := s.PutDoc(ctx, db, "live", body(t, `{"v":1}`), nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	rev, _, err := s.PutDoc(ctx, db, "deleted", map[string]any{}, nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutDoc(ctx, db, "deleted", map[string]any{}, nil, &rev, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		`DROP TRIGGER docs_count_insert ON %[1]s.docs;
		 DROP TRIGGER docs_count_update ON %[1]s.docs;
		 DROP TRIGGER docs_count_delete ON %[1]s.docs`, db.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE couchgres.databases
		 SET doc_count = 0, doc_del_count = 0, doc_counts_initialized = false
		 WHERE name = $1`, db.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 1, 1)
	if _, _, err := s.PutDoc(ctx, db, "after-migration", body(t, `{"v":1}`), nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	assertDocumentCounts(t, s, db, 2, 1)
}

func TestDocumentCountsRemainExactUnderConcurrentCreation(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_doc_count_race")

	start := make(chan struct{})
	errs := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for i := range 2 {
		go func() {
			ready.Done()
			<-start
			_, _, err := s.PutDoc(ctx, db, "same-id",
				map[string]any{"writer": json.Number(strconv.Itoa(i))}, nil, nil, false, nil)
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	succeeded := 0
	for range 2 {
		err := <-errs
		if err == nil {
			succeeded++
			continue
		}
		if ce, ok := err.(*couch.Error); !ok || ce.Err != "conflict" {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	if succeeded == 0 {
		t.Fatal("both concurrent creates failed")
	}
	assertDocumentCounts(t, s, db, 1, 0)
}

func assertDocumentCounts(t *testing.T, s *Store, db *DB, wantLive, wantDeleted int64) {
	t.Helper()
	info, err := s.DBInfo(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if info.DocCount != wantLive || info.DocDelCount != wantDeleted {
		t.Fatalf("metadata counts = (%d, %d), want (%d, %d)",
			info.DocCount, info.DocDelCount, wantLive, wantDeleted)
	}
	var actualLive, actualDeleted int64
	if err := s.pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FILTER (WHERE NOT deleted),
		        count(*) FILTER (WHERE deleted) FROM %s.docs`, db.Schema),
	).Scan(&actualLive, &actualDeleted); err != nil {
		t.Fatal(err)
	}
	if actualLive != info.DocCount || actualDeleted != info.DocDelCount {
		t.Fatalf("metadata counts = (%d, %d), table counts = (%d, %d)",
			info.DocCount, info.DocDelCount, actualLive, actualDeleted)
	}
}

func TestRevsLimitPrunesHistory(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	freshDB(t, s, "it_prune")
	if err := s.SetRevsLimit(ctx, "it_prune", 2); err != nil {
		t.Fatal(err)
	}
	db, err := s.GetDB(ctx, "it_prune")
	if err != nil || db.RevsLimit != 2 {
		t.Fatalf("revs_limit: %+v %v", db, err)
	}

	var rev *couch.Rev
	for i := range 5 {
		next, _, err := s.PutDoc(ctx, db, "doc",
			map[string]any{"n": json.Number(itoa(i))}, nil, rev, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		rev = &next
	}
	history, err := s.AllRevs(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Num != 5 || history[1].Num != 4 {
		t.Fatalf("pruned history: %+v", history)
	}
}

func TestAllDocsRangesAndKeys(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_alldocs")

	for _, id := range []string{"alpha", "beta", "gamma", "delta"} {
		if _, _, err := s.PutDoc(ctx, db, id, body(t, `{"x":1}`), nil, nil, false, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A deleted doc must not appear in ranges but must appear in keys mode.
	zrev, _, err := s.PutDoc(ctx, db, "zeta", map[string]any{}, nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutDoc(ctx, db, "zeta", map[string]any{}, nil, &zrev, true, nil); err != nil {
		t.Fatal(err)
	}

	page, err := s.AllDocs(ctx, db, &AllDocsParams{})
	if err != nil {
		t.Fatal(err)
	}
	if ids := rowIDs(page); !slices.Equal(ids, []string{"alpha", "beta", "delta", "gamma"}) {
		t.Fatalf("byte order: %v", ids)
	}
	if page.TotalRows != 4 || page.Offset == nil || *page.Offset != 0 {
		t.Fatalf("page meta: %+v", page)
	}

	start, end := "beta", "delta"
	page, err = s.AllDocs(ctx, db, &AllDocsParams{
		StartKey: &start, EndKey: &end, InclusiveEnd: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ids := rowIDs(page); !slices.Equal(ids, []string{"beta", "delta"}) {
		t.Fatalf("range: %v", ids)
	}
	if *page.Offset != 1 {
		t.Fatalf("range offset: %d", *page.Offset)
	}

	two := int64(2)
	page, err = s.AllDocs(ctx, db, &AllDocsParams{
		Descending: true, Limit: &two, Skip: 1, IncludeDocs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ids := rowIDs(page); !slices.Equal(ids, []string{"delta", "beta"}) {
		t.Fatalf("descending: %v", ids)
	}
	if page.Rows[0].Body == nil || page.Rows[1].Body == nil {
		t.Fatalf("seek pagination lost included docs: %+v", page.Rows)
	}

	page, err = s.AllDocsKeys(ctx, db,
		[]string{"gamma", "zeta", "nope", "gamma"}, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	rows := page.Rows
	if len(rows) != 4 {
		t.Fatalf("keys rows: %+v", rows)
	}
	if rows[0].Missing || rows[0].Body == nil {
		t.Fatalf("gamma row: %+v", rows[0])
	}
	if !rows[1].Deleted {
		t.Fatalf("zeta row should be deleted: %+v", rows[1])
	}
	if !rows[2].Missing {
		t.Fatalf("nope row should be missing: %+v", rows[2])
	}
	if rows[3].ID != "gamma" {
		t.Fatalf("duplicate key not preserved: %+v", rows[3])
	}
	if page.Offset != nil {
		t.Fatal("keys mode must not report offset")
	}
	if page.TotalRows != 4 {
		t.Fatalf("keys total_rows: %d", page.TotalRows)
	}
}

func TestPendingAfterCachesCommittedGenerations(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_pending")

	rev1, seq1, err := s.PutDoc(ctx, db, "one", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rev2, _, err := s.PutDoc(ctx, db, "two", body(t, `{"n":2}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, seq3, err := s.PutDoc(ctx, db, "three", body(t, `{"n":3}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	pending, err := s.PendingAfter(ctx, db, seq1)
	if err != nil || pending != 2 {
		t.Fatalf("initial pending = %d, %v", pending, err)
	}
	if got := pendingCacheEntry(t, &s.pendingCounts, db.Schema, seq1, seq3, 0); got != 2 {
		t.Fatalf("initial cache = %d", got)
	}
	if pending, err = s.PendingAfter(ctx, db, seq1); err != nil || pending != 2 {
		t.Fatalf("cached pending = %d, %v", pending, err)
	}
	if pending, err = s.PendingAfter(ctx, db, seq3); err != nil || pending != 0 {
		t.Fatalf("caught-up pending = %d, %v", pending, err)
	}

	_, seq4, err := s.PutDoc(ctx, db, "one", body(t, `{"n":4}`), nil, &rev1, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err = s.PendingAfter(ctx, db, seq1); err != nil || pending != 3 {
		t.Fatalf("new generation pending = %d, %v", pending, err)
	}
	if got := pendingCacheEntry(t, &s.pendingCounts, db.Schema, seq1, seq4, 0); got != 3 {
		t.Fatalf("new generation cache = %d", got)
	}

	if _, err := s.Purge(ctx, db, map[string][]couch.Rev{"two": {rev2}}); err != nil {
		t.Fatal(err)
	}
	if pending, err = s.PendingAfter(ctx, db, seq1); err != nil || pending != 2 {
		t.Fatalf("post-purge pending = %d, %v", pending, err)
	}
	if got := pendingCacheEntry(t, &s.pendingCounts, db.Schema, seq1, seq4, 1); got != 2 {
		t.Fatalf("post-purge cache = %d", got)
	}
}

func TestPendingAfterHandlesOutOfOrderCommits(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_pending_commit_order")

	_, _, err := s.PutDoc(ctx, db, "one", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.PutDoc(ctx, db, "two", body(t, `{"n":2}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	rev3, seq3, err := s.PutDoc(ctx, db, "three", body(t, `{"n":3}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	slow, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Rollback(ctx)
	if _, err := slow.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.docs SET seq = nextval('%s.update_seq') WHERE id = 'two'",
		db.Schema, db.Schema)); err != nil {
		t.Fatal(err)
	}
	_, seq5, err := s.PutDoc(ctx, db, "three", body(t, `{"n":5}`), nil, &rev3, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err := s.PendingAfter(ctx, db, seq3); err != nil || pending != 1 {
		t.Fatalf("pending before older commit = %d, %v", pending, err)
	}
	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.PendingAfter(ctx, db, seq3); err != nil || pending != 2 {
		t.Fatalf("pending after older commit = %d, %v", pending, err)
	}
	if seq5 <= seq3 {
		t.Fatal("test setup did not advance the update sequence")
	}
}

func pendingCacheEntry(t *testing.T, cache *pendingCountCache, schema string, after, maxSeq, purgeSeq int64) int64 {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for key, value := range cache.values {
		if key.schema == schema && key.after == after && key.maxSeq == maxSeq && key.purgeSeq == purgeSeq {
			return value
		}
	}
	t.Fatal("pending count was not cached")
	return 0
}

func TestPendingCountCacheIsBounded(t *testing.T) {
	var cache pendingCountCache
	for i := 0; i < pendingCountCacheCapacity+10; i++ {
		cache.put(pendingCountKey{schema: "db", after: int64(i)}, int64(i))
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.values) != pendingCountCacheCapacity || len(cache.order) != pendingCountCacheCapacity {
		t.Fatalf("cache sizes = (%d, %d), want %d",
			len(cache.values), len(cache.order), pendingCountCacheCapacity)
	}
	if _, ok := cache.values[pendingCountKey{schema: "db", after: 0}]; ok {
		t.Fatal("oldest cache entry was not evicted")
	}
}

func rowIDs(page *AllDocsPage) []string {
	ids := make([]string, len(page.Rows))
	for i, r := range page.Rows {
		ids[i] = r.ID
	}
	return ids
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func TestBulkPutDocs(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_bulk")

	// Seed one doc to update and one to conflict against.
	seedRev, _, err := s.PutDoc(ctx, db, "upd", body(t, `{"v":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	staleRev := couch.Rev{Num: 1, Hash: "00000000000000000000000000000000"}

	res, err := s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "new1", Body: body(t, `{"n":1}`), RawBody: []byte(`{"n":1}`)},
		{ID: "upd", Body: body(t, `{"v":2}`), Expected: &seedRev},
		{ID: "upd2", Body: body(t, `{"x":1}`), Expected: &staleRev}, // Missing document with a revision.
		{ID: "gone", Body: map[string]any{}, Deleted: true},         // A deleted document is created without a revision.
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || res[0].Rev.Num != 1 {
		t.Fatalf("new1: %+v", res[0])
	}
	if res[1].Err != nil || res[1].Rev.Num != 2 {
		t.Fatalf("upd: %+v", res[1])
	}
	if ce := couchErr(t, res[2].Err); ce.Status != 409 {
		t.Fatalf("stale rev should conflict: %+v", res[2])
	}
	if res[3].Err != nil || res[3].Rev.Num != 1 {
		t.Fatalf("deleted create: %+v", res[3])
	}

	// The batch revision matches what the sequential path would generate.
	want, err := couch.NextRev(false, nil, []byte(`{"n":1}`), nil)
	if err != nil || res[0].Rev != want {
		t.Fatalf("bulk rev %v, sequential rev %v (%v)", res[0].Rev, want, err)
	}

	// Winner cache and changes reflect the batch.
	row, err := s.GetDoc(ctx, db, "upd", nil)
	if err != nil || row.Rev != res[1].Rev {
		t.Fatalf("winner after bulk: %+v %v", row, err)
	}

	// Delete and recreate the same ID inside one batch. The recreate sees
	// the tombstone written earlier in the batch.
	res, err = s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "upd", Body: map[string]any{}, Expected: &res[1].Rev, Deleted: true},
		{ID: "upd", Body: body(t, `{"back":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil || res[0].Rev.Num != 3 {
		t.Fatalf("in-batch delete: %+v", res[0])
	}
	if res[1].Err != nil || res[1].Rev.Num != 4 {
		t.Fatalf("in-batch recreate on tombstone: %+v", res[1])
	}
	row, err = s.GetDoc(ctx, db, "upd", nil)
	if err != nil || row.Rev != res[1].Rev || row.Deleted {
		t.Fatalf("winner after delete+recreate: %+v %v", row, err)
	}
	// The tombstone must not linger as a deleted-conflict leaf.
	leaves, err := s.Leaves(ctx, db, "upd")
	if err != nil || len(leaves) != 1 {
		t.Fatalf("leaves after in-batch chain: %+v %v", leaves, err)
	}

	// A second write to the same ID without the first revision conflicts.
	res, err = s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "dup", Body: body(t, `{"a":1}`)},
		{ID: "dup", Body: body(t, `{"a":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil {
		t.Fatalf("first dup write: %+v", res[0])
	}
	if ce := couchErr(t, res[1].Err); ce.Status != 409 {
		t.Fatalf("second dup write should conflict: %+v", res[1])
	}
}
