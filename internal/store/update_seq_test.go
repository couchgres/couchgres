package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

type numericViewMapper struct{}

func (numericViewMapper) MapDocs(
	_ context.Context,
	_ string,
	fns []string,
	_ map[string]any,
	docs []json.RawMessage,
) ([][][]ViewEmit, error) {
	out := make([][][]ViewEmit, len(docs))
	for i, raw := range docs {
		var doc struct {
			N json.RawMessage `json:"n"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
		out[i] = make([][]ViewEmit, len(fns))
		for j := range fns {
			out[i][j] = []ViewEmit{{Key: doc.N, Value: json.RawMessage("null")}}
		}
	}
	return out, nil
}

func prepareDocumentUpdate(
	ctx context.Context,
	tx pgx.Tx,
	s *Store,
	db *DB,
	id string,
	expected couch.Rev,
	n int,
) (couch.Rev, error) {
	winner, found, err := lockWinner(ctx, tx, db, id)
	if err != nil {
		return couch.Rev{}, err
	}
	if !found || winner.Rev != expected {
		return couch.Rev{}, fmt.Errorf("winner for %s is %+v, want %s", id, winner, expected.String())
	}
	body := map[string]any{"n": n}
	raw := couch.CanonicalBody(body)
	parent := winner.Rev
	rev, err := couch.NextRev(false, &parent, raw, nil)
	if err != nil {
		return couch.Rev{}, err
	}
	if err := insertLeaf(ctx, tx, db, id, rev, &parent, false, body, int64(len(raw)),
		s.keepSuperseded.Load()); err != nil {
		return couch.Rev{}, err
	}
	return rev, nil
}

func assertNumericViewKeys(
	t *testing.T,
	s *Store,
	db *DB,
	vg *ViewGroup,
	lastSeq int64,
	want ...string,
) {
	t.Helper()
	result, err := s.QueryView(t.Context(), db, vg, "by_n",
		&ViewQuery{LastSeqHint: lastSeq}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("view rows = %+v, want keys %v", result.Rows, want)
	}
	for i, row := range result.Rows {
		if string(row.Key) != want[i] {
			t.Fatalf("view key %d = %s, want %s", i, row.Key, want[i])
		}
	}
}

func TestUpdateSequenceCommitOrderProtectsChangesAndViews(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_update_seq_order")

	slowRev, _, err := s.PutDoc(ctx, db, "slow", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	fastRev, baseline, err := s.PutDoc(ctx, db, "fast", body(t, `{"n":2}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	vg := &ViewGroup{
		Sig:       "commit_order",
		Views:     map[string]ViewDef{"by_n": {Map: "function(doc){emit(doc.n,null);}"}},
		ViewOrder: []string{"by_n"},
	}
	mapper := numericViewMapper{}
	lastSeq, err := s.SyncViewGroup(ctx, db, vg, mapper)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != baseline {
		t.Fatalf("initial view sequence = %d, want %d", lastSeq, baseline)
	}
	assertNumericViewKeys(t, s, db, vg, lastSeq, "1", "2")

	slow, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Rollback(ctx)
	if _, err := prepareDocumentUpdate(ctx, slow, s, db, "slow", slowRev, 10); err != nil {
		t.Fatal(err)
	}
	slowSeq, err := finishWrite(ctx, slow, db, "slow")
	if err != nil {
		t.Fatal(err)
	}

	fast, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Rollback(ctx)
	if _, err := prepareDocumentUpdate(ctx, fast, s, db, "fast", fastRev, 20); err != nil {
		t.Fatal(err)
	}
	type finishResult struct {
		seq int64
		err error
	}
	started := make(chan struct{})
	finished := make(chan finishResult, 1)
	go func() {
		close(started)
		seq, err := finishWrite(ctx, fast, db, "fast")
		if err == nil {
			err = fast.Commit(ctx)
		}
		finished <- finishResult{seq: seq, err: err}
	}()
	<-started

	select {
	case result := <-finished:
		_ = slow.Rollback(ctx)
		if result.err != nil {
			t.Fatalf("higher-sequence writer failed before slow commit: %v", result.err)
		}
		t.Fatalf("higher sequence %d committed before lower sequence %d", result.seq, slowSeq)
	case <-time.After(100 * time.Millisecond):
	}

	// The counter update and both document revisions are still uncommitted.
	// Readers must see the previous committed watermark and view contents.
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != baseline {
		t.Fatalf("current sequence during slow commit = %d, %v; want %d", current, err, baseline)
	}
	lastSeq, err = s.SyncViewGroup(ctx, db, vg, mapper)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != baseline {
		t.Fatalf("view advanced over uncommitted update: got %d, want %d", lastSeq, baseline)
	}
	assertNumericViewKeys(t, s, db, vg, lastSeq, "1", "2")

	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var fastResult finishResult
	select {
	case fastResult = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the higher-sequence writer")
	}
	if fastResult.err != nil {
		t.Fatal(fastResult.err)
	}
	if slowSeq != baseline+1 || fastResult.seq != slowSeq+1 {
		t.Fatalf("committed sequences = (%d, %d), want (%d, %d)",
			slowSeq, fastResult.seq, baseline+1, baseline+2)
	}

	changes, err := s.Changes(ctx, db, &ChangesParams{Since: baseline})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].ID != "slow" || changes[0].Seq != slowSeq ||
		changes[1].ID != "fast" || changes[1].Seq != fastResult.seq {
		t.Fatalf("changes after concurrent commits = %+v", changes)
	}
	lastSeq, err = s.SyncViewGroup(ctx, db, vg, mapper)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != fastResult.seq {
		t.Fatalf("final view sequence = %d, want %d", lastSeq, fastResult.seq)
	}
	assertNumericViewKeys(t, s, db, vg, lastSeq, "10", "20")
}

func TestUpdateSequenceReservationRollsBack(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_update_seq_rollback")
	rev, first, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareDocumentUpdate(ctx, tx, s, db, "doc", rev, 2); err != nil {
		t.Fatal(err)
	}
	reserved, err := finishWrite(ctx, tx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if reserved != first+1 {
		t.Fatalf("reserved sequence = %d, want %d", reserved, first+1)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != first {
		t.Fatalf("sequence after rollback = %d, %v; want %d", current, err, first)
	}
	_, seq, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":3}`), nil, &rev, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if seq != first+1 {
		t.Fatalf("sequence after rollback = %d, want %d", seq, first+1)
	}
}

func TestBulkUpdateSequenceRangeSkipsConflicts(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_update_seq_bulk")

	results, err := s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "a", Body: body(t, `{"n":1}`)},
		{ID: "b", Body: body(t, `{"n":2}`)},
		{ID: "c", Body: body(t, `{"n":3}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("bulk result %d: %v", i, result.Err)
		}
	}
	changes, err := s.Changes(ctx, db, &ChangesParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("bulk changes = %+v", changes)
	}
	for i, change := range changes {
		if change.ID != string(rune('a'+i)) || change.Seq != int64(i+1) {
			t.Fatalf("bulk change %d = %+v", i, change)
		}
	}

	stale := couch.Rev{Num: 1, Hash: "00000000000000000000000000000000"}
	conflicts, err := s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "a", Body: body(t, `{"n":10}`), Expected: &stale},
		{ID: "missing", Body: body(t, `{"n":11}`), Expected: &stale},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range conflicts {
		if ce, ok := result.Err.(*couch.Error); !ok || ce.Status != 409 {
			t.Fatalf("conflict result %d = %+v", i, result)
		}
	}
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != 3 {
		t.Fatalf("sequence after all-conflict bulk = %d, %v; want 3", current, err)
	}

	results, err = s.BulkPutDocs(ctx, db, []BulkWrite{
		{ID: "a", Body: body(t, `{"n":10}`), Expected: &results[0].Rev},
		{ID: "missing", Body: body(t, `{"n":11}`), Expected: &stale},
		{ID: "d", Body: body(t, `{"n":4}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[2].Err != nil {
		t.Fatalf("mixed bulk results = %+v", results)
	}
	changes, err = s.Changes(ctx, db, &ChangesParams{Since: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].ID != "a" || changes[0].Seq != 4 ||
		changes[1].ID != "d" || changes[1].Seq != 5 {
		t.Fatalf("mixed bulk changes = %+v", changes)
	}
}

func TestBootstrapMigratesLegacyUpdateSequence(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_update_seq_migration")

	if _, _, err := s.PutDoc(ctx, db, "before", body(t, `{"n":1}`), nil, nil, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		"CREATE SEQUENCE %s.update_seq", db.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		"SELECT setval('%s.update_seq', 41, true)", db.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE couchgres.databases
		 SET update_seq = 0, update_seq_initialized = false WHERE name = $1`, db.Name); err != nil {
		t.Fatal(err)
	}
	if err := s.initializeUpdateSeq(ctx, db.Schema); err != nil {
		t.Fatal(err)
	}
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != 41 {
		t.Fatalf("migrated sequence = %d, %v; want 41", current, err)
	}
	_, seq, err := s.PutDoc(ctx, db, "after", body(t, `{"n":2}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 {
		t.Fatalf("post-migration sequence = %d, want 42", seq)
	}

	var legacyExists bool
	if err := s.pool.QueryRow(ctx,
		"SELECT to_regclass($1) IS NOT NULL", db.Schema+".update_seq",
	).Scan(&legacyExists); err != nil {
		t.Fatal(err)
	}
	if legacyExists {
		t.Fatal("legacy update sequence still exists after migration")
	}
	// A repeated bootstrap is a no-op and keeps the committed cursor.
	if err := s.initializeUpdateSeq(ctx, db.Schema); err != nil {
		t.Fatal(err)
	}
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != 42 {
		t.Fatalf("legacy sequence changed committed cursor to %d, %v", current, err)
	}
}

func TestUpdateSequenceMigrationWaitsForLegacyAllocator(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_update_seq_migration_wait")

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		"CREATE SEQUENCE %s.update_seq", db.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE couchgres.databases
		 SET update_seq = 0, update_seq_initialized = false WHERE name = $1`, db.Name); err != nil {
		t.Fatal(err)
	}

	legacy, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Rollback(ctx)
	var legacySeq int64
	if err := legacy.QueryRow(ctx, fmt.Sprintf(
		"SELECT nextval('%s.update_seq')", db.Schema),
	).Scan(&legacySeq); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		close(started)
		finished <- s.initializeUpdateSeq(ctx, db.Schema)
	}()
	<-started
	select {
	case err := <-finished:
		t.Fatalf("migration passed an active legacy allocator: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := legacy.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for update-sequence migration")
	}
	if current, err := s.CurrentSeq(ctx, db); err != nil || current != legacySeq {
		t.Fatalf("migrated sequence = %d, %v; want %d", current, err, legacySeq)
	}
	_, seq, err := s.PutDoc(ctx, db, "after", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if seq != legacySeq+1 {
		t.Fatalf("post-migration sequence = %d, want %d", seq, legacySeq+1)
	}
}
