package store

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

type blockingPurgeMapper struct {
	once    sync.Once
	entered chan struct{}
	release <-chan struct{}
}

func (m *blockingPurgeMapper) MapDocs(
	ctx context.Context,
	sig string,
	fns []string,
	lib map[string]any,
	docs []json.RawMessage,
) ([][][]ViewEmit, error) {
	m.once.Do(func() { close(m.entered) })
	select {
	case <-m.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return (numericViewMapper{}).MapDocs(ctx, sig, fns, lib, docs)
}

func TestViewPurgeGenerationStartsCurrentAndTracksPurges(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_view_purge_generation")
	const sig = "fresh_view"

	// A view first used after earlier purges starts at the current generation.
	if _, err := s.pool.Exec(ctx,
		"UPDATE couchgres.databases SET purge_seq = 7 WHERE name = $1", db.Name); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureViewTables(ctx, db, sig); err != nil {
		t.Fatal(err)
	}
	lastSeq, purgeSeq, err := s.ViewGroupSnapshot(ctx, db, sig)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 0 || purgeSeq != 7 {
		t.Fatalf("initial view state = (%d, %d), want (0, 7)", lastSeq, purgeSeq)
	}

	// Later purges advance every existing view-state row transactionally.
	if _, err := s.pool.Exec(ctx,
		"UPDATE couchgres.databases SET purge_seq = purge_seq + 1 WHERE name = $1", db.Name); err != nil {
		t.Fatal(err)
	}
	_, purgeSeq, err = s.ViewGroupSnapshot(ctx, db, sig)
	if err != nil {
		t.Fatal(err)
	}
	if purgeSeq != 8 {
		t.Fatalf("trigger-mirrored purge sequence = %d, want 8", purgeSeq)
	}
}

func TestPurgeAndViewUpdateUseCompatibleLockOrder(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_view_purge_lock_order")
	rev, _, err := s.PutDoc(ctx, db, "doc", map[string]any{"n": 1}, nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	vg := &ViewGroup{
		Sig:       "purge_lock_order",
		Views:     map[string]ViewDef{"by_n": {Map: "function(doc){emit(doc.n,null);}"}},
		ViewOrder: []string{"by_n"},
	}
	if _, err := s.SyncViewGroup(ctx, db, vg, numericViewMapper{}); err != nil {
		t.Fatal(err)
	}
	rev, _, err = s.PutDoc(ctx, db, "doc", map[string]any{"n": 2}, nil, &rev, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	mapper := &blockingPurgeMapper{entered: make(chan struct{}), release: release}
	viewDone := make(chan error, 1)
	go func() {
		_, err := s.SyncViewGroup(ctx, db, vg, mapper)
		viewDone <- err
	}()
	select {
	case <-mapper.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for view mapper")
	}

	type purgeResult struct {
		purged map[string][]couch.Rev
		err    error
	}
	purgeDone := make(chan purgeResult, 1)
	go func() {
		purged, err := s.Purge(ctx, db, map[string][]couch.Rev{"doc": {rev}})
		purgeDone <- purgeResult{purged: purged, err: err}
	}()
	select {
	case result := <-purgeDone:
		close(release)
		<-viewDone
		t.Fatalf("purge passed the locked view state: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	select {
	case err := <-viewDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for view update")
	}
	select {
	case result := <-purgeDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.purged["doc"]) != 1 || result.purged["doc"][0] != rev {
			t.Fatalf("purged revisions = %+v", result.purged)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for purge")
	}

	lastSeq, purgeSeq, err := s.SyncViewGroupSnapshot(ctx, db, vg, numericViewMapper{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.QueryView(ctx, db, vg, "by_n", &ViewQuery{
		LastSeqHint: lastSeq, PurgeSeqHint: &purgeSeq,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalRows != 0 || len(result.Rows) != 0 {
		t.Fatalf("view after concurrent purge = %+v", result)
	}
}
