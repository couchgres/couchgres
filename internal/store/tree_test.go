package store

// Revision-tree tests cover replicated writes (new_edits=false), conflicts,
// winner selection, and the tree queries replication uses.

import (
	"slices"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
)

func rev(num int, hash string) couch.Rev {
	return couch.Rev{Num: num, Hash: hash}
}

func TestForceRevCreatesConflicts(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_tree")

	// An interactive write then a replicated sibling at the same depth creates the
	// classic conflict a two-master edit produces.
	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"local":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	remote := rev(2, "ffffffffffffffffffffffffffffffff")
	err = s.ForceRev(ctx, db, "doc", body(t, `{"remote":1}`),
		[]couch.Rev{remote, rev1}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	local2, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"local":2}`), nil, &rev1, false, nil)
	// The interactive write extended rev1, which ForceRev already extended.
	// rev1 is no longer the winner. Interactive writes must target the winner.
	// remote (2-fff...) wins over nothing yet. After ForceRev, remote is the
	// winner, so this PUT
	// against rev1 conflicts.
	if err == nil {
		t.Fatalf("PUT against a superseded rev accepted: %v", local2)
	}

	// The replicated rev is the winner (only live leaf).
	winner, err := s.GetDocAny(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if winner.Rev != remote {
		t.Fatalf("winner: %v, want %v", winner.Rev, remote)
	}

	// A second replicated branch from rev1 creates a true conflict.
	sibling := rev(2, "00000000000000000000000000000000")
	err = s.ForceRev(ctx, db, "doc", body(t, `{"remote":2}`),
		[]couch.Rev{sibling, rev1}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	leaves, err := s.Leaves(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if len(leaves) != 2 {
		t.Fatalf("want 2 conflict leaves, got %+v", leaves)
	}
	// open_revs order is ascending. The winner with the greater hash comes last.
	if leaves[0].Rev != sibling || leaves[1].Rev != remote {
		t.Fatalf("leaf order: %+v", leaves)
	}
	if leaves[0].Body["remote"] == nil {
		t.Fatalf("conflict leaf must keep its body: %+v", leaves[0])
	}
	winner, err = s.GetDocAny(ctx, db, "doc")
	if err != nil || winner.Rev != remote {
		t.Fatalf("winner after conflict: %+v %v", winner, err)
	}

	// Deleting the winner promotes the conflict.
	del, _, err := s.PutDoc(ctx, db, "doc", map[string]any{}, nil, &remote, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if del.Num != 3 {
		t.Fatalf("tombstone rev: %v", del)
	}
	winner, err = s.GetDocAny(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if winner.Rev != sibling || winner.Deleted {
		t.Fatalf("conflict not promoted: %+v", winner)
	}
}

func TestForceRevMergesPathsAndIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_merge")

	// A replicator sends revision 3 with its full ancestry. Nothing exists yet.
	path := []couch.Rev{
		rev(3, "cccccccccccccccccccccccccccccccc"),
		rev(2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		rev(1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	if err := s.ForceRev(ctx, db, "doc", body(t, `{"v":3}`), path, false, nil); err != nil {
		t.Fatal(err)
	}
	all, err := s.AllRevs(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("path not merged: %+v", all)
	}
	seqBefore, err := s.CurrentSeq(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	// Re-sending the same revision is a no-op. It creates no revisions or sequence bump.
	if err := s.ForceRev(ctx, db, "doc", body(t, `{"v":3}`), path, false, nil); err != nil {
		t.Fatal(err)
	}
	seqAfter, err := s.CurrentSeq(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if seqAfter != seqBefore {
		t.Fatal("idempotent replicated write bumped the seq")
	}

	// Extend the branch with revision 4. Its path overlaps the existing tree.
	extended := append([]couch.Rev{rev(4, "dddddddddddddddddddddddddddddddd")}, path...)
	if err := s.ForceRev(ctx, db, "doc", body(t, `{"v":4}`), extended, false, nil); err != nil {
		t.Fatal(err)
	}
	leaves, err := s.Leaves(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if len(leaves) != 1 || leaves[0].Rev.Num != 4 {
		t.Fatalf("extension created a spurious conflict: %+v", leaves)
	}

	// Ancestry from the leaf covers the whole path.
	ancestry, err := s.Ancestry(ctx, db, "doc", leaves[0].Rev)
	if err != nil {
		t.Fatal(err)
	}
	var nums []int
	for _, entry := range ancestry {
		nums = append(nums, entry.Num)
	}
	if !slices.Equal(nums, []int{4, 3, 2, 1}) {
		t.Fatalf("ancestry: %v", nums)
	}
}

func TestRevsDiff(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_diff")

	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"a":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	unknown := rev(2, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	missing, ancestors, err := s.RevsDiff(ctx, db, "doc", []couch.Rev{rev1, unknown})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != unknown {
		t.Fatalf("missing: %v", missing)
	}
	if len(ancestors) != 1 || ancestors[0] != rev1 {
		t.Fatalf("possible ancestors: %v", ancestors)
	}

	// An unknown document has no known revisions.
	missing, _, err = s.RevsDiff(ctx, db, "ghost", []couch.Rev{unknown})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Fatalf("ghost missing: %v", missing)
	}
}

func TestChangesStyles(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_changes")

	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"a":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	sibling := rev(2, "11111111111111111111111111111111")
	if err := s.ForceRev(ctx, db, "doc", body(t, `{"b":1}`),
		[]couch.Rev{sibling, rev1}, false, nil); err != nil {
		t.Fatal(err)
	}
	other := rev(2, "99999999999999999999999999999999")
	if err := s.ForceRev(ctx, db, "doc", body(t, `{"c":1}`),
		[]couch.Rev{other, rev1}, false, nil); err != nil {
		t.Fatal(err)
	}

	// One row per doc, winner rev only by default.
	changes, err := s.Changes(ctx, db, &ChangesParams{})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || len(changes[0].LeafRevs) != 1 {
		t.Fatalf("main_only changes: %+v", changes)
	}
	if changes[0].LeafRevs[0] != other {
		t.Fatalf("winner in changes: %v", changes[0].LeafRevs)
	}

	// style=all_docs surfaces every leaf.
	changes, err = s.Changes(ctx, db, &ChangesParams{AllDocsStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes[0].LeafRevs) != 2 {
		t.Fatalf("all_docs leaves: %+v", changes[0].LeafRevs)
	}

	// Since filtering.
	changes, err = s.Changes(ctx, db, &ChangesParams{Since: changes[0].Seq})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("since filter: %+v", changes)
	}
}

func TestAttachmentsFollowRevisions(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_atts")

	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"v":1}`), nil, nil, false,
		[]AttachmentWrite{{Name: "a.txt", ContentType: "text/plain", Data: []byte("hello")}})
	if err != nil {
		t.Fatal(err)
	}
	atts, err := s.Attachments(ctx, db, "doc", rev1)
	if err != nil || len(atts) != 1 {
		t.Fatalf("attachments: %+v %v", atts, err)
	}
	if atts[0].RevPos != 1 || atts[0].Length != 5 {
		t.Fatalf("attachment meta: %+v", atts[0])
	}

	// Stub carries it to the next revision unchanged.
	rev2, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"v":2}`), nil, &rev1, false,
		[]AttachmentWrite{{Name: "a.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	att, err := s.GetAttachment(ctx, db, "doc", rev2, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(att.Data) != "hello" || att.RevPos != 1 {
		t.Fatalf("carried attachment: %+v", att)
	}

	// A stub for a nonexistent attachment is CouchDB's 412 missing_stub.
	_, _, err = s.PutDoc(ctx, db, "doc", body(t, `{"v":3}`), nil, &rev2, false,
		[]AttachmentWrite{{Name: "ghost.bin"}})
	ce, ok := err.(*couch.Error)
	if !ok || ce.Status != 412 || ce.Err != "missing_stub" {
		t.Fatalf("missing stub: %v", err)
	}
}
