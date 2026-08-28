package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

type countingQueryer struct {
	dbQueryer
	queries int
}

func (q *countingQueryer) Query(
	ctx context.Context,
	sql string,
	args ...any,
) (pgx.Rows, error) {
	q.queries++
	return q.dbQueryer.Query(ctx, sql, args...)
}

type afterBatchQueryer struct {
	dbBatchQueryer
	after func() error
}

func (q *afterBatchQueryer) SendBatch(
	ctx context.Context,
	batch *pgx.Batch,
) pgx.BatchResults {
	return &afterBatchResults{
		BatchResults: q.dbBatchQueryer.SendBatch(ctx, batch), after: q.after,
	}
}

type afterBatchResults struct {
	pgx.BatchResults
	once     sync.Once
	after    func() error
	afterErr error
}

func (r *afterBatchResults) Close() error {
	err := r.BatchResults.Close()
	if err == nil {
		r.once.Do(func() { r.afterErr = r.after() })
	}
	if err != nil {
		return err
	}
	return r.afterErr
}

type afterFirstRowsQueryer struct {
	dbQueryer
	after    func() error
	wrapped  bool
	afterErr error
}

func (q *afterFirstRowsQueryer) Query(
	ctx context.Context,
	sql string,
	args ...any,
) (pgx.Rows, error) {
	if q.afterErr != nil {
		return nil, q.afterErr
	}
	rows, err := q.dbQueryer.Query(ctx, sql, args...)
	if err != nil || q.wrapped {
		return rows, err
	}
	q.wrapped = true
	return &afterRows{Rows: rows, after: func() { q.afterErr = q.after() }}, nil
}

type afterRows struct {
	pgx.Rows
	once  sync.Once
	after func()
}

func (r *afterRows) Close() {
	r.Rows.Close()
	r.once.Do(r.after)
}

func TestAttachViewDocsBatchesHydration(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_view_doc_batch")

	base, _, err := s.PutDoc(ctx, db, "target", body(t, `{"branch":"base"}`),
		nil, nil, false, []AttachmentWrite{{
			Name: "hello.txt", ContentType: "text/plain", Data: []byte("hello"),
		}})
	if err != nil {
		t.Fatal(err)
	}
	branchA := rev(2, "11111111111111111111111111111111")
	if err := s.ForceRev(ctx, db, "target", body(t, `{"branch":"a"}`),
		[]couch.Rev{branchA, base}, false, []AttachmentWrite{{Name: "hello.txt"}}); err != nil {
		t.Fatal(err)
	}
	branchB := rev(2, "99999999999999999999999999999999")
	if err := s.ForceRev(ctx, db, "target", body(t, `{"branch":"b"}`),
		[]couch.Rev{branchB, base}, false, []AttachmentWrite{{Name: "hello.txt"}}); err != nil {
		t.Fatal(err)
	}
	dead, _, err := s.PutDoc(ctx, db, "dead", body(t, `{"alive":true}`),
		nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutDoc(ctx, db, "dead", map[string]any{},
		nil, &dead, true, nil); err != nil {
		t.Fatal(err)
	}

	rows := []ViewRow{
		{DocID: "target", Value: json.RawMessage(`null`)},
		{DocID: "target", Value: json.RawMessage(`null`)},
		{
			DocID: "source",
			Value: json.RawMessage(fmt.Sprintf(
				`{"_id":"target","_rev":%q}`, branchA.String())),
		},
		{DocID: "source", Value: json.RawMessage(`{"_id":"missing"}`)},
		{DocID: "dead", Value: json.RawMessage(`null`)},
	}
	qx := &countingQueryer{dbQueryer: s.pool}
	if err := s.attachViewDocs(ctx, qx, db, rows, true, false); err != nil {
		t.Fatal(err)
	}
	if qx.queries != 3 {
		t.Fatalf("hydration queries = %d, want 3", qx.queries)
	}
	for _, i := range []int{0, 1} {
		if rows[i].Doc == nil || rows[i].Doc.Rev != branchB ||
			rows[i].Doc.Body["branch"] != "b" {
			t.Fatalf("winner row %d: %+v", i, rows[i])
		}
		if len(rows[i].DocConflicts) != 1 || rows[i].DocConflicts[0] != branchA.String() {
			t.Fatalf("winner conflicts %d: %+v", i, rows[i].DocConflicts)
		}
		if len(rows[i].DocAttachments) != 1 || rows[i].DocAttachments[0].Data != nil {
			t.Fatalf("winner attachments %d: %+v", i, rows[i].DocAttachments)
		}
	}
	if rows[2].Doc == nil || rows[2].Doc.Rev != branchA || rows[2].DocID != "target" {
		t.Fatalf("pinned linked row: %+v", rows[2])
	}
	if len(rows[2].DocConflicts) != 1 || rows[2].DocConflicts[0] != branchB.String() {
		t.Fatalf("pinned conflicts: %+v", rows[2].DocConflicts)
	}
	if rows[3].Doc != nil || rows[3].DocError != "missing" {
		t.Fatalf("missing linked row: %+v", rows[3])
	}
	if rows[4].Doc != nil || rows[4].DocError != "deleted" {
		t.Fatalf("deleted row: %+v", rows[4])
	}
}

func TestChangesBatchesIncludeDocsHydration(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_changes_doc_batch")

	if _, _, err := s.PutDoc(ctx, db, "attached", body(t, `{"kind":"attached"}`),
		nil, nil, false, []AttachmentWrite{{
			Name: "hello.txt", ContentType: "text/plain", Data: []byte("hello"),
		}}); err != nil {
		t.Fatal(err)
	}
	base, _, err := s.PutDoc(ctx, db, "conflicted", body(t, `{"branch":"base"}`),
		nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	branchA := rev(2, "11111111111111111111111111111111")
	if err := s.ForceRev(ctx, db, "conflicted", body(t, `{"branch":"a"}`),
		[]couch.Rev{branchA, base}, false, nil); err != nil {
		t.Fatal(err)
	}
	branchB := rev(2, "99999999999999999999999999999999")
	if err := s.ForceRev(ctx, db, "conflicted", body(t, `{"branch":"b"}`),
		[]couch.Rev{branchB, base}, false, nil); err != nil {
		t.Fatal(err)
	}

	qx := &countingQueryer{dbQueryer: s.pool}
	changes, err := s.changes(ctx, qx, db, &ChangesParams{
		IncludeDocs: true, Conflicts: true,
		IncludeAttachments: true, AttachmentData: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if qx.queries != 3 {
		t.Fatalf("changes hydration queries = %d, want 3", qx.queries)
	}
	if len(changes) != 2 {
		t.Fatalf("changes: %+v", changes)
	}
	byID := make(map[string]Change, len(changes))
	for _, change := range changes {
		byID[change.ID] = change
	}
	attached := byID["attached"]
	if len(attached.Attachments) != 1 ||
		string(attached.Attachments[0].Data) != "hello" {
		t.Fatalf("attached change: %+v", attached)
	}
	conflicted := byID["conflicted"]
	if len(conflicted.ConflictRevs) != 1 || conflicted.ConflictRevs[0] != branchA {
		t.Fatalf("conflicted change: %+v", conflicted)
	}
}

func TestViewIncludeDocsUsesOneReadSnapshot(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_view_doc_snapshot")
	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":1}`), nil, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	vg := &ViewGroup{
		Sig:       "include_doc_snapshot",
		Views:     map[string]ViewDef{"by_n": {Map: "function(doc){emit(doc.n,null);}"}},
		ViewOrder: []string{"by_n"},
	}
	lastSeq, purgeSeq, err := s.SyncViewGroupSnapshot(ctx, db, vg, numericViewMapper{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.beginReadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	qx := &afterBatchQueryer{dbBatchQueryer: tx}
	qx.after = func() error {
		_, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":2}`), nil, &rev1, false, nil)
		return err
	}
	limit := int64(1)
	result, err := s.queryViewMap(ctx, qx, db, vg, "by_n", &ViewQuery{
		InclusiveEnd: true, IncludeDocs: true, Limit: &limit,
		LastSeqHint: lastSeq, PurgeSeqHint: &purgeSeq,
	}, &ViewResult{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Doc == nil ||
		fmt.Sprint(result.Rows[0].Doc.Body["n"]) != "1" {
		t.Fatalf("included doc escaped read snapshot: %+v", result.Rows)
	}
	current, err := s.GetDocAny(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(current.Body["n"]) != "2" {
		t.Fatalf("concurrent update did not commit: %+v", current.Body)
	}
}

func TestChangesIncludeDocsUsesOneReadSnapshot(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_changes_doc_snapshot")
	rev1, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":1}`),
		nil, nil, false, []AttachmentWrite{{
			Name: "old.txt", ContentType: "text/plain", Data: []byte("old"),
		}})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.beginReadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	qx := &afterFirstRowsQueryer{dbQueryer: tx}
	qx.after = func() error {
		_, _, err := s.PutDoc(ctx, db, "doc", body(t, `{"n":2}`),
			nil, &rev1, false, []AttachmentWrite{{
				Name: "new.txt", ContentType: "text/plain", Data: []byte("new"),
			}})
		return err
	}
	changes, err := s.changes(ctx, qx, db, &ChangesParams{
		IncludeDocs:        true,
		Conflicts:          true,
		IncludeAttachments: true,
		AttachmentData:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].LeafRevs[0] != rev1 ||
		fmt.Sprint(changes[0].Body["n"]) != "1" ||
		len(changes[0].ConflictRevs) != 0 {
		t.Fatalf("included change escaped read snapshot: %+v", changes)
	}
	if len(changes[0].Attachments) != 1 ||
		changes[0].Attachments[0].Name != "old.txt" ||
		string(changes[0].Attachments[0].Data) != "old" {
		t.Fatalf("included attachments escaped read snapshot: %+v", changes[0].Attachments)
	}
	current, err := s.GetDocAny(ctx, db, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(current.Body["n"]) != "2" {
		t.Fatalf("concurrent update did not commit: %+v", current.Body)
	}
}

func BenchmarkIncludeDocsHydration(b *testing.B) {
	s := testStore(b)
	const dbName = "bench_include_doc_hydration"
	_ = s.DeleteDatabase(b.Context(), dbName)
	if err := s.CreateDatabase(b.Context(), dbName, false); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.DeleteDatabase(context.Background(), dbName) })
	db, err := s.GetDB(b.Context(), dbName)
	if err != nil {
		b.Fatal(err)
	}
	const rowCount = 20
	writes := make([]BulkWrite, rowCount)
	template := make([]ViewRow, rowCount)
	for i := range writes {
		id := fmt.Sprintf("doc-%02d", i)
		writes[i] = BulkWrite{ID: id, Body: map[string]any{"n": i}}
		template[i] = ViewRow{DocID: id, Value: json.RawMessage(`null`)}
	}
	results, err := s.BulkPutDocs(b.Context(), db, writes)
	if err != nil {
		b.Fatal(err)
	}
	for i, result := range results {
		if result.Err != nil {
			b.Fatalf("seed %d: %v", i, result.Err)
		}
	}

	b.Run("individual_queries", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			for _, row := range template {
				doc, err := s.GetDocAny(b.Context(), db, row.DocID)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := s.Attachments(b.Context(), db, row.DocID, doc.Rev); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(2*rowCount, "queries/op")
		b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/sec")
	})
	b.Run("batched", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			rows := append([]ViewRow(nil), template...)
			if err := s.attachViewDocs(
				b.Context(), s.pool, db, rows, false, false); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(2, "queries/op")
		b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/sec")
	})
}
