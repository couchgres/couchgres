package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// dbQueryer is the common read surface of pgxpool.Pool and pgx.Tx. Batched
// response hydration runs through a repeatable-read transaction while ordinary
// callers can keep using the pool directly.
type dbQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type dbBatchQueryer interface {
	dbQueryer
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

// docRef names a winner when Rev is zero and an exact revision otherwise.
// Revision numbers start at one, so the zero value is unambiguous.
type docRef struct {
	ID  string
	Rev couch.Rev
}

type leafRevision struct {
	Rev     couch.Rev
	Deleted bool
}

func (s *Store) beginReadSnapshot(ctx context.Context) (pgx.Tx, error) {
	return s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
}

func uniqueDocRefs(refs []docRef) []docRef {
	seen := make(map[docRef]struct{}, len(refs))
	out := make([]docRef, 0, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

// batchDocs resolves current winners and pinned revisions in one query. Missing
// documents, compacted revisions, and other unavailable bodies are absent from
// the result so callers can preserve their endpoint-specific missing shape.
func batchDocs(
	ctx context.Context,
	q dbQueryer,
	db *DB,
	refs []docRef,
) (map[docRef]*DocRow, error) {
	refs = uniqueDocRefs(refs)
	out := make(map[docRef]*DocRow, len(refs))
	if len(refs) == 0 {
		return out, nil
	}
	ids := make([]string, len(refs))
	nums := make([]int32, len(refs))
	hashes := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = ref.ID
		nums[i] = int32(ref.Rev.Num)
		hashes[i] = ref.Rev.Hash
	}
	rows, err := q.Query(ctx, fmt.Sprintf(
		`WITH wanted(id, rev_num, rev_hash) AS (
		   SELECT * FROM unnest($1::text[], $2::int[], $3::text[])
		 )
		 SELECT w.id, w.rev_num, w.rev_hash,
		        r.rev_num, r.rev_hash, r.deleted, r.body,
		        coalesce(d.seq, 0)
		 FROM wanted w
		 LEFT JOIN %s.docs d ON w.rev_num = 0 AND d.id = w.id
		 JOIN %s.revs r ON r.id = w.id
		   AND r.rev_num = CASE WHEN w.rev_num = 0 THEN d.rev_num ELSE w.rev_num END
		   AND r.rev_hash = CASE WHEN w.rev_num = 0 THEN d.rev_hash ELSE w.rev_hash END
		 WHERE r.body IS NOT NULL`, db.Schema, db.Schema), ids, nums, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref docRef
		row := &DocRow{}
		var requestedNum int
		var raw []byte
		if err := rows.Scan(&ref.ID, &requestedNum, &ref.Rev.Hash,
			&row.Rev.Num, &row.Rev.Hash, &row.Deleted, &raw, &row.Seq); err != nil {
			return nil, err
		}
		ref.Rev.Num = requestedNum
		row.ID = ref.ID
		if row.Body, err = decodeBody(raw); err != nil {
			return nil, err
		}
		out[ref] = row
	}
	return out, rows.Err()
}

// batchLeafRevisions returns leaf metadata for many document IDs in one query.
// Callers apply their endpoint-specific ordering and winner exclusion.
func batchLeafRevisions(
	ctx context.Context,
	q dbQueryer,
	db *DB,
	ids []string,
) (map[string][]leafRevision, error) {
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	out := make(map[string][]leafRevision, len(unique))
	if len(unique) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, fmt.Sprintf(
		`SELECT id, rev_num, rev_hash, deleted FROM %s.revs
		 WHERE id = ANY($1) AND leaf
		 ORDER BY id, rev_num, rev_hash`, db.Schema), unique)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var leaf leafRevision
		if err := rows.Scan(&id, &leaf.Rev.Num, &leaf.Rev.Hash, &leaf.Deleted); err != nil {
			return nil, err
		}
		out[id] = append(out[id], leaf)
	}
	return out, rows.Err()
}

// batchAttachments loads metadata or bodies for exact document revisions in
// one query. Results retain attachment introduction/name order for each rev.
func batchAttachments(
	ctx context.Context,
	q dbQueryer,
	db *DB,
	refs []docRef,
	includeData bool,
) (map[docRef][]Attachment, error) {
	refs = uniqueDocRefs(refs)
	out := make(map[docRef][]Attachment, len(refs))
	if len(refs) == 0 {
		return out, nil
	}
	ids := make([]string, len(refs))
	nums := make([]int32, len(refs))
	hashes := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = ref.ID
		nums[i] = int32(ref.Rev.Num)
		hashes[i] = ref.Rev.Hash
	}
	dataExpr := "NULL::bytea"
	if includeData {
		dataExpr = "a.data"
	}
	rows, err := q.Query(ctx, fmt.Sprintf(
		`WITH wanted(id, rev_num, rev_hash) AS (
		   SELECT * FROM unnest($1::text[], $2::int[], $3::text[])
		 )
		 SELECT w.id, w.rev_num, w.rev_hash,
		        a.name, a.content_type, a.digest, a.revpos,
		        a.length, a.encoding, a.encoded_length, %s
		 FROM wanted w
		 JOIN %s.attachments a ON a.doc_id = w.id
		   AND a.rev_num = w.rev_num AND a.rev_hash = w.rev_hash
		 ORDER BY w.id, w.rev_num, w.rev_hash, a.revpos, a.name`,
		dataExpr, db.Schema), ids, nums, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref docRef
		var att Attachment
		if err := rows.Scan(&ref.ID, &ref.Rev.Num, &ref.Rev.Hash,
			&att.Name, &att.ContentType, &att.Digest, &att.RevPos,
			&att.Length, &att.Encoding, &att.EncodedLength, &att.Data); err != nil {
			return nil, err
		}
		out[ref] = append(out[ref], att)
	}
	return out, rows.Err()
}
