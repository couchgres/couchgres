package store

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// DocRow is a stored document revision. It includes metadata and the body map
// (which never contains underscore members).
type DocRow struct {
	ID      string
	Rev     couch.Rev
	Deleted bool
	Body    map[string]any
	Seq     int64
}

// JSON assembles the client-visible document object.
func (d *DocRow) JSON() map[string]any {
	out := make(map[string]any, len(d.Body)+3)
	out["_id"] = d.ID
	out["_rev"] = d.Rev.String()
	if d.Deleted {
		out["_deleted"] = true
	}
	for k, v := range d.Body {
		out[k] = v
	}
	return out
}

// Leaf is one leaf of a document's revision tree.
type Leaf struct {
	Rev     couch.Rev
	Deleted bool
	Body    map[string]any
}

// RevInfo is one entry of a document's revision history.
type RevInfo struct {
	Num     int
	Hash    string
	Deleted bool
	Leaf    bool
	// Available reports whether the body is still retrievable.
	Available bool
}

// PutDoc performs an interactive write (new_edits=true). The supplied revision
// must match the winning leaf, and the new revision extends it.
//
// rawBody is the document JSON as the client sent it, with member order intact.
// It feeds CouchDB's exact revision hash. Server-assembled bodies pass nil and
// hash the canonical, key-sorted form instead. The result is deterministic but
// may differ from the revision a CouchDB instance would generate for the write.
func (s *Store) PutDoc(
	ctx context.Context,
	db *DB,
	id string,
	body map[string]any,
	rawBody []byte,
	expected *couch.Rev,
	deleted bool,
	atts []AttachmentWrite,
) (couch.Rev, int64, error) {
	fail := func(err error) (couch.Rev, int64, error) { return couch.Rev{}, 0, err }

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback(ctx)

	winner, found, err := lockWinner(ctx, tx, db, id)
	if err != nil {
		return fail(err)
	}
	var parent *couch.Rev
	if found {
		switch {
		case expected != nil && *expected == winner.Rev:
		case expected == nil && winner.Deleted:
			// Recreating on top of a tombstone needs no rev.
		default:
			return fail(couch.Conflict())
		}
		parent = &winner.Rev
	} else if expected != nil {
		return fail(couch.Conflict())
	}

	raw := rawBody
	if raw == nil {
		raw = couch.CanonicalBody(body)
	}
	revAtts, err := revAttsForWrite(ctx, tx, db, id, parent, atts)
	if err != nil {
		return fail(err)
	}
	rev, err := couch.NextRev(deleted, parent, raw, revAtts)
	if err != nil {
		return fail(err)
	}
	if err := insertLeaf(ctx, tx, db, id, rev, parent, deleted, body,
		s.keepSuperseded.Load()); err != nil {
		return fail(err)
	}
	if err := writeAttachments(ctx, tx, db, id, rev, parent, atts); err != nil {
		return fail(err)
	}
	seq, err := finishWrite(ctx, tx, db, id)
	if err != nil {
		return fail(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fail(err)
	}
	s.notifyChanged(db.Name)
	return rev, seq, nil
}

// BulkWrite is one document of a BulkPutDocs batch.
type BulkWrite struct {
	ID       string
	Body     map[string]any
	RawBody  []byte // Client JSON with member order intact. nil uses the canonical form.
	Expected *couch.Rev
	Deleted  bool
	Atts     []AttachmentWrite
}

// BulkResult is one BulkWrite outcome. If Err is nil, Rev is the new revision.
type BulkResult struct {
	Rev couch.Rev
	Err error
}

// BulkPutDocs applies a _bulk_docs batch of interactive writes in a single
// transaction with set-based statements. The per-document PutDoc loop pays
// about nine Postgres round trips per document, which dominates bulk ingest.
// CouchDB returns per-document outcomes rather than using batch atomicity.
// Conflict checks against the locked winner set preserve that behavior. A later write to
// an id already written in the same batch sees the earlier outcome.
// bulkPrep is one write's database-free work, done in parallel before the
// transaction opens. It contains the canonical body bytes the leaf insert needs and
// the CouchDB-exact rev hash computed under the assumption that the
// write's parent is its Expected rev. The assumption holds for every
// accepted write except a rev-less recreate of a tombstone (its parent is
// the tombstone, known only after the winner lock. That case hashes inside
// the transaction. Conflicted writes discard their precomputed result.
type bulkPrep struct {
	rev       couch.Rev
	hashed    bool
	canonical []byte
}

func precomputeBulk(writes []BulkWrite) []bulkPrep {
	prep := make([]bulkPrep, len(writes))
	workers := min(runtime.GOMAXPROCS(0), len(writes))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(writes); i += workers {
				wr := &writes[i]
				prep[i].canonical = couch.CanonicalBody(wr.Body)
				if len(wr.Atts) != 0 {
					continue // attachment digests may need the database
				}
				raw := wr.RawBody
				if raw == nil {
					raw = prep[i].canonical
				}
				rev, err := couch.NextRev(wr.Deleted, wr.Expected, raw, nil)
				if err != nil {
					continue // the in-transaction path surfaces the error
				}
				prep[i].rev = rev
				prep[i].hashed = true
			}
		}()
	}
	wg.Wait()
	return prep
}

func (s *Store) BulkPutDocs(ctx context.Context, db *DB, writes []BulkWrite) ([]BulkResult, error) {
	results := make([]BulkResult, len(writes))
	if len(writes) == 0 {
		return results, nil
	}
	prep := precomputeBulk(writes)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Lock and load every existing winner in one statement.
	ids := make([]string, 0, len(writes))
	seen := make(map[string]bool, len(writes))
	for _, w := range writes {
		if !seen[w.ID] {
			seen[w.ID] = true
			ids = append(ids, w.ID)
		}
	}
	type winnerState struct {
		rev     couch.Rev
		deleted bool
	}
	winners := make(map[string]winnerState, len(ids))
	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT id, rev_num, rev_hash, deleted FROM %s.docs WHERE id = ANY($1) FOR UPDATE",
		db.Schema), ids)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var w winnerState
		if err := rows.Scan(&id, &w.rev.Num, &w.rev.Hash, &w.deleted); err != nil {
			rows.Close()
			return nil, err
		}
		winners[id] = w
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Decide each write in order against the in-memory winner set.
	type accepted struct {
		idx    int
		rev    couch.Rev
		parent *couch.Rev
	}
	var acc []accepted
	maxRevNum := 0
	for i, w := range writes {
		var parent *couch.Rev
		if winner, found := winners[w.ID]; found {
			switch {
			case w.Expected != nil && *w.Expected == winner.rev:
			case w.Expected == nil && winner.deleted:
				// Recreating on top of a tombstone needs no rev.
			default:
				results[i].Err = couch.Conflict()
				continue
			}
			rev := winner.rev
			parent = &rev
		} else if w.Expected != nil {
			results[i].Err = couch.Conflict()
			continue
		}
		var rev couch.Rev
		// The precomputed hash assumed parent == Expected. Use it only
		// when that held (it always does except tombstone recreates).
		if prep[i].hashed &&
			(parent == nil) == (w.Expected == nil) &&
			(parent == nil || *parent == *w.Expected) {
			rev = prep[i].rev
		} else {
			revAtts, err := revAttsForWrite(ctx, tx, db, w.ID, parent, w.Atts)
			if err != nil {
				if _, ok := err.(*couch.Error); !ok {
					return nil, err
				}
				results[i].Err = err
				continue
			}
			raw := w.RawBody
			if raw == nil {
				raw = prep[i].canonical
			}
			if rev, err = couch.NextRev(w.Deleted, parent, raw, revAtts); err != nil {
				results[i].Err = err
				continue
			}
		}
		results[i].Rev = rev
		acc = append(acc, accepted{idx: i, rev: rev, parent: parent})
		winners[w.ID] = winnerState{rev: rev, deleted: w.Deleted}
		if rev.Num > maxRevNum {
			maxRevNum = rev.Num
		}
	}
	if len(acc) == 0 {
		return results, tx.Rollback(ctx)
	}

	// Insert all new leaves at once. An identical concurrent creation has no
	// winner row to lock, so its revision is a no-op. The content and revision
	// match, so "ok" remains accurate.
	leafIDs := make([]string, len(acc))
	nums := make([]int32, len(acc))
	hashes := make([]string, len(acc))
	parentNums := make([]*int32, len(acc))
	parentHashes := make([]*string, len(acc))
	deleteds := make([]bool, len(acc))
	bodies := make([]string, len(acc))
	for j, a := range acc {
		w := writes[a.idx]
		leafIDs[j] = w.ID
		nums[j] = int32(a.rev.Num)
		hashes[j] = a.rev.Hash
		if a.parent != nil {
			pn := int32(a.parent.Num)
			ph := a.parent.Hash
			parentNums[j] = &pn
			parentHashes[j] = &ph
		}
		deleteds[j] = w.Deleted
		bodies[j] = string(prep[a.idx].canonical)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.revs (id, rev_num, rev_hash, parent_num, parent_hash,
		   deleted, leaf, body)
		 SELECT i, n, h, pn, ph, d, true, b::jsonb
		 FROM unnest($1::text[], $2::int[], $3::text[], $4::int[], $5::text[],
		             $6::bool[], $7::text[]) AS t(i, n, h, pn, ph, d, b)
		 ON CONFLICT (id, rev_num, rev_hash) DO NOTHING`, db.Schema),
		leafIDs, nums, hashes, parentNums, parentHashes, deleteds, bodies,
	); err != nil {
		return nil, err
	}
	// Parents stop being leaves after the insert. A parent written
	// earlier in this same batch (delete then recreate) is covered too.
	var pIDs []string
	var pNums []int32
	var pHashes []string
	for j, a := range acc {
		if a.parent != nil {
			pIDs = append(pIDs, leafIDs[j])
			pNums = append(pNums, int32(a.parent.Num))
			pHashes = append(pHashes, a.parent.Hash)
		}
	}
	if len(pIDs) > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`UPDATE %[1]s.revs r SET %[2]s
			 FROM unnest($1::text[], $2::int[], $3::text[]) AS t(i, n, h)
			 WHERE r.id = t.i AND r.rev_num = t.n AND r.rev_hash = t.h`,
			db.Schema, unleafSet(s.keepSuperseded.Load())),
			pIDs, pNums, pHashes,
		); err != nil {
			return nil, err
		}
	}

	// Attachments ride per accepted doc (rare in bulk batches).
	// revAttsForWrite already validated stubs. An error here means the batch
	// state is inconsistent, so return it.
	for _, a := range acc {
		w := writes[a.idx]
		if len(w.Atts) == 0 {
			continue
		}
		if err := writeAttachments(ctx, tx, db, w.ID, a.rev, a.parent, w.Atts); err != nil {
			return nil, err
		}
	}

	// finishWrite uses set-based statements. It recomputes winners, refreshes
	// the docs cache, assigns sequences in input order, and sends one notification.
	written := make([]string, 0, len(leafIDs))
	wseen := make(map[string]bool, len(leafIDs))
	for _, id := range leafIDs {
		if !wseen[id] {
			wseen[id] = true
			written = append(written, id)
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`WITH ordered AS (
		   SELECT t.id, t.ord FROM unnest($1::text[]) WITH ORDINALITY AS t(id, ord)
		 ), w AS (
		   SELECT DISTINCT ON (r.id) r.id, r.rev_num, r.rev_hash, r.deleted
		   FROM %[1]s.revs r JOIN ordered o ON o.id = r.id
		   WHERE r.leaf
		   ORDER BY r.id, r.deleted ASC, r.rev_num DESC, r.rev_hash DESC
		 )
		 INSERT INTO %[1]s.docs (id, rev_num, rev_hash, deleted, seq)
		 SELECT w.id, w.rev_num, w.rev_hash, w.deleted, nextval('%[1]s.update_seq')
		 FROM w JOIN ordered o ON o.id = w.id
		 ORDER BY o.ord
		 ON CONFLICT (id) DO UPDATE SET rev_num = EXCLUDED.rev_num,
		   rev_hash = EXCLUDED.rev_hash, deleted = EXCLUDED.deleted,
		   seq = EXCLUDED.seq`, db.Schema),
		written,
	); err != nil {
		return nil, err
	}
	if maxRevNum > db.RevsLimit {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`DELETE FROM %[1]s.revs r USING %[1]s.docs d
			 WHERE r.id = d.id AND d.id = ANY($1)
			   AND NOT r.leaf AND r.rev_num <= d.rev_num - $2`, db.Schema),
			written, db.RevsLimit,
		); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.notifyChanged(db.Name)
	return results, nil
}

// ForceRev applies a replicated write (new_edits=false). It merges the
// supplied revision path into the tree. Sibling leaves are expected conflicts.
// Writing an already-known revision is a no-op.
func (s *Store) ForceRev(
	ctx context.Context,
	db *DB,
	id string,
	body map[string]any,
	path []couch.Rev, // newest first
	deleted bool,
	atts []AttachmentWrite,
) error {
	if len(path) == 0 {
		return couch.BadRequest("new_edits=false requires a _rev or _revisions")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, _, err := lockWinner(ctx, tx, db, id); err != nil {
		return err
	}

	newest := path[0]
	if known, _, err := revExists(ctx, tx, db, id, newest); err != nil {
		return err
	} else if known {
		return tx.Rollback(ctx) // No-op. Rollback releases the lock.
	}

	// Find the deepest ancestor of the path already present. Everything
	// between it and the new leaf is inserted as body-less path entries.
	connectAt := len(path) // index into path of the first existing ancestor
	var ancestorWasLeaf bool
	for i := 1; i < len(path); i++ {
		known, wasLeaf, err := revExists(ctx, tx, db, id, path[i])
		if err != nil {
			return err
		}
		if known {
			connectAt = i
			ancestorWasLeaf = wasLeaf
			break
		}
	}

	// Insert missing intermediates, oldest first. Each entry's parent is the
	// next-older path entry. It is already in the tree when i+1 == connectAt,
	// just inserted when i+1 < connectAt, or absent at the path root.
	for i := connectAt - 1; i >= 1; i-- {
		var parent *couch.Rev
		if i+1 < len(path) {
			parent = &path[i+1]
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.revs (id, rev_num, rev_hash, parent_num, parent_hash,
			   deleted, leaf, body)
			 VALUES ($1, $2, $3, $4, $5, false, false, NULL)
			 ON CONFLICT DO NOTHING`, db.Schema),
			id, path[i].Num, path[i].Hash, parentNum(parent), parentHash(parent),
		); err != nil {
			return err
		}
	}

	// The ancestor we connected to stops being a leaf (its branch grew).
	if connectAt < len(path) && ancestorWasLeaf {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`UPDATE %s.revs SET %s
			 WHERE id = $1 AND rev_num = $2 AND rev_hash = $3`,
			db.Schema, unleafSet(s.keepSuperseded.Load())),
			id, path[connectAt].Num, path[connectAt].Hash,
		); err != nil {
			return err
		}
	}

	var parent *couch.Rev
	if len(path) > 1 {
		parent = &path[1]
	}
	if err := insertLeafRev(ctx, tx, db, id, newest, parent, deleted, body); err != nil {
		return err
	}
	if err := writeAttachments(ctx, tx, db, id, newest, parent, atts); err != nil {
		return err
	}
	if _, err := finishWrite(ctx, tx, db, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.notifyChanged(db.Name)
	return nil
}

// winnerJoin joins docs (aliased d) to its winning revs leaf row (aliased r).
// Document bodies live only in the revision tree, so reads of a winner's body
// use this join.
func winnerJoin(schema string) string {
	return fmt.Sprintf(
		"JOIN %s.revs r ON r.id = d.id AND r.rev_num = d.rev_num AND r.rev_hash = d.rev_hash",
		schema)
}

// winnerBody is the body expression to select through winnerJoin.
const winnerBody = "coalesce(r.body, '{}'::jsonb)"

// lockWinner locks the doc row (when present) and returns the current
// winning leaf.
func lockWinner(ctx context.Context, tx pgx.Tx, db *DB, id string) (*DocRow, bool, error) {
	row := &DocRow{ID: id}
	var raw []byte
	err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT d.rev_num, d.rev_hash, d.deleted, %s, d.seq
		 FROM %s.docs d %s WHERE d.id = $1 FOR UPDATE OF d`,
		winnerBody, db.Schema, winnerJoin(db.Schema)), id,
	).Scan(&row.Rev.Num, &row.Rev.Hash, &row.Deleted, &raw, &row.Seq)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if row.Body, err = decodeBody(raw); err != nil {
		return nil, false, err
	}
	return row, true, nil
}

func revExists(ctx context.Context, tx pgx.Tx, db *DB, id string, rev couch.Rev) (known, isLeaf bool, err error) {
	err = tx.QueryRow(ctx, fmt.Sprintf(
		"SELECT leaf FROM %s.revs WHERE id = $1 AND rev_num = $2 AND rev_hash = $3",
		db.Schema), id, rev.Num, rev.Hash,
	).Scan(&isLeaf)
	if err == pgx.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, isLeaf, nil
}

// unleafSet is the SET clause that retires a superseded leaf. Its body
// goes with it unless the keep_superseded_bodies option is holding
// bodies for _compact.
func unleafSet(keepBody bool) string {
	if keepBody {
		return "leaf = false"
	}
	return "leaf = false, body = NULL"
}

// insertLeaf writes a new leaf that extends parent on the interactive path. The
// parent stops being a leaf and, unless keepBody, drops its body.
func insertLeaf(ctx context.Context, tx pgx.Tx, db *DB, id string, rev couch.Rev,
	parent *couch.Rev, deleted bool, body map[string]any, keepBody bool) error {
	if parent != nil {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`UPDATE %s.revs SET %s
			 WHERE id = $1 AND rev_num = $2 AND rev_hash = $3`,
			db.Schema, unleafSet(keepBody)),
			id, parent.Num, parent.Hash,
		); err != nil {
			return err
		}
	}
	return insertLeafRev(ctx, tx, db, id, rev, parent, deleted, body)
}

func insertLeafRev(ctx context.Context, tx pgx.Tx, db *DB, id string, rev couch.Rev,
	parent *couch.Rev, deleted bool, body map[string]any) error {
	tag, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.revs (id, rev_num, rev_hash, parent_num, parent_hash,
		   deleted, leaf, body)
		 VALUES ($1, $2, $3, $4, $5, $6, true, $7)
		 ON CONFLICT (id, rev_num, rev_hash) DO NOTHING`, db.Schema),
		id, rev.Num, rev.Hash, parentNum(parent), parentHash(parent),
		deleted, couch.CanonicalBody(body),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// A concurrent identical write is valid for replication and a conflict for
		// interactive writes. The caller holds the document lock, so this only
		// happens for brand-new docs racing on creation).
		return couch.Conflict()
	}
	return nil
}

// finishWrite recomputes the winner, refreshes the docs cache row, bumps the
// sequence, prunes deep history, and notifies the changes broker.
func finishWrite(ctx context.Context, tx pgx.Tx, db *DB, id string) (int64, error) {
	var winner struct {
		num     int
		hash    string
		deleted bool
	}
	// CouchDB selects winners deterministically. Live leaves beat deleted ones, then
	// highest rev number, then lexicographically greater hash.
	err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT rev_num, rev_hash, deleted FROM %s.revs
		 WHERE id = $1 AND leaf
		 ORDER BY deleted ASC, rev_num DESC, rev_hash DESC LIMIT 1`, db.Schema), id,
	).Scan(&winner.num, &winner.hash, &winner.deleted)
	if err != nil {
		return 0, fmt.Errorf("recomputing winner: %w", err)
	}

	var seq int64
	if err := tx.QueryRow(ctx,
		fmt.Sprintf("SELECT nextval('%s.update_seq')", db.Schema),
	).Scan(&seq); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.docs (id, rev_num, rev_hash, deleted, seq)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET rev_num = $2, rev_hash = $3,
		   deleted = $4, seq = $5`, db.Schema),
		id, winner.num, winner.hash, winner.deleted, seq,
	); err != nil {
		return 0, err
	}

	// Prune ancestor path entries beyond revs_limit. Leaves always survive.
	if pruneBelow := winner.num - db.RevsLimit; pruneBelow > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.revs WHERE id = $1 AND rev_num <= $2 AND NOT leaf",
			db.Schema), id, pruneBelow,
		); err != nil {
			return 0, err
		}
	}
	return seq, nil
}

// GetDoc fetches a document, optionally at a specific revision. Any leaf is
// retrievable. Interior, superseded revisions report missing, as after
// compaction. A deleted winner is 404 "deleted" unless its rev was named.
func (s *Store) GetDoc(ctx context.Context, db *DB, id string, rev *couch.Rev) (*DocRow, error) {
	if rev == nil {
		row, err := s.GetDocAny(ctx, db, id)
		if err != nil {
			return nil, err
		}
		if row.Deleted {
			return nil, couch.DocDeleted()
		}
		return row, nil
	}
	row := &DocRow{ID: id, Rev: *rev}
	var raw []byte
	var isLeaf bool
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT deleted, leaf, body FROM %s.revs WHERE id = $1 AND rev_num = $2 AND rev_hash = $3",
		db.Schema), id, rev.Num, rev.Hash,
	).Scan(&row.Deleted, &isLeaf, &raw)
	if err == pgx.ErrNoRows || (err == nil && raw == nil) {
		return nil, couch.DocMissing()
	}
	if err != nil {
		return nil, err
	}
	if row.Body, err = decodeBody(raw); err != nil {
		return nil, err
	}
	return row, nil
}

// GetDocAny fetches the winning revision even when it is a tombstone.
func (s *Store) GetDocAny(ctx context.Context, db *DB, id string) (*DocRow, error) {
	row := &DocRow{ID: id}
	var raw []byte
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT d.rev_num, d.rev_hash, d.deleted, %s, d.seq
		 FROM %s.docs d %s WHERE d.id = $1`,
		winnerBody, db.Schema, winnerJoin(db.Schema)),
		id,
	).Scan(&row.Rev.Num, &row.Rev.Hash, &row.Deleted, &raw, &row.Seq)
	if err == pgx.ErrNoRows {
		return nil, couch.DocMissing()
	}
	if err != nil {
		return nil, err
	}
	if row.Body, err = decodeBody(raw); err != nil {
		return nil, err
	}
	return row, nil
}

// GetDocsAny fetches winner rows, including tombstones, for many IDs in one
// query. _bulk_get serves hundreds of documents per request. Fetching
// them one by one would spend the request on round trips. Missing ids are
// simply absent from the result.
func (s *Store) GetDocsAny(ctx context.Context, db *DB, ids []string) (map[string]*DocRow, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT d.id, d.rev_num, d.rev_hash, d.deleted, %s, d.seq
		 FROM %s.docs d %s WHERE d.id = ANY($1)`,
		winnerBody, db.Schema, winnerJoin(db.Schema)), ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]*DocRow, len(ids))
	for rows.Next() {
		row := &DocRow{}
		var raw []byte
		if err := rows.Scan(&row.ID, &row.Rev.Num, &row.Rev.Hash,
			&row.Deleted, &raw, &row.Seq); err != nil {
			return nil, err
		}
		if row.Body, err = decodeBody(raw); err != nil {
			return nil, err
		}
		out[row.ID] = row
	}
	return out, rows.Err()
}

// Leaves returns every leaf of a document's tree in CouchDB's open_revs
// order. It sorts by ascending revision number, then hash.
func (s *Store) Leaves(ctx context.Context, db *DB, id string) ([]Leaf, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT rev_num, rev_hash, deleted, body FROM %s.revs
		 WHERE id = $1 AND leaf
		 ORDER BY rev_num ASC, rev_hash ASC`, db.Schema), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leaves []Leaf
	for rows.Next() {
		var leaf Leaf
		var raw []byte
		if err := rows.Scan(&leaf.Rev.Num, &leaf.Rev.Hash, &leaf.Deleted, &raw); err != nil {
			return nil, err
		}
		if raw != nil {
			if leaf.Body, err = decodeBody(raw); err != nil {
				return nil, err
			}
		}
		leaves = append(leaves, leaf)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(leaves) == 0 {
		return nil, couch.DocMissing()
	}
	return leaves, nil
}

// Ancestry walks parent pointers from the given rev downward, newest first
// (for ?revs=true).
func (s *Store) Ancestry(ctx context.Context, db *DB, id string, from couch.Rev) ([]RevInfo, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`WITH RECURSIVE chain AS (
		   SELECT rev_num, rev_hash, parent_num, parent_hash, deleted, leaf,
		          body IS NOT NULL AS available
		   FROM %[1]s.revs WHERE id = $1 AND rev_num = $2 AND rev_hash = $3
		   UNION ALL
		   SELECT r.rev_num, r.rev_hash, r.parent_num, r.parent_hash, r.deleted,
		          r.leaf, r.body IS NOT NULL
		   FROM %[1]s.revs r JOIN chain c
		     ON r.id = $1 AND r.rev_num = c.parent_num AND r.rev_hash = c.parent_hash
		 )
		 SELECT rev_num, rev_hash, deleted, leaf, available FROM chain`, db.Schema),
		id, from.Num, from.Hash)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[RevInfo])
}

// AllRevs lists every rev of a document, newest first (for _revs_diff and
// revs_info).
func (s *Store) AllRevs(ctx context.Context, db *DB, id string) ([]RevInfo, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT rev_num, rev_hash, deleted, leaf, body IS NOT NULL FROM %s.revs
		 WHERE id = $1 ORDER BY rev_num DESC, rev_hash DESC`, db.Schema), id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[RevInfo])
}

// RevsDiff reports which of the supplied revs are unknown here, plus
// possible ancestors the peer could send deltas against.
func (s *Store) RevsDiff(ctx context.Context, db *DB, id string, revs []couch.Rev) (missing []couch.Rev, possibleAncestors []couch.Rev, err error) {
	known, err := s.AllRevs(ctx, db, id)
	if err != nil {
		return nil, nil, err
	}
	knownSet := make(map[couch.Rev]bool, len(known))
	var leaves []couch.Rev
	for _, r := range known {
		rev := couch.Rev{Num: r.Num, Hash: r.Hash}
		knownSet[rev] = true
		if r.Leaf {
			leaves = append(leaves, rev)
		}
	}
	maxMissing := 0
	for _, rev := range revs {
		if !knownSet[rev] {
			missing = append(missing, rev)
			if rev.Num > maxMissing {
				maxMissing = rev.Num
			}
		}
	}
	if len(missing) > 0 {
		for _, leaf := range leaves {
			if leaf.Num < maxMissing {
				possibleAncestors = append(possibleAncestors, leaf)
			}
		}
	}
	return missing, possibleAncestors, nil
}

func parentNum(parent *couch.Rev) any {
	if parent == nil {
		return nil
	}
	return parent.Num
}

func parentHash(parent *couch.Rev) any {
	if parent == nil {
		return nil
	}
	return parent.Hash
}

func decodeBody(raw []byte) (map[string]any, error) {
	v, err := couch.DecodeJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("stored body is not valid JSON: %w", err)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("stored body is not a JSON object")
	}
	return obj, nil
}
