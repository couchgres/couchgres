package store

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/collate"
	"github.com/couchgres/couchgres/internal/couch"
)

// ViewEmit is one emitted view row (raw JSON key and value).
type ViewEmit struct {
	Key   json.RawMessage
	Value json.RawMessage
}

// ViewMapper runs map functions. The JS engine pool implements it, while the
// interface keeps store free of the cgo dependency. Implementations must
// be safe for concurrent MapDocs calls. Index builds fan one batch's
// chunks out in parallel.
type ViewMapper interface {
	MapDocs(ctx context.Context, sig string, fns []string, lib map[string]any, docs []json.RawMessage) ([][][]ViewEmit, error)
}

// ViewReduceGroup is one group's input to a JS reduce function.
type ViewReduceGroup struct {
	Keys   []json.RawMessage
	Values []json.RawMessage
}

// ViewReducer runs JS reduce functions.
type ViewReducer interface {
	ReduceGroups(ctx context.Context, sig, fn string, groups []ViewReduceGroup, rereduce bool) ([]json.RawMessage, error)
}

// ViewDef is one view's function pair.
type ViewDef struct {
	Map    string
	Reduce string
}

// ViewGroup is the indexable content of one design document. All its views
// share a signature and a physical table.
type ViewGroup struct {
	DDocID    string
	Sig       string
	Language  string
	Views     map[string]ViewDef
	ViewOrder []string       // deterministic function indexing
	Lib       map[string]any // require() tree. {"views": {"lib": ...}}
	// MangoIndexes holds the index definitions when Language == "query"
	// One entry exists per ViewOrder item. Views' Map members are unused then.
	MangoIndexes []MangoIndexDef
	// PartitionedOption mirrors the ddoc's options.partitioned member.
	PartitionedOption *bool
	// IncludeDesign (options.include_design) also maps design documents.
	// LocalSeq (options.local_seq) exposes _local_seq to map functions.
	IncludeDesign bool
	LocalSeq      bool
	// The caller sets Partitioned when the database is partitioned and the
	// design document has not opted out. Partitioned groups key rows by partition and
	// refuse global queries.
	Partitioned bool
}

// ResolvePartitioned combines the db's partitioning with the ddoc option.
func (vg *ViewGroup) ResolvePartitioned(db *DB) {
	vg.Partitioned = db.Partitioned &&
		(vg.PartitionedOption == nil || *vg.PartitionedOption)
}

// MangoIndexDef is one Mango json index inside a query-language ddoc.
type MangoIndexDef struct {
	Name            string
	Fields          []MangoIndexField
	PartialSelector map[string]any // nil when absent or {}
}

// MangoIndexField is one indexed field with its direction.
type MangoIndexField struct {
	Field string
	Desc  bool
}

// ViewGroupFromDDoc parses a design document body (underscore members
// already stripped) into its view group. Returns nil when the ddoc has no
// views.
func ViewGroupFromDDoc(ddocID string, body map[string]any) (*ViewGroup, error) {
	language, _ := body["language"].(string)
	if language == "" {
		language = "javascript"
	}
	rawViews, ok := body["views"].(map[string]any)
	if !ok || len(rawViews) == 0 {
		return nil, nil
	}
	vg := &ViewGroup{
		DDocID:   ddocID,
		Language: language,
		Views:    make(map[string]ViewDef),
	}
	if options, ok := body["options"].(map[string]any); ok {
		if p, ok := options["partitioned"].(bool); ok {
			vg.PartitionedOption = &p
		}
		vg.IncludeDesign, _ = options["include_design"].(bool)
		vg.LocalSeq, _ = options["local_seq"].(bool)
	}
	sigInput := map[string]any{"views": map[string]any{}, "language": language}
	// Indexing options change the rows, so they change the signature. When
	// absent, they stay out of the signature and existing indexes keep their tables.
	// options.partitioned also matters. A global design document must not share a
	// table with a partitioned one carrying the same views.
	if vg.IncludeDesign || vg.LocalSeq || vg.PartitionedOption != nil {
		options := map[string]any{
			"include_design": vg.IncludeDesign, "local_seq": vg.LocalSeq,
		}
		if vg.PartitionedOption != nil {
			options["partitioned"] = *vg.PartitionedOption
		}
		sigInput["options"] = options
	}
	sigViews := sigInput["views"].(map[string]any)
	for name, raw := range rawViews {
		if name == "lib" {
			vg.Lib = map[string]any{"views": map[string]any{"lib": raw}}
			sigViews["lib"] = raw
			continue
		}
		def, ok := raw.(map[string]any)
		if !ok {
			return nil, couch.BadRequest(fmt.Sprintf("View `%s` is not an object", name))
		}
		if language == "query" {
			idx, err := mangoIndexFromView(name, def)
			if err != nil {
				return nil, err
			}
			vg.Views[name] = ViewDef{Reduce: "_count"}
			vg.MangoIndexes = append(vg.MangoIndexes, *idx)
			sigViews[name] = def
			continue
		}
		mapSrc, ok := def["map"].(string)
		if !ok {
			return nil, couch.BadRequest(fmt.Sprintf("View `%s` has no map function", name))
		}
		reduceSrc, _ := def["reduce"].(string)
		vg.Views[name] = ViewDef{Map: mapSrc, Reduce: reduceSrc}
		sigViews[name] = map[string]any{"map": mapSrc, "reduce": reduceSrc}
	}
	if len(vg.Views) == 0 {
		return nil, nil
	}
	for name := range vg.Views {
		vg.ViewOrder = append(vg.ViewOrder, name)
	}
	sort.Strings(vg.ViewOrder)
	sort.Slice(vg.MangoIndexes, func(i, j int) bool {
		return vg.MangoIndexes[i].Name < vg.MangoIndexes[j].Name
	})
	sum := md5.Sum(couch.CanonicalBody(sigInput))
	vg.Sig = hex.EncodeToString(sum[:])
	return vg, nil
}

// mangoIndexFromView reads one query-language view. Field order comes from
// options.def.fields. The map.fields object loses order in JSON.
func mangoIndexFromView(name string, def map[string]any) (*MangoIndexDef, error) {
	idx := &MangoIndexDef{Name: name}
	options, _ := def["options"].(map[string]any)
	indexDef, _ := options["def"].(map[string]any)
	fields, ok := indexDef["fields"].([]any)
	if !ok || len(fields) == 0 {
		return nil, couch.BadRequest(fmt.Sprintf("Invalid index `%s`: no fields", name))
	}
	for _, raw := range fields {
		switch t := raw.(type) {
		case string:
			idx.Fields = append(idx.Fields, MangoIndexField{Field: t})
		case map[string]any:
			for field, dir := range t {
				idx.Fields = append(idx.Fields, MangoIndexField{
					Field: field,
					Desc:  dir == "desc",
				})
			}
		default:
			return nil, couch.BadRequest(fmt.Sprintf("Invalid index `%s`: bad field entry", name))
		}
	}
	if mapDef, ok := def["map"].(map[string]any); ok {
		if partial, ok := mapDef["partial_filter_selector"].(map[string]any); ok && len(partial) > 0 {
			idx.PartialSelector = partial
		}
	}
	return idx, nil
}

// mapSources returns the map functions in ViewOrder.
func (vg *ViewGroup) mapSources() []string {
	fns := make([]string, len(vg.ViewOrder))
	for i, name := range vg.ViewOrder {
		fns[i] = vg.Views[name].Map
	}
	return fns
}

func viewTable(sig string) string { return "v_" + sig }

// ensureViewTables creates the signature's data table and state row. The result
// is cached per process because every view read passes through here.
func (s *Store) ensureViewTables(ctx context.Context, db *DB, sig string) error {
	cacheKey := db.Schema + "/" + sig
	if _, ok := s.ensuredViews.Load(cacheKey); ok {
		return nil
	}
	table := viewTable(sig)
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s.view_state (
    sig       text PRIMARY KEY,
    last_seq  bigint NOT NULL DEFAULT 0,
    purge_seq bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
    view_name   text NOT NULL,
    part        text NOT NULL DEFAULT '',
    key_collate bytea NOT NULL,
    key         json NOT NULL,
    value       json NOT NULL,
    doc_id      text COLLATE "C" NOT NULL,
    seq         bigint NOT NULL
);
ALTER TABLE %[1]s.%[2]s ADD COLUMN IF NOT EXISTS part text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS %[2]s_scan ON %[1]s.%[2]s (view_name, part, key_collate, doc_id);
CREATE INDEX IF NOT EXISTS %[2]s_doc ON %[1]s.%[2]s (doc_id);
`, db.Schema, table)
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.view_state (sig, purge_seq)
		 SELECT $1, purge_seq FROM couchgres.databases WHERE name = $2 FOR SHARE
		 ON CONFLICT (sig) DO NOTHING`, db.Schema), sig, db.Name); err != nil {
		return err
	}
	s.ensuredViews.Store(cacheKey, struct{}{})
	return nil
}

// One build transaction processes viewBatchSize documents. The map work is
// split into mapChunkSize chunks across the JS pool. Wide fan-out needs the
// sharded-allocator libc fork (see modernc.org/libc replacement in go.mod).
// Stock libc serializes every VM on one malloc mutex. Ten parallel VMs then
// spin instead of mapping (1.45s per 31k-document build versus 0.61s sharded).
const (
	viewBatchSize = 2000
	mapChunkSize  = 100
)

// UpdateViewGroup folds all documents newer than the group's last indexed
// seq through its map functions. It is safe to call concurrently. The state row
// lock serializes updaters of one signature.
//
// Long builds can outrun autoanalyze. Without fresh statistics, the planner
// sequentially scans the growing view table for every batch's doc_id DELETE.
// This makes the build O(n²) and misjudges the docs scan after a bulk load. ANALYZE up
// front and periodically to keep batch plans on the indexes.
func (s *Store) UpdateViewGroup(ctx context.Context, db *DB, vg *ViewGroup, mapper ViewMapper) error {
	_, err := s.SyncViewGroup(ctx, db, vg, mapper)
	return err
}

// SyncViewGroup runs UpdateViewGroup and returns the resulting last_seq. The
// read path uses it for ETags and total-cache validation. The currency check
// provides it and saves two round trips per query.
func (s *Store) SyncViewGroup(ctx context.Context, db *DB, vg *ViewGroup, mapper ViewMapper) (int64, error) {
	lastSeq, _, err := s.SyncViewGroupSnapshot(ctx, db, vg, mapper)
	return lastSeq, err
}

// SyncViewGroupSnapshot updates the group and captures its last_seq together
// with the database purge generation before the caller reads view rows. Purges
// remove rows without advancing last_seq, so both values define a reusable
// view-read snapshot.
func (s *Store) SyncViewGroupSnapshot(
	ctx context.Context,
	db *DB,
	vg *ViewGroup,
	mapper ViewMapper,
) (int64, int64, error) {
	if err := s.ensureViewTables(ctx, db, vg.Sig); err != nil {
		return 0, 0, err
	}
	// Fast path. Most queries find the index current. A lockless read avoids
	// opening the batch transaction. SELECT ... FOR UPDATE writes a row-lock
	// WAL record on every view read. A write landing
	// between this check and the query was equally invisible to the old
	// locked read. Semantics are unchanged.
	var lastSeq int64
	var purgeSeq int64
	var current bool
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT v.last_seq, v.last_seq >= coalesce(
		   (SELECT seq FROM %[1]s.docs ORDER BY seq DESC LIMIT 1), 0),
		   v.purge_seq
		 FROM %[1]s.view_state v
		 WHERE v.sig = $1`, db.Schema), vg.Sig,
	).Scan(&lastSeq, &current, &purgeSeq); err == nil && current {
		return lastSeq, purgeSeq, nil
	}
	if err := s.updateViewGroupSlow(ctx, db, vg, mapper); err != nil {
		return 0, 0, err
	}
	return s.ViewGroupSnapshot(ctx, db, vg.Sig)
}

// ViewReadValid reports whether a view response captured at (ddocSeq, lastSeq,
// purgeSeq) is still byte-exact. It requires the same design document revision,
// index state, and purge generation, plus an index current with the database.
// This takes one round trip on the response-cache fast path.
func (s *Store) ViewReadValid(
	ctx context.Context,
	db *DB,
	ddocID, sig string,
	ddocSeq, lastSeq, purgeSeq int64,
) bool {
	var ok bool
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce((SELECT seq FROM %[1]s.docs WHERE id = $1), -1) = $2
		    AND EXISTS (SELECT 1 FROM %[1]s.view_state
		                WHERE sig = $3 AND last_seq = $4 AND purge_seq = $5)
		    AND $4 >= coalesce((SELECT seq FROM %[1]s.docs ORDER BY seq DESC LIMIT 1), 0)
		`, db.Schema), ddocID, ddocSeq, sig, lastSeq, purgeSeq).Scan(&ok)
	return err == nil && ok
}

// updateViewGroupSlow runs the batch loop until the index is current.
func (s *Store) updateViewGroupSlow(ctx context.Context, db *DB, vg *ViewGroup, mapper ViewMapper) error {
	// ANALYZE is best-effort and concurrent. It does not block DML, so the
	// build keeps mapping while statistics refresh. A failed ANALYZE only
	// affects plan quality. Run only one at a time.
	var analyzing atomic.Bool
	var analyzeWG sync.WaitGroup
	defer analyzeWG.Wait()
	analyze := func() {
		if !analyzing.CompareAndSwap(false, true) {
			return
		}
		analyzeWG.Add(1)
		go func() {
			defer analyzeWG.Done()
			defer analyzing.Store(false)
			s.pool.Exec(ctx, fmt.Sprintf("ANALYZE %s.docs, %s.revs, %s.%s",
				db.Schema, db.Schema, db.Schema, viewTable(vg.Sig)))
		}()
	}
	for batches := 0; ; batches++ {
		more, err := s.updateViewBatch(ctx, db, vg, mapper)
		if err != nil {
			return err
		}
		if !more {
			if batches >= 8 {
				// A big build leaves no visibility-map bits, so the offset
				// counts on the read path can't run index-only until the
				// table is vacuumed. Do not wait for autovacuum.
				s.pool.Exec(ctx, fmt.Sprintf(
					"VACUUM (ANALYZE) %s.%s", db.Schema, viewTable(vg.Sig)))
			}
			return nil
		}
		// Only a build with more batches coming pays for statistics. The
		// per-query no-op update check never reaches here.
		if batches%8 == 0 {
			analyze()
		}
	}
}

// mapDocsParallel folds one batch's docs through the map functions,
// fanning mapChunkSize chunks out as concurrent MapDocs calls. The JS pool
// hands each to a free worker. Results land at their chunk's offset, so
// the output order matches docs regardless of completion order.
func mapDocsParallel(ctx context.Context, vg *ViewGroup, mapper ViewMapper, docs []json.RawMessage) ([][][]ViewEmit, error) {
	fns := vg.mapSources()
	if len(docs) <= mapChunkSize {
		out, err := mapper.MapDocs(ctx, vg.Sig, fns, vg.Lib, docs)
		if err == nil && len(out) != len(docs) {
			return nil, fmt.Errorf("mapper returned %d results for %d docs", len(out), len(docs))
		}
		return out, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	emits := make([][][]ViewEmit, len(docs))
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for start := 0; start < len(docs); start += mapChunkSize {
		end := min(start+mapChunkSize, len(docs))
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := mapper.MapDocs(ctx, vg.Sig, fns, vg.Lib, docs[start:end])
			if err == nil && len(out) != end-start {
				err = fmt.Errorf("mapper returned %d results for %d docs", len(out), end-start)
			}
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel() // stop the chunks still queued behind this one
				})
				return
			}
			copy(emits[start:end], out)
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return emits, nil
}

type viewSourceDoc struct {
	id      string
	rev     couch.Rev
	deleted bool
	body    []byte
	seq     int64
}

func (s *Store) updateViewBatch(ctx context.Context, db *DB, vg *ViewGroup, mapper ViewMapper) (more bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var lastSeq int64
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		"SELECT last_seq FROM %s.view_state WHERE sig = $1 FOR UPDATE", db.Schema),
		vg.Sig,
	).Scan(&lastSeq); err != nil {
		return false, err
	}

	// The batch comes off docs_seq_idx. Each row's body uses one revs
	// primary-key probe. LATERAL LIMIT 1 is a no-op because the reference
	// is a primary key. It stops the planner from decorrelating into a hash
	// join that scans all revs per batch. That plan is
	// O(database) per batch, an O(n²) build.
	rows, err := tx.Query(ctx, fmt.Sprintf(
		`SELECT d.id, d.rev_num, d.rev_hash, d.deleted,
		        coalesce(r.body, '{}'::jsonb), d.seq
		 FROM (SELECT id, rev_num, rev_hash, deleted, seq FROM %[1]s.docs
		       WHERE seq > $1 ORDER BY seq LIMIT %[2]d) d
		 CROSS JOIN LATERAL (
		   SELECT body FROM %[1]s.revs r
		   WHERE r.id = d.id AND r.rev_num = d.rev_num
		     AND r.rev_hash = d.rev_hash LIMIT 1) r
		 ORDER BY d.seq`,
		db.Schema, viewBatchSize),
		lastSeq)
	if err != nil {
		return false, err
	}
	var batch []viewSourceDoc
	for rows.Next() {
		var d viewSourceDoc
		if err := rows.Scan(&d.id, &d.rev.Num, &d.rev.Hash, &d.deleted, &d.body, &d.seq); err != nil {
			rows.Close()
			return false, err
		}
		batch = append(batch, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(batch) == 0 {
		return false, nil
	}
	newSeq := batch[len(batch)-1].seq

	// Only live, non-design documents get mapped. The include_design option
	// also maps design documents. Everything in the batch has its old rows
	// deleted.
	var mapped []viewSourceDoc
	for _, d := range batch {
		if !d.deleted && (vg.IncludeDesign || !isDesignDocID(d.id)) {
			mapped = append(mapped, d)
		}
	}
	docJSONs, err := s.viewInputDocs(ctx, tx, db, mapped, vg.LocalSeq)
	if err != nil {
		return false, err
	}
	var emits [][][]ViewEmit
	if len(mapped) > 0 {
		emits, err = mapDocsParallel(ctx, vg, mapper, docJSONs)
		if err != nil {
			return false, err
		}
	}

	ids := make([]string, len(batch))
	for i, d := range batch {
		ids[i] = d.id
	}
	table := db.Schema + "." + viewTable(vg.Sig)
	if _, err := tx.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE doc_id = ANY($1)", table), ids,
	); err != nil {
		return false, err
	}

	var names, parts []string
	var collates [][]byte
	var keys, values, docIDs []string
	var seqs []int64
	for d, perView := range emits {
		doc := mapped[d]
		part := ""
		if vg.Partitioned {
			part = PartitionOf(doc.id)
		}
		for v, rows := range perView {
			viewName := vg.ViewOrder[v]
			for _, row := range rows {
				enc, err := collate.Key(row.Key)
				if err != nil {
					// Skip unencodable keys, such as numbers out of range.
					// This matches CouchDB's log-and-skip behavior for map errors.
					continue
				}
				names = append(names, viewName)
				parts = append(parts, part)
				collates = append(collates, enc)
				keys = append(keys, string(row.Key))
				values = append(values, string(row.Value))
				docIDs = append(docIDs, doc.id)
				seqs = append(seqs, doc.seq)
			}
		}
	}
	if len(names) > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (view_name, part, key_collate, key, value, doc_id, seq)
			 SELECT n, p, c, k::json, v::json, d, s
			 FROM unnest($1::text[], $2::text[], $3::bytea[], $4::text[], $5::text[], $6::text[], $7::bigint[])
			      AS t(n, p, c, k, v, d, s)`, table),
			names, parts, collates, keys, values, docIDs, seqs,
		); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.view_state SET last_seq = $1 WHERE sig = $2", db.Schema),
		newSeq, vg.Sig,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return len(batch) == viewBatchSize, nil
}

// viewInputDocs assembles the JSON documents map functions see. They include _id, _rev,
// body members, and _attachments stubs (_local_seq with the option).
func (s *Store) viewInputDocs(ctx context.Context, tx pgx.Tx, db *DB, docs []viewSourceDoc, localSeq bool) ([]json.RawMessage, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.id
	}
	stubs := make(map[string]map[string]any)
	rows, err := tx.Query(ctx, fmt.Sprintf(
		`SELECT a.doc_id, a.name, a.content_type, a.digest, a.revpos, a.length
		 FROM %[1]s.attachments a JOIN %[1]s.docs d
		   ON a.doc_id = d.id AND a.rev_num = d.rev_num AND a.rev_hash = d.rev_hash
		 WHERE d.id = ANY($1)`, db.Schema), ids)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var docID, name, contentType, digest string
		var revpos int
		var length int64
		if err := rows.Scan(&docID, &name, &contentType, &digest, &revpos, &length); err != nil {
			rows.Close()
			return nil, err
		}
		if stubs[docID] == nil {
			stubs[docID] = make(map[string]any)
		}
		stubs[docID][name] = map[string]any{
			"content_type": contentType,
			"revpos":       revpos,
			"digest":       digest,
			"length":       length,
			"stub":         true,
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Map functions see _conflicts, like CouchDB's view server.
	conflicts := make(map[string][]string)
	crows, err := tx.Query(ctx, fmt.Sprintf(
		`SELECT r.id, r.rev_num, r.rev_hash
		 FROM %[1]s.revs r JOIN %[1]s.docs d ON r.id = d.id
		 WHERE d.id = ANY($1) AND r.leaf AND NOT r.deleted
		   AND (r.rev_num, r.rev_hash) <> (d.rev_num, d.rev_hash)
		 ORDER BY r.rev_num DESC, r.rev_hash DESC`, db.Schema), ids)
	if err != nil {
		return nil, err
	}
	for crows.Next() {
		var docID, hash string
		var num int
		if err := crows.Scan(&docID, &num, &hash); err != nil {
			crows.Close()
			return nil, err
		}
		conflicts[docID] = append(conflicts[docID], couch.Rev{Num: num, Hash: hash}.String())
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return nil, err
	}

	out := make([]json.RawMessage, len(docs))
	for i, d := range docs {
		body, err := decodeBody(d.body)
		if err != nil {
			return nil, err
		}
		doc := make(map[string]any, len(body)+4)
		doc["_id"] = d.id
		doc["_rev"] = d.rev.String()
		if localSeq {
			doc["_local_seq"] = d.seq
		}
		for k, v := range body {
			doc[k] = v
		}
		if atts := stubs[d.id]; atts != nil {
			doc["_attachments"] = atts
		}
		if revs := conflicts[d.id]; len(revs) > 0 {
			doc["_conflicts"] = revs
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		out[i] = raw
	}
	return out, nil
}

func isDesignDocID(id string) bool {
	return len(id) > 8 && id[:8] == "_design/"
}

// PartitionOf extracts the partition from a partitioned doc id.
func PartitionOf(id string) string {
	for i := 0; i < len(id); i++ {
		if id[i] == ':' {
			return id[:i]
		}
	}
	return ""
}

// PartitionRange is the doc-id byte range covering one partition
// [p:", "p;"). ';' is the byte after ':'.
func PartitionRange(partition string) (lo, hi string) {
	return partition + ":", partition + ";"
}

// CountDocsRange counts docs with lo <= id < hi (for partition info and
// partition _all_docs totals).
func (s *Store) CountDocsRange(ctx context.Context, db *DB, lo, hi string, deleted bool) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.docs WHERE deleted = $3 AND id >= $1 AND id < $2",
		db.Schema), lo, hi, deleted).Scan(&n)
	return n, err
}

// CountDocsThrough counts documents with lo <= id <= hi. The upper bound is
// inclusive because Postgres text cannot hold the NUL suffix trick.
func (s *Store) CountDocsThrough(ctx context.Context, db *DB, lo, hi string, deleted bool) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.docs WHERE deleted = $3 AND id >= $1 AND id <= $2",
		db.Schema), lo, hi, deleted).Scan(&n)
	return n, err
}

// SizeDocsAll is SizeDocsRange over the whole database. It returns the "external"
// size, consistent with per-partition sums.
func (s *Store) SizeDocsAll(ctx context.Context, db *DB) (int64, error) {
	var bodies, atts int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT coalesce(sum(length(%s::text)), 0) FROM %s.docs d %s WHERE NOT d.deleted",
		winnerBody, db.Schema, winnerJoin(db.Schema))).Scan(&bodies)
	if err != nil {
		return 0, err
	}
	err = s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(sum(a.length), 0)
		 FROM %[1]s.attachments a
		 JOIN %[1]s.docs d ON a.doc_id = d.id AND a.rev_num = d.rev_num AND a.rev_hash = d.rev_hash
		 WHERE NOT d.deleted`, db.Schema)).Scan(&atts)
	return bodies + atts, err
}

// DocBodySize is the stored JSON text length of the live winner (0 when
// missing or deleted). It feeds the partition-limit projection.
func (s *Store) DocBodySize(ctx context.Context, db *DB, id string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT coalesce(length(%s::text), 0) FROM %s.docs d %s WHERE d.id = $1 AND NOT d.deleted",
		winnerBody, db.Schema, winnerJoin(db.Schema)), id).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return n, err
}

// SizeDocsRange sums the JSON body text plus attachment bytes of live docs
// with lo <= id < hi (the partition's "external" size).
func (s *Store) SizeDocsRange(ctx context.Context, db *DB, lo, hi string) (int64, error) {
	var bodies, atts int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(sum(length(%s::text)), 0) FROM %s.docs d %s
		 WHERE NOT d.deleted AND d.id >= $1 AND d.id < $2`,
		winnerBody, db.Schema, winnerJoin(db.Schema)), lo, hi).Scan(&bodies)
	if err != nil {
		return 0, err
	}
	err = s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(sum(a.length), 0)
		 FROM %[1]s.attachments a
		 JOIN %[1]s.docs d ON a.doc_id = d.id AND a.rev_num = d.rev_num AND a.rev_hash = d.rev_hash
		 WHERE NOT d.deleted AND d.id >= $1 AND d.id < $2`,
		db.Schema), lo, hi).Scan(&atts)
	return bodies + atts, err
}

// ViewGroupState reads the group's last indexed seq (0 when never built).
func (s *Store) ViewGroupState(ctx context.Context, db *DB, sig string) (int64, error) {
	var lastSeq int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT last_seq FROM %s.view_state WHERE sig = $1", db.Schema), sig,
	).Scan(&lastSeq)
	if err != nil {
		return 0, nil // Missing table or row. Never built.
	}
	return lastSeq, nil
}

// ViewGroupSnapshot returns the index and purge generations in one database
// snapshot. A missing state row is an unbuilt view at sequence zero.
func (s *Store) ViewGroupSnapshot(ctx context.Context, db *DB, sig string) (int64, int64, error) {
	var lastSeq, purgeSeq int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT last_seq, purge_seq FROM %s.view_state WHERE sig = $1", db.Schema), sig,
	).Scan(&lastSeq, &purgeSeq)
	if err == pgx.ErrNoRows {
		purgeSeq, err = s.PurgeSeq(ctx, db)
		return 0, purgeSeq, err
	}
	return lastSeq, purgeSeq, err
}

// ViewGroupSize reports the on-disk size of the group's table (for _info).
func (s *Store) ViewGroupSize(ctx context.Context, db *DB, sig string) int64 {
	var size int64
	err := s.pool.QueryRow(ctx,
		"SELECT pg_total_relation_size(($1 || '.' || $2)::regclass)",
		db.Schema, viewTable(sig),
	).Scan(&size)
	if err != nil {
		return 0
	}
	return size
}

// CleanupViews drops view tables whose signature no current design doc
// references.
func (s *Store) CleanupViews(ctx context.Context, db *DB, liveSigs map[string]bool) error {
	rows, err := s.pool.Query(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = $1 AND table_name LIKE 'v\_%'`, db.Schema)
	if err != nil {
		return err
	}
	var orphans []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		if !liveSigs[name[len("v_"):]] {
			orphans = append(orphans, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, table := range orphans {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			"DROP TABLE IF EXISTS %s.%s", db.Schema, table)); err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.view_state WHERE sig = $1", db.Schema), table[len("v_"):]); err != nil {
			return err
		}
	}
	if len(orphans) > 0 {
		s.clearViewCaches(db)
	}
	return nil
}

// MaxDesignSeq is the highest seq any design doc (deleted included) has
// touched. This provides a cheap staleness check for the validation-function cache.
// The range predicate (not LIKE) matches docs_design_seq_idx, keeping this
// an O(1) index read no matter what the statistics say.
func (s *Store) MaxDesignSeq(ctx context.Context, db *DB) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(max(seq), 0) FROM %s.docs
		 WHERE id >= '_design/' AND id < '_design0'`,
		db.Schema)).Scan(&seq)
	return seq, err
}

// CountDesignDocs counts live design docs, optionally only those up to
// bound. It is exclusive by default. Inclusive adds the bound itself. Used for
// _design_docs offsets.
func (s *Store) CountDesignDocs(ctx context.Context, db *DB, bound *string, inclusive bool) (int64, error) {
	sql := fmt.Sprintf(
		`SELECT count(*) FROM %s.docs
		 WHERE id >= '_design/' AND id < '_design0' AND NOT deleted`,
		db.Schema)
	var args []any
	if bound != nil {
		op := "<"
		if inclusive {
			op = "<="
		}
		sql += fmt.Sprintf(" AND id %s $1", op)
		args = append(args, *bound)
	}
	var n int64
	err := s.pool.QueryRow(ctx, sql, args...).Scan(&n)
	return n, err
}

// DesignDocs returns the live design documents (for _view_cleanup and the
// VDU/filter caches).
func (s *Store) DesignDocs(ctx context.Context, db *DB) ([]*DocRow, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT d.id, d.rev_num, d.rev_hash, %s, d.seq FROM %s.docs d %s
		 WHERE d.id >= '_design/' AND d.id < '_design0' AND NOT d.deleted
		 ORDER BY d.id`, winnerBody, db.Schema, winnerJoin(db.Schema)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []*DocRow
	for rows.Next() {
		row := &DocRow{}
		var raw []byte
		if err := rows.Scan(&row.ID, &row.Rev.Num, &row.Rev.Hash, &raw, &row.Seq); err != nil {
			return nil, err
		}
		if row.Body, err = decodeBody(raw); err != nil {
			return nil, err
		}
		docs = append(docs, row)
	}
	return docs, rows.Err()
}
