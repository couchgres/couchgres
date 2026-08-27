package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/collate"
	"github.com/couchgres/couchgres/internal/couch"
)

// ViewQuery is a parsed view request. Key bounds are raw JSON text.
type ViewQuery struct {
	Key           []byte
	Keys          [][]byte
	StartKey      []byte
	EndKey        []byte
	StartKeyDocID string
	EndKeyDocID   string
	Descending    bool
	InclusiveEnd  bool // default true
	Skip          int64
	Limit         *int64
	Reduce        *bool // nil = default (reduce when the view has one)
	Group         bool
	GroupLevel    *int64
	IncludeDocs   bool
	Conflicts     bool // With IncludeDocs, documents carry _conflicts.
	UpdateSeq     bool
	// ReduceLimit enables the reduce_overflow_error guard
	// (query_server_config/reduce_limit, default true).
	ReduceLimit bool
	// Partition scopes the query to one partition ("" means global). Queries on
	// partitioned view groups must set it.
	Partition string
	// LastSeqHint carries the group's last_seq when the caller already
	// knows it (SyncViewGroup returns it). <= 0 means unknown and the
	// query re-reads view_state where needed.
	LastSeqHint int64
	// PurgeSeqHint carries the durable purge generation captured before this
	// query. Together with LastSeqHint it makes process-local total_rows caches
	// safe when another Couchgres process purges view rows.
	PurgeSeqHint *int64
}

// ViewRow is one result row. Doc members are set for include_docs.
type ViewRow struct {
	ID       string          `json:"id,omitempty"`
	Key      json.RawMessage `json:"key"`
	Value    json.RawMessage `json:"value"`
	DocID    string          `json:"-"` // Target document for include_docs (linked documents).
	Doc      *DocRow         `json:"-"`
	DocError string          `json:"-"`
	// DocConflicts holds the doc's live non-winner leaves (conflicts=true).
	DocConflicts []string `json:"-"`
	// Error/ErrReason turn the row into an error row (reduce overflow).
	Error     string `json:"-"`
	ErrReason string `json:"-"`
}

// ViewResult is a complete view response.
type ViewResult struct {
	TotalRows int64
	Offset    int64
	UpdateSeq int64
	Rows      []ViewRow
	Reduced   bool // rows carry no id/total_rows/offset
}

// QueryView executes a view query against the (already updated) index.
func (s *Store) QueryView(ctx context.Context, db *DB, vg *ViewGroup, viewName string, q *ViewQuery, reducer ViewReducer) (*ViewResult, error) {
	def, ok := vg.Views[viewName]
	if !ok {
		return nil, couch.NewError(404, "not_found", "missing_named_view")
	}
	if vg.Partitioned && q.Partition == "" {
		return nil, couch.NewError(400, "query_parse_error",
			"`partition` parameter is mandatory for queries to this view.")
	}
	if !vg.Partitioned && q.Partition != "" {
		return nil, couch.NewError(400, "query_parse_error",
			"`partition` parameter is not supported in this design doc")
	}
	wantReduce := def.Reduce != ""
	if q.Reduce != nil {
		if *q.Reduce && !wantReduce {
			return nil, couch.NewError(400, "query_parse_error", "Reduce is invalid for map-only views.")
		}
		wantReduce = *q.Reduce && wantReduce
	}
	if (q.Group || q.GroupLevel != nil) && !wantReduce {
		return nil, couch.NewError(400, "query_parse_error", "Invalid use of grouping on a map view.")
	}
	if q.IncludeDocs && wantReduce {
		return nil, couch.NewError(400, "query_parse_error", "`include_docs` is invalid for reduce")
	}
	if q.StartKey != nil && q.EndKey != nil {
		startEnc, err := collate.Key(q.StartKey)
		if err != nil {
			return nil, couch.QueryParseError("startkey", string(q.StartKey))
		}
		endEnc, err := collate.Key(q.EndKey)
		if err != nil {
			return nil, couch.QueryParseError("endkey", string(q.EndKey))
		}
		cmp := bytes.Compare(startEnc, endEnc)
		if (!q.Descending && cmp > 0) || (q.Descending && cmp < 0) {
			return nil, couch.NewError(400, "query_parse_error",
				"No rows can match your key range, reverse your start_key and end_key or set descending=true")
		}
	}
	if q.Keys != nil {
		if q.Key != nil || q.StartKey != nil || q.EndKey != nil {
			return nil, couch.NewError(400, "query_parse_error",
				"`keys` is incompatible with `key`, `start_key` and `end_key`")
		}
		if wantReduce && !q.Group {
			return nil, couch.NewError(400, "query_parse_error",
				"Multi-key fetches for reduce views must use `group=true`")
		}
	}

	result := &ViewResult{}
	if q.UpdateSeq {
		if q.LastSeqHint > 0 {
			result.UpdateSeq = q.LastSeqHint
		} else {
			seq, err := s.ViewGroupState(ctx, db, vg.Sig)
			if err != nil {
				return nil, err
			}
			result.UpdateSeq = seq
		}
	}
	if wantReduce {
		result.Reduced = true
		rows, err := s.queryViewReduce(ctx, db, vg, viewName, def.Reduce, q, reducer)
		if err != nil {
			return nil, err
		}
		result.Rows = rows
		return result, nil
	}
	return s.queryViewMap(ctx, db, vg, viewName, q, result)
}

// viewCond builds the WHERE fragment for the query's key range. Argument
// numbering starts after $1 (view_name).
type viewCond struct {
	where string
	args  []any
}

func (c *viewCond) add(clause string, args ...any) {
	n := len(c.args)
	for i := range args {
		clause = strings.Replace(clause, fmt.Sprintf("@%d", i+1), fmt.Sprintf("$%d", n+i+2), 1)
	}
	c.where += " AND " + clause
	c.args = append(c.args, args...)
}

// rangeConds translates the query bounds into SQL. In a descending
// traversal startkey is the upper bound.
func rangeConds(q *ViewQuery) (*viewCond, error) {
	c := &viewCond{}
	if q.Partition != "" {
		c.add("part = @1", q.Partition)
	}
	if q.Key != nil {
		enc, err := collate.Key(q.Key)
		if err != nil {
			return nil, couch.QueryParseError("key", string(q.Key))
		}
		c.add("key_collate = @1", enc)
		return c, nil
	}
	if q.StartKey != nil {
		enc, err := collate.Key(q.StartKey)
		if err != nil {
			return nil, couch.QueryParseError("startkey", string(q.StartKey))
		}
		op := ">="
		if q.Descending {
			op = "<="
		}
		if q.StartKeyDocID != "" {
			c.add(fmt.Sprintf("(key_collate, doc_id) %s (@1, @2)", op), enc, q.StartKeyDocID)
		} else {
			c.add(fmt.Sprintf("key_collate %s @1", op), enc)
		}
	}
	if q.EndKey != nil {
		enc, err := collate.Key(q.EndKey)
		if err != nil {
			return nil, couch.QueryParseError("endkey", string(q.EndKey))
		}
		op := "<="
		if q.Descending {
			op = ">="
		}
		if !q.InclusiveEnd {
			op = op[:1]
		}
		if q.EndKeyDocID != "" {
			c.add(fmt.Sprintf("(key_collate, doc_id) %s (@1, @2)", op), enc, q.EndKeyDocID)
		} else {
			c.add(fmt.Sprintf("key_collate %s @1", op), enc)
		}
	}
	return c, nil
}

func (s *Store) queryViewMap(ctx context.Context, db *DB, vg *ViewGroup, viewName string, q *ViewQuery, result *ViewResult) (*ViewResult, error) {
	table := db.Schema + "." + viewTable(vg.Sig)

	if q.Keys != nil {
		return s.queryViewKeys(ctx, db, vg, viewName, q, result)
	}

	cond, err := rangeConds(q)
	if err != nil {
		return nil, err
	}

	dir := "ASC"
	if q.Descending {
		dir = "DESC"
	}

	// Count total view rows, rows in range, and rows preceding the range in
	// traversal order (CouchDB's offset). The before condition is kept
	// as data so each statement can number its own placeholders.
	var beforeArgs []any
	beforePair := false
	beforeOp := "<"
	if q.Descending {
		beforeOp = ">"
	}
	if q.StartKey != nil || q.Key != nil {
		start := q.StartKey
		if start == nil {
			start = q.Key
		}
		enc, err := collate.Key(start)
		if err != nil {
			return nil, couch.QueryParseError("startkey", string(start))
		}
		if q.StartKeyDocID != "" && q.Key == nil {
			beforePair = true
			beforeArgs = []any{enc, q.StartKeyDocID}
		} else {
			beforeArgs = []any{enc}
		}
	}
	// beforeExpr renders the before condition with placeholders starting
	// at $n ("false" when the traversal has no start bound).
	beforeExpr := func(n int) string {
		if beforeArgs == nil {
			return "false"
		}
		if beforePair {
			return fmt.Sprintf("(key_collate, doc_id) %s ($%d, $%d)", beforeOp, n, n+1)
		}
		return fmt.Sprintf("key_collate %s $%d", beforeOp, n)
	}
	beforeCond := beforeExpr(len(cond.args) + 2)
	countArgs := append([]any{viewName}, cond.args...)
	countArgs = append(countArgs, beforeArgs...)
	// part is always constrained ('' on global views). Without it, the
	// (view_name, part, key_collate) index can't bound on key_collate and
	// every count scans the whole view. total_rows is partition-scoped on
	// partitioned views.
	countArgs = append(countArgs, q.Partition)
	baseWhere := fmt.Sprintf("view_name = $1 AND part = $%d", len(countArgs))
	// The counts and row scan travel as one pipelined batch. The round trip,
	// not the SQL, is the read path's dominant cost.
	var total, inRange, before int64
	cached, lastSeq, purgeSeq, hit := s.cachedViewTotal(
		ctx, db, vg.Sig, viewName, q.Partition, q.LastSeqHint, q.PurgeSeqHint,
	)
	batch := &pgx.Batch{}
	scanCounts := func(pgx.Row) error { return nil }
	switch {
	case hit && q.Skip == 0 && beforeArgs == nil:
		// No range start and no skip. Offset is 0 and the total is cached.
		// Nothing needs counting.
		total = cached
	case hit && q.Skip == 0:
		// offset = before-count only (min(skip, inRange) is 0), a single
		// one-sided index range instead of a whole-view scan.
		total = cached
		args := []any{viewName, q.Partition}
		where := "view_name = $1 AND part = $2"
		beforeOnly := beforeExpr(len(args) + 1)
		args = append(args, beforeArgs...)
		batch.Queue(fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE %s AND (%s)", table, where, beforeOnly),
			args...)
		scanCounts = func(row pgx.Row) error { return row.Scan(&before) }
	case hit:
		// total_rows comes from the cache. Only rows at or below the range
		// still get counted, so the scan is bounded by the request instead
		// of the view size.
		total = cached
		batch.Queue(fmt.Sprintf(
			`SELECT count(*) FILTER (WHERE true %s),
			        count(*) FILTER (WHERE %s)
			 FROM %s WHERE %s AND (true %s OR %s)`,
			cond.where, beforeCond, table, baseWhere, cond.where, beforeCond),
			countArgs...)
		scanCounts = func(row pgx.Row) error { return row.Scan(&inRange, &before) }
	default:
		batch.Queue(fmt.Sprintf(
			`SELECT count(*),
			        count(*) FILTER (WHERE true %s),
			        count(*) FILTER (WHERE %s)
			 FROM %s WHERE %s`, cond.where, beforeCond, table, baseWhere),
			countArgs...)
		scanCounts = func(row pgx.Row) error { return row.Scan(&total, &inRange, &before) }
	}

	limit := int64(math.MaxInt64)
	if q.Limit != nil {
		limit = *q.Limit
	}

	// The main scan re-binds only the range args (no before-cond extras).
	cond2, _ := rangeConds(q)
	scanArgs := append([]any{viewName}, cond2.args...)
	scanArgs = append(scanArgs, limit, q.Skip, q.Partition)
	countQueued := batch.Len() > 0
	batch.Queue(fmt.Sprintf(
		`SELECT key, value, doc_id FROM %s
		 WHERE view_name = $1 AND part = $%d %s
		 ORDER BY key_collate %s, doc_id %s
		 LIMIT $%d OFFSET $%d`,
		table, len(cond2.args)+4, cond2.where, dir, dir,
		len(cond2.args)+2, len(cond2.args)+3), scanArgs...)

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	if countQueued {
		if err := scanCounts(br.QueryRow()); err != nil {
			return nil, err
		}
		if !hit {
			s.storeViewTotal(db, vg.Sig, viewName, q.Partition, lastSeq, purgeSeq, total)
		}
	}
	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var row ViewRow
		var key, value []byte
		if err := rows.Scan(&key, &value, &row.ID); err != nil {
			rows.Close()
			return nil, err
		}
		row.Key = json.RawMessage(key)
		row.Value = json.RawMessage(value)
		row.DocID = row.ID
		result.Rows = append(result.Rows, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := br.Close(); err != nil {
		return nil, err
	}
	result.TotalRows = total
	result.Offset = before + min64(q.Skip, inRange)
	if q.IncludeDocs {
		if err := s.attachViewDocs(ctx, db, result.Rows, q.Conflicts); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// queryViewKeys serves ?keys=[...]. It returns rows for each requested key in request
// order. The reported offset is the first key's position in traversal order
// (single-shard semantics). Clustered CouchDB's value is shard-merge noise.
func (s *Store) queryViewKeys(ctx context.Context, db *DB, vg *ViewGroup, viewName string, q *ViewQuery, result *ViewResult) (*ViewResult, error) {
	table := db.Schema + "." + viewTable(vg.Sig)
	// part is always constrained ('' on global views) so the scan index
	// can bound on key_collate. It also provides partition scoping.
	partCond, partArgs := " AND part = $2", []any{q.Partition}
	if cached, lastSeq, purgeSeq, hit := s.cachedViewTotal(
		ctx, db, vg.Sig, viewName, q.Partition, q.LastSeqHint, q.PurgeSeqHint,
	); hit {
		result.TotalRows = cached
	} else {
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE view_name = $1%s", table, partCond),
			append([]any{viewName}, partArgs...)...,
		).Scan(&result.TotalRows); err != nil {
			return nil, err
		}
		s.storeViewTotal(db, vg.Sig, viewName, q.Partition, lastSeq, purgeSeq, result.TotalRows)
	}
	// Descending reverses the traversal. Keys use reverse request order,
	// docids descending within each key.
	keysList := q.Keys
	if q.Descending {
		keysList = make([][]byte, len(q.Keys))
		for i, k := range q.Keys {
			keysList[len(q.Keys)-1-i] = k
		}
	}
	if len(keysList) > 0 {
		enc, err := collate.Key(keysList[0])
		if err != nil {
			return nil, couch.QueryParseError("keys", string(keysList[0]))
		}
		op := "<"
		if q.Descending {
			op = ">"
		}
		args := append([]any{viewName}, partArgs...)
		args = append(args, enc)
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE view_name = $1%s AND key_collate %s $%d",
			table, partCond, op, len(args)),
			args...,
		).Scan(&result.Offset); err != nil {
			return nil, err
		}
	}
	dir := "ASC"
	if q.Descending {
		dir = "DESC"
	}
	limit := int64(math.MaxInt64)
	if q.Limit != nil {
		limit = *q.Limit
	}
	skip := q.Skip
	for _, rawKey := range keysList {
		if limit <= 0 {
			break
		}
		enc, err := collate.Key(rawKey)
		if err != nil {
			return nil, couch.QueryParseError("keys", string(rawKey))
		}
		keyArgs := append([]any{viewName}, partArgs...)
		keyArgs = append(keyArgs, enc)
		encIdx := len(keyArgs)
		// startkey_docid/endkey_docid bound the docid scan within every key.
		docidCond := ""
		if q.StartKeyDocID != "" {
			op := ">="
			if q.Descending {
				op = "<="
			}
			keyArgs = append(keyArgs, q.StartKeyDocID)
			docidCond += fmt.Sprintf(" AND doc_id %s $%d", op, len(keyArgs))
		}
		if q.EndKeyDocID != "" {
			op := "<="
			if q.Descending {
				op = ">="
			}
			keyArgs = append(keyArgs, q.EndKeyDocID)
			docidCond += fmt.Sprintf(" AND doc_id %s $%d", op, len(keyArgs))
		}
		rows, err := s.pool.Query(ctx, fmt.Sprintf(
			`SELECT key, value, doc_id FROM %s
			 WHERE view_name = $1%s AND key_collate = $%d%s
			 ORDER BY doc_id %s`, table, partCond, encIdx, docidCond, dir),
			keyArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var row ViewRow
			var key, value []byte
			if err := rows.Scan(&key, &value, &row.ID); err != nil {
				rows.Close()
				return nil, err
			}
			if skip > 0 {
				skip--
				continue
			}
			if limit <= 0 {
				break
			}
			limit--
			row.Key = json.RawMessage(key)
			row.Value = json.RawMessage(value)
			row.DocID = row.ID
			result.Rows = append(result.Rows, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	if q.IncludeDocs {
		if err := s.attachViewDocs(ctx, db, result.Rows, q.Conflicts); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// attachViewDocs resolves include_docs, including linked documents. A value
// carrying an _id member redirects the document fetch, and a _rev member pins
// the fetched revision. conflicts adds live non-winner leaves.
func (s *Store) attachViewDocs(ctx context.Context, db *DB, rows []ViewRow, conflicts bool) error {
	for i := range rows {
		docID := rows[i].DocID
		var pinnedRev *couch.Rev
		var value map[string]any
		if json.Unmarshal(rows[i].Value, &value) == nil {
			if linked, ok := value["_id"].(string); ok && linked != "" {
				docID = linked
				rows[i].DocID = linked
			}
			if revStr, ok := value["_rev"].(string); ok && revStr != "" {
				if rev, err := couch.ParseRev(revStr); err == nil {
					pinnedRev = &rev
				}
			}
		}
		var doc *DocRow
		var err error
		if pinnedRev != nil {
			doc, err = s.GetDoc(ctx, db, docID, pinnedRev)
		} else {
			doc, err = s.GetDocAny(ctx, db, docID)
		}
		if err != nil {
			if couchErr, ok := err.(*couch.Error); ok && couchErr.Status == 404 {
				rows[i].DocError = "missing"
				continue
			}
			return err
		}
		if doc.Deleted {
			rows[i].DocError = "deleted"
			continue
		}
		rows[i].Doc = doc
		if conflicts {
			leaves, err := s.Leaves(ctx, db, docID)
			if err != nil {
				return err
			}
			for _, leaf := range leaves {
				if leaf.Rev != doc.Rev && !leaf.Deleted {
					rows[i].DocConflicts = append(rows[i].DocConflicts, leaf.Rev.String())
				}
			}
		}
	}
	return nil
}

// queryViewReduce serves reduce=true. It scans the range in key order, splits
// into groups, and fold each group through the reduce function.
func (s *Store) queryViewReduce(ctx context.Context, db *DB, vg *ViewGroup, viewName, reduceFn string, q *ViewQuery, reducer ViewReducer) ([]ViewRow, error) {
	table := db.Schema + "." + viewTable(vg.Sig)

	// Builtin names match by prefix. "_sum\n" and "_sumorama" are _sum.
	if strings.HasPrefix(reduceFn, "_") {
		if canonical, err := couch.ResolveBuiltinReduce(reduceFn); err == nil {
			reduceFn = canonical
		}
	}

	// group_level uses -1 for the whole key (group=true) and math.MaxInt for no grouping.
	level := -1
	if q.GroupLevel != nil {
		level = int(*q.GroupLevel)
	} else if !q.Group {
		level = -2 // single group over the whole range
	}

	// Builtin reduces aggregate inside Postgres when possible. The streaming
	// path below is the fallback and handles _sum's array-of-numbers
	// semantics plus the exact builtin_reduce_error messages).
	if strings.HasPrefix(reduceFn, "_") && q.Keys == nil {
		rows, ok, err := s.queryViewReduceSQL(ctx, table, viewName, reduceFn, q, level)
		if err != nil || ok {
			return rows, err
		}
	}

	scan := func(cond *viewCond) ([]reduceRowGroup, error) {
		args := append([]any{viewName}, cond.args...)
		args = append(args, q.Partition)
		rows, err := s.pool.Query(ctx, fmt.Sprintf(
			`SELECT key, value, doc_id FROM %s
			 WHERE view_name = $1 AND part = $%d %s
			 ORDER BY key_collate ASC, doc_id ASC`, table, len(args), cond.where),
			args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var groups []reduceRowGroup
		var currentIdent []byte
		for rows.Next() {
			var key, value []byte
			var docID string
			if err := rows.Scan(&key, &value, &docID); err != nil {
				return nil, err
			}
			groupKey, ident, err := groupIdentity(key, level)
			if err != nil {
				return nil, err
			}
			if len(groups) == 0 || !bytes.Equal(ident, currentIdent) {
				groups = append(groups, reduceRowGroup{key: groupKey})
				currentIdent = ident
			}
			g := &groups[len(groups)-1]
			docIDJSON, err := marshalViewJSON(docID)
			if err != nil {
				return nil, err
			}
			pair, err := marshalViewJSON([2]json.RawMessage{json.RawMessage(key), docIDJSON})
			if err != nil {
				return nil, err
			}
			g.keys = append(g.keys, pair)
			g.values = append(g.values, json.RawMessage(value))
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return groups, nil
	}

	var groups []reduceRowGroup
	if q.Keys != nil {
		// keys= with reduce requires group=true (CouchDB rule).
		if level == -2 {
			return nil, couch.NewError(400, "query_parse_error", "Multi-key fetches for reduce views must use `group=true`")
		}
		for _, rawKey := range q.Keys {
			enc, err := collate.Key(rawKey)
			if err != nil {
				return nil, couch.QueryParseError("keys", string(rawKey))
			}
			c := &viewCond{}
			if q.Partition != "" {
				c.add("part = @1", q.Partition)
			}
			c.add("key_collate = @1", enc)
			part, err := scan(c)
			if err != nil {
				return nil, err
			}
			groups = append(groups, part...)
		}
	} else {
		cond, err := rangeConds(q)
		if err != nil {
			return nil, err
		}
		if groups, err = scan(cond); err != nil {
			return nil, err
		}
	}

	values, err := s.reduceGroups(ctx, vg.Sig, reduceFn, groups, reducer)
	if err != nil {
		return nil, err
	}
	out := make([]ViewRow, len(groups))
	for i := range groups {
		key := groups[i].key
		if level == -2 {
			key = json.RawMessage(`null`)
		}
		out[i] = ViewRow{Key: key, Value: values[i]}
		// reduce_limit guard for JS reduces, like CouchDB. It catches output that
		// grew past 200 bytes and half the input didn't reduce.
		if q.ReduceLimit && !strings.HasPrefix(reduceFn, "_") {
			inSize := 0
			for j := range groups[i].values {
				inSize += len(groups[i].keys[j]) + len(groups[i].values[j])
			}
			if outSize := len(values[i]); outSize > 200 && outSize*2 > inSize {
				out[i] = ViewRow{
					Key:   key,
					Error: "reduce_overflow_error",
					ErrReason: fmt.Sprintf(
						"Reduce output must shrink more rapidly: input size: %d output size: %d context: ",
						inSize, outSize),
				}
			}
		}
	}
	return finishReduceRows(out, q), nil
}

// finishReduceRows applies traversal order and paging to grouped reduce
// rows. Descending reverses the result, then skip and limit count groups.
func finishReduceRows(out []ViewRow, q *ViewQuery) []ViewRow {
	if q.Descending {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	if q.Skip > 0 {
		if q.Skip >= int64(len(out)) {
			out = nil
		} else {
			out = out[q.Skip:]
		}
	}
	if q.Limit != nil && int64(len(out)) > *q.Limit {
		out = out[:*q.Limit]
	}
	return out
}

// queryViewReduceSQL evaluates builtin reduces inside Postgres instead of
// streaming every view row through Go. Ungrouped queries collapse to a
// single aggregate row. Grouped queries produce one SQL row per distinct key,
// merged down to group_level here (collate keys can't be truncated in
// SQL). _sum/_stats push down only while every value in range is a plain
// JSON number. Anything else returns ok=false and the streaming path
// takes over with its array semantics and exact error messages.
func (s *Store) queryViewReduceSQL(ctx context.Context, table, viewName, fn string, q *ViewQuery, level int) ([]ViewRow, bool, error) {
	switch fn {
	case "_count", "_sum", "_stats", "_approx_count_distinct":
	default:
		return nil, false, nil
	}
	cond, err := rangeConds(q)
	if err != nil {
		return nil, false, err
	}
	args := append([]any{viewName}, cond.args...)
	args = append(args, q.Partition)
	partCond := fmt.Sprintf("AND part = $%d", len(args))

	// A JSON value is a number if its text starts with a digit or minus.
	// The value column holds verbatim JSON text, so this is exact and
	// avoids a ::jsonb cast, which would choke on NUL escapes in strings).
	const isNum = `left(value::text, 1) ~ '[0-9-]'`
	needVals := fn == "_sum" || fn == "_stats"
	valAggs := `, true, 0::float8, 0::float8, 0::float8, 0::float8`
	if needVals {
		valAggs = fmt.Sprintf(`,
			coalesce(bool_and(%[1]s), true),
			coalesce(sum(CASE WHEN %[1]s THEN (value::text)::float8 END), 0),
			coalesce(min(CASE WHEN %[1]s THEN (value::text)::float8 END), 0),
			coalesce(max(CASE WHEN %[1]s THEN (value::text)::float8 END), 0),
			coalesce(sum(CASE WHEN %[1]s THEN (value::text)::float8 * (value::text)::float8 END), 0)`, isNum)
	}

	rowValue := func(g *sqlReduceGroup) (json.RawMessage, error) {
		switch fn {
		case "_count":
			return marshalViewJSON(g.count)
		case "_approx_count_distinct":
			return marshalViewJSON(g.distinct)
		case "_sum":
			return jsonNumber(g.sum)
		default: // _stats
			return statsJSON(g.sum, g.count, g.min, g.max, g.sumsqr)
		}
	}

	if level == -2 {
		// A single group covers the whole range and produces one aggregate row.
		countExpr := "count(*)"
		if fn == "_approx_count_distinct" {
			countExpr = "count(DISTINCT key_collate)"
		}
		var g sqlReduceGroup
		var allNum bool
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT %s%s FROM %s WHERE view_name = $1 %s %s",
			countExpr, valAggs, table, partCond, cond.where),
			args...,
		).Scan(&g.count, &allNum, &g.sum, &g.min, &g.max, &g.sumsqr); err != nil {
			return nil, false, err
		}
		if needVals && !allNum {
			return nil, false, nil
		}
		if g.count == 0 {
			return nil, true, nil
		}
		g.distinct = g.count
		value, err := rowValue(&g)
		if err != nil {
			return nil, false, err
		}
		return []ViewRow{{Key: json.RawMessage(`null`), Value: value}}, true, nil
	}

	// Grouped queries aggregate each distinct key in SQL, then merge to group_level here.
	// For group=true the SQL groups are the output groups, so paging pushes
	// down too and a limited query never materializes the rest.
	paged := level == -1
	dir, paging := "ASC", ""
	if paged {
		if q.Descending {
			dir = "DESC"
		}
		if q.Skip > 0 {
			paging += fmt.Sprintf(" OFFSET %d", q.Skip)
		}
		if q.Limit != nil {
			paging += fmt.Sprintf(" LIMIT %d", *q.Limit)
		}
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT (array_agg(key ORDER BY doc_id))[1]::text, count(*)%s
		 FROM %s WHERE view_name = $1 %s %s
		 GROUP BY key_collate ORDER BY key_collate %s%s`,
		valAggs, table, partCond, cond.where, dir, paging),
		args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var groups []sqlReduceGroup
	var currentIdent []byte
	for rows.Next() {
		var key []byte
		var sg sqlReduceGroup
		var allNum bool
		if err := rows.Scan(&key, &sg.count, &allNum, &sg.sum, &sg.min, &sg.max, &sg.sumsqr); err != nil {
			return nil, false, err
		}
		if needVals && !allNum {
			return nil, false, nil
		}
		groupKey, ident, err := groupIdentity(key, level)
		if err != nil {
			return nil, false, err
		}
		if len(groups) == 0 || !bytes.Equal(ident, currentIdent) {
			groups = append(groups, sqlReduceGroup{key: groupKey, min: sg.min})
			currentIdent = ident
		}
		g := &groups[len(groups)-1]
		g.count += sg.count
		g.distinct++
		g.sum += sg.sum
		g.min = math.Min(g.min, sg.min)
		g.max = math.Max(g.max, sg.max)
		g.sumsqr += sg.sumsqr
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	out := make([]ViewRow, len(groups))
	for i := range groups {
		value, err := rowValue(&groups[i])
		if err != nil {
			return nil, false, err
		}
		out[i] = ViewRow{Key: groups[i].key, Value: value}
	}
	if paged {
		return out, true, nil // Ordered and paged by SQL already.
	}
	return finishReduceRows(out, q), true, nil
}

type sqlReduceGroup struct {
	key                   json.RawMessage
	count, distinct       int64
	sum, min, max, sumsqr float64
}

func statsJSON(sum float64, count int64, minV, maxV, sumsqr float64) (json.RawMessage, error) {
	sumJSON, err := jsonNumber(sum)
	if err != nil {
		return nil, err
	}
	minJSON, err := jsonNumber(minV)
	if err != nil {
		return nil, err
	}
	maxJSON, err := jsonNumber(maxV)
	if err != nil {
		return nil, err
	}
	sumsqrJSON, err := jsonNumber(sumsqr)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(fmt.Sprintf(`{"sum":%s,"count":%d,"min":%s,"max":%s,"sumsqr":%s}`,
		sumJSON, count, minJSON, maxJSON, sumsqrJSON)), nil
}

type reduceRowGroup struct {
	key    json.RawMessage
	keys   []json.RawMessage // [key, docid] pairs
	values []json.RawMessage
}

// groupIdentity computes the group key (raw JSON) and its collation
// identity for a row. level -2 lumps everything into one group.
func groupIdentity(rawKey []byte, level int) (json.RawMessage, []byte, error) {
	if level == -2 {
		return json.RawMessage(`null`), []byte{0}, nil
	}
	truncated, err := collate.TruncateKey(rawKey, level)
	if err != nil {
		return nil, nil, err
	}
	ident, err := collate.Key(truncated)
	if err != nil {
		return nil, nil, err
	}
	return truncated, ident, nil
}

// reduceGroups folds groups through a builtin or JS reduce function.
func (s *Store) reduceGroups(ctx context.Context, sig, fn string, groups []reduceRowGroup, reducer ViewReducer) ([]json.RawMessage, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	if strings.HasPrefix(fn, "_") {
		return builtinReduce(fn, groups)
	}
	if reducer == nil {
		return nil, couch.NewError(500, "unknown_error", "no JS engine available")
	}
	// One JS call for all first-pass chunks, then one rereduce call for the
	// groups that needed more than one chunk.
	const chunkSize = 1000
	type chunkRef struct{ group, index int }
	var chunks []ViewReduceGroup
	var refs []chunkRef
	for gi := range groups {
		g := &groups[gi]
		for start := 0; start < len(g.values); start += chunkSize {
			end := min(start+chunkSize, len(g.values))
			chunks = append(chunks, ViewReduceGroup{
				Keys:   g.keys[start:end],
				Values: g.values[start:end],
			})
			refs = append(refs, chunkRef{group: gi, index: start / chunkSize})
		}
	}
	partials, err := reducer.ReduceGroups(ctx, sig, fn, chunks, false)
	if err != nil {
		return nil, couch.NewError(500, "reduce_error", err.Error())
	}
	byGroup := make([][]json.RawMessage, len(groups))
	for i, ref := range refs {
		byGroup[ref.group] = append(byGroup[ref.group], partials[i])
	}
	out := make([]json.RawMessage, len(groups))
	var rereduce []ViewReduceGroup
	var rereduceIdx []int
	for gi, parts := range byGroup {
		if len(parts) == 1 {
			out[gi] = parts[0]
		} else {
			rereduce = append(rereduce, ViewReduceGroup{Values: parts})
			rereduceIdx = append(rereduceIdx, gi)
		}
	}
	if len(rereduce) > 0 {
		finals, err := reducer.ReduceGroups(ctx, sig, fn, rereduce, true)
		if err != nil {
			return nil, couch.NewError(500, "reduce_error", err.Error())
		}
		for i, gi := range rereduceIdx {
			out[gi] = finals[i]
		}
	}
	return out, nil
}

// builtinReduce evaluates _sum/_count/_stats/_approx_count_distinct without
// touching JS.
func builtinReduce(fn string, groups []reduceRowGroup) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, len(groups))
	for gi := range groups {
		g := &groups[gi]
		switch fn {
		case "_count":
			count, err := marshalViewJSON(len(g.values))
			if err != nil {
				return nil, err
			}
			out[gi] = count
		case "_sum":
			sum, err := sumValues(g.values)
			if err != nil {
				return nil, err
			}
			out[gi] = sum
		case "_stats":
			stats, err := statsValues(g.values)
			if err != nil {
				return nil, err
			}
			out[gi] = stats
		case "_approx_count_distinct":
			distinct := 0
			var prev []byte
			for _, pair := range g.keys {
				var kv [2]json.RawMessage
				if err := json.Unmarshal(pair, &kv); err != nil {
					return nil, err
				}
				ident, err := collate.Key(kv[0])
				if err != nil {
					return nil, err
				}
				if prev == nil || !bytes.Equal(ident, prev) {
					distinct++
					prev = ident
				}
			}
			distinctJSON, err := marshalViewJSON(distinct)
			if err != nil {
				return nil, err
			}
			out[gi] = distinctJSON
		case "_first":
			out[gi] = g.values[0]
		case "_last":
			out[gi] = g.values[len(g.values)-1]
		default:
			if n, ok := strings.CutPrefix(fn, "_top_"); ok {
				top, err := extremeValues(g.values, n, true)
				if err != nil {
					return nil, err
				}
				out[gi] = top
			} else if n, ok := strings.CutPrefix(fn, "_bottom_"); ok {
				bottom, err := extremeValues(g.values, n, false)
				if err != nil {
					return nil, err
				}
				out[gi] = bottom
			} else {
				return nil, couch.QueryParseError("reduce", "Invalid built-in reduce function: "+fn)
			}
		}
	}
	return out, nil
}

// extremeValues implements _top_N/_bottom_N. It returns the N greatest (or smallest)
// distinct values in collation order. _top returns greatest-first values.
func extremeValues(values []json.RawMessage, nStr string, top bool) (json.RawMessage, error) {
	n, err := strconv.Atoi(nStr)
	if err != nil || n < 0 {
		return nil, couch.QueryParseError("reduce", "Invalid built-in reduce function")
	}
	type entry struct {
		ident []byte
		raw   json.RawMessage
	}
	var distinct []entry
	for _, raw := range values {
		ident, err := collate.Key(raw)
		if err != nil {
			return nil, err
		}
		found := false
		for _, e := range distinct {
			if bytes.Equal(e.ident, ident) {
				found = true
				break
			}
		}
		if !found {
			distinct = append(distinct, entry{ident, raw})
		}
	}
	sort.Slice(distinct, func(i, j int) bool {
		less := bytes.Compare(distinct[i].ident, distinct[j].ident) < 0
		if top {
			return !less
		}
		return less
	})
	if len(distinct) > n {
		distinct = distinct[:n]
	}
	out := make([]json.RawMessage, len(distinct))
	for i, e := range distinct {
		out[i] = e.raw
	}
	return marshalViewJSON(out)
}

// sumValues implements _sum for numbers or arrays of numbers summed
// elementwise (ragged lengths keep the longer tail).
func sumValues(values []json.RawMessage) (json.RawMessage, error) {
	scalar := 0.0
	var vector []float64
	sawVector := false
	for _, raw := range values {
		v, err := couch.DecodeJSON(raw)
		if err != nil {
			return nil, err
		}
		switch t := v.(type) {
		case json.Number:
			f, _ := strconv.ParseFloat(t.String(), 64)
			scalar += f
		case []any:
			sawVector = true
			for i, elem := range t {
				num, ok := elem.(json.Number)
				if !ok {
					return nil, builtinReduceError("_sum", raw)
				}
				f, _ := strconv.ParseFloat(num.String(), 64)
				if i < len(vector) {
					vector[i] += f
				} else {
					vector = append(vector, f)
				}
			}
		default:
			return nil, builtinReduceError("_sum", raw)
		}
	}
	if sawVector {
		if scalar != 0 {
			// Mixed scalars and arrays. CouchDB adds the scalar to the first
			// element.
			if len(vector) == 0 {
				vector = []float64{0}
			}
			vector[0] += scalar
		}
		return marshalViewJSON(vector)
	}
	return jsonNumber(scalar)
}

func statsValues(values []json.RawMessage) (json.RawMessage, error) {
	if len(values) == 0 {
		return json.RawMessage(`null`), nil
	}
	var sum, minV, maxV, sumsqr float64
	first := true
	for _, raw := range values {
		v, err := couch.DecodeJSON(raw)
		if err != nil {
			return nil, err
		}
		num, ok := v.(json.Number)
		if !ok {
			return nil, builtinReduceError("_stats", raw)
		}
		f, _ := strconv.ParseFloat(num.String(), 64)
		sum += f
		sumsqr += f * f
		if first || f < minV {
			minV = f
		}
		if first || f > maxV {
			maxV = f
		}
		first = false
	}
	return statsJSON(sum, int64(len(values)), minV, maxV, sumsqr)
}

func builtinReduceError(fn string, value json.RawMessage) error {
	return couch.NewError(500, "builtin_reduce_error",
		fmt.Sprintf("Builtin %s function requires map values to be numbers or arrays of numbers, got %s", fn, value))
}

// jsonNumber renders a float the way JSON does, preferring integer form.
func jsonNumber(f float64) (json.RawMessage, error) {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return json.RawMessage(strconv.FormatInt(int64(f), 10)), nil
	}
	return marshalViewJSON(f)
}

func marshalViewJSON(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal view JSON: %w", err)
	}
	return raw, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
