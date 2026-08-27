package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
)

type AllDocsParams struct {
	// StartKey/EndKey are interpreted in scan direction.
	StartKey     *string
	EndKey       *string
	InclusiveEnd bool
	Descending   bool
	Limit        *int64
	Skip         int64
	IncludeDocs  bool
	Conflicts    bool // With IncludeDocs, report live non-winner leaves.
	UpdateSeq    bool
	// OmitOffset lets partition-scoped callers avoid a database-wide preceding
	// count when they will calculate the partition-relative offset themselves.
	OmitOffset bool
}

type AllDocsRow struct {
	ID      string
	Rev     couch.Rev
	Deleted bool
	Body    map[string]any // nil unless IncludeDocs and not deleted
	// ConflictRevs holds live non-winner leaves (conflicts=true with
	// include_docs. It feeds the embedded document's _conflicts member.
	ConflictRevs []string
	Missing      bool
}

type AllDocsPage struct {
	TotalRows int64
	Offset    *int64 // nil in keys mode
	Rows      []AllDocsRow
	UpdateSeq *int64
}

// AllDocs range-scans non-deleted docs in doc-id byte order.
func (s *Store) AllDocs(ctx context.Context, db *DB, p *AllDocsParams) (*AllDocsPage, error) {
	conditions := []string{"NOT d.deleted"}
	var args []any
	if p.StartKey != nil {
		args = append(args, *p.StartKey)
		op := ">="
		if p.Descending {
			op = "<="
		}
		conditions = append(conditions, fmt.Sprintf("d.id %s $%d", op, len(args)))
	}
	if p.EndKey != nil {
		args = append(args, *p.EndKey)
		var op string
		switch {
		case !p.Descending && p.InclusiveEnd:
			op = "<="
		case !p.Descending:
			op = "<"
		case p.InclusiveEnd:
			op = ">="
		default:
			op = ">"
		}
		conditions = append(conditions, fmt.Sprintf("d.id %s $%d", op, len(args)))
	}
	order := "ASC"
	if p.Descending {
		order = "DESC"
	}
	limit := ""
	if p.Limit != nil {
		limit = fmt.Sprintf("LIMIT %d", *p.Limit)
	}
	// Bodies live in revs. Only include_docs pays for the join.
	bodySel, join := "null::jsonb", ""
	if p.IncludeDocs {
		bodySel, join = winnerBody, " "+winnerJoin(db.Schema)
	}
	// CouchDB's numeric skip cannot be turned into a pure cursor: PostgreSQL
	// must still find the Nth matching id. Keep that work on the narrow primary
	// key index, then seek from the boundary so skipped rows never pay for the
	// winner-body join or result decoding.
	prefix := ""
	offsetSQL := ""
	if p.Skip > 0 {
		prefix = fmt.Sprintf(
			`WITH skip_boundary AS MATERIALIZED (
			   SELECT d.id FROM %s.docs d WHERE %s
			   ORDER BY d.id %s OFFSET %d LIMIT 1
			 ) `,
			db.Schema, strings.Join(conditions, " AND "), order, p.Skip-1)
		op := ">"
		if p.Descending {
			op = "<"
		}
		conditions = append(conditions,
			fmt.Sprintf("d.id %s (SELECT id FROM skip_boundary)", op))
	} else if p.Skip < 0 {
		// Parameter validation normally rejects this. Preserve PostgreSQL's
		// existing error if an internal caller bypasses that boundary.
		offsetSQL = fmt.Sprintf("OFFSET %d", p.Skip)
	}
	query := fmt.Sprintf(
		"%sSELECT d.id, d.rev_num, d.rev_hash, %s FROM %s.docs d%s WHERE %s ORDER BY d.id %s %s %s",
		prefix, bodySel, db.Schema, join, strings.Join(conditions, " AND "), order, offsetSQL, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	page := &AllDocsPage{}
	for rows.Next() {
		var r AllDocsRow
		var raw []byte
		if err := rows.Scan(&r.ID, &r.Rev.Num, &r.Rev.Hash, &raw); err != nil {
			return nil, err
		}
		if p.IncludeDocs {
			if r.Body, err = decodeBody(raw); err != nil {
				return nil, err
			}
		}
		page.Rows = append(page.Rows, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if p.IncludeDocs && p.Conflicts {
		if err := s.attachDocConflicts(ctx, db, page.Rows); err != nil {
			return nil, err
		}
	}

	if !p.OmitOffset {
		// offset is documents strictly before the iteration start, plus skip.
		// This range count is required for CouchDB's exact response field.
		preceding := int64(0)
		if p.StartKey != nil {
			op := "<"
			if p.Descending {
				op = ">"
			}
			if err := s.pool.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s.docs WHERE NOT deleted AND id %s $1", db.Schema, op),
				*p.StartKey,
			).Scan(&preceding); err != nil {
				return nil, err
			}
		}
		offset := preceding + p.Skip
		page.Offset = &offset
	}

	if err := s.pool.QueryRow(ctx,
		"SELECT doc_count FROM couchgres.databases WHERE name = $1", db.Name,
	).Scan(&page.TotalRows); err != nil {
		return nil, err
	}
	if p.UpdateSeq {
		seq, err := s.CurrentSeq(ctx, db)
		if err != nil {
			return nil, err
		}
		page.UpdateSeq = &seq
	}
	return page, nil
}

// attachDocConflicts fills ConflictRevs (live non-winner leaves) for rows
// whose winner is live.
func (s *Store) attachDocConflicts(ctx context.Context, db *DB, rows []AllDocsRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]string, 0, len(rows))
	index := make(map[string]int, len(rows))
	for i, r := range rows {
		if r.Missing || r.Deleted {
			continue
		}
		ids = append(ids, r.ID)
		index[r.ID] = i
	}
	if len(ids) == 0 {
		return nil
	}
	leafRows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT id, rev_num, rev_hash FROM %s.revs
		 WHERE id = ANY($1) AND leaf AND NOT deleted
		 ORDER BY id, rev_num DESC, rev_hash DESC`, db.Schema), ids)
	if err != nil {
		return err
	}
	defer leafRows.Close()
	for leafRows.Next() {
		var id string
		var rev couch.Rev
		if err := leafRows.Scan(&id, &rev.Num, &rev.Hash); err != nil {
			return err
		}
		i := index[id]
		if rev != rows[i].Rev {
			rows[i].ConflictRevs = append(rows[i].ConflictRevs, rev.String())
		}
	}
	return leafRows.Err()
}

// AllDocsKeys serves keys mode. It returns one row per requested key in request order,
// including deleted docs and missing keys.
func (s *Store) AllDocsKeys(
	ctx context.Context,
	db *DB,
	keys []string,
	includeDocs, conflicts, updateSeq bool,
) (*AllDocsPage, error) {
	bodySel, join := "null::jsonb", ""
	if includeDocs {
		bodySel, join = winnerBody, " "+winnerJoin(db.Schema)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT d.id, d.rev_num, d.rev_hash, d.deleted, %s
		 FROM %s.docs d %s WHERE d.id = ANY($1)`,
		bodySel, db.Schema, join), keys)
	if err != nil {
		return nil, err
	}
	type hit struct {
		rev     couch.Rev
		deleted bool
		raw     []byte
	}
	byID := make(map[string]hit)
	for rows.Next() {
		var id string
		var h hit
		if err := rows.Scan(&id, &h.rev.Num, &h.rev.Hash, &h.deleted, &h.raw); err != nil {
			return nil, err
		}
		byID[id] = h
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	page := &AllDocsPage{}
	for _, key := range keys {
		h, ok := byID[key]
		if !ok {
			page.Rows = append(page.Rows, AllDocsRow{ID: key, Missing: true})
			continue
		}
		r := AllDocsRow{ID: key, Rev: h.rev, Deleted: h.deleted}
		if includeDocs && !h.deleted {
			if r.Body, err = decodeBody(h.raw); err != nil {
				return nil, err
			}
		}
		page.Rows = append(page.Rows, r)
	}
	if includeDocs && conflicts {
		if err := s.attachDocConflicts(ctx, db, page.Rows); err != nil {
			return nil, err
		}
	}

	if err := s.pool.QueryRow(ctx,
		"SELECT doc_count FROM couchgres.databases WHERE name = $1", db.Name,
	).Scan(&page.TotalRows); err != nil {
		return nil, err
	}
	if updateSeq {
		seq, err := s.CurrentSeq(ctx, db)
		if err != nil {
			return nil, err
		}
		page.UpdateSeq = &seq
	}
	return page, nil
}

func (s *Store) CurrentSeq(ctx context.Context, db *DB) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM %s.update_seq", db.Schema),
	).Scan(&seq)
	return seq, err
}
