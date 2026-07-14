package store

import (
	"context"
	"fmt"
)

// MangoIndexRow is one index row surfaced to the _find executor. It contains
// the memcomparable key (bookmark material) and the current winning document.
type MangoIndexRow struct {
	KeyCollate []byte
	Doc        *DocRow
}

// ScanMangoIndex walks one index's rows in key order within [start, end)
// byte bounds (either may be nil), joining the winning document bodies.
// AfterKey/AfterID resume past a bookmark position. fn returns false to
// stop early.
//
// The executor usually stops after `limit` matches, but the extended
// protocol runs an unbounded query to completion whether or not the
// consumer keeps reading, and every row costs a docs lookup. The walk
// fetches in growing LIMIT chunks and resumes past the last row only when
// the consumer still wants more.
func (s *Store) ScanMangoIndex(
	ctx context.Context,
	db *DB,
	sig, viewName string,
	partition string,
	start, end []byte,
	afterKey []byte,
	afterID string,
	descending bool,
	fn func(row *MangoIndexRow) (bool, error),
) error {
	table := db.Schema + "." + viewTable(sig)
	dir, cmp := "ASC", ">"
	if descending {
		dir, cmp = "DESC", "<"
	}

	chunk := 64
	for {
		// part is always constrained ('' on global indexes). Without it, the
		// (view_name, part, key_collate) index can't bound on key_collate.
		conds := "v.view_name = $1 AND v.part = $2"
		args := []any{viewName, partition}
		add := func(cond string, val any) {
			args = append(args, val)
			conds += " AND " + fmt.Sprintf(cond, len(args))
		}
		// start/end are the logical [start, end) range regardless of
		// direction. Descending only reverses the scan order.
		if start != nil {
			add("v.key_collate >= $%d", start)
		}
		if end != nil {
			add("v.key_collate < $%d", end)
		}
		if afterKey != nil {
			conds += fmt.Sprintf(" AND (v.key_collate, v.doc_id) %s ($%d, $%d)",
				cmp, len(args)+1, len(args)+2)
			args = append(args, afterKey, afterID)
		}
		// LATERAL LIMIT 1 is a no-op because the reference is revs' primary
		// key. It keeps the body fetch a per-row index probe. When decorrelated,
		// the planner hash-joins all revs per chunk. This is the same trap as the view build
		// read in views.go.
		rows, err := s.pool.Query(ctx, fmt.Sprintf(
			`SELECT v.key_collate, d.id, d.rev_num, d.rev_hash, d.deleted,
			        coalesce(r.body, '{}'::jsonb)
			 FROM %[1]s v JOIN %[2]s.docs d ON d.id = v.doc_id
			 CROSS JOIN LATERAL (
			   SELECT body FROM %[2]s.revs r
			   WHERE r.id = d.id AND r.rev_num = d.rev_num
			     AND r.rev_hash = d.rev_hash LIMIT 1) r
			 WHERE %[3]s
			 ORDER BY v.key_collate %[4]s, v.doc_id %[4]s
			 LIMIT %[5]d`,
			table, db.Schema, conds, dir, chunk), args...)
		if err != nil {
			return err
		}
		n := 0
		stopped := false
		for rows.Next() {
			row := &MangoIndexRow{Doc: &DocRow{}}
			var raw []byte
			if err := rows.Scan(&row.KeyCollate, &row.Doc.ID, &row.Doc.Rev.Num,
				&row.Doc.Rev.Hash, &row.Doc.Deleted, &raw); err != nil {
				rows.Close()
				return err
			}
			if row.Doc.Body, err = decodeBody(raw); err != nil {
				rows.Close()
				return err
			}
			n++
			afterKey, afterID = row.KeyCollate, row.Doc.ID
			more, err := fn(row)
			if err != nil {
				rows.Close()
				return err
			}
			if !more {
				stopped = true
				break
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if stopped || n < chunk {
			return nil
		}
		if chunk < 1024 {
			chunk *= 2
		}
	}
}

// ScanDocs walks live non-design documents in id order starting after
// afterID (the _all_docs fallback scan for _find). lo/hi bound the id range
// when non-empty (partition scoping).
func (s *Store) ScanDocs(
	ctx context.Context,
	db *DB,
	afterID string,
	lo, hi string,
	fn func(doc *DocRow) (bool, error),
) error {
	conds := `NOT d.deleted AND d.id > $1 AND d.id NOT LIKE '\_design/%'`
	args := []any{afterID}
	if lo != "" {
		args = append(args, lo)
		conds += fmt.Sprintf(" AND d.id >= $%d", len(args))
	}
	if hi != "" {
		args = append(args, hi)
		conds += fmt.Sprintf(" AND d.id < $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT d.id, d.rev_num, d.rev_hash, %s FROM %s.docs d %s
		 WHERE %s ORDER BY d.id`,
		winnerBody, db.Schema, winnerJoin(db.Schema), conds), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		doc := &DocRow{}
		var raw []byte
		if err := rows.Scan(&doc.ID, &doc.Rev.Num, &doc.Rev.Hash, &raw); err != nil {
			return err
		}
		if doc.Body, err = decodeBody(raw); err != nil {
			return err
		}
		more, err := fn(doc)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
	return rows.Err()
}
