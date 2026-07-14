package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// Local documents (_local/{id}) are unversioned per-node documents used by
// replicators for checkpoints. Revisions use CouchDB's cosmetic "0-N" counters.
// Writes always win. IDs include the "_local/" prefix.

type LocalDoc struct {
	ID     string // full id including _local/
	RevNum int
	Body   map[string]any
}

func (d *LocalDoc) Rev() string {
	return "0-" + strconv.Itoa(d.RevNum)
}

func (s *Store) GetLocalDoc(ctx context.Context, db *DB, id string) (*LocalDoc, error) {
	doc := &LocalDoc{ID: id}
	var raw []byte
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT rev_num, body FROM %s.local_docs WHERE id = $1", db.Schema), id,
	).Scan(&doc.RevNum, &raw)
	if err == pgx.ErrNoRows {
		return nil, couch.DocMissing()
	}
	if err != nil {
		return nil, err
	}
	if doc.Body, err = decodeBody(raw); err != nil {
		return nil, err
	}
	return doc, nil
}

// PutLocalDoc upserts. The revision counter increments and writes always win.
func (s *Store) PutLocalDoc(ctx context.Context, db *DB, id string, body map[string]any) (*LocalDoc, error) {
	doc := &LocalDoc{ID: id, Body: body}
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`INSERT INTO %s.local_docs (id, rev_num, body) VALUES ($1, 1, $2)
		 ON CONFLICT (id) DO UPDATE
		   SET rev_num = %s.local_docs.rev_num + 1, body = $2
		 RETURNING rev_num`, db.Schema, db.Schema),
		id, couch.CanonicalBody(body),
	).Scan(&doc.RevNum)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (s *Store) DeleteLocalDoc(ctx context.Context, db *DB, id string) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s.local_docs WHERE id = $1", db.Schema), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return couch.DocMissing()
	}
	return nil
}

// ListLocalDocs returns all local docs in id byte order (_local_docs).
func (s *Store) ListLocalDocs(ctx context.Context, db *DB, includeBodies bool) ([]LocalDoc, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT id, rev_num, body FROM %s.local_docs ORDER BY id`, db.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var docs []LocalDoc
	for rows.Next() {
		var doc LocalDoc
		var raw []byte
		if err := rows.Scan(&doc.ID, &doc.RevNum, &raw); err != nil {
			return nil, err
		}
		if includeBodies {
			if doc.Body, err = decodeBody(raw); err != nil {
				return nil, err
			}
		}
		docs = append(docs, doc)
	}
	return docs, rows.Err()
}
