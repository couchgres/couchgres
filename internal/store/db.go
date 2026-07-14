package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// DB is a resolved database. It contains the registry row needed to address its schema.
type DB struct {
	Name              string
	Schema            string
	Partitioned       bool
	RevsLimit         int
	InstanceStartTime string
}

// validateDBUse guards read/delete paths. Unlike creation, an unknown
// reserved name (underscore prefix) is a missing database, not an illegal
// one. CouchDB routes /_bogus to no handler and returns 404.
func validateDBUse(name string) error {
	if err := couch.ValidateDBName(name); err != nil {
		if name != "" && name[0] == '_' {
			return couch.DBNotFound()
		}
		return err
	}
	return nil
}

// Info holds the numbers behind GET /{db}.
type Info struct {
	DocCount    int64
	DocDelCount int64
	UpdateSeq   int64
	SizeBytes   int64
}

func (s *Store) CreateDatabase(ctx context.Context, name string, partitioned bool) error {
	if err := couch.ValidateDBName(name); err != nil {
		return err
	}
	schema := "couchgres_" + randomHex(16)
	startTime := strconv.FormatInt(time.Now().Unix(), 10)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`INSERT INTO couchgres.databases
		   (name, schema_name, partitioned, instance_start_time)
		 VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO NOTHING`,
		name, schema, partitioned, startTime,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return couch.DBExists()
	}
	if _, err := tx.Exec(ctx, dbDDL(schema)); err != nil {
		return fmt.Errorf("creating schema for %s: %w", name, err)
	}
	// CouchDB 3.x sets default_security = admin_only. Fresh databases are
	// admin-only until _security is opened up explicitly.
	security := `{"members":{"roles":["_admin"]},"admins":{"roles":["_admin"]}}`
	if v, ok, err := s.ConfigGet(ctx, "couchdb", "default_security"); err != nil {
		return err
	} else if ok && v == "everyone" {
		security = "{}"
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.security (obj) VALUES ($1::jsonb)", schema), security,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO couchgres.db_events (db_name, type) VALUES ($1, 'created')", name,
	); err != nil {
		return err
	}
	if err := bumpCacheVersionTx(ctx, tx, CacheDBRegistry); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.notifyChanged(name)
	s.notifyInvalidated(CacheDBRegistry)
	return nil
}

func (s *Store) DeleteDatabase(ctx context.Context, name string) error {
	if err := validateDBUse(name); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var schema string
	err = tx.QueryRow(ctx,
		"DELETE FROM couchgres.databases WHERE name = $1 RETURNING schema_name", name,
	).Scan(&schema)
	if err == pgx.ErrNoRows {
		return couch.DBNotFound()
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		return err
	}
	s.dbCache.Delete(name)
	s.clearViewCaches(&DB{Schema: schema})
	if _, err := tx.Exec(ctx,
		"INSERT INTO couchgres.db_events (db_name, type) VALUES ($1, 'deleted')", name,
	); err != nil {
		return err
	}
	if err := bumpCacheVersionTx(ctx, tx, CacheDBRegistry); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.notifyChanged(name)
	s.notifyInvalidated(CacheDBRegistry)
	return nil
}

// GetDB resolves a database. Rows are cached. Every request pays this lookup,
// and a database's row changes only through CreateDatabase,
// DeleteDatabase, and SetRevsLimit, which invalidate. Callers must not
// mutate the returned DB.
func (s *Store) GetDB(ctx context.Context, name string) (*DB, error) {
	if err := validateDBUse(name); err != nil {
		return nil, err
	}
	if cached, ok := s.dbCache.Load(name); ok {
		return cached.(*DB), nil
	}
	db := &DB{}
	err := s.pool.QueryRow(ctx,
		`SELECT name, schema_name, partitioned, revs_limit, instance_start_time
		 FROM couchgres.databases WHERE name = $1`, name,
	).Scan(&db.Name, &db.Schema, &db.Partitioned, &db.RevsLimit,
		&db.InstanceStartTime)
	if err == pgx.ErrNoRows {
		return nil, couch.DBNotFound()
	}
	if err != nil {
		return nil, err
	}
	s.dbCache.Store(name, db)
	return db, nil
}

// ListDatabases returns names in byte order (the _all_dbs order).
func (s *Store) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name FROM couchgres.databases ORDER BY name COLLATE "C"`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) DBInfo(ctx context.Context, db *DB) (*Info, error) {
	info := &Info{}
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT
		   (SELECT count(*) FROM %[1]s.docs WHERE NOT deleted),
		   (SELECT count(*) FROM %[1]s.docs WHERE deleted),
		   (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM %[1]s.update_seq),
		   pg_total_relation_size('%[1]s.docs')
		   + pg_total_relation_size('%[1]s.revs')
		   + pg_total_relation_size('%[1]s.attachments')`, db.Schema),
	).Scan(&info.DocCount, &info.DocDelCount, &info.UpdateSeq, &info.SizeBytes)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// Compact implements CouchDB-observable compaction. Revision histories are
// stemmed to limit. Superseded revision bodies retained by
// the keep_superseded_bodies option are dropped, and attachment rows no
// leaf revision references are reclaimed. File-size reclamation stays
// PostgreSQL's business (VACUUM/autovacuum).
func (s *Store) Compact(ctx context.Context, db *DB, limit int) error {
	if err := s.StemRevs(ctx, db, limit); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.revs SET body = NULL WHERE NOT leaf AND body IS NOT NULL",
		db.Schema)); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %[1]s.attachments a
		 WHERE NOT EXISTS (
		   SELECT 1 FROM %[1]s.revs r
		   WHERE r.id = a.doc_id AND r.rev_num = a.rev_num
		     AND r.rev_hash = a.rev_hash AND r.leaf)`, db.Schema))
	return err
}

// StemRevs prunes revision history deeper than limit from every leaf. This is
// what CouchDB compaction does after _revs_limit is lowered. Leaf rows
// (the live bodies) are never touched.
func (s *Store) StemRevs(ctx context.Context, db *DB, limit int) error {
	if limit < 1 {
		limit = 1
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`WITH RECURSIVE keep AS (
		   SELECT id, rev_num, rev_hash, parent_num, parent_hash, 1 AS depth
		   FROM %[1]s.revs WHERE leaf
		   UNION ALL
		   SELECT r.id, r.rev_num, r.rev_hash, r.parent_num, r.parent_hash, k.depth + 1
		   FROM %[1]s.revs r
		   JOIN keep k ON r.id = k.id AND r.rev_num = k.parent_num AND r.rev_hash = k.parent_hash
		   WHERE k.depth < $1
		 )
		 DELETE FROM %[1]s.revs t
		 WHERE NOT EXISTS (
		   SELECT 1 FROM keep
		   WHERE keep.id = t.id AND keep.rev_num = t.rev_num AND keep.rev_hash = t.rev_hash
		 )`, db.Schema), limit)
	return err
}

func (s *Store) SetRevsLimit(ctx context.Context, name string, limit int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		"UPDATE couchgres.databases SET revs_limit = $2 WHERE name = $1", name, limit)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return couch.DBNotFound()
	}
	if err := bumpCacheVersionTx(ctx, tx, CacheDBRegistry); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.dbCache.Delete(name)
	s.notifyInvalidated(CacheDBRegistry)
	return nil
}
