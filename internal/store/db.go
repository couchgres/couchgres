package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	DocCount     int64
	DocDelCount  int64
	UpdateSeq    int64
	PurgeSeq     int64
	SizeBytes    int64
	ExternalSize int64
}

// DatabaseInfo combines a registry row with the mutable and calculated values
// used by GET /{db} and /_dbs_info.
type DatabaseInfo struct {
	DB   DB
	Info Info
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
		   (name, schema_name, partitioned, instance_start_time,
		    doc_counts_initialized, update_seq_initialized)
		 VALUES ($1, $2, $3, $4, true, true) ON CONFLICT (name) DO NOTHING`,
		name, schema, partitioned, startTime,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return couch.DBExists()
	}
	if _, err := tx.Exec(ctx, dbDDL(schema, partitioned)); err != nil {
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

// ListDatabasesParams contains the ordering and pagination options for
// CouchDB's _all_dbs endpoint. Bounds are inclusive.
type ListDatabasesParams struct {
	Descending bool
	StartKey   *string
	EndKey     *string
	Skip       *int64
	Limit      *int64
}

// ListDatabases returns names in CouchDB byte order. PostgreSQL applies all
// bounds and pagination so callers never need to materialize the registry.
func (s *Store) ListDatabases(ctx context.Context, p *ListDatabasesParams) ([]string, error) {
	if p == nil {
		p = &ListDatabasesParams{}
	}
	query := `SELECT name FROM couchgres.databases`
	conditions := make([]string, 0, 2)
	args := make([]any, 0, 4)
	addBound := func(value *string, ascendingOp, descendingOp string) {
		if value == nil {
			return
		}
		op := ascendingOp
		if p.Descending {
			op = descendingOp
		}
		column := `name COLLATE "C"`
		arg := any(*value)
		// PostgreSQL text cannot contain NUL. The bytea fallback preserves the
		// endpoint's byte-order comparison for arbitrary JSON string bounds.
		if strings.IndexByte(*value, 0) >= 0 {
			column = `convert_to(name, 'UTF8')`
			arg = []byte(*value)
		}
		args = append(args, arg)
		conditions = append(conditions,
			fmt.Sprintf("%s %s $%d", column, op, len(args)))
	}
	addBound(p.StartKey, ">=", "<=")
	addBound(p.EndKey, "<=", ">=")
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += ` ORDER BY name COLLATE "C"`
	if p.Descending {
		query += " DESC"
	}
	if p.Limit != nil {
		args = append(args, *p.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if p.Skip != nil {
		args = append(args, *p.Skip)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) DBInfo(ctx context.Context, db *DB) (*Info, error) {
	infos, err := s.DatabaseInfos(ctx, []string{db.Name})
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, couch.DBNotFound()
	}
	info := infos[0].Info
	return &info, nil
}

const databaseInfoBatchSize = 128

// DatabaseInfos returns database information in database-name byte order. A nil
// names slice selects every database; a non-nil slice selects the existing,
// unique names it contains. Registry lookup is one query, and calculated values
// are fetched in bounded UNION ALL batches instead of several round trips per
// database.
func (s *Store) DatabaseInfos(ctx context.Context, names []string) ([]DatabaseInfo, error) {
	if names != nil {
		valid := make([]string, 0, len(names))
		seen := make(map[string]bool, len(names))
		for _, name := range names {
			if seen[name] || validateDBUse(name) != nil {
				continue
			}
			seen[name] = true
			valid = append(valid, name)
		}
		if len(valid) == 0 {
			return []DatabaseInfo{}, nil
		}
		names = valid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	query := `SELECT name, schema_name, partitioned, revs_limit,
	                 instance_start_time, doc_count, doc_del_count, purge_seq,
	                 update_seq
	          FROM couchgres.databases`
	var args []any
	if names != nil {
		query += " WHERE name = ANY($1)"
		args = append(args, names)
	}
	// Keep schemas alive while the dynamic detail query resolves their
	// relations. KEY SHARE still permits normal count/purge metadata updates.
	query += ` ORDER BY name COLLATE "C" FOR KEY SHARE`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	infos := make([]DatabaseInfo, 0)
	for rows.Next() {
		var item DatabaseInfo
		if err := rows.Scan(
			&item.DB.Name, &item.DB.Schema, &item.DB.Partitioned,
			&item.DB.RevsLimit, &item.DB.InstanceStartTime,
			&item.Info.DocCount, &item.Info.DocDelCount, &item.Info.PurgeSeq,
			&item.Info.UpdateSeq,
		); err != nil {
			rows.Close()
			return nil, err
		}
		infos = append(infos, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	byName := make(map[string]int, len(infos))
	for i := range infos {
		byName[infos[i].DB.Name] = i
	}
	for start := 0; start < len(infos); start += databaseInfoBatchSize {
		end := min(start+databaseInfoBatchSize, len(infos))
		detailQuery, detailArgs := databaseInfoDetailsQuery(infos[start:end])
		details, err := tx.Query(ctx, detailQuery, detailArgs...)
		if err != nil {
			return nil, err
		}
		for details.Next() {
			var name string
			var sizeBytes, externalSize int64
			if err := details.Scan(&name, &sizeBytes, &externalSize); err != nil {
				details.Close()
				return nil, err
			}
			i, ok := byName[name]
			if !ok {
				details.Close()
				return nil, fmt.Errorf("database info returned unknown database %q", name)
			}
			infos[i].Info.SizeBytes = sizeBytes
			infos[i].Info.ExternalSize = externalSize
		}
		details.Close()
		if err := details.Err(); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return infos, nil
}

func databaseInfoDetailsQuery(infos []DatabaseInfo) (string, []any) {
	var query strings.Builder
	args := make([]any, len(infos))
	for i, item := range infos {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		args[i] = item.DB.Name
		externalSize := fmt.Sprintf(
			"(SELECT coalesce(sum(external_size), 0) FROM %s.docs)", item.DB.Schema)
		if item.DB.Partitioned {
			externalSize = fmt.Sprintf(
				"(SELECT coalesce(sum(external_size), 0) FROM %s.partition_stats)",
				item.DB.Schema)
		}
		fmt.Fprintf(&query, `SELECT $%d::text,
		   pg_total_relation_size('%[2]s.docs')
		     + pg_total_relation_size('%[2]s.revs')
		     + pg_total_relation_size('%[2]s.attachments'), %[3]s`,
			i+1, item.DB.Schema, externalSize)
	}
	return query.String(), args
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
