// Package store is the PostgreSQL layer. It uses one Postgres schema per
// CouchDB database and a global couchgres schema for the registry and config tree.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/couchgres/couchgres/internal/couch"
)

type Store struct {
	pool *pgxpool.Pool
	// dbCache holds database registry rows. GetDB runs on every request.
	// Rows change only through CreateDatabase, DeleteDatabase, and SetRevsLimit.
	// Those methods invalidate the cache. Keyed by database name.
	dbCache sync.Map
	// ensuredViews remembers signatures whose view tables exist, avoiding
	// CREATE ... IF NOT EXISTS on every view query and the catalog lock
	// traffic that comes with it. It is keyed by "<schema>/<sig>".
	ensuredViews sync.Map
	// viewTotals caches per-view row counts keyed "<schema>/<sig>/<view>/
	// <part>". It is valid for one last_seq. Otherwise, total_rows requires a full
	// view count on every read. Purge deletes view rows without
	// bumping last_seq, so it clears the cache instead.
	viewTotals sync.Map

	// Change notifications use notifyChanged after commit instead of pg_notify
	// inside the transaction. Postgres serializes every notifying commit on one
	// global queue lock, which convoys concurrent writers. Local subscribers wake
	// through broker directly. A debounced
	// loop coalesces the cross-process pg_notify.
	broker        atomic.Pointer[Broker]
	notifyMu      sync.Mutex
	notifyPending map[string]struct{}
	invPending    map[string]struct{}
	notifyKick    chan struct{}

	// keepSuperseded (config couchgres/keep_superseded_bodies) makes
	// superseded revision bodies survive until _compact, CouchDB-style,
	// so admins can read back overwritten or deleted data with ?rev=.
	// Off (the default), a superseded body is dropped by the write that
	// replaces it.
	keepSuperseded atomic.Bool
}

// SetKeepSupersededBodies switches whether superseded revision bodies are
// retained until compaction (mirrored from the config tree by the HTTP
// layer at startup and on config writes).
func (s *Store) SetKeepSupersededBodies(keep bool) {
	s.keepSuperseded.Store(keep)
}

type viewTotal struct {
	lastSeq int64
	total   int64
}

// cachedViewTotal returns the cached row count for one view when it is
// still valid for the index's current last_seq. It also returns last_seq for
// storage after a recount. lastSeqHint > 0 avoids the view_state read. During
// a concurrent index update, the count can lag one batch. total_rows under
// concurrent updates is approximate in CouchDB too.
func (s *Store) cachedViewTotal(ctx context.Context, db *DB, sig, view, part string, lastSeqHint int64) (int64, int64, bool) {
	lastSeq := lastSeqHint
	if lastSeq <= 0 {
		var err error
		lastSeq, err = s.ViewGroupState(ctx, db, sig)
		if err != nil {
			return 0, 0, false
		}
	}
	if e, ok := s.viewTotals.Load(viewTotalKey(db, sig, view, part)); ok {
		if vt := e.(viewTotal); vt.lastSeq == lastSeq {
			return vt.total, lastSeq, true
		}
	}
	return 0, lastSeq, false
}

func (s *Store) storeViewTotal(db *DB, sig, view, part string, lastSeq, total int64) {
	s.viewTotals.Store(viewTotalKey(db, sig, view, part), viewTotal{lastSeq: lastSeq, total: total})
}

func viewTotalKey(db *DB, sig, view, part string) string {
	return db.Schema + "/" + sig + "/" + view + "/" + part
}

// clearViewCaches forgets cached view bookkeeping for one database (schema
// drops, view cleanup, purge).
func (s *Store) clearViewCaches(db *DB) {
	prefix := db.Schema + "/"
	s.ensuredViews.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), prefix) {
			s.ensuredViews.Delete(k)
		}
		return true
	})
	s.viewTotals.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), prefix) {
			s.viewTotals.Delete(k)
		}
		return true
	})
}

func New(ctx context.Context, url string, poolSize int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres url: %w", err)
	}
	if poolSize > 0 {
		cfg.MaxConns = poolSize
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	return &Store{
		pool:          pool,
		notifyPending: make(map[string]struct{}),
		invPending:    make(map[string]struct{}),
		notifyKick:    make(chan struct{}, 1),
	}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// Bootstrap creates global state and the system databases, and persists the
// server UUID on first run. Returns the server UUID. It is idempotent and safe to run
// concurrently (advisory lock).
func (s *Store) Bootstrap(ctx context.Context) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// Serialize concurrent bootstraps (CREATE SCHEMA IF NOT EXISTS races).
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLockBootstrap); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, globalDDL); err != nil {
		return "", fmt.Errorf("global DDL: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO couchgres.cache_versions (name, version)
		 VALUES ($1, 0), ($2, 0) ON CONFLICT DO NOTHING`,
		CacheConfig, CacheDBRegistry,
	); err != nil {
		return "", err
	}

	var serverUUID string
	err = tx.QueryRow(ctx,
		`INSERT INTO couchgres.server_config (section, key, value)
		 VALUES ('couchdb', 'uuid', $1)
		 ON CONFLICT (section, key) DO UPDATE SET value = couchgres.server_config.value
		 RETURNING value`,
		randomHex(16),
	).Scan(&serverUUID)
	if err != nil {
		return "", err
	}
	// The session-cookie HMAC secret, generated once and persisted.
	if _, err := tx.Exec(ctx,
		`INSERT INTO couchgres.server_config (section, key, value)
		 VALUES ('couch_httpd_auth', 'secret', $1) ON CONFLICT DO NOTHING`,
		randomHex(16),
	); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	// Databases created before docs_design_seq_idx existed get it here,
	// and ones from when docs still carried bodies lose the column
	// (winner bodies live in revs). Both operations are idempotent. New schemas match.
	rows, err := s.pool.Query(ctx, "SELECT schema_name FROM couchgres.databases")
	if err != nil {
		return "", err
	}
	schemas, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	for _, schema := range schemas {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS docs_design_seq_idx ON %s.docs (seq)
			 WHERE id >= '_design/' AND id < '_design0'`, schema)); err != nil {
			return "", fmt.Errorf("migrating %s: %w", schema, err)
		}
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s.docs DROP COLUMN IF EXISTS body", schema)); err != nil {
			return "", fmt.Errorf("migrating %s: %w", schema, err)
		}
	}

	for _, name := range couch.SystemDBs {
		if err := s.CreateDatabase(ctx, name, false); err != nil {
			var ce *couch.Error
			if asCouchError(err, &ce) && ce.Err == "file_exists" {
				continue
			}
			return "", err
		}
	}

	// _users is admin-only by default in CouchDB 3.x. Authenticated users
	// reach their own doc through the per-doc exception, not membership.
	users, err := s.GetDB(ctx, "_users")
	if err != nil {
		return "", err
	}
	security, err := s.GetSecurity(ctx, users)
	if err != nil {
		return "", err
	}
	if len(security) == 0 {
		adminOnly := map[string]any{
			"admins":  map[string]any{"names": []any{}, "roles": []any{"_admin"}},
			"members": map[string]any{"names": []any{}, "roles": []any{"_admin"}},
		}
		if err := s.SetSecurity(ctx, users, adminOnly); err != nil {
			return "", err
		}
	}
	return serverUUID, nil
}

// ConfigGet reads one value from the CouchDB-compatible config tree.
func (s *Store) ConfigGet(ctx context.Context, section, key string) (string, bool, error) {
	var value string
	err := s.pool.QueryRow(ctx,
		"SELECT value FROM couchgres.server_config WHERE section = $1 AND key = $2",
		section, key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

func asCouchError(err error, target **couch.Error) bool {
	ce, ok := err.(*couch.Error)
	if ok {
		*target = ce
	}
	return ok
}
