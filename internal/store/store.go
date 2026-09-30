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
	// <part>". Entries are valid for one (last_seq, purge_seq) generation;
	// otherwise total_rows requires a full view count.
	viewTotals sync.Map
	// pendingCounts caches exact _changes pending counts against committed
	// document and purge generations. It is bounded because clients control
	// the since sequence.
	pendingCounts pendingCountCache

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
	lastSeq  int64
	purgeSeq int64
	total    int64
}

// cachedViewTotal returns the cached row count for one view when it is
// still valid for the index and purge generations. It also returns both
// generations for storage after a recount. Hints from SyncViewGroupSnapshot
// avoid another database read on the normal HTTP path. During a concurrent
// index update, the count can lag one batch. total_rows under concurrent
// updates is approximate in CouchDB too.
func (s *Store) cachedViewTotal(
	ctx context.Context,
	db *DB,
	sig, view, part string,
	lastSeqHint int64,
	purgeSeqHint *int64,
) (total, lastSeq, purgeSeq int64, hit bool) {
	lastSeq = lastSeqHint
	if lastSeq <= 0 || purgeSeqHint == nil {
		var err error
		lastSeq, purgeSeq, err = s.ViewGroupSnapshot(ctx, db, sig)
		if err != nil {
			return 0, 0, 0, false
		}
	} else {
		purgeSeq = *purgeSeqHint
	}
	if e, ok := s.viewTotals.Load(viewTotalKey(db, sig, view, part)); ok {
		if vt := e.(viewTotal); vt.lastSeq == lastSeq && vt.purgeSeq == purgeSeq {
			return vt.total, lastSeq, purgeSeq, true
		}
	}
	return 0, lastSeq, purgeSeq, false
}

func (s *Store) storeViewTotal(db *DB, sig, view, part string, lastSeq, purgeSeq, total int64) {
	s.viewTotals.Store(viewTotalKey(db, sig, view, part), viewTotal{
		lastSeq: lastSeq, purgeSeq: purgeSeq, total: total,
	})
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
	s.clearViewTotals(db)
}

func (s *Store) clearViewTotals(db *DB) {
	prefix := db.Schema + "/"
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
	// ones from when docs still carried bodies lose the column (winner bodies
	// live in revs), and databases from before transactional update sequences
	// receive a one-time sequence backfill. New schemas already match.
	rows, err := s.pool.Query(ctx, "SELECT schema_name FROM couchgres.databases")
	if err != nil {
		return "", err
	}
	schemas, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	for _, schema := range schemas {
		if err := s.migrateDatabaseSchema(ctx, schema); err != nil {
			return "", fmt.Errorf("migrating %s: %w", schema, err)
		}
		if err := s.initializeUpdateSeq(ctx, schema); err != nil {
			return "", fmt.Errorf("migrating update sequence for %s: %w", schema, err)
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

func (s *Store) migrateDatabaseSchema(ctx context.Context, schema string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Serialize schema migrations with each other and with global DDL. This
	// avoids concurrent index creation and keeps a global ALTER TABLE from
	// queuing ahead of writers that must finish before our schema DDL can run.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLockBootstrap); err != nil {
		return err
	}

	// The schema list may be stale. DeleteDatabase removes the registry row
	// before dropping its schema, so a key-share lock both rechecks existence
	// and keeps the schema alive until our DDL commits. Unlike FOR UPDATE, it
	// lets existing document writers finish updating the registry's sequence
	// while we wait for their table locks.
	err = tx.QueryRow(ctx,
		`SELECT schema_name FROM couchgres.databases
		 WHERE schema_name = $1 FOR KEY SHARE`, schema,
	).Scan(&schema)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	// Even IF EXISTS/IF NOT EXISTS DDL takes table locks. Current schemas
	// must not queue those locks ahead of another process's active requests.
	var indexExists, bodyExists bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass($1) IS NOT NULL, EXISTS (
		    SELECT 1 FROM pg_attribute
		    WHERE attrelid = to_regclass($2) AND attname = 'body' AND NOT attisdropped
		)`, schema+".docs_design_seq_idx", schema+".docs",
	).Scan(&indexExists, &bodyExists); err != nil {
		return err
	}
	if !indexExists {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS docs_design_seq_idx ON %s.docs (seq)
			 WHERE id >= '_design/' AND id < '_design0'`, schema)); err != nil {
			return err
		}
	}
	if bodyExists {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s.docs DROP COLUMN IF EXISTS body", schema)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// initializeUpdateSeq migrates a database from the old nontransactional
// per-schema sequence to the committed counter in the database registry. The
// registry-row lock serializes new writers and concurrent migrations. ALTER
// fences legacy nextval callers before the sequence is sampled and dropped:
// callers that already allocated finish first, while older binaries that reach
// nextval afterward fail instead of silently bypassing the committed counter.
// New databases set update_seq_initialized at creation and skip this path.
func (s *Store) initializeUpdateSeq(ctx context.Context, schema string) error {
	var initialized bool
	err := s.pool.QueryRow(ctx,
		`SELECT update_seq_initialized FROM couchgres.databases
		 WHERE schema_name = $1`, schema,
	).Scan(&initialized)
	if err == pgx.ErrNoRows || initialized {
		return nil
	}
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var current int64
	err = tx.QueryRow(ctx,
		`SELECT update_seq, update_seq_initialized
		 FROM couchgres.databases WHERE schema_name = $1 FOR UPDATE`, schema,
	).Scan(&current, &initialized)
	if err == pgx.ErrNoRows || initialized {
		return tx.Rollback(ctx)
	}
	if err != nil {
		return err
	}

	legacy := int64(0)
	var hasLegacy bool
	if err := tx.QueryRow(ctx,
		"SELECT to_regclass($1) IS NOT NULL", schema+".update_seq",
	).Scan(&hasLegacy); err != nil {
		return err
	}
	if hasLegacy {
		// ALTER SEQUENCE conflicts with nextval's lock and holds that fence until
		// commit. CACHE 1 is the legacy DDL default, so this is semantically a
		// no-op apart from acquiring the lock.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"ALTER SEQUENCE %s.update_seq CACHE 1", schema)); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			"SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM %s.update_seq", schema),
		).Scan(&legacy); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DROP SEQUENCE %s.update_seq", schema)); err != nil {
			return err
		}
	}

	// A current writer that touched document rows but has not reached its
	// registry update is blocked behind our row lock. Its eventual increment is
	// therefore greater than this watermark. A legacy writer either completed
	// before ALTER acquired its lock or will fail at nextval after commit.
	var visible int64
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		"SELECT coalesce(max(seq), 0) FROM %s.docs", schema),
	).Scan(&visible); err != nil {
		return err
	}
	current = max(current, visible, legacy)
	if _, err := tx.Exec(ctx,
		`UPDATE couchgres.databases
		 SET update_seq = $1, update_seq_initialized = true
		 WHERE schema_name = $2`, current, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
