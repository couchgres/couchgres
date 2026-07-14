package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	CacheConfig     = "config"
	CacheDBRegistry = "db_registry"
)

// CacheVersions returns the durable invalidation versions for process-local
// caches. Notifications are only wakeups; these versions are the source of
// truth after reconnects or missed NOTIFY events.
func (s *Store) CacheVersions(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, "SELECT name, version FROM couchgres.cache_versions")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make(map[string]int64)
	for rows.Next() {
		var name string
		var version int64
		if err := rows.Scan(&name, &version); err != nil {
			return nil, err
		}
		versions[name] = version
	}
	return versions, rows.Err()
}

func bumpCacheVersionTx(ctx context.Context, tx pgx.Tx, name string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO couchgres.cache_versions (name, version) VALUES ($1, 1)
		 ON CONFLICT (name) DO UPDATE
		 SET version = couchgres.cache_versions.version + 1,
		     updated_at = now()`,
		name)
	return err
}

// ClearDatabaseCaches clears process-local database registry and derived view
// caches after another process changes the database registry.
func (s *Store) ClearDatabaseCaches() {
	s.dbCache.Range(func(k, _ any) bool {
		s.dbCache.Delete(k)
		return true
	})
	s.ensuredViews.Range(func(k, _ any) bool {
		s.ensuredViews.Delete(k)
		return true
	})
	s.viewTotals.Range(func(k, _ any) bool {
		s.viewTotals.Delete(k)
		return true
	})
}
