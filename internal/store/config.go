package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ConfigEntry is one row of the CouchDB-compatible config tree.
type ConfigEntry struct {
	Section string
	Key     string
	Value   string
}

func (s *Store) ConfigAll(ctx context.Context) ([]ConfigEntry, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT section, key, value FROM couchgres.server_config ORDER BY section, key")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ConfigEntry])
}

// ConfigSet stores a value and returns the previous one ("" when absent).
func (s *Store) ConfigSet(ctx context.Context, section, key, value string) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var previous string
	err = tx.QueryRow(ctx,
		"SELECT value FROM couchgres.server_config WHERE section = $1 AND key = $2",
		section, key).Scan(&previous)
	if err == pgx.ErrNoRows {
		previous = ""
	} else if err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO couchgres.server_config (section, key, value) VALUES ($1, $2, $3)
		 ON CONFLICT (section, key) DO UPDATE SET value = $3`,
		section, key, value); err != nil {
		return "", err
	}
	if err := bumpCacheVersionTx(ctx, tx, CacheConfig); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	s.notifyInvalidated(CacheConfig)
	return previous, nil
}

// ConfigDelete removes a value, reporting whether it existed and what it was.
func (s *Store) ConfigDelete(ctx context.Context, section, key string) (string, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var value string
	err = tx.QueryRow(ctx,
		"DELETE FROM couchgres.server_config WHERE section = $1 AND key = $2 RETURNING value",
		section, key).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if err := bumpCacheVersionTx(ctx, tx, CacheConfig); err != nil {
		return "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	s.notifyInvalidated(CacheConfig)
	return value, true, nil
}

// GetSecurity returns the db's _security object ({} when never set).
func (s *Store) GetSecurity(ctx context.Context, db *DB) (map[string]any, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT obj FROM %s.security", db.Schema)).Scan(&raw)
	if err == pgx.ErrNoRows {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("stored security object invalid: %w", err)
	}
	return obj, nil
}

func (s *Store) SetSecurity(ctx context.Context, db *DB, obj map[string]any) error {
	raw, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.security (singleton, obj) VALUES (1, $1)
		 ON CONFLICT (singleton) DO UPDATE SET obj = $1`, db.Schema), raw)
	return err
}
