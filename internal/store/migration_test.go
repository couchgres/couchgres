package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestSchemaMigrationSkipsDeletedDatabase(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_schema_migration_deleted")
	if err := s.DeleteDatabase(ctx, db.Name); err != nil {
		t.Fatal(err)
	}
	// Bootstrap may have collected this schema before another process deleted
	// the database. A replacement with the same name has a different schema.
	replacement := freshDB(t, s, db.Name)
	if err := s.migrateDatabaseSchema(ctx, db.Schema); err != nil {
		t.Fatalf("migrating a deleted database: %v", err)
	}
	if _, err := s.DBInfo(ctx, replacement); err != nil {
		t.Fatalf("replacement database: %v", err)
	}
}

func TestSchemaMigrationLocksDatabaseLifecycle(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	db := freshDB(t, s, "it_schema_migration_locks")
	if _, err := s.pool.Exec(ctx, "ALTER TABLE "+db.Schema+".docs ADD COLUMN body jsonb"); err != nil {
		t.Fatal(err)
	}

	// A separate one-connection pool lets us observe exactly when the migration
	// is waiting for an existing writer's table lock.
	migrator, err := New(ctx, testStoreURL(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(migrator.Close)
	var migrationPID int32
	if err := migrator.pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&migrationPID); err != nil {
		t.Fatal(err)
	}
	writer, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(ctx)
	if _, err := writer.Exec(ctx, "LOCK TABLE "+db.Schema+".docs IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- migrator.migrateDatabaseSchema(ctx, db.Schema) }()
	for {
		var blocked bool
		if err := s.pool.QueryRow(ctx,
			"SELECT $1::int = ANY(pg_blocking_pids($2))",
			writer.Conn().PgConn().PID(), migrationPID,
		).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("migration did not wait for writer: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	deletion, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer deletion.Rollback(ctx)
	if _, err := deletion.Exec(ctx, "SET LOCAL lock_timeout = '100ms'"); err != nil {
		t.Fatal(err)
	}
	_, err = deletion.Exec(ctx, "DELETE FROM couchgres.databases WHERE name = $1", db.Name)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("deletion was not blocked by migration: %v", err)
	}
	if err := deletion.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Writers must still be able to finish their registry update while DDL
	// waits for them; a stronger registry-row lock would deadlock here.
	if _, err := writer.Exec(ctx,
		"UPDATE couchgres.databases SET update_seq = update_seq + 1 WHERE name = $1", db.Name,
	); err != nil {
		t.Fatalf("finishing an existing writer: %v", err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.DeleteDatabase(ctx, db.Name); err != nil {
		t.Fatalf("deleting after migration: %v", err)
	}
}

func TestSchemaMigrationSkipsCurrentSchemaDDL(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db := freshDB(t, s, "it_schema_migration_current")
	writer, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(ctx)
	if _, err := writer.Exec(ctx, "LOCK TABLE "+db.Schema+".docs IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if err := s.migrateDatabaseSchema(ctx, db.Schema); err != nil {
		t.Fatalf("current schema migration waited for an active writer: %v", err)
	}
}

func TestConcurrentSchemaMigrations(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	db := freshDB(t, s, "it_schema_migration_concurrent")
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		"DROP INDEX %s.docs_design_seq_idx; ALTER TABLE %s.docs ADD COLUMN body jsonb",
		db.Schema, db.Schema,
	)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- s.migrateDatabaseSchema(ctx, db.Schema) }()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	var indexExists, bodyExists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT to_regclass($1) IS NOT NULL, EXISTS (
		    SELECT 1 FROM information_schema.columns
		    WHERE table_schema = $2 AND table_name = 'docs' AND column_name = 'body'
		)`, db.Schema+".docs_design_seq_idx", db.Schema,
	).Scan(&indexExists, &bodyExists); err != nil {
		t.Fatal(err)
	}
	if !indexExists || bodyExists {
		t.Fatalf("schema after migration: index exists = %v, body exists = %v", indexExists, bodyExists)
	}
}
