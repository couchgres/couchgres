package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	advisoryLockBootstrap  = 268244737
	advisoryLockReplicator = 268244738

	replicatorLockHeartbeat     = 5 * time.Second
	replicatorLockUnlockTimeout = 5 * time.Second
)

// RunWithReplicatorLock runs fn while this process owns the singleton
// _replicator worker advisory lock. The lock is held by a pinned PostgreSQL
// session and released before that session returns to the pool.
func (s *Store) RunWithReplicatorLock(ctx context.Context, fn func(context.Context)) (bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	release := true
	defer func() {
		if release {
			conn.Release()
		}
	}()

	var acquired bool
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)", advisoryLockReplicator,
	).Scan(&acquired); err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	release = false

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(workerCtx)
	}()

	ticker := time.NewTicker(replicatorLockHeartbeat)
	defer ticker.Stop()

	finish := func(runErr error) (bool, error) {
		cancel()
		unlockErr := releaseReplicatorLock(conn)
		if runErr != nil {
			return true, runErr
		}
		return true, unlockErr
	}

	for {
		select {
		case <-done:
			return finish(nil)
		case <-ctx.Done():
			cancel()
			<-done
			return finish(ctx.Err())
		case <-ticker.C:
			if err := conn.Ping(ctx); err != nil {
				cancel()
				<-done
				return finish(fmt.Errorf("replicator advisory lock connection lost: %w", err))
			}
		}
	}
}

func releaseReplicatorLock(conn *pgxpool.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), replicatorLockUnlockTimeout)
	defer cancel()

	var unlocked bool
	err := conn.QueryRow(ctx,
		"SELECT pg_advisory_unlock($1)", advisoryLockReplicator,
	).Scan(&unlocked)
	if err == nil && unlocked {
		conn.Release()
		return nil
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), replicatorLockUnlockTimeout)
	defer closeCancel()
	_ = conn.Hijack().Close(closeCtx)
	if err != nil {
		return err
	}
	return fmt.Errorf("replicator advisory lock was not held")
}
