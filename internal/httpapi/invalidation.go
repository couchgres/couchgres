package httpapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/couchgres/couchgres/internal/store"
)

const cacheInvalidationPoll = 30 * time.Second

func (s *Server) watchInvalidations(ctx context.Context) {
	configWake, unsubscribeConfig := s.broker.SubscribeInvalidation(store.CacheConfig)
	defer unsubscribeConfig()
	dbWake, unsubscribeDB := s.broker.SubscribeInvalidation(store.CacheDBRegistry)
	defer unsubscribeDB()

	seen := make(map[string]int64)
	s.refreshInvalidations(ctx, seen)

	ticker := time.NewTicker(cacheInvalidationPoll)
	defer ticker.Stop()
	for {
		select {
		case <-configWake:
			s.refreshInvalidations(ctx, seen)
		case <-dbWake:
			s.refreshInvalidations(ctx, seen)
		case <-ticker.C:
			s.refreshInvalidations(ctx, seen)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) refreshInvalidations(ctx context.Context, seen map[string]int64) {
	versions, err := s.store.CacheVersions(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("cache invalidation poll failed", "error", err)
		}
		return
	}
	s.refreshInvalidation(ctx, seen, versions, store.CacheConfig)
	s.refreshInvalidation(ctx, seen, versions, store.CacheDBRegistry)
}

func (s *Server) refreshInvalidation(
	ctx context.Context,
	seen, versions map[string]int64,
	name string,
) {
	version, ok := versions[name]
	if !ok || version <= seen[name] {
		return
	}
	switch name {
	case store.CacheConfig:
		if err := s.config.reload(ctx, s.store); err != nil {
			if ctx.Err() == nil {
				slog.Warn("config cache reload failed", "error", err)
			}
			return
		}
		s.applyStoreConfig()
	case store.CacheDBRegistry:
		s.store.ClearDatabaseCaches()
	}
	seen[name] = version
}
