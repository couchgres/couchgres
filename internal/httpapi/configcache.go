package httpapi

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// configCache is an in-memory copy of the CouchDB-compatible config tree
// (couchgres.server_config), read on every request by the auth middleware.
// Writes go through Set/Delete, and other processes reload through
// PostgreSQL-backed cache invalidations.
type configCache struct {
	mu   sync.RWMutex
	tree map[string]map[string]string // section -> key -> value
}

func loadConfigCache(ctx context.Context, st *store.Store) (*configCache, error) {
	entries, err := st.ConfigAll(ctx)
	if err != nil {
		return nil, err
	}
	c := &configCache{tree: make(map[string]map[string]string)}
	for _, e := range entries {
		c.insert(e.Section, e.Key, e.Value)
	}
	return c, nil
}

func (c *configCache) reload(ctx context.Context, st *store.Store) error {
	entries, err := st.ConfigAll(ctx)
	if err != nil {
		return err
	}
	tree := make(map[string]map[string]string)
	for _, e := range entries {
		if tree[e.Section] == nil {
			tree[e.Section] = make(map[string]string)
		}
		tree[e.Section][e.Key] = e.Value
	}
	c.mu.Lock()
	c.tree = tree
	c.mu.Unlock()
	return nil
}

func (c *configCache) insert(section, key, value string) {
	if c.tree[section] == nil {
		c.tree[section] = make(map[string]string)
	}
	c.tree[section][key] = value
}

func (c *configCache) get(section, key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.tree[section][key]
	return value, ok
}

func (c *configCache) getOr(section, key, fallback string) string {
	if value, ok := c.get(section, key); ok {
		return value
	}
	return fallback
}

func (c *configCache) getBool(section, key string, fallback bool) bool {
	if value, ok := c.get(section, key); ok {
		return value == "true"
	}
	return fallback
}

func (c *configCache) getInt(section, key string, fallback int) int {
	if value, ok := c.get(section, key); ok {
		if n, err := strconv.Atoi(value); err == nil {
			return n
		}
	}
	return fallback
}

// section returns a copy of one section's key/value pairs.
func (c *configCache) section(name string) map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.tree[name]))
	for k, v := range c.tree[name] {
		out[k] = v
	}
	return out
}

// all returns a copy of the whole tree.
func (c *configCache) all() map[string]map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]string, len(c.tree))
	for section, keys := range c.tree {
		copied := make(map[string]string, len(keys))
		for k, v := range keys {
			copied[k] = v
		}
		out[section] = copied
	}
	return out
}

func (c *configCache) set(ctx context.Context, st *store.Store, section, key, value string) (string, error) {
	previous, err := st.ConfigSet(ctx, section, key, value)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.insert(section, key, value)
	c.mu.Unlock()
	return previous, nil
}

func (c *configCache) delete(ctx context.Context, st *store.Store, section, key string) (string, bool, error) {
	previous, existed, err := st.ConfigDelete(ctx, section, key)
	if err != nil {
		return "", false, err
	}
	c.mu.Lock()
	delete(c.tree[section], key)
	c.mu.Unlock()
	return previous, existed, nil
}

// Frequently used settings.

func (c *configCache) sessionTimeout() int64 {
	return int64(c.getInt("couch_httpd_auth", "timeout", 600))
}

func (c *configCache) cookieSecret() string {
	return c.getOr("couch_httpd_auth", "secret", "")
}

func (c *configCache) passwordIterations() int {
	iterations, _, _, err := c.passwordIterationSettings("", "")
	if err != nil {
		return couch.DefaultIterations
	}
	return iterations
}

func (c *configCache) passwordIterationBounds() (int, int) {
	_, minIterations, maxIterations, err := c.passwordIterationSettings("", "")
	if err != nil {
		return couch.MinPasswordIterations, couch.MaxPasswordIterations
	}
	return minIterations, maxIterations
}

func (c *configCache) validatePasswordIterationChange(key, value string) error {
	_, _, _, err := c.passwordIterationSettings(key, value)
	return err
}

func (c *configCache) passwordIterationSettings(
	overrideKey, overrideValue string,
) (iterations, minIterations, maxIterations int, err error) {
	read := func(key string, fallback int) (int, error) {
		value := c.getOr("couch_httpd_auth", key, strconv.Itoa(fallback))
		if key == overrideKey {
			value = overrideValue
		}
		n, parseErr := strconv.Atoi(value)
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return n, nil
	}
	iterations, err = read("iterations", couch.DefaultIterations)
	if err != nil {
		return 0, 0, 0, err
	}
	minIterations, err = read("min_iterations", couch.MinPasswordIterations)
	if err != nil {
		return 0, 0, 0, err
	}
	maxIterations, err = read("max_iterations", couch.MaxPasswordIterations)
	if err != nil {
		return 0, 0, 0, err
	}
	if err = couch.ValidatePasswordIterationRange(minIterations, maxIterations); err != nil {
		return 0, 0, 0, err
	}
	if iterations < minIterations || iterations > maxIterations {
		return 0, 0, 0, fmt.Errorf(
			"iterations must be between min_iterations (%d) and max_iterations (%d)",
			minIterations, maxIterations)
	}
	return iterations, minIterations, maxIterations, nil
}

func (c *configCache) passwordPolicy() passwordPolicy {
	minIterations, maxIterations := c.passwordIterationBounds()
	mode := c.getOr("chttpd_auth_lockout", "mode", "enforce")
	if mode != "off" && mode != "warn" && mode != "enforce" {
		mode = "enforce"
	}
	threshold := c.getInt("chttpd_auth_lockout", "threshold", 5)
	if threshold < 1 || threshold > 1_000 {
		threshold = 5
	}
	maxObjects := c.getInt("chttpd_auth_lockout", "max_objects", 10_000)
	if maxObjects < 1 || maxObjects > 100_000 {
		maxObjects = 10_000
	}
	lifetimeMS := c.getInt("chttpd_auth_lockout", "max_lifetime", 300_000)
	if lifetimeMS < 1_000 || lifetimeMS > int((24*time.Hour)/time.Millisecond) {
		lifetimeMS = 300_000
	}
	return passwordPolicy{
		minIterations: minIterations,
		maxIterations: maxIterations,
		lockoutMode:   mode,
		lockoutLimit:  threshold,
		lockoutPeriod: time.Duration(lifetimeMS) * time.Millisecond,
		lockoutMax:    maxObjects,
	}
}

func (c *configCache) adminPassword(name string) (string, bool) {
	return c.get("admins", name)
}

func (c *configCache) adminOnlyAllDBs() bool {
	return c.getBool("chttpd", "admin_only_all_dbs", true)
}

func (c *configCache) requireValidUser() bool {
	return c.getBool("chttpd", "require_valid_user", false) ||
		c.getBool("couch_httpd_auth", "require_valid_user", false)
}

func (c *configCache) proxyAuthEnabled() bool {
	return c.getBool("chttpd_auth", "proxy_authentication", false)
}
