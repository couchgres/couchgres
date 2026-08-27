package httpapi

// viewRespCache caches whole view responses keyed by request path+query.
// Each hit checks the durable database and purge generations before returning
// a cached body. Local clears provide eager eviction, but correctness does not
// depend on process-local state.

import (
	"sync"
)

const (
	viewCacheMaxEntries = 2048
	viewCacheMaxBody    = 256 << 10
)

type viewRespEntry struct {
	schema   string
	ddocID   string
	sig      string
	etag     string
	ddocSeq  int64
	lastSeq  int64
	purgeSeq int64
	body     []byte
}

type viewRespCache struct {
	mu      sync.Mutex
	entries map[string]*viewRespEntry
}

func newViewRespCache() *viewRespCache {
	return &viewRespCache{entries: make(map[string]*viewRespEntry)}
}

func (c *viewRespCache) get(key string) *viewRespEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key]
}

func (c *viewRespCache) put(key string, e *viewRespEntry) {
	if len(e.body) > viewCacheMaxBody {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= viewCacheMaxEntries {
		// Cheap pressure valve: evict an arbitrary quarter. LRU bookkeeping
		// isn't worth the per-hit cost for a revalidated cache.
		drop := viewCacheMaxEntries / 4
		for k := range c.entries {
			delete(c.entries, k)
			if drop--; drop <= 0 {
				break
			}
		}
	}
	c.entries[key] = e
}

func (c *viewRespCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// clearSchema forgets every entry of one database (purge).
func (c *viewRespCache) clearSchema(schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if e.schema == schema {
			delete(c.entries, k)
		}
	}
}
