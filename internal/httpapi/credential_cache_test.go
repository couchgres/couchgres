package httpapi

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

func TestCredentialCacheEvictsQuarterNotAll(t *testing.T) {
	c := newCredentialCache()
	user := &couch.UserCtx{Name: "u", Authenticated: "default"}

	for i := 0; i < credentialCacheMax; i++ {
		c.put(fmt.Sprintf("user%d", i), "pw", user)
	}
	if n := len(c.entries); n != credentialCacheMax {
		t.Fatalf("filled cache size: %d", n)
	}

	c.put("overflow", "pw", user)
	n := len(c.entries)
	// One insert after capacity drops a quarter then adds one (3/4 max + 1).
	want := credentialCacheMax - credentialCacheMax/4 + 1
	if n != want {
		t.Fatalf("after overflow: got %d entries, want %d", n, want)
	}
	if _, ok := c.get("overflow", "pw"); !ok {
		t.Fatal("newly inserted credential missing")
	}
}

func TestCredentialCachePurgesExpiredBeforeEvict(t *testing.T) {
	c := newCredentialCache()
	user := &couch.UserCtx{Name: "u", Authenticated: "default"}

	for i := 0; i < credentialCacheMax; i++ {
		c.put(fmt.Sprintf("user%d", i), "pw", user)
	}
	// Age half the entries past TTL without going through put.
	now := time.Now()
	i := 0
	for k, e := range c.entries {
		if i >= credentialCacheMax/2 {
			break
		}
		e.expires = now.Add(-time.Second)
		c.entries[k] = e
		i++
	}

	c.put("fresh", "pw", user)
	if n := len(c.entries); n > credentialCacheMax {
		t.Fatalf("cache grew past max: %d", n)
	}
	if _, ok := c.get("fresh", "pw"); !ok {
		t.Fatal("fresh credential missing")
	}
	// Expired purge alone should have freed space. Live entries stay.
	live := 0
	for _, e := range c.entries {
		if time.Now().Before(e.expires) {
			live++
		}
	}
	if live < credentialCacheMax/2 {
		t.Fatalf("too many live entries dropped: %d live", live)
	}
}

func TestCredentialCacheGetDropsExpired(t *testing.T) {
	c := newCredentialCache()
	c.put("alice", "secret", &couch.UserCtx{Name: "alice"})
	key := credentialKey("alice", "secret")
	e := c.entries[key]
	e.expires = time.Now().Add(-time.Second)
	c.entries[key] = e

	if _, ok := c.get("alice", "secret"); ok {
		t.Fatal("expired entry should miss")
	}
	if _, still := c.entries[key]; still {
		t.Fatal("expired entry should be deleted on get")
	}
}
