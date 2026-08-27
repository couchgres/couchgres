package httpapi

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func testTwoHandlers(t *testing.T) (http.Handler, http.Handler) {
	t.Helper()
	stA := testHTTPStore(t)
	stB := testHTTPStore(t)
	return testServer(t, stA), testServer(t, stB)
}

func TestConfigInvalidatesAcrossServers(t *testing.T) {
	hA, hB := testTwoHandlers(t)
	admin := adminAuth()
	path := "/_node/_local/_config/couchgres_test_invalidation/flag"
	value := "seen-" + time.Now().UTC().Format("20060102150405.000000000")
	send(t, hA, "DELETE", path, nil, testAdminAuth, admin)
	t.Cleanup(func() {
		send(t, hA, "DELETE", path, nil, testAdminAuth, admin)
	})

	resp := send(t, hA, "PUT", path, value, testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("config put: %+v", resp)
	}

	waitFor(t, "remote config cache invalidation", 10*time.Second, func() bool {
		resp := send(t, hB, "GET", path, nil, testAdminAuth, admin)
		return resp.status == 200 && resp.scalar == value
	})
	resp = send(t, hB, "GET", path, nil, testAdminAuth, admin)
	if resp.status != 200 || resp.scalar != value {
		t.Fatalf("remote config get: %+v", resp)
	}
}

func TestDBCacheInvalidatesAcrossServers(t *testing.T) {
	hA, hB := testTwoHandlers(t)
	admin := adminAuth()
	db := "cache_invalidation_db"
	send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin)
	if resp := send(t, hA, "PUT", "/"+db, nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	if resp := send(t, hB, "GET", "/"+db, nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("cache db on server B: %+v", resp)
	}

	if resp := send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("delete db: %+v", resp)
	}
	waitFor(t, "remote db cache invalidation", 10*time.Second, func() bool {
		return send(t, hB, "GET", "/"+db, nil, testAdminAuth, admin).status == 404
	})
}

func TestViewCachePurgeInvalidatesAcrossServers(t *testing.T) {
	stA := testHTTPStore(t)
	stB := testHTTPStore(t)
	hA := testServer(t, stA)
	hB := testServer(t, stB)
	admin := adminAuth()
	const db = "view_purge_invalidation"

	send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin)
	if resp := send(t, hA, "PUT", "/"+db, nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, hA, db)
	t.Cleanup(func() {
		send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin)
	})

	if resp := send(t, hA, "PUT", "/"+db+"/keep", decode(t, `{"n":1}`)); resp.status != 201 {
		t.Fatalf("put keep: %+v", resp)
	}
	gone := send(t, hA, "PUT", "/"+db+"/gone", decode(t, `{"n":2}`))
	if gone.status != 201 {
		t.Fatalf("put gone: %+v", gone)
	}
	goneRev := gone.body["rev"].(string)
	if resp := send(t, hA, "PUT", "/"+db+"/_design/v", decode(t,
		`{"views":{"byn":{"map":"function(doc){if(doc.n !== undefined) emit(doc.n,null);}"}}}`),
		testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("put design doc: %+v", resp)
	}

	const q = "/" + db + "/_design/v/_view/byn?reduce=false"
	warm := send(t, hA, "GET", q, nil)
	if warm.status != 200 || warm.body["total_rows"] != float64(2) ||
		len(warm.body["rows"].([]any)) != 2 {
		t.Fatalf("warm view: %+v", warm)
	}
	// The second read exercises the whole-response cache. Its stored generation
	// and the store's total_rows cache both belong only to server A.
	if cached := send(t, hA, "GET", q, nil); cached.status != 200 {
		t.Fatalf("cached view: %+v", cached)
	}
	cacheEntry := hA.viewCache.get(q)
	if cacheEntry == nil {
		t.Fatal("server A did not cache the warmed view")
	}
	oldETag := warm.header.Get("Etag")

	purge := send(t, hB, "POST", "/"+db+"/_purge",
		decode(t, fmt.Sprintf(`{"gone":[%q]}`, goneRev)), testAdminAuth, admin)
	if purge.status != 201 {
		t.Fatalf("remote purge: %+v", purge)
	}
	// Do not wait for broker delivery. Server B cannot directly clear server A's
	// process-local cache, so this proves the durable generation is the boundary.
	if hA.viewCache.get(q) == nil {
		t.Fatal("remote purge unexpectedly evicted server A's local cache")
	}

	fresh := send(t, hA, "GET", q, nil, "If-None-Match", oldETag)
	if fresh.status != 200 || fresh.body["total_rows"] != float64(1) {
		t.Fatalf("view after remote purge: %+v", fresh)
	}
	rows := fresh.body["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "keep" {
		t.Fatalf("rows after remote purge: %+v", rows)
	}
	if fresh.header.Get("Etag") == oldETag {
		t.Fatal("purge did not change the view ETag")
	}
	newEntry := hA.viewCache.get(q)
	if newEntry == nil || newEntry.purgeSeq <= cacheEntry.purgeSeq {
		t.Fatalf("cached purge generation did not advance: old=%d new=%+v",
			cacheEntry.purgeSeq, newEntry)
	}
}
