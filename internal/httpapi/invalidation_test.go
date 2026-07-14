package httpapi

import (
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
