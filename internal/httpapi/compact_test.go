package httpapi

import (
	"net/http"
	"testing"
)

// TestKeepSupersededBodiesAndCompact covers the couchgres/
// keep_superseded_bodies option and the observable parts of _compact:
// with the option on, superseded and pre-deletion bodies stay readable
// via ?rev= until compaction. Compaction drops them and reclaims
// attachment rows no leaf revision references.
func TestKeepSupersededBodiesAndCompact(t *testing.T) {
	h := testHandler(t)
	const db = "/keepbodies"
	send(t, h, "DELETE", db, nil, testAdminAuth, adminAuth()) // previous run's leftovers
	if resp := send(t, h, "PUT", db, nil, testAdminAuth, adminAuth()); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	t.Cleanup(func() {
		// The database and config tree persist in the shared test
		// database. Do not leak the option into other tests.
		send(t, h, "DELETE", db, nil, testAdminAuth, adminAuth())
		send(t, h, "DELETE", "/_node/_local/_config/couchgres/keep_superseded_bodies",
			nil, testAdminAuth, adminAuth())
	})
	put := func(path string, body any) response {
		t.Helper()
		return send(t, h, "PUT", path, body, testAdminAuth, adminAuth())
	}
	get := func(path string) response {
		t.Helper()
		return send(t, h, "GET", path, nil, testAdminAuth, adminAuth())
	}
	rev := func(resp response) string {
		t.Helper()
		if resp.status != 201 {
			t.Fatalf("write failed: %+v", resp)
		}
		return resp.body["rev"].(string)
	}

	// Default: a superseded body is gone as soon as it is replaced.
	rev1 := rev(put(db+"/doc", map[string]any{"v": 1}))
	rev2 := rev(put(db+"/doc", map[string]any{"_rev": rev1, "v": 2}))
	if resp := get(db + "/doc?rev=" + rev1); resp.status != 404 {
		t.Fatalf("superseded rev readable with option off: %+v", resp)
	}

	if resp := put("/_node/_local/_config/couchgres/keep_superseded_bodies",
		"true"); resp.status != 200 {
		t.Fatalf("set config: %+v", resp)
	}

	// Now superseded bodies survive, including the one a deletion replaces.
	rev3 := rev(put(db+"/doc", map[string]any{"_rev": rev2, "v": 3}))
	if resp := get(db + "/doc?rev=" + rev2); resp.status != 200 || resp.body["v"] != float64(2) {
		t.Fatalf("superseded rev not kept: %+v", resp)
	}
	if resp := send(t, h, "DELETE", db+"/doc?rev="+rev3, nil,
		testAdminAuth, adminAuth()); resp.status != 200 {
		t.Fatalf("delete doc: %+v", resp)
	}
	if resp := get(db + "/doc"); resp.status != 404 {
		t.Fatalf("deleted doc should 404: %+v", resp)
	}
	if resp := get(db + "/doc?rev=" + rev3); resp.status != 200 || resp.body["v"] != float64(3) {
		t.Fatalf("pre-deletion body not recoverable: %+v", resp)
	}

	// An attachment carried across an update leaves rows under the old
	// rev. Those serve ?rev= reads until compaction reclaims them.
	arev1 := rev(put(db+"/att1", map[string]any{"a": 1}))
	arev2 := rev(put(db+"/att1/file.txt?rev="+arev1, "hello"))
	docResp := get(db + "/att1")
	if docResp.status != 200 {
		t.Fatalf("read attachment doc: %+v", docResp)
	}
	if resp := put(db+"/att1", map[string]any{
		"_rev": arev2, "a": 2, "_attachments": docResp.body["_attachments"],
	}); resp.status != 201 {
		t.Fatalf("carry attachment: %+v", resp)
	}
	if resp := get(db + "/att1/file.txt?rev=" + arev2); resp.status != 200 {
		t.Fatalf("old-rev attachment should serve before compact: %+v", resp)
	}

	// Compaction: kept bodies drop, orphaned attachment rows go, and the
	// live document plus its current attachment are untouched.
	compact := send(t, h, "POST", db+"/_compact", map[string]any{},
		testAdminAuth, adminAuth())
	if compact.status != 202 {
		t.Fatalf("compact: %+v", compact)
	}
	for _, r := range []string{rev2, rev3} {
		if resp := get(db + "/doc?rev=" + r); resp.status != 404 {
			t.Fatalf("rev %s should be gone after compact: %+v", r, resp)
		}
	}
	if resp := get(db + "/att1/file.txt"); resp.status != http.StatusOK {
		t.Fatalf("live attachment lost by compact: %+v", resp)
	}
	if resp := get(db + "/att1/file.txt?rev=" + arev2); resp.status != 404 {
		t.Fatalf("old-rev attachment should be reclaimed: %+v", resp)
	}
	if resp := get(db + "/att1"); resp.status != 200 || resp.body["a"] != float64(2) {
		t.Fatalf("live doc damaged by compact: %+v", resp)
	}
}
