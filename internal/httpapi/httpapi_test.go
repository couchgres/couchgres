package httpapi

// Integration tests for the HTTP API against Postgres via httptest.
// Skipped when Postgres is unavailable.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// Test credentials: hashed with few pbkdf2 iterations so tests stay fast.
const testAdminAuth = "Authorization"

func basicAuth(name, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(name+":"+password))
}

func adminAuth() string {
	return basicAuth("admin", "secret")
}

func testPostgresURL() string {
	url := os.Getenv("COUCHGRES_TEST_PG_URL")
	if url == "" {
		url = "postgres://localhost/couchgres_test"
	}
	return url
}

func testHTTPStore(t testing.TB) *store.Store {
	t.Helper()
	st, err := store.New(t.Context(), testPostgresURL(), 4)
	if err != nil {
		t.Skipf("Postgres unavailable at %s: %v", testPostgresURL(), err)
	}
	t.Cleanup(st.Close)
	return st
}

func testServer(t testing.TB, st *store.Store) *Server {
	t.Helper()
	serverUUID, err := st.Bootstrap(t.Context())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	seed := func(section, key, value string) {
		if _, err := st.ConfigSet(t.Context(), section, key, value); err != nil {
			t.Fatalf("config seed: %v", err)
		}
	}
	seed("couch_httpd_auth", "iterations", "10")
	seed("admins", "admin", couch.HashAdminPassword("secret", 10))
	handler, err := New(t.Context(), st, serverUUID)
	if err != nil {
		t.Fatalf("httpapi.New: %v", err)
	}
	return handler
}

func testHandler(t testing.TB) http.Handler {
	t.Helper()
	return testServer(t, testHTTPStore(t))
}

// openSecurity makes a db public ({} security), like pre-3.x CouchDB
// defaults. Fresh databases are admin-only.
func openSecurity(t testing.TB, h http.Handler, db string) {
	t.Helper()
	resp := send(t, h, "PUT", "/"+db+"/_security", decode(t, `{}`),
		testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("open security on %s: %+v", db, resp)
	}
}

type response struct {
	status int
	body   map[string]any
	array  []any
	scalar any
	header http.Header
}

func send(t testing.TB, handler http.Handler, method, path string, body any,
	headers ...string) response {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := response{status: rec.Code, header: rec.Header()}
	raw := rec.Body.Bytes()
	if len(raw) > 0 {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			switch tv := v.(type) {
			case map[string]any:
				resp.body = tv
			case []any:
				resp.array = tv
			default:
				resp.scalar = tv
			}
		}
	}
	return resp
}

func decode(t testing.TB, s string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestFullDocumentLifecycle(t *testing.T) {
	h := testHandler(t)
	send(t, h, "DELETE", "/api_lifecycle", nil, testAdminAuth, adminAuth())

	// Welcome + headers CouchDB clients sniff.
	resp := send(t, h, "GET", "/", nil)
	if resp.status != 200 || resp.body["couchdb"] != "Welcome" ||
		resp.body["version"] != "3.5.0" {
		t.Fatalf("welcome: %+v", resp)
	}
	if !strings.HasPrefix(resp.header.Get("Server"), "CouchDB/") {
		t.Fatalf("Server header: %q", resp.header.Get("Server"))
	}
	if resp.header.Get("X-Couch-Request-ID") == "" {
		t.Fatal("missing X-Couch-Request-ID")
	}

	// Create a database. A duplicate returns 412 file_exists.
	if resp := send(t, h, "PUT", "/api_lifecycle", nil, testAdminAuth, adminAuth()); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	resp = send(t, h, "PUT", "/api_lifecycle", nil, testAdminAuth, adminAuth())
	if resp.status != 412 || resp.body["error"] != "file_exists" {
		t.Fatalf("duplicate db: %+v", resp)
	}
	openSecurity(t, h, "api_lifecycle")

	// Document creation returns ETag and Location. The rev is shaped N-32hex.
	resp = send(t, h, "PUT", "/api_lifecycle/mydoc", decode(t, `{"x":1}`))
	if resp.status != 201 {
		t.Fatalf("create doc: %+v", resp)
	}
	rev1 := resp.body["rev"].(string)
	if !strings.HasPrefix(rev1, "1-") || len(rev1) != 34 {
		t.Fatalf("rev shape: %q", rev1)
	}
	if resp.header.Get("ETag") != `"`+rev1+`"` {
		t.Fatalf("ETag: %q", resp.header.Get("ETag"))
	}
	if !strings.HasSuffix(resp.header.Get("Location"), "/api_lifecycle/mydoc") {
		t.Fatalf("Location: %q", resp.header.Get("Location"))
	}

	// GET with revs and local_seq. Then make a conditional GET.
	resp = send(t, h, "GET", "/api_lifecycle/mydoc?revs=true&local_seq=true", nil)
	if resp.status != 200 || resp.body["_rev"] != rev1 {
		t.Fatalf("doc get: %+v", resp)
	}
	revisions := resp.body["_revisions"].(map[string]any)
	if revisions["start"].(float64) != 1 {
		t.Fatalf("_revisions: %+v", revisions)
	}
	if _, ok := resp.body["_local_seq"].(float64); !ok {
		t.Fatalf("_local_seq missing: %+v", resp.body)
	}
	resp = send(t, h, "GET", "/api_lifecycle/mydoc", nil, "If-None-Match", `"`+rev1+`"`)
	if resp.status != 304 {
		t.Fatalf("conditional get: %+v", resp)
	}

	// An update without a rev returns 409. An update with a rev returns 201.
	resp = send(t, h, "PUT", "/api_lifecycle/mydoc", decode(t, `{"x":2}`))
	if resp.status != 409 || resp.body["error"] != "conflict" {
		t.Fatalf("conflict: %+v", resp)
	}
	resp = send(t, h, "PUT", "/api_lifecycle/mydoc?rev="+rev1, decode(t, `{"x":2}`))
	if resp.status != 201 {
		t.Fatalf("update: %+v", resp)
	}
	rev2 := resp.body["rev"].(string)

	// COPY with Destination header.
	resp = send(t, h, "COPY", "/api_lifecycle/mydoc", nil, "Destination", "copied")
	if resp.status != 201 || resp.body["id"] != "copied" {
		t.Fatalf("copy: %+v", resp)
	}
	resp = send(t, h, "GET", "/api_lifecycle/copied", nil)
	if resp.status != 200 || resp.body["x"].(float64) != 2 {
		t.Fatalf("copied doc: %+v", resp)
	}

	// Unknown method on a doc -> CouchDB-shaped 405.
	resp = send(t, h, "PATCH", "/api_lifecycle/mydoc", nil)
	if resp.status != 405 || resp.body["error"] != "method_not_allowed" {
		t.Fatalf("405 shape: %+v", resp)
	}

	// POST with an auto-generated id. batch=ok returns 202 without a rev.
	resp = send(t, h, "POST", "/api_lifecycle", decode(t, `{"auto":true}`))
	if resp.status != 201 || len(resp.body["id"].(string)) != 32 {
		t.Fatalf("post: %+v", resp)
	}
	resp = send(t, h, "POST", "/api_lifecycle?batch=ok", decode(t, `{"b":1}`))
	if resp.status != 202 {
		t.Fatalf("batch: %+v", resp)
	}
	if _, hasRev := resp.body["rev"]; hasRev {
		t.Fatal("batch response must not include rev")
	}

	// _all_docs with include_docs.
	resp = send(t, h, "GET", "/api_lifecycle/_all_docs?include_docs=true", nil)
	if resp.status != 200 || resp.body["total_rows"].(float64) != 4 {
		t.Fatalf("all_docs: %+v", resp)
	}
	foundMydoc := false
	for _, row := range resp.body["rows"].([]any) {
		obj := row.(map[string]any)
		if obj["id"] == "mydoc" {
			foundMydoc = obj["doc"].(map[string]any)["x"].(float64) == 2
		}
	}
	if !foundMydoc {
		t.Fatal("mydoc not in _all_docs with body")
	}

	// keys mode: missing key row shape, null offset.
	resp = send(t, h, "POST", "/api_lifecycle/_all_docs",
		decode(t, `{"keys":["mydoc","absent"]}`))
	rows := resp.body["rows"].([]any)
	if rows[0].(map[string]any)["id"] != "mydoc" {
		t.Fatalf("keys row 0: %+v", rows[0])
	}
	if rows[1].(map[string]any)["error"] != "not_found" {
		t.Fatalf("keys row 1: %+v", rows[1])
	}
	if offset, present := resp.body["offset"]; !present || offset != nil {
		t.Fatalf("keys offset should be null: %v", resp.body["offset"])
	}

	// Delete doc (rev via If-Match), then 404 "deleted".
	resp = send(t, h, "DELETE", "/api_lifecycle/mydoc", nil, "If-Match", `"`+rev2+`"`)
	if resp.status != 200 {
		t.Fatalf("delete: %+v", resp)
	}
	resp = send(t, h, "GET", "/api_lifecycle/mydoc", nil)
	if resp.status != 404 || resp.body["reason"] != "deleted" {
		t.Fatalf("deleted get: %+v", resp)
	}

	// Design doc CRUD through the _design route (writes need a db admin).
	resp = send(t, h, "PUT", "/api_lifecycle/_design/app",
		decode(t, `{"views":{"all":{"map":"function(doc){emit(doc._id,null);}"}}}`),
		testAdminAuth, adminAuth())
	if resp.status != 201 || resp.body["id"] != "_design/app" {
		t.Fatalf("ddoc put: %+v", resp)
	}
	resp = send(t, h, "GET", "/api_lifecycle/_design/app", nil)
	if resp.status != 200 || resp.body["_id"] != "_design/app" {
		t.Fatalf("ddoc get: %+v", resp)
	}

	// Error shapes.
	resp = send(t, h, "GET", "/api_lifecycle/_no_such_endpoint", nil)
	if resp.status != 400 || resp.body["error"] != "illegal_docid" {
		t.Fatalf("reserved id: %+v", resp)
	}
	resp = send(t, h, "GET", "/nodb/doc", nil)
	if resp.status != 404 || resp.body["reason"] != "Database does not exist." {
		t.Fatalf("missing db: %+v", resp)
	}
	resp = send(t, h, "PUT", "/api_lifecycle/nul", decode(t, `{"s":"a\u0000b"}`))
	if resp.status != 400 {
		t.Fatalf("NUL string: %+v", resp)
	}

	if resp := send(t, h, "DELETE", "/api_lifecycle", nil, testAdminAuth, adminAuth()); resp.status != 200 {
		t.Fatalf("delete db: %+v", resp)
	}
}

func TestServerEndpoints(t *testing.T) {
	h := testHandler(t)

	resp := send(t, h, "GET", "/_uuids?count=3", nil)
	if resp.status != 200 || len(resp.body["uuids"].([]any)) != 3 {
		t.Fatalf("uuids: %+v", resp)
	}
	if resp := send(t, h, "GET", "/_uuids?count=1001", nil); resp.status != 400 {
		t.Fatalf("uuids cap: %+v", resp)
	}

	resp = send(t, h, "GET", "/_all_dbs", nil, testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("_all_dbs: %+v", resp)
	}
	foundUsers := false
	for _, name := range resp.array {
		if name == "_users" {
			foundUsers = true
		}
	}
	if !foundUsers {
		t.Fatalf("_users missing from _all_dbs: %v", resp.array)
	}

	resp = send(t, h, "POST", "/_dbs_info", decode(t, `{"keys":["_users","nope"]}`), testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("_dbs_info: %+v", resp)
	}
	first := resp.array[0].(map[string]any)
	if first["info"].(map[string]any)["db_name"] != "_users" {
		t.Fatalf("_dbs_info first: %+v", first)
	}
	if resp.array[1].(map[string]any)["error"] != "not_found" {
		t.Fatalf("_dbs_info second: %+v", resp.array[1])
	}

	resp = send(t, h, "GET", "/_up", nil)
	if resp.status != 200 || resp.body["status"] != "ok" {
		t.Fatalf("_up: %+v", resp)
	}

	// Fallback 404 is couch-shaped.
	resp = send(t, h, "GET", "/x/y/z/deep", nil)
	if resp.status != 404 || resp.body["error"] != "not_found" {
		t.Fatalf("fallback: %+v", resp)
	}
}
