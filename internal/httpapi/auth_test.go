package httpapi

// Authentication and authorization tests against real Postgres.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSessionLifecycle(t *testing.T) {
	h := testHandler(t)

	// Anonymous session: null name, no authenticated marker.
	resp := send(t, h, "GET", "/_session", nil)
	if resp.status != 200 {
		t.Fatalf("anonymous session: %+v", resp)
	}
	userCtx := resp.body["userCtx"].(map[string]any)
	if userCtx["name"] != nil {
		t.Fatalf("anonymous name: %v", userCtx["name"])
	}
	if _, present := resp.body["info"].(map[string]any)["authenticated"]; present {
		t.Fatal("anonymous session must not report authenticated")
	}

	// Wrong password -> CouchDB's exact 401.
	resp = send(t, h, "POST", "/_session",
		decode(t, `{"name":"admin","password":"wrong"}`))
	if resp.status != 401 || resp.body["reason"] != "Name or password is incorrect." {
		t.Fatalf("bad login: %+v", resp)
	}

	// JSON login sets the cookie and reports roles.
	resp = send(t, h, "POST", "/_session",
		decode(t, `{"name":"admin","password":"secret"}`))
	if resp.status != 200 || resp.body["name"] != "admin" {
		t.Fatalf("login: %+v", resp)
	}
	roles := resp.body["roles"].([]any)
	if len(roles) != 1 || roles[0] != "_admin" {
		t.Fatalf("admin roles: %v", roles)
	}
	setCookie := resp.header.Get("Set-Cookie")
	if !strings.Contains(setCookie, "AuthSession=") ||
		!strings.Contains(setCookie, "HttpOnly") {
		t.Fatalf("Set-Cookie: %q", setCookie)
	}
	cookie := strings.SplitN(strings.TrimPrefix(setCookie, "AuthSession="), ";", 2)[0]

	// The cookie authenticates requests and carries admin powers.
	resp = send(t, h, "GET", "/_session", nil, "Cookie", "AuthSession="+cookie)
	userCtx = resp.body["userCtx"].(map[string]any)
	if userCtx["name"] != "admin" {
		t.Fatalf("cookie session: %+v", resp.body)
	}
	if resp.body["info"].(map[string]any)["authenticated"] != "cookie" {
		t.Fatalf("authenticated marker: %+v", resp.body)
	}
	resp = send(t, h, "PUT", "/auth_cookie_db", nil, "Cookie", "AuthSession="+cookie)
	if resp.status != 201 {
		t.Fatalf("cookie admin op: %+v", resp)
	}
	send(t, h, "DELETE", "/auth_cookie_db", nil, "Cookie", "AuthSession="+cookie)

	// A tampered cookie degrades to anonymous.
	resp = send(t, h, "PUT", "/auth_tampered", nil, "Cookie", "AuthSession=AAAA"+cookie)
	if resp.status != 401 {
		t.Fatalf("tampered cookie: %+v", resp)
	}

	// Form-encoded login works too.
	resp = sendForm(t, h, "/_session", "name=admin&password=secret")
	if resp.status != 200 {
		t.Fatalf("form login: %+v", resp)
	}

	// Logout clears the cookie.
	resp = send(t, h, "DELETE", "/_session", nil)
	if resp.status != 200 {
		t.Fatalf("logout: %+v", resp)
	}
	if !strings.Contains(resp.header.Get("Set-Cookie"), "Max-Age=0") {
		t.Fatalf("logout Set-Cookie: %q", resp.header.Get("Set-Cookie"))
	}
}

func TestServerAdminEnforcement(t *testing.T) {
	h := testHandler(t)

	// Anonymous cannot create/delete dbs or list them.
	resp := send(t, h, "PUT", "/auth_admin_only", nil)
	if resp.status != 401 || resp.body["reason"] != "You are not a server admin." {
		t.Fatalf("anonymous db create: %+v", resp)
	}
	if resp := send(t, h, "GET", "/_all_dbs", nil); resp.status != 401 {
		t.Fatalf("anonymous _all_dbs: %+v", resp)
	}
	if resp := send(t, h, "GET", "/_node/_local/_config", nil); resp.status != 401 {
		t.Fatalf("anonymous _config: %+v", resp)
	}

	// Basic admin auth works everywhere.
	resp = send(t, h, "PUT", "/auth_admin_only", nil, testAdminAuth, adminAuth())
	if resp.status != 201 {
		t.Fatalf("admin db create: %+v", resp)
	}
	defer send(t, h, "DELETE", "/auth_admin_only", nil, testAdminAuth, adminAuth())

	// Config endpoints round-trip for admins.
	resp = send(t, h, "PUT", "/_node/_local/_config/couchgres_test/knob",
		decode(t, `"eleven"`), testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("config put: %+v", resp)
	}
	resp = send(t, h, "GET", "/_node/_local/_config/couchgres_test/knob", nil,
		testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("config get: %+v", resp)
	}
	resp = send(t, h, "DELETE", "/_node/_local/_config/couchgres_test/knob", nil,
		testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("config delete: %+v", resp)
	}
	resp = send(t, h, "DELETE", "/_node/_local/_config/couchgres_test/knob", nil,
		testAdminAuth, adminAuth())
	if resp.status != 404 || resp.body["reason"] != "unknown_config_value" {
		t.Fatalf("config double delete: %+v", resp)
	}
	// Unknown node names fall through to generic routing, where unknown
	// reserved names are missing databases. CouchDB says 404 "no such node",
	// but our reason names a database. This assertion checks
	// the current shape so a change is noticed.
	resp = send(t, h, "GET", "/_node/nope/_config", nil, testAdminAuth, adminAuth())
	if resp.status != 404 || resp.body["error"] != "not_found" {
		t.Fatalf("bad node: %+v", resp)
	}
}

func TestUsersDBAndMembership(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()

	// Admin creates two users with plaintext passwords (hashed server-side).
	for _, name := range []string{"bob", "alice"} {
		resp := send(t, h, "PUT", "/_users/org.couchdb.user:"+name, decode(t,
			`{"type":"user","name":"`+name+`","roles":[],"password":"pw-`+name+`"}`),
			testAdminAuth, admin)
		if resp.status != 201 && resp.status != 409 {
			t.Fatalf("create user %s: %+v", name, resp)
		}
		if resp.status == 409 { // leftover from a previous run: reset password
			get := send(t, h, "GET", "/_users/org.couchdb.user:"+name, nil,
				testAdminAuth, admin)
			rev := get.body["_rev"].(string)
			resp = send(t, h, "PUT", "/_users/org.couchdb.user:"+name+"?rev="+rev,
				decode(t, `{"type":"user","name":"`+name+`","roles":[],"password":"pw-`+name+`"}`),
				testAdminAuth, admin)
			if resp.status != 201 {
				t.Fatalf("reset user %s: %+v", name, resp)
			}
		}
	}
	bob := basicAuth("bob", "pw-bob")

	// The stored doc has pbkdf2 fields, not the plaintext.
	resp := send(t, h, "GET", "/_users/org.couchdb.user:bob", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["password"] != nil ||
		resp.body["derived_key"] == nil {
		t.Fatalf("stored user doc: %+v", resp.body)
	}

	// Bob authenticates but is no server admin.
	resp = send(t, h, "GET", "/_session", nil, testAdminAuth, bob)
	if resp.body["userCtx"].(map[string]any)["name"] != "bob" {
		t.Fatalf("bob session: %+v", resp.body)
	}
	resp = send(t, h, "PUT", "/bob_wants_a_db", nil, testAdminAuth, bob)
	if resp.status != 401 || resp.body["reason"] != "You are not a server admin." {
		t.Fatalf("bob db create: %+v", resp)
	}

	// Own-doc access: bob reads his doc, not alice's. Role escalation is blocked.
	resp = send(t, h, "GET", "/_users/org.couchdb.user:bob", nil, testAdminAuth, bob)
	if resp.status != 200 {
		t.Fatalf("bob reads own doc: %+v", resp)
	}
	bobRev := resp.body["_rev"].(string)
	resp = send(t, h, "GET", "/_users/org.couchdb.user:alice", nil, testAdminAuth, bob)
	if resp.status != 403 {
		t.Fatalf("bob reads alice: %+v", resp)
	}
	resp = send(t, h, "PUT", "/_users/org.couchdb.user:bob?rev="+bobRev, decode(t,
		`{"type":"user","name":"bob","roles":["superuser"]}`), testAdminAuth, bob)
	if resp.status != 403 || resp.body["reason"] != "Only _admin may edit roles" {
		t.Fatalf("bob role escalation: %+v", resp)
	}
	resp = send(t, h, "GET", "/_users/_all_docs", nil, testAdminAuth, bob)
	if resp.status != 403 && resp.status != 401 {
		t.Fatalf("bob lists users: %+v", resp)
	}

	// Membership: a db restricted to alice locks bob out but lets alice in.
	send(t, h, "DELETE", "/auth_members", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/auth_members", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create members db: %+v", resp)
	}
	defer send(t, h, "DELETE", "/auth_members", nil, testAdminAuth, admin)
	resp = send(t, h, "PUT", "/auth_members/_security", decode(t,
		`{"members":{"names":["alice"],"roles":[]}}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("set security: %+v", resp)
	}

	alice := basicAuth("alice", "pw-alice")
	if resp := send(t, h, "GET", "/auth_members", nil); resp.status != 401 {
		t.Fatalf("anonymous member read: %+v", resp)
	}
	resp = send(t, h, "GET", "/auth_members", nil, testAdminAuth, bob)
	if resp.status != 403 || resp.body["reason"] != "You are not allowed to access this db." {
		t.Fatalf("bob member read: %+v", resp)
	}
	if resp := send(t, h, "GET", "/auth_members", nil, testAdminAuth, alice); resp.status != 200 {
		t.Fatalf("alice member read: %+v", resp)
	}
	resp = send(t, h, "PUT", "/auth_members/doc1", decode(t, `{"x":1}`),
		testAdminAuth, alice)
	if resp.status != 201 {
		t.Fatalf("alice member write: %+v", resp)
	}

	// Members cannot write design docs. DB admins can.
	resp = send(t, h, "PUT", "/auth_members/_design/app", decode(t, `{"a":1}`),
		testAdminAuth, alice)
	if resp.status != 403 {
		t.Fatalf("alice ddoc write: %+v", resp)
	}
	resp = send(t, h, "PUT", "/auth_members/_security", decode(t,
		`{"admins":{"names":["alice"],"roles":[]},"members":{"names":["alice"],"roles":[]}}`),
		testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("grant db admin: %+v", resp)
	}
	resp = send(t, h, "PUT", "/auth_members/_design/app", decode(t, `{"a":1}`),
		testAdminAuth, alice)
	if resp.status != 201 {
		t.Fatalf("alice db-admin ddoc write: %+v", resp)
	}

	// Session login via _users db credentials.
	resp = send(t, h, "POST", "/_session", decode(t, `{"name":"bob","password":"pw-bob"}`))
	if resp.status != 200 || resp.body["name"] != "bob" {
		t.Fatalf("bob session login: %+v", resp)
	}
}

func sendForm(t *testing.T, h http.Handler, path, form string) response {
	t.Helper()
	req := httptest.NewRequest("POST", path, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	resp := response{status: rec.Code, header: rec.Header()}
	if raw := rec.Body.Bytes(); len(raw) > 0 {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			if obj, ok := v.(map[string]any); ok {
				resp.body = obj
			}
		}
	}
	return resp
}
