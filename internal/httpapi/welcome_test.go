package httpapi

import (
	"net/http"
	"testing"

	"github.com/couchgres/couchgres/internal/buildinfo"
)

func TestWelcomeAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name            string
		method          string
		path            string
		cookie          bool
		badBasic        bool
		requiredSection string
		wantStatus      int
	}{
		{name: "anonymous", method: "GET", path: "/", wantStatus: 200},
		{name: "anonymous head", method: "HEAD", path: "/", wantStatus: 200},
		{name: "invalid cookie", method: "GET", path: "/", cookie: true, wantStatus: 200},
		{name: "invalid cookie head", method: "HEAD", path: "/", cookie: true, wantStatus: 200},
		{name: "invalid explicit credentials", method: "GET", path: "/", badBasic: true, wantStatus: 401},
		{name: "invalid explicit credentials and cookie", method: "GET", path: "/", badBasic: true, cookie: true, wantStatus: 401},
		{name: "authentication required", method: "GET", path: "/", requiredSection: "chttpd", wantStatus: 401},
		{name: "authentication required with cookie", method: "GET", path: "/", cookie: true, requiredSection: "chttpd", wantStatus: 401},
		{name: "authentication required for head", method: "HEAD", path: "/", cookie: true, requiredSection: "chttpd", wantStatus: 401},
		{name: "legacy authentication required", method: "GET", path: "/", cookie: true, requiredSection: "couch_httpd_auth", wantStatus: 401},
		{name: "cookie exception excludes writes", method: "POST", path: "/", cookie: true, wantStatus: 401},
		{name: "cookie exception excludes databases", method: "GET", path: "/private_db", cookie: true, wantStatus: 401},
		{name: "cookie exception excludes admin endpoints", method: "GET", path: "/_all_dbs", cookie: true, wantStatus: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &configCache{tree: make(map[string]map[string]string)}
			if tc.requiredSection != "" {
				config.insert(tc.requiredSection, "require_valid_user", "true")
			}
			s := &Server{config: config, serverUUID: "test-server-uuid"}
			handler := couchHeaders(s.authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !userOf(r).IsAnonymous() {
					t.Fatal("invalid or absent credentials must not authenticate a user")
				}
				h(s.welcome).ServeHTTP(w, r)
			})))
			var headers []string
			if tc.cookie {
				headers = append(headers, "Cookie", "AuthSession=invalid")
			}
			if tc.badBasic {
				headers = append(headers, "Authorization", "Basic invalid")
			}
			resp := send(t, handler, tc.method, tc.path, nil, headers...)
			if resp.status != tc.wantStatus {
				t.Fatalf("status: got %+v, want %d", resp, tc.wantStatus)
			}
			if tc.wantStatus != 200 {
				return
			}
			build := buildinfo.Current()
			vendor, ok := resp.body["vendor"].(map[string]any)
			if !ok || vendor["name"] != "couchgres" || vendor["version"] != build.Version ||
				resp.body["git_sha"] != build.GitSHA || resp.body["version"] != couchDBVersion ||
				resp.body["couchdb"] != "Welcome" || resp.body["uuid"] != s.serverUUID {
				t.Fatalf("welcome metadata: %+v", resp.body)
			}
			if resp.header.Get("Server") != "CouchDB/"+couchDBVersion+" (Erlang OTP/26)" {
				t.Fatalf("compatibility header: %q", resp.header.Get("Server"))
			}
		})
	}
}
