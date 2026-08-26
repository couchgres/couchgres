package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
)

func rewriteTestServer(secure bool) *Server {
	tree := make(map[string]map[string]string)
	config := &configCache{tree: tree}
	if !secure {
		config.insert("chttpd", "secure_rewrites", "false")
	}
	config.insert("couch_httpd_auth", "x_auth_username", "X-Custom-User")
	config.insert("couch_httpd_auth", "x_auth_roles", "X-Custom-Roles")
	config.insert("couch_httpd_auth", "x_auth_token", "X-Custom-Token")
	config.insert("chttpd", "x_forwarded_host", "X-Custom-Forwarded-Host")
	return &Server{config: config}
}

func rewriteTestRequest(method string, user *couch.UserCtx) *http.Request {
	r := httptest.NewRequest(method, "/source/_design/app/_rewrite", nil)
	return r.WithContext(context.WithValue(r.Context(), userCtxKey{}, user))
}

func TestRewritePathWithinDatabase(t *testing.T) {
	for _, path := range []string{
		"/source", "/source/doc", "/source/_design/app/_show/fn",
	} {
		if !rewritePathWithinDatabase(path, "source") {
			t.Errorf("same-database path rejected: %q", path)
		}
	}
	for _, path := range []string{
		"/source2/doc", "/_node/_local/_config", "/source/%2e%2e/_node",
		"/source/%2F..%2F_node", "/source/%zz",
	} {
		if rewritePathWithinDatabase(path, "source") {
			t.Errorf("cross-scope path accepted: %q", path)
		}
	}
}

func TestAuthorizeRewrite(t *testing.T) {
	member := &couch.UserCtx{Name: "member", Authenticated: "cookie"}
	admin := &couch.UserCtx{Name: "admin", Roles: []string{"_admin"}, Authenticated: "cookie"}

	t.Run("same_scope_and_method", func(t *testing.T) {
		s := rewriteTestServer(true)
		method, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, member), "source", "/source/doc", "")
		if err != nil || method != http.MethodGet {
			t.Fatalf("method=%q error=%v", method, err)
		}
	})
	t.Run("cross_scope_default", func(t *testing.T) {
		s := rewriteTestServer(true)
		_, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, admin), "source", "/_node/_local/_config", "")
		assertRewriteError(t, err, 500, "insecure_rewrite_rule")
	})
	t.Run("malformed_config_fails_closed", func(t *testing.T) {
		s := rewriteTestServer(true)
		s.config.insert("chttpd", "secure_rewrites", "invalid")
		_, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, admin), "source", "/other/doc", "")
		assertRewriteError(t, err, 500, "insecure_rewrite_rule")
	})
	t.Run("method_change_default", func(t *testing.T) {
		s := rewriteTestServer(true)
		_, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, admin), "source", "/source/doc", "PUT")
		assertRewriteError(t, err, 500, "insecure_rewrite_rule")
	})
	t.Run("insecure_mode_requires_server_admin", func(t *testing.T) {
		s := rewriteTestServer(false)
		_, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, member), "source", "/other/doc", "PUT")
		assertRewriteError(t, err, 401, "unauthorized")
	})
	t.Run("insecure_mode_server_admin", func(t *testing.T) {
		s := rewriteTestServer(false)
		method, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, admin), "source", "/other/doc", "put")
		if err != nil || method != http.MethodPut {
			t.Fatalf("method=%q error=%v", method, err)
		}
	})
	t.Run("invalid_method", func(t *testing.T) {
		s := rewriteTestServer(false)
		_, err := s.authorizeRewrite(
			rewriteTestRequest(http.MethodGet, admin), "source", "/source/doc", "GET /admin")
		assertRewriteError(t, err, 500, "insecure_rewrite_rule")
	})
}

func TestRewriteHeadersAreSanitizedAndCannotBeReintroduced(t *testing.T) {
	s := rewriteTestServer(true)
	r := rewriteTestRequest(http.MethodGet, &couch.UserCtx{
		Name: "admin", Roles: []string{"_admin"}, Authenticated: "cookie",
	})
	for key, value := range map[string]string{
		"Authorization":           "Basic secret",
		"Cookie":                  "AuthSession=secret",
		"X-Custom-User":           "admin",
		"X-Custom-Roles":          "_admin",
		"X-Custom-Token":          "secret",
		"X-Custom-Forwarded-Host": "internal.example",
		"X-Forwarded-Host":        "internal.example",
		"X-HTTP-Method-Override":  "PUT",
		"Content-Type":            "application/json",
	} {
		r.Header.Set(key, value)
	}
	r.Header["authorization"] = []string{"Basic noncanonical-secret"}
	clone := s.sanitizedRewriteRequest(r)
	if got := clone.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("safe header removed: %q", got)
	}
	for _, key := range []string{
		"Authorization", "Cookie", "X-Custom-User", "X-Custom-Roles",
		"X-Custom-Token", "X-Custom-Forwarded-Host", "X-Forwarded-Host",
		"X-HTTP-Method-Override",
	} {
		if value := clone.Header.Get(key); value != "" {
			t.Errorf("sensitive header %s survived: %q", key, value)
		}
		if value := r.Header.Get(key); value == "" {
			t.Errorf("sanitizing clone mutated original header %s", key)
		}
		if err := s.validateRewriteHeaders(map[string]string{key: "value"}); err == nil {
			t.Errorf("rewrite was allowed to set %s", key)
		}
	}
	if _, ok := clone.Header["authorization"]; ok {
		t.Fatal("non-canonical authorization header survived")
	}
	if err := s.validateRewriteHeaders(
		map[string]string{"Content-Type": "application/json"}); err != nil {
		t.Fatalf("safe rewrite header rejected: %v", err)
	}
	if err := s.validateRewriteHeaders(
		map[string]string{"Bad Header": "value"}); err == nil {
		t.Fatal("invalid header name accepted")
	}
	if err := s.validateRewriteHeaders(
		map[string]string{"X-Test": "ok\r\ninjected: true"}); err == nil {
		t.Fatal("invalid header value accepted")
	}
}

func assertRewriteError(t *testing.T, err error, status int, name string) {
	t.Helper()
	ce, ok := err.(*couch.Error)
	if !ok || ce.Status != status || ce.Err != name {
		t.Fatalf("error=%v, want status=%d name=%q", err, status, name)
	}
}
