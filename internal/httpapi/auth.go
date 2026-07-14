package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

type userCtxKey struct{}

// userOf returns the request's authenticated principal (never nil).
func userOf(r *http.Request) *couch.UserCtx {
	if u, ok := r.Context().Value(userCtxKey{}).(*couch.UserCtx); ok {
		return u
	}
	return couch.Anonymous()
}

// authenticate resolves credentials in CouchDB's handler order (cookie,
// Basic, JWT bearer, and proxy headers) and stores the userCtx in the request
// context. Bad credentials fail the request immediately with CouchDB's
// error shapes. Absent credentials mean anonymous.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, refresh, err := s.resolveUser(r)
		if err != nil {
			writeError(w, err)
			return
		}
		if refresh != "" {
			setSessionCookie(w, refresh)
		}
		if user.IsAnonymous() && s.config.requireValidUser() &&
			!(r.URL.Path == "/_session" || r.URL.Path == "/_up") {
			writeError(w, couch.Unauthorized("Authentication required."))
			return
		}
		ctx := context.WithValue(r.Context(), userCtxKey{}, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolveUser returns the principal and, for cookie sessions past 10% of
// their window, a fresh cookie value to slide the expiry.
func (s *Server) resolveUser(r *http.Request) (*couch.UserCtx, string, error) {
	if cookie, err := r.Cookie("AuthSession"); err == nil && cookie.Value != "" {
		return s.cookieUser(r, cookie.Value)
	}
	authorization := r.Header.Get("Authorization")
	if value, ok := strings.CutPrefix(authorization, "Basic "); ok {
		user, err := s.basicUser(r, value)
		return user, "", err
	}
	if value, ok := strings.CutPrefix(authorization, "Bearer "); ok {
		user, err := s.jwtUser(value)
		return user, "", err
	}
	if s.config.proxyAuthEnabled() {
		if user, err := s.proxyUser(r); user != nil || err != nil {
			return user, "", err
		}
	}
	return couch.Anonymous(), "", nil
}

func (s *Server) cookieUser(r *http.Request, token string) (*couch.UserCtx, string, error) {
	anonymous := func() (*couch.UserCtx, string, error) {
		// Malformed, expired, or stale cookies degrade to anonymous
		// (browser-friendly). TODO(compat): Confirm CouchDB's behavior for
		// bad MACs specifically.
		return couch.Anonymous(), "", nil
	}
	cookie, ok := couch.DecodeSessionCookie(token)
	if !ok {
		return anonymous()
	}
	record, found, err := s.lookupUser(r.Context(), cookie.Name)
	if err != nil {
		return nil, "", err
	}
	if !found || !cookie.Verify(s.config.cookieSecret(), record.Salt) {
		return anonymous()
	}
	timeout := s.config.sessionTimeout()
	age := time.Now().Unix() - cookie.IssuedAt
	if age < 0 || age > timeout {
		return anonymous()
	}
	user := &couch.UserCtx{
		Name:          record.Name,
		Roles:         record.Roles,
		Authenticated: "cookie",
	}
	refresh := ""
	if age > timeout/10 {
		refresh = couch.EncodeSessionCookie(
			s.config.cookieSecret(), record.Salt, record.Name, time.Now().Unix())
	}
	return user, refresh, nil
}

func (s *Server) basicUser(r *http.Request, encoded string) (*couch.UserCtx, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, couch.Unauthorized("Name or password is incorrect.")
	}
	name, password, ok := strings.Cut(string(raw), ":")
	if !ok {
		return nil, couch.Unauthorized("Name or password is incorrect.")
	}
	if user, ok := s.credentialCache.get(name, password); ok {
		return user, nil
	}
	record, found, err := s.lookupUser(r.Context(), name)
	if err != nil {
		return nil, err
	}
	if !found || record.Password == nil ||
		!couch.VerifyPassword(password, record.Password) {
		return nil, couch.Unauthorized("Name or password is incorrect.")
	}
	user := &couch.UserCtx{
		Name:          record.Name,
		Roles:         record.Roles,
		Authenticated: "default",
	}
	s.credentialCache.put(name, password, user)
	return user, nil
}

func (s *Server) jwtUser(token string) (*couch.UserCtx, error) {
	keys := s.config.section("jwt_keys")
	if len(keys) == 0 {
		return nil, couch.Unauthorized("JWT authentication is not configured.")
	}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{
		"HS256", "HS384", "HS512", "RS256", "RS384", "RS512",
		"ES256", "ES384", "ES512", "PS256", "PS384", "PS512",
	}))
	claims := jwt.MapClaims{}
	_, err := parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		alg, _ := t.Header["alg"].(string)
		family := "hmac"
		switch {
		case strings.HasPrefix(alg, "RS"), strings.HasPrefix(alg, "PS"):
			family = "rsa"
		case strings.HasPrefix(alg, "ES"):
			family = "ec"
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			kid = "_default"
		}
		value, ok := keys[family+":"+kid]
		if !ok {
			return nil, couch.Unauthorized("No key configured for " + family + ":" + kid)
		}
		switch family {
		case "hmac":
			// CouchDB stores HMAC secrets base64-encoded.
			secret, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return nil, couch.Unauthorized("Invalid HMAC key configuration.")
			}
			return secret, nil
		case "rsa":
			return jwt.ParseRSAPublicKeyFromPEM([]byte(value))
		default:
			return jwt.ParseECPublicKeyFromPEM([]byte(value))
		}
	})
	if err != nil {
		if ce, ok := err.(*couch.Error); ok {
			return nil, ce
		}
		return nil, couch.Unauthorized("Token verification failed: " + err.Error())
	}

	for _, required := range strings.Split(
		s.config.getOr("jwt_auth", "required_claims", ""), ",") {
		required = strings.TrimSpace(required)
		if required == "" {
			continue
		}
		if _, present := claims[required]; !present {
			return nil, couch.Unauthorized("Token missing required claim: " + required)
		}
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return nil, couch.Unauthorized("Token is missing a sub claim.")
	}

	rolesClaim := s.config.getOr("jwt_auth", "roles_claim_name", "_couchdb.roles")
	var roles []string
	switch v := claims[rolesClaim].(type) {
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				roles = append(roles, s)
			}
		}
	case string:
		for _, role := range strings.Split(v, ",") {
			if role = strings.TrimSpace(role); role != "" {
				roles = append(roles, role)
			}
		}
	}
	return &couch.UserCtx{Name: sub, Roles: roles, Authenticated: "jwt"}, nil
}

// proxyUser handles X-Auth-CouchDB-* headers. Returns (nil, nil) when the
// headers are absent.
func (s *Server) proxyUser(r *http.Request) (*couch.UserCtx, error) {
	name := r.Header.Get(
		s.config.getOr("couch_httpd_auth", "x_auth_username", "X-Auth-CouchDB-UserName"))
	if name == "" {
		return nil, nil
	}
	if s.config.getBool("couch_httpd_auth", "proxy_use_secret", false) {
		secret := s.config.getOr("couch_httpd_auth", "secret", "")
		token := r.Header.Get(
			s.config.getOr("couch_httpd_auth", "x_auth_token", "X-Auth-CouchDB-Token"))
		mac := hmac.New(sha1.New, []byte(secret))
		mac.Write([]byte(name))
		expected := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(expected), []byte(token)) {
			return nil, couch.Unauthorized("Proxy token is incorrect.")
		}
	}
	var roles []string
	rolesHeader := r.Header.Get(
		s.config.getOr("couch_httpd_auth", "x_auth_roles", "X-Auth-CouchDB-Roles"))
	for _, role := range strings.Split(rolesHeader, ",") {
		if role = strings.TrimSpace(role); role != "" {
			roles = append(roles, role)
		}
	}
	return &couch.UserCtx{Name: name, Roles: roles, Authenticated: "proxy"}, nil
}

// lookupUser finds credentials for a name: the admins config section first
// (roles ["_admin"], salt from the stored hash), then the _users database.
func (s *Server) lookupUser(ctx context.Context, name string) (*couch.UserRecord, bool, error) {
	if stored, ok := s.config.adminPassword(name); ok {
		record := &couch.UserRecord{Name: name, Roles: []string{"_admin"}}
		if h, ok := couch.ParseAdminPassword(stored); ok {
			record.Salt = h.Salt
			record.Password = h
		}
		return record, true, nil
	}
	users, err := s.store.GetDB(ctx, "_users")
	if err != nil {
		return nil, false, err
	}
	row, err := s.store.GetDoc(ctx, users, couch.UserDocPrefix+name, nil)
	if err != nil {
		if ce, ok := err.(*couch.Error); ok && ce.Status == 404 {
			return nil, false, nil
		}
		return nil, false, err
	}
	record, ok := couch.UserRecordFromDoc(row.Body)
	if !ok {
		return nil, false, nil
	}
	return record, true, nil
}

func setSessionCookie(w http.ResponseWriter, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     "AuthSession",
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     "AuthSession",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
}

// credentialCache remembers recent successful Basic verifications so every
// request doesn't pay a full pbkdf2 derivation (CouchDB keeps a similar
// auth cache). Entries expire after a minute, which also bounds how long a
// password change takes to propagate to cached credentials.
type credentialCache struct {
	mu      sync.Mutex
	entries map[[32]byte]credentialEntry
}

type credentialEntry struct {
	user    *couch.UserCtx
	expires time.Time
}

const credentialCacheMax = 500

func newCredentialCache() *credentialCache {
	return &credentialCache{entries: make(map[[32]byte]credentialEntry)}
}

func credentialKey(name, password string) [32]byte {
	return sha256.Sum256([]byte(name + "\x00" + password))
}

func (c *credentialCache) get(name, password string) (*couch.UserCtx, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[credentialKey(name, password)]
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}
	return entry.user, true
}

func (c *credentialCache) put(name, password string, user *couch.UserCtx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= credentialCacheMax {
		clear(c.entries)
	}
	c.entries[credentialKey(name, password)] = credentialEntry{
		user:    user,
		expires: time.Now().Add(time.Minute),
	}
}
