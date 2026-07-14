package httpapi

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

func (s *Server) sessionPost(w http.ResponseWriter, r *http.Request) error {
	name, password, err := sessionCredentials(r)
	if err != nil {
		return err
	}
	record, found, err := s.lookupUser(r.Context(), name)
	if err != nil {
		return err
	}
	if !found || record.Password == nil ||
		!couch.VerifyPassword(password, record.Password) {
		return couch.Unauthorized("Name or password is incorrect.")
	}

	token := couch.EncodeSessionCookie(
		s.config.cookieSecret(), record.Salt, record.Name, time.Now().Unix())
	setSessionCookie(w, token)

	if next := r.URL.Query().Get("next"); strings.HasPrefix(next, "/") {
		w.Header().Set("Location", next)
		writeJSON(w, 302, map[string]any{"ok": true})
		return nil
	}
	writeJSON(w, 200, map[string]any{
		"ok":    true,
		"name":  record.Name,
		"roles": rolesOrEmpty(record.Roles),
	})
	return nil
}

func (s *Server) sessionGet(w http.ResponseWriter, r *http.Request) error {
	user := userOf(r)
	handlers := []string{"cookie", "default"}
	if len(s.config.section("jwt_keys")) > 0 {
		handlers = append(handlers, "jwt")
	}
	if s.config.proxyAuthEnabled() {
		handlers = append(handlers, "proxy")
	}
	info := map[string]any{
		"authentication_handlers": handlers,
	}
	// CouchDB only names the db and method for authenticated sessions.
	if user.Authenticated != "" {
		info["authentication_db"] = "_users"
		info["authenticated"] = user.Authenticated
	}
	writeJSON(w, 200, map[string]any{
		"ok": true,
		"userCtx": map[string]any{
			"name":  user.NameJSON(),
			"roles": user.RolesJSON(),
		},
		"info": info,
	})
	return nil
}

func (s *Server) sessionDelete(w http.ResponseWriter, r *http.Request) error {
	clearSessionCookie(w)
	writeJSON(w, 200, map[string]any{"ok": true})
	return nil
}

// sessionCredentials reads name/password from a JSON or form-encoded body.
func sessionCredentials(r *http.Request) (string, string, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/x-www-form-urlencoded") {
		data, err := readHTTPBody(r)
		if err != nil {
			return "", "", err
		}
		form, err := url.ParseQuery(string(data))
		if err != nil {
			return "", "", couch.BadRequest("invalid form body")
		}
		name := form.Get("name")
		if name == "" {
			name = form.Get("username") // accepted alias, like CouchDB
		}
		return requireCredentials(name, form.Get("password"))
	}
	body, err := readJSONBody(r)
	if err != nil {
		return "", "", err
	}
	obj, _ := body.(map[string]any)
	name, _ := obj["name"].(string)
	if name == "" {
		name, _ = obj["username"].(string) // accepted alias, like CouchDB
	}
	password, _ := obj["password"].(string)
	return requireCredentials(name, password)
}

func requireCredentials(name, password string) (string, string, error) {
	if name == "" {
		// TODO(compat): confirm CouchDB's exact reason string.
		return "", "", couch.BadRequest("request body must contain a username")
	}
	return name, password, nil
}

func rolesOrEmpty(roles []string) []string {
	if roles == nil {
		return []string{}
	}
	return roles
}
