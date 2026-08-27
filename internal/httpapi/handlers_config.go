package httpapi

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
)

// /_node/{node}/_config* is the CouchDB-compatible config tree. It is admin-only.
// Valid node names are routed literally in server.go. Unknown nodes fall
// through to generic routing (currently a 400 for the reserved _node name).
// TODO(compat): CouchDB says 404 "no such node: X".

func (s *Server) configAll(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, s.config.all())
	return nil
}

func (s *Server) configSection(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, s.config.section(r.PathValue("section")))
	return nil
}

func (s *Server) configGet(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	value, ok := s.config.get(r.PathValue("section"), r.PathValue("key"))
	if !ok {
		return couch.NewError(404, "not_found", "unknown_config_value")
	}
	writeJSON(w, 200, value)
	return nil
}

func (s *Server) configPut(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	value, ok := body.(string)
	if !ok {
		return couch.BadRequest("Config value must be a JSON string")
	}
	section, key := r.PathValue("section"), r.PathValue("key")
	if section == "chttpd" && key == "secure_rewrites" &&
		value != "true" && value != "false" {
		return couch.BadRequest("secure_rewrites must be true or false")
	}
	if err := validateProxyAuthConfigChange(section, key, value); err != nil {
		return couch.BadRequest(err.Error())
	}
	if err := validateRequestBodySizeChange(section, key, value); err != nil {
		return couch.BadRequest(err.Error())
	}
	if section == "couch_httpd_auth" &&
		(key == "iterations" || key == "min_iterations" || key == "max_iterations") {
		if err := s.config.validatePasswordIterationChange(key, value); err != nil {
			return couch.BadRequest(err.Error())
		}
	}
	// Plaintext admin passwords hash before they persist, as in CouchDB.
	// already-hashed values pass through so backups restore cleanly.
	if section == "admins" {
		value, err = s.passwordAuth.hashAdminPassword(
			r.Context(), value, s.config.passwordIterations())
		if err != nil {
			if r.Context().Err() != nil {
				return r.Context().Err()
			}
			return couch.BadRequest("Invalid administrator password hash")
		}
		if h, parsed := couch.ParseAdminPassword(value); parsed {
			minIterations, maxIterations := s.config.passwordIterationBounds()
			if err := h.Validate(minIterations, maxIterations); err != nil {
				return couch.BadRequest("Administrator password iterations are outside the configured range")
			}
		} else if strings.HasPrefix(value, "-pbkdf2") {
			return couch.BadRequest("Invalid administrator password hash")
		}
	}
	previous, err := s.config.set(r.Context(), s.store, section, key, value)
	if err != nil {
		return err
	}
	s.applyStoreConfig()
	writeJSON(w, 200, previous)
	return nil
}

func validateProxyAuthConfigChange(section, key, value string) error {
	if (section == "chttpd_auth" && key == "proxy_authentication") ||
		(section == "couch_httpd_auth" &&
			(key == "proxy_use_secret" || key == "proxy_allow_insecure_headers")) {
		if value != "true" && value != "false" {
			return fmt.Errorf("%s must be true or false", key)
		}
	}
	if section == "couch_httpd_auth" && key == "proxy_trusted_cidrs" {
		_, err := parseProxyTrustedCIDRs(value)
		return err
	}
	return nil
}

func (s *Server) configDelete(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	previous, existed, err := s.config.delete(r.Context(), s.store,
		r.PathValue("section"), r.PathValue("key"))
	if err != nil {
		return err
	}
	if !existed {
		return couch.NewError(404, "not_found", "unknown_config_value")
	}
	s.applyStoreConfig()
	writeJSON(w, 200, previous)
	return nil
}

// applyStoreConfig mirrors config-tree settings into long-lived runtime
// components. It is called at startup and after every config write or reload.
func (s *Server) applyStoreConfig() {
	s.store.SetKeepSupersededBodies(
		s.config.getBool("couchgres", "keep_superseded_bodies", false))
	bodyLimits := s.config.requestBodyLimits()
	s.bodyLimiter.configure(defaultRequestBodyLimiterConfig(
		bodyLimits.maxUpload()))
}
