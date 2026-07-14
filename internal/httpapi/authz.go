package httpapi

import (
	"net/http"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

func (s *Server) requireServerAdmin(r *http.Request) error {
	if !userOf(r).IsServerAdmin() {
		return couch.ServerAdminRequired()
	}
	return nil
}

func (s *Server) security(r *http.Request, db *store.DB) (*couch.SecurityObj, error) {
	obj, err := s.store.GetSecurity(r.Context(), db)
	if err != nil {
		return nil, err
	}
	return couch.ParseSecurity(obj), nil
}

func (s *Server) requireMember(r *http.Request, db *store.DB) error {
	user := userOf(r)
	if user.IsServerAdmin() {
		return nil
	}
	security, err := s.security(r, db)
	if err != nil {
		return err
	}
	if !security.IsDBMember(user) {
		return couch.MemberRequired(user)
	}
	return nil
}

func (s *Server) requireDBAdmin(r *http.Request, db *store.DB) error {
	user := userOf(r)
	if user.IsServerAdmin() {
		return nil
	}
	security, err := s.security(r, db)
	if err != nil {
		return err
	}
	if !security.IsDBAdmin(user) {
		return couch.DBAdminRequired(user)
	}
	return nil
}
