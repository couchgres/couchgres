package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

func (s *Server) dbGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	infos, err := s.store.DatabaseInfos(r.Context(), []string{db.Name})
	if err != nil {
		return err
	}
	if len(infos) == 0 {
		return couch.DBNotFound()
	}
	writeJSON(w, 200, dbInfoJSON(infos[0]))
	return nil
}

func (s *Server) securityGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	obj, err := s.store.GetSecurity(r.Context(), db)
	if err != nil {
		return err
	}
	writeJSON(w, 200, obj)
	return nil
}

func (s *Server) securityPut(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	if err := couch.ValidateSecurity(body); err != nil {
		return err
	}
	if err := s.store.SetSecurity(r.Context(), db, body.(map[string]any)); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
	return nil
}

func dbInfoJSON(item store.DatabaseInfo) map[string]any {
	db, info := &item.DB, &item.Info
	props := map[string]any{}
	if db.Partitioned {
		props["partitioned"] = true
	}
	// external is the user-data size (doc JSON + attachment bytes), so it
	// matches the sum of the partition sizes. file/active are the physical
	// relations, which are Postgres's business.
	return map[string]any{
		"db_name":       db.Name,
		"doc_count":     info.DocCount,
		"doc_del_count": info.DocDelCount,
		"update_seq":    seqString(info.UpdateSeq),
		"purge_seq":     strconv.FormatInt(info.PurgeSeq, 10) + "-couchgres",
		"sizes": map[string]int64{
			"file":     info.SizeBytes,
			"external": info.ExternalSize,
			"active":   info.SizeBytes,
		},
		"props":               props,
		"compact_running":     false,
		"disk_format_version": 8,
		"cluster":             map[string]int{"n": 1, "q": 2, "r": 1, "w": 1},
		"instance_start_time": db.InstanceStartTime,
	}
}

func (s *Server) dbPut(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	q := r.URL.Query()
	partitioned, err := boolParam(q, "partitioned", false)
	if err != nil {
		return couch.BadRequest("Invalid `partitioned` parameter")
	}
	name := r.PathValue("db")
	if partitioned && strings.HasPrefix(name, "_") {
		return couch.BadRequest("Cannot partition a system database")
	}
	// ?q= and ?n= are accepted for compatibility. Sharding is Postgres's job.
	if err := s.store.CreateDatabase(r.Context(), name, partitioned); err != nil {
		return err
	}
	// Location keeps the escaped path so names with %2F round-trip.
	w.Header().Set("Location", "http://"+s.externalHost(r)+r.URL.EscapedPath())
	writeJSON(w, 201, map[string]bool{"ok": true})
	return nil
}

func (s *Server) dbDelete(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	if r.URL.Query().Has("rev") {
		return couch.BadRequest("You tried to DELETE a database with a ?=rev parameter. " +
			"Did you mean to DELETE a document instead?")
	}
	if err := s.store.DeleteDatabase(r.Context(), r.PathValue("db")); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
	return nil
}

func (s *Server) dbMethodNotAllowed(_ http.ResponseWriter, r *http.Request) error {
	return couch.MethodNotAllowed("DELETE,GET,HEAD,POST,PUT")
}

func (s *Server) revsLimitGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	writeJSON(w, 200, db.RevsLimit)
	return nil
}

func (s *Server) revsLimitPut(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("db")
	db, err := s.store.GetDB(r.Context(), name)
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	num, ok := body.(json.Number)
	limit, convErr := num.Int64()
	if !ok || convErr != nil || limit < 1 {
		// TODO(differential): confirm CouchDB's exact reason string.
		return couch.BadRequest("Rev limit is not an integer")
	}
	if err := s.store.SetRevsLimit(r.Context(), name, int(limit)); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
	return nil
}

func seqString(seq int64) string {
	return strconv.FormatInt(seq, 10) + "-couchgres"
}

func randomHex32() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func jsonStringParam(q url.Values, names ...string) (*string, error) {
	for _, name := range names {
		if !q.Has(name) {
			continue
		}
		v, err := couch.DecodeJSON([]byte(q.Get(name)))
		if err != nil {
			return nil, couch.QueryParseError(name, q.Get(name))
		}
		s, ok := v.(string)
		if !ok {
			return nil, couch.QueryParseError(name, q.Get(name))
		}
		return &s, nil
	}
	return nil, nil
}
