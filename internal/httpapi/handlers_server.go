package httpapi

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

func (s *Server) welcome(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, 200, map[string]any{
		"couchdb": "Welcome",
		"version": couchDBVersion,
		"git_sha": "couchgres",
		"uuid":    s.serverUUID,
		"features": []string{
			"access-ready", "partitioned", "pluggable-storage-engines",
			"reshard", "scheduler",
		},
		"vendor": map[string]string{"name": "couchgres"},
	})
	return nil
}

func (s *Server) favicon(http.ResponseWriter, *http.Request) error {
	return couch.DocMissing()
}

func (s *Server) up(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, 200, map[string]any{"status": "ok", "seeds": map[string]any{}})
	return nil
}

func (s *Server) notFound(http.ResponseWriter, *http.Request) error {
	return couch.NotFound()
}

func (s *Server) serverMethodNotAllowed(http.ResponseWriter, *http.Request) error {
	return couch.MethodNotAllowed("GET,HEAD")
}

func (s *Server) uuids(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	count := int64(1)
	if n, err := nonNegParam(q, "count"); err != nil {
		return err
	} else if n != nil {
		count = *n
	}
	if count > int64(s.maxUUIDCount) {
		// TODO(differential): confirm CouchDB's exact error for this.
		return couch.BadRequest("count parameter too large")
	}
	uuids := make([]string, count)
	algorithm := s.config.getOr("uuids", "algorithm", "random")
	for i := range uuids {
		uuids[i] = s.nextUUID(algorithm)
	}
	// Cache-busting headers, like CouchDB (uuids must never be cached).
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Pragma", "no-cache")
	setETag(w, randomUUID())
	writeJSON(w, 200, map[string]any{"uuids": uuids})
	return nil
}

// nextUUID generates one uuid with CouchDB's configured algorithm: random
// (default), sequential (rolling random prefix + monotonic suffix),
// utc_random (hex microseconds + random), or utc_id (hex microseconds +
// the configured suffix).
func (s *Server) nextUUID(algorithm string) string {
	switch algorithm {
	case "sequential":
		s.uuidMu.Lock()
		defer s.uuidMu.Unlock()
		if s.uuidPrefix == "" || s.uuidSeq > 0xfff000 {
			s.uuidPrefix = randomUUID()[:26]
			s.uuidSeq = 0
		}
		s.uuidSeq += rand.IntN(15) + 1
		return fmt.Sprintf("%s%06x", s.uuidPrefix, s.uuidSeq)
	case "utc_random":
		return fmt.Sprintf("%014x%s", s.nextUUIDMicros(), randomUUID()[:18])
	case "utc_id":
		return fmt.Sprintf("%014x%s", s.nextUUIDMicros(),
			s.config.getOr("uuids", "utc_id_suffix", ""))
	default:
		return randomUUID()
	}
}

// nextUUIDMicros returns a strictly increasing microsecond timestamp so
// utc_random/utc_id uuids sort even within one microsecond.
func (s *Server) nextUUIDMicros() int64 {
	s.uuidMu.Lock()
	defer s.uuidMu.Unlock()
	now := time.Now().UnixMicro()
	if now <= s.uuidLastMicros {
		now = s.uuidLastMicros + 1
	}
	s.uuidLastMicros = now
	return now
}

// requireAllDBsAccess enforces CouchDB 3.x's admin_only_all_dbs default.
func (s *Server) requireAllDBsAccess(r *http.Request) error {
	if !s.config.adminOnlyAllDBs() {
		return nil
	}
	return s.requireServerAdmin(r)
}

func (s *Server) allDBs(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireAllDBsAccess(r); err != nil {
		return err
	}
	q := r.URL.Query()
	names, err := s.store.ListDatabases(r.Context())
	if err != nil {
		return err
	}
	descending, err := boolParam(q, "descending", false)
	if err != nil {
		return err
	}
	if descending {
		reverse(names)
	}
	start, err := jsonStringParam(q, "startkey", "start_key")
	if err != nil {
		return err
	}
	end, err := jsonStringParam(q, "endkey", "end_key")
	if err != nil {
		return err
	}
	skip, err := nonNegParam(q, "skip")
	if err != nil {
		return err
	}
	limit, err := nonNegParam(q, "limit")
	if err != nil {
		return err
	}

	filtered := names[:0]
	for _, name := range names {
		afterStart := start == nil ||
			(!descending && name >= *start) || (descending && name <= *start)
		beforeEnd := end == nil ||
			(!descending && name <= *end) || (descending && name >= *end)
		if afterStart && beforeEnd {
			filtered = append(filtered, name)
		}
	}
	if skip != nil && *skip < int64(len(filtered)) {
		filtered = filtered[*skip:]
	} else if skip != nil {
		filtered = filtered[:0]
	}
	if limit != nil && *limit < int64(len(filtered)) {
		filtered = filtered[:*limit]
	}
	writeJSON(w, 200, filtered)
	return nil
}

func (s *Server) dbsInfoAll(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireAllDBsAccess(r); err != nil {
		return err
	}
	infos, err := s.store.DatabaseInfos(r.Context(), nil)
	if err != nil {
		return err
	}
	out := make([]any, 0, len(infos))
	for _, info := range infos {
		out = append(out, dbInfoJSON(info))
	}
	writeJSON(w, 200, out)
	return nil
}

func (s *Server) dbsInfo(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireAllDBsAccess(r); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, _ := body.(map[string]any)
	keys, ok := obj["keys"].([]any)
	if !ok {
		return couch.BadRequest("`keys` member must exist.")
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		name, ok := key.(string)
		if ok {
			names = append(names, name)
		}
	}
	infos, err := s.store.DatabaseInfos(r.Context(), names)
	if err != nil {
		return err
	}
	byName := make(map[string]store.DatabaseInfo, len(infos))
	for _, info := range infos {
		byName[info.DB.Name] = info
	}
	out := make([]any, 0, len(keys))
	for _, key := range keys {
		name, isStr := key.(string)
		if !isStr {
			out = append(out, map[string]any{"key": key, "error": "not_found"})
			continue
		}
		info, ok := byName[name]
		if !ok {
			out = append(out, map[string]any{"key": name, "error": "not_found"})
			continue
		}
		out = append(out, map[string]any{"key": name, "info": dbInfoJSON(info)})
	}
	writeJSON(w, 200, out)
	return nil
}

func randomUUID() string {
	return randomHex32()
}

func reverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// requireJSONContentType enforces CouchDB's validate_ctype on POST bodies
// that must be JSON.
func requireJSONContentType(r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return couch.BadContentType("Content-Type must be application/json")
	}
	return nil
}

func readJSONBody(r *http.Request) (any, error) {
	body, _, err := readRawJSONBody(r)
	return body, err
}

// readHTTPBody reads the request body, mapping an http.MaxBytesError
// (from the root MaxBytesReader) to CouchDB's 413 too_large.
func readHTTPBody(r *http.Request) ([]byte, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, mapBodyReadErr(err, "could not read request body")
	}
	return data, nil
}

func mapBodyReadErr(err error, fallback string) error {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return requestTooLarge()
	}
	return couch.BadRequest(fallback)
}

// readRawJSONBody also hands back the undecoded bytes: document writes hash
// the body in its original member order (CouchDB's rev algorithm), which the
// decoded map has lost.
func readRawJSONBody(r *http.Request) (any, []byte, error) {
	data, err := readHTTPBody(r)
	if err != nil {
		return nil, nil, err
	}
	body, err := couch.DecodeJSON(data)
	return body, data, err
}
