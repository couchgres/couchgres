package httpapi

// Single-node cluster stubs and node introspection endpoints. couchgres is
// one healthy node named nonode@nohost with one shard. These endpoints say
// so truthfully.

import (
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/fauxton"
)

const nodeName = "nonode@nohost"

func (s *Server) membership(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"all_nodes":     []string{nodeName},
		"cluster_nodes": []string{nodeName},
	})
	return nil
}

func (s *Server) clusterSetupGet(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"state": "single_node_enabled"})
	return nil
}

func (s *Server) clusterSetupPost(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 201, map[string]any{"ok": true})
	return nil
}

func (s *Server) reshard(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"state": "running", "state_reason": nil,
		"completed": 0, "failed": 0, "running": 0, "stopped": 0, "total": 0,
	})
	return nil
}

func (s *Server) reshardState(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"state": "running", "reason": nil})
	return nil
}

func (s *Server) shards(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"shards": map[string]any{"00000000-ffffffff": []string{nodeName}},
	})
	return nil
}

func (s *Server) shardsDoc(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	if _, err := s.store.GetDocAny(r.Context(), db, r.PathValue("docid")); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"range": "00000000-ffffffff", "nodes": []string{nodeName},
	})
	return nil
}

func (s *Server) syncShards(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	writeJSON(w, 202, map[string]any{"ok": true})
	return nil
}

// searchUnavailable mirrors a CouchDB without Clouseau. Nouveau endpoints
// 404 like the handler doesn't exist (that's what a nouveau-less 3.5 does).
func (s *Server) searchUnavailable(w http.ResponseWriter, r *http.Request) error {
	return couch.NewError(503, "service unavailable", "Search is not available")
}

// Node introspection.

func (s *Server) nodeVersions(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"javascript_engine": map[string]any{"name": "quickjs"},
		"collation_driver": map[string]any{
			"name": "golang.org/x/text/collate",
		},
		"go": map[string]any{"version": runtime.Version()},
	})
	return nil
}

func (s *Server) nodeSystem(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	writeJSON(w, 200, map[string]any{
		"uptime":           int64(time.Since(s.startedAt).Seconds()),
		"memory":           map[string]any{"total": mem.Sys, "processes": mem.HeapInuse, "other": mem.StackInuse},
		"run_queue":        0,
		"process_count":    runtime.NumGoroutine(),
		"process_limit":    0,
		"context_switches": 0, "reductions": 0, "garbage_collection_count": mem.NumGC,
		"words_reclaimed": 0, "io_input": 0, "io_output": 0,
		"os_proc_count": 0, "stale_proc_count": 0, "distribution": map[string]any{},
		"message_queues": map[string]any{}, "internal_replication_jobs": 0,
	})
	return nil
}

// nodeStats serves the CouchDB stats tree shape with zeroed counters (real
// metrics not implemented yet).
func (s *Server) nodeStats(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	value := func(desc string) map[string]any {
		return map[string]any{"value": 0, "type": "counter", "desc": desc}
	}
	writeJSON(w, 200, map[string]any{
		"couchdb": map[string]any{
			"request_time": map[string]any{"value": map[string]any{"min": 0, "max": 0, "arithmetic_mean": 0, "n": 0}, "type": "histogram", "desc": "length of a request inside CouchDB without MochiWeb"},
			"httpd_request_methods": map[string]any{
				"GET": value("number of HTTP GET requests"), "POST": value("number of HTTP POST requests"),
				"PUT": value("number of HTTP PUT requests"), "DELETE": value("number of HTTP DELETE requests"),
			},
			"httpd_status_codes": map[string]any{},
			"database_reads":     value("number of times a document was read from a database"),
			"database_writes":    value("number of times a database was changed"),
			"open_databases":     value("number of open databases"),
			"open_os_files":      value("number of file descriptors CouchDB has open"),
		},
		"couch_replicator": map[string]any{},
		"couch_log":        map[string]any{},
		"fabric":           map[string]any{},
		"ddoc_cache":       map[string]any{},
		"global_changes":   map[string]any{},
		"mango":            map[string]any{},
		"mem3":             map[string]any{},
		"pread":            map[string]any{},
		"rexi":             map[string]any{},
		"fsync":            map[string]any{},
	})
	return nil
}

// Fauxton.

// utils serves the embedded Fauxton build (internal/fauxton).
func (s *Server) utils(w http.ResponseWriter, r *http.Request) error {
	// The trailing-slash-stripping middleware makes /_utils/ arrive as
	// /_utils, so a canonicalizing redirect would loop. Serve the index.
	rest := strings.TrimPrefix(r.URL.Path, "/_utils")
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		rest = "index.html"
	}
	clean := filepath.Clean(rest)
	if strings.HasPrefix(clean, "..") {
		return couch.DocMissing()
	}
	http.ServeFileFS(w, r, fauxton.FS(), clean)
	return nil
}
