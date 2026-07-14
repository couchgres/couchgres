package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// _local/{docid} stores unversioned per-node documents (replication checkpoints).

func (s *Server) localDB(r *http.Request) (*store.DB, error) {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return nil, err
	}
	if err := s.requireMember(r, db); err != nil {
		return nil, err
	}
	return db, nil
}

func (s *Server) localGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.localDB(r)
	if err != nil {
		return err
	}
	doc, err := s.store.GetLocalDoc(r.Context(), db, "_local/"+r.PathValue("docid"))
	if err != nil {
		return err
	}
	out := map[string]any{"_id": doc.ID, "_rev": doc.Rev()}
	for k, v := range doc.Body {
		out[k] = v
	}
	writeJSON(w, 200, out)
	return nil
}

func (s *Server) localPut(w http.ResponseWriter, r *http.Request) error {
	db, err := s.localDB(r)
	if err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	// Local docs are unversioned. Any _rev (like the "0-N" counters
	// replicators send back) is accepted and ignored. Writes always win.
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Document must be a JSON object")
	}
	// The path names the doc. A body _id is ignored entirely (CouchDB
	// stores {"_id":"anything"} PUT to /_local/foo as _local/foo).
	id := "_local/" + r.PathValue("docid")
	content := make(map[string]any, len(obj))
	for k, v := range obj {
		if k == "_id" || k == "_rev" || k == "_revisions" {
			continue
		}
		content[k] = v
	}
	doc, err := s.store.PutLocalDoc(r.Context(), db, id, content)
	if err != nil {
		return err
	}
	writeJSON(w, 201, map[string]any{"ok": true, "id": id, "rev": doc.Rev()})
	return nil
}

func (s *Server) localDelete(w http.ResponseWriter, r *http.Request) error {
	db, err := s.localDB(r)
	if err != nil {
		return err
	}
	id := "_local/" + r.PathValue("docid")
	if err := s.store.DeleteLocalDoc(r.Context(), db, id); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"ok": true, "id": id, "rev": "0-0"})
	return nil
}

// localDocs handles GET /{db}/_local_docs and lists local documents.
func (s *Server) localDocs(w http.ResponseWriter, r *http.Request) error {
	db, err := s.localDB(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	// POST carries params in the body. A query-string param wins over its
	// body twin (probed CouchDB precedence for _local_docs).
	var body map[string]any
	if r.Method == "POST" {
		raw, err := readHTTPBody(r)
		if err != nil {
			return err
		}
		if len(raw) > 0 {
			if v, err := couch.DecodeJSON(raw); err == nil {
				body, _ = v.(map[string]any)
			}
		}
	}
	getParam := func(name string) (any, bool) {
		if q.Has(name) {
			v, err := couch.DecodeJSON([]byte(q.Get(name)))
			if err == nil {
				return v, true
			}
		}
		if body != nil {
			if v, ok := body[name]; ok {
				return v, true
			}
		}
		return nil, false
	}
	includeDocs, err := boolParam(q, "include_docs", false)
	if err != nil {
		return err
	}
	var keys []string
	if v, ok := getParam("keys"); ok {
		items, isArray := v.([]any)
		if !isArray {
			return couch.BadRequest("`keys` member must be an array.")
		}
		keys = make([]string, 0, len(items))
		for _, item := range items {
			if s, isStr := item.(string); isStr {
				keys = append(keys, s)
			}
		}
	}
	limit, skip := int64(-1), int64(0)
	if v, ok := getParam("limit"); ok {
		if n, isNum := v.(json.Number); isNum {
			limit, _ = n.Int64()
		}
	}
	if v, ok := getParam("skip"); ok {
		if n, isNum := v.(json.Number); isNum {
			skip, _ = n.Int64()
		}
	}
	docs, err := s.store.ListLocalDocs(r.Context(), db, includeDocs)
	if err != nil {
		return err
	}
	if keys != nil {
		wanted := make(map[string]bool, len(keys))
		for _, k := range keys {
			wanted[k] = true
		}
		filtered := docs[:0]
		for _, doc := range docs {
			if wanted[doc.ID] {
				filtered = append(filtered, doc)
			}
		}
		docs = filtered
	}
	if skip > 0 {
		if skip >= int64(len(docs)) {
			docs = docs[:0]
		} else {
			docs = docs[skip:]
		}
	}
	if limit >= 0 && int64(len(docs)) > limit {
		docs = docs[:limit]
	}
	rows := make([]any, 0, len(docs))
	for _, doc := range docs {
		row := map[string]any{
			"id":    doc.ID,
			"key":   doc.ID,
			"value": map[string]string{"rev": doc.Rev()},
		}
		if includeDocs {
			body := map[string]any{"_id": doc.ID, "_rev": doc.Rev()}
			for k, v := range doc.Body {
				body[k] = v
			}
			row["doc"] = body
		}
		rows = append(rows, row)
	}
	writeJSON(w, 200, map[string]any{
		"total_rows": len(rows),
		"offset":     nil,
		"rows":       rows,
	})
	return nil
}
