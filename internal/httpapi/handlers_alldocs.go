package httpapi

import (
	"net/http"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// authorizeList gates _all_docs: db members, except _users where listing
// other people's docs is db-admin-only.
func (s *Server) authorizeList(r *http.Request, db *store.DB) error {
	if db.Name == "_users" {
		return s.requireDBAdmin(r, db)
	}
	return s.requireMember(r, db)
}

func (s *Server) allDocsGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.authorizeList(r, db); err != nil {
		return err
	}
	req, err := buildAllDocsRequest(r.URL.Query(), nil)
	if err != nil {
		return err
	}
	page, err := s.runAllDocs(r, db, req)
	if err != nil {
		return err
	}
	writeJSON(w, 200, pageJSON(page, req.params.IncludeDocs))
	return nil
}

func (s *Server) allDocsPost(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.authorizeList(r, db); err != nil {
		return err
	}
	if err := requireJSONContentType(r); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	req, err := buildAllDocsRequest(r.URL.Query(), obj)
	if err != nil {
		return err
	}
	page, err := s.runAllDocs(r, db, req)
	if err != nil {
		return err
	}
	writeJSON(w, 200, pageJSON(page, req.params.IncludeDocs))
	return nil
}

func (s *Server) allDocsQueries(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.authorizeList(r, db); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, _ := body.(map[string]any)
	queries, ok := obj["queries"].([]any)
	if !ok {
		return couch.BadRequest("`queries` member must exist.")
	}
	results := make([]any, 0, len(queries))
	for _, entry := range queries {
		queryObj, ok := entry.(map[string]any)
		if !ok {
			return couch.BadRequest("Query must be an object")
		}
		req, err := buildAllDocsRequest(r.URL.Query(), queryObj)
		if err != nil {
			return err
		}
		page, err := s.runAllDocs(r, db, req)
		if err != nil {
			return err
		}
		results = append(results, pageJSON(page, req.params.IncludeDocs))
	}
	writeJSON(w, 200, map[string]any{"results": results})
	return nil
}

func (s *Server) runAllDocs(r *http.Request, db *store.DB, req *allDocsRequest) (*store.AllDocsPage, error) {
	if req.alwaysEmpty {
		zero := int64(0)
		empty, err := s.store.AllDocs(r.Context(), db,
			&store.AllDocsParams{Limit: &zero})
		if err != nil {
			return nil, err
		}
		empty.Rows = nil
		empty.Offset = &zero
		return empty, nil
	}
	if req.sendKeys {
		page, err := s.store.AllDocsKeys(r.Context(), db, req.keys,
			req.params.IncludeDocs, req.params.Conflicts, req.params.UpdateSeq)
		if err != nil {
			return nil, err
		}
		// Keys mode still honors descending (reversed request order), skip,
		// and limit, applied to the per-key rows in that order.
		if req.params.Descending {
			for i, j := 0, len(page.Rows)-1; i < j; i, j = i+1, j-1 {
				page.Rows[i], page.Rows[j] = page.Rows[j], page.Rows[i]
			}
		}
		if req.params.Skip > 0 {
			if req.params.Skip >= int64(len(page.Rows)) {
				page.Rows = nil
			} else {
				page.Rows = page.Rows[req.params.Skip:]
			}
		}
		if req.params.Limit != nil && *req.params.Limit < int64(len(page.Rows)) {
			page.Rows = page.Rows[:*req.params.Limit]
		}
		return page, nil
	}
	return s.store.AllDocs(r.Context(), db, &req.params)
}

func pageJSON(page *store.AllDocsPage, includeDocs bool) map[string]any {
	rows := make([]any, 0, len(page.Rows))
	for _, row := range page.Rows {
		if row.Missing {
			rows = append(rows, map[string]any{"key": row.ID, "error": "not_found"})
			continue
		}
		value := map[string]any{"rev": row.Rev.String()}
		if row.Deleted {
			value["deleted"] = true
		}
		out := map[string]any{"id": row.ID, "key": row.ID, "value": value}
		if includeDocs {
			if row.Body != nil {
				doc := (&store.DocRow{ID: row.ID, Rev: row.Rev, Body: row.Body}).JSON()
				if len(row.ConflictRevs) > 0 {
					doc["_conflicts"] = row.ConflictRevs
				}
				out["doc"] = doc
			} else {
				out["doc"] = nil
			}
		}
		rows = append(rows, out)
	}
	result := map[string]any{
		"total_rows": page.TotalRows,
		"offset":     page.Offset, // null in keys mode, matching CouchDB
		"rows":       rows,
	}
	if page.UpdateSeq != nil {
		result["update_seq"] = seqString(*page.UpdateSeq)
	}
	return result
}
