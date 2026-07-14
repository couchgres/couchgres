package httpapi

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/mango"
	"github.com/couchgres/couchgres/internal/store"
)

// changesFilter decides which changes a filtered feed emits.
type changesFilter func(ctx context.Context, changes []store.Change) ([]bool, error)

// buildSelectorChangesFilter resolves filter=_selector: the selector rides
// in the POST body.
func buildSelectorChangesFilter(body map[string]any) (changesFilter, error) {
	rawSelector, present := body["selector"]
	if body == nil || !present {
		return nil, couch.BadRequest("Selector must be specified in POST payload")
	}
	selector, err := mango.Parse(rawSelector)
	if err != nil {
		// The changes endpoint reports selector problems as plain
		// bad_request (unlike _find's typed errors).
		if ce, ok := err.(*couch.Error); ok {
			return nil, couch.BadRequest(ce.Reason)
		}
		return nil, err
	}
	return func(ctx context.Context, changes []store.Change) ([]bool, error) {
		out := make([]bool, len(changes))
		for i, c := range changes {
			doc := make(map[string]any, len(c.Body)+3)
			doc["_id"] = c.ID
			if len(c.LeafRevs) > 0 {
				doc["_rev"] = c.LeafRevs[0].String()
			}
			if c.Deleted {
				doc["_deleted"] = true
			}
			for k, v := range c.Body {
				doc[k] = v
			}
			out[i] = selector.Matches(doc)
		}
		return out, nil
	}, nil
}

// buildJSChangesFilter resolves filter=ddocname/filtername to a JS filter
// function over batches of changed documents.
func (s *Server) buildJSChangesFilter(r *http.Request, db *store.DB, filter string) (changesFilter, error) {
	ddocName, filterName, found := strings.Cut(filter, "/")
	if !found || ddocName == "" || filterName == "" {
		return nil, couch.BadRequest("`filter` must be of the form `designname/filtername`")
	}
	ddoc, err := s.store.GetDoc(r.Context(), db, "_design/"+ddocName, nil)
	if err != nil {
		return nil, err
	}
	filters, _ := ddoc.Body["filters"].(map[string]any)
	src, _ := filters[filterName].(string)
	if src == "" {
		return nil, couch.NewError(404, "not_found", "missing json key: "+filterName)
	}
	sum := md5.Sum([]byte(src))
	sig := "filter-" + ddoc.Rev.String() + "-" + hex.EncodeToString(sum[:])
	reqRaw, err := s.filterRequestJSON(r, db)
	if err != nil {
		return nil, err
	}
	ddocRaw := ddocJSON(ddoc)
	return func(ctx context.Context, changes []store.Change) ([]bool, error) {
		docs := changeDocsJSON(changes)
		return s.js.FilterDocs(ctx, sig, src, docs, reqRaw, ddocRaw)
	}, nil
}

// buildViewChangesFilter resolves filter=_view&view=ddoc/name: a change
// passes when the view's map function emits at least one row for it.
func (s *Server) buildViewChangesFilter(r *http.Request, db *store.DB, view string) (changesFilter, error) {
	ddocName, viewName, found := strings.Cut(view, "/")
	if !found || ddocName == "" || viewName == "" {
		return nil, couch.BadRequest("`view` filter parameter is not provided.")
	}
	vg, err := s.loadViewGroup(r, db, ddocName)
	if err != nil {
		return nil, err
	}
	def, ok := vg.Views[viewName]
	if !ok {
		return nil, couch.NewError(404, "not_found", "missing_named_view")
	}
	sig := vg.Sig + "-changesfilter"
	return func(ctx context.Context, changes []store.Change) ([]bool, error) {
		docs := changeDocsJSON(changes)
		emits, err := s.js.MapDocs(ctx, sig, []string{def.Map}, vg.Lib, docs)
		if err != nil {
			return nil, err
		}
		out := make([]bool, len(changes))
		for i := range emits {
			out[i] = len(emits[i][0]) > 0
		}
		return out, nil
	}, nil
}

// changeDocsJSON renders the documents a filter function sees, tombstones
// included.
func changeDocsJSON(changes []store.Change) []json.RawMessage {
	docs := make([]json.RawMessage, len(changes))
	for i, c := range changes {
		doc := make(map[string]any, len(c.Body)+3)
		doc["_id"] = c.ID
		if len(c.LeafRevs) > 0 {
			doc["_rev"] = c.LeafRevs[0].String()
		}
		if c.Deleted {
			doc["_deleted"] = true
		}
		for k, v := range c.Body {
			doc[k] = v
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			raw = []byte(`{}`)
		}
		docs[i] = raw
	}
	return docs
}

// filterRequestJSON builds the CouchDB request object filters receive as
// their second argument.
func (s *Server) filterRequestJSON(r *http.Request, db *store.DB) (json.RawMessage, error) {
	query := make(map[string]any)
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			query[key] = values[0]
		}
	}
	user := userOf(r)
	var name any
	if !user.IsAnonymous() {
		name = user.Name
	}
	roles := user.Roles
	if roles == nil {
		roles = []string{}
	}
	return json.Marshal(map[string]any{
		"info":           map[string]any{"db_name": db.Name},
		"id":             nil,
		"method":         r.Method,
		"path":           strings.Split(strings.Trim(r.URL.Path, "/"), "/"),
		"requested_path": strings.Split(strings.Trim(r.URL.RequestURI(), "/"), "/"),
		"raw_path":       r.URL.RequestURI(),
		"query":          query,
		"headers":        map[string]any{},
		"body":           "undefined",
		"peer":           r.RemoteAddr,
		"form":           map[string]any{},
		"cookie":         map[string]any{},
		"userCtx": map[string]any{
			"db": db.Name, "name": name, "roles": roles,
		},
		"secObj": map[string]any{},
	})
}

// applyChangesFilter runs the filter and returns only passing changes,
// clearing doc bodies the client didn't ask for.
func applyChangesFilter(ctx context.Context, req *changesRequest, changes []store.Change) ([]store.Change, error) {
	if req.filterFn == nil || len(changes) == 0 {
		return changes, nil
	}
	pass, err := req.filterFn(ctx, changes)
	if err != nil {
		return nil, err
	}
	out := changes[:0]
	for i, ok := range pass {
		if !ok {
			continue
		}
		c := changes[i]
		if !req.userIncludeDocs {
			c.Body = nil
		}
		out = append(out, c)
	}
	return out, nil
}
