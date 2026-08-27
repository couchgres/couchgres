package httpapi

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/jsengine"
	"github.com/couchgres/couchgres/internal/store"
)

// jsMapper and jsReducer adapt the JS pool to the store's view interfaces.
type jsMapper struct{ pool *jsengine.Pool }

// jsPoolError shapes pool failures for HTTP. A timed-out script answers
// like CouchDB's killed couchjs.
func jsPoolError(err error) error {
	if errors.Is(err, jsengine.ErrClosed) {
		return couch.NewError(503, "service_unavailable", "JavaScript process pool is shutting down.")
	}
	if errors.Is(err, jsengine.ErrTimeout) {
		return couch.NewError(500, "os_process_error", "OS process timed out.")
	}
	if errors.Is(err, jsengine.ErrMemoryLimit) || errors.Is(err, jsengine.ErrOutputLimit) {
		return couch.NewError(500, "os_process_error", "OS process exceeded resource limit.")
	}
	return err
}

func isJSProcessError(err error) bool {
	return errors.Is(err, jsengine.ErrClosed) ||
		errors.Is(err, jsengine.ErrTimeout) ||
		errors.Is(err, jsengine.ErrMemoryLimit) ||
		errors.Is(err, jsengine.ErrOutputLimit)
}

func (m jsMapper) MapDocs(ctx context.Context, sig string, fns []string, lib map[string]any, docs []json.RawMessage) ([][][]store.ViewEmit, error) {
	raw, err := m.pool.MapDocs(ctx, sig, fns, lib, docs)
	if err != nil {
		return nil, jsPoolError(err)
	}
	out := make([][][]store.ViewEmit, len(raw))
	for d, perView := range raw {
		out[d] = make([][]store.ViewEmit, len(perView))
		for v, rows := range perView {
			emits := make([]store.ViewEmit, len(rows))
			for i, row := range rows {
				emits[i] = store.ViewEmit{Key: row.Key, Value: row.Value}
			}
			out[d][v] = emits
		}
	}
	return out, nil
}

type jsReducer struct{ pool *jsengine.Pool }

func (r jsReducer) ReduceGroups(ctx context.Context, sig, fn string, groups []store.ViewReduceGroup, rereduce bool) ([]json.RawMessage, error) {
	converted := make([]jsengine.ReduceGroup, len(groups))
	for i, g := range groups {
		converted[i] = jsengine.ReduceGroup{Keys: g.Keys, Values: g.Values}
	}
	out, err := r.pool.ReduceGroups(ctx, sig, fn, converted, rereduce)
	if err != nil {
		return nil, jsPoolError(err)
	}
	return out, nil
}

// loadViewGroup fetches the design doc and parses its views.
func (s *Server) loadViewGroup(r *http.Request, db *store.DB, ddocName string) (*store.ViewGroup, error) {
	vg, _, err := s.loadViewGroupSeq(r, db, ddocName)
	return vg, err
}

// loadViewGroupSeq also reports the design doc's seq. The response cache
// keys freshness on it.
func (s *Server) loadViewGroupSeq(r *http.Request, db *store.DB, ddocName string) (*store.ViewGroup, int64, error) {
	ddocID := "_design/" + ddocName
	doc, err := s.store.GetDoc(r.Context(), db, ddocID, nil)
	if err != nil {
		return nil, 0, err
	}
	vg, err := store.ViewGroupFromDDoc(ddocID, doc.Body)
	if err != nil {
		return nil, 0, err
	}
	if vg == nil {
		return nil, 0, couch.NewError(404, "not_found", "missing_named_view")
	}
	vg.ResolvePartitioned(db)
	return vg, doc.Seq, nil
}

// viewRequest is one parsed view query plus the transport options that
// don't live in store.ViewQuery.
type viewRequest struct {
	query       *store.ViewQuery
	update      string // "true", "false", "lazy"
	attachments bool
}

// buildViewQuery merges query-string parameters with an optional POST body.
// On a collision, the query string wins (CouchDB says "query takes precedence").
// Raw JSON key bounds are preserved verbatim from the query string for
// collation fidelity.
func buildViewQuery(q url.Values, body map[string]any) (*viewRequest, error) {
	req := &viewRequest{query: &store.ViewQuery{InclusiveEnd: true}, update: "true"}
	vq := req.query

	// getRaw returns a key parameter as raw JSON text.
	getRaw := func(names ...string) ([]byte, error) {
		for _, name := range names {
			if q.Has(name) {
				raw := []byte(q.Get(name))
				if !json.Valid(raw) {
					return nil, couch.BadRequest("invalid UTF-8 JSON")
				}
				return raw, nil
			}
		}
		for _, name := range names {
			if body != nil {
				if v, ok := body[name]; ok {
					raw, err := json.Marshal(v)
					if err != nil {
						return nil, couch.QueryParseError(name, "unencodable")
					}
					return raw, nil
				}
			}
		}
		return nil, nil
	}
	getString := func(names ...string) string {
		for _, name := range names {
			if q.Has(name) {
				return q.Get(name)
			}
		}
		for _, name := range names {
			if body != nil {
				if v, ok := body[name].(string); ok {
					return v
				}
			}
		}
		return ""
	}
	getBool := func(name string, def bool) (bool, error) {
		if !q.Has(name) && body != nil {
			if v, ok := body[name]; ok {
				b, isBool := v.(bool)
				if !isBool {
					return false, couch.QueryParseError(name, "not a boolean")
				}
				return b, nil
			}
		}
		return boolParam(q, name, def)
	}
	getInt := func(name string) (*int64, error) {
		if !q.Has(name) && body != nil {
			if v, ok := body[name]; ok {
				num, isNum := v.(json.Number)
				if isNum {
					if n, err := num.Int64(); err == nil && n >= 0 {
						return &n, nil
					}
				}
				return nil, couch.QueryParseError(name, "not a non-negative integer")
			}
		}
		return nonNegParam(q, name)
	}

	var err error
	if vq.Key, err = getRaw("key"); err != nil {
		return nil, err
	}
	if vq.StartKey, err = getRaw("startkey", "start_key"); err != nil {
		return nil, err
	}
	if vq.EndKey, err = getRaw("endkey", "end_key"); err != nil {
		return nil, err
	}
	vq.StartKeyDocID = getString("startkey_docid", "start_key_doc_id")
	vq.EndKeyDocID = getString("endkey_docid", "end_key_doc_id")
	if vq.Descending, err = getBool("descending", false); err != nil {
		return nil, err
	}
	if vq.InclusiveEnd, err = getBool("inclusive_end", true); err != nil {
		return nil, err
	}
	if vq.IncludeDocs, err = getBool("include_docs", false); err != nil {
		return nil, err
	}
	if vq.Conflicts, err = getBool("conflicts", false); err != nil {
		return nil, err
	}
	vq.Conflicts = vq.Conflicts && vq.IncludeDocs
	if vq.UpdateSeq, err = getBool("update_seq", false); err != nil {
		return nil, err
	}
	if vq.Group, err = getBool("group", false); err != nil {
		return nil, err
	}
	if req.attachments, err = getBool("attachments", false); err != nil {
		return nil, err
	}
	// Accepted with no behavioral difference. Results are always sorted
	// and reads never touch a half-built index.
	if _, err = getBool("sorted", true); err != nil {
		return nil, err
	}
	if _, err = getBool("stable", false); err != nil {
		return nil, err
	}
	if vq.GroupLevel, err = getInt("group_level"); err != nil {
		return nil, err
	}
	if vq.Limit, err = getInt("limit"); err != nil {
		return nil, err
	}
	skip, err := getInt("skip")
	if err != nil {
		return nil, err
	}
	if skip != nil {
		vq.Skip = *skip
	}
	if q.Has("reduce") {
		b, err := boolParam(q, "reduce", true)
		if err != nil {
			return nil, err
		}
		vq.Reduce = &b
	} else if body != nil {
		if v, ok := body["reduce"]; ok {
			b, isBool := v.(bool)
			if !isBool {
				return nil, couch.QueryParseError("reduce", "not a boolean")
			}
			vq.Reduce = &b
		}
	}

	if keysRaw, err := getRaw("keys"); err != nil {
		return nil, err
	} else if keysRaw != nil {
		var keys []json.RawMessage
		if err := json.Unmarshal(keysRaw, &keys); err != nil {
			return nil, couch.BadRequest("`keys` member must be an array.")
		}
		vq.Keys = make([][]byte, len(keys))
		for i, k := range keys {
			vq.Keys[i] = k
		}
	}

	switch update := getString("update"); update {
	case "":
	case "true", "false", "lazy":
		req.update = update
	default:
		return nil, couch.QueryParseError("update", update)
	}
	switch stale := getString("stale"); stale {
	case "":
	case "ok":
		req.update = "false"
	case "update_after":
		req.update = "lazy"
	default:
		return nil, couch.QueryParseError("stale", stale)
	}
	return req, nil
}

func (s *Server) viewGet(w http.ResponseWriter, r *http.Request) error {
	return s.viewImpl(w, r, nil)
}

func (s *Server) viewPost(w http.ResponseWriter, r *http.Request) error {
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
	return s.viewImpl(w, r, obj)
}

func (s *Server) viewImpl(w http.ResponseWriter, r *http.Request, body map[string]any) error {
	return s.viewImplPartition(w, r, body, "")
}

func (s *Server) viewImplPartition(w http.ResponseWriter, r *http.Request, body map[string]any, partition string) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	if partition != "" {
		if !db.Partitioned {
			return couch.BadRequest("Database is not partitioned")
		}
		if err := validatePartitionName(partition); err != nil {
			return err
		}
	}
	// Cached responses for a current index are byte-exact as long as neither
	// the database nor the design doc moved. This takes one
	// validation round trip instead of the whole read. Misses fall through
	// to the normal path (and its normal error ordering).
	upd := r.URL.Query().Get("update")
	cacheable := r.Method == "GET" && body == nil && (upd == "" || upd == "true")
	var cacheKey string
	if cacheable {
		cacheKey = r.URL.EscapedPath() + "?" + r.URL.RawQuery
		if e := s.viewCache.get(cacheKey); e != nil {
			if e.schema == db.Schema &&
				s.store.ViewReadValid(r.Context(), db, e.ddocID, e.sig, e.ddocSeq, e.lastSeq) {
				return writeViewResponse(w, r, e.etag, e.body)
			}
			s.viewCache.drop(cacheKey)
		}
	}

	vg, ddocSeq, err := s.loadViewGroupSeq(r, db, r.PathValue("ddoc"))
	if err != nil {
		return err
	}
	req, err := buildViewQuery(r.URL.Query(), body)
	if err != nil {
		return err
	}
	if partition != "" {
		if err := s.partitionQueryLimit(req.query.Limit); err != nil {
			return err
		}
		if qp := r.URL.Query().Get("partition"); qp != "" && qp != partition {
			return couch.BadRequest("Conflicting value for `partition` in query string")
		}
	}
	req.query.Partition = partition
	result, seq, err := s.runView(r, db, vg, r.PathValue("view"), req)
	if err != nil {
		return err
	}
	// The view ETag covers the index state and the exact query. A matching
	// If-None-Match short-circuits to 304. The update path already knows
	// the index seq. Only stale-serving modes re-read it.
	cacheable = cacheable && seq >= 0
	if seq < 0 {
		if seq, err = s.store.ViewGroupState(r.Context(), db, vg.Sig); err != nil {
			return err
		}
	}
	etag := viewETag(vg.Sig, r.PathValue("view"), seq, r.URL.RawQuery, body)
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if cacheable {
		s.viewCache.put(cacheKey, &viewRespEntry{
			schema: db.Schema, ddocID: vg.DDocID, sig: vg.Sig, etag: etag,
			ddocSeq: ddocSeq, lastSeq: seq, body: raw,
		})
	}
	return writeViewResponse(w, r, etag, raw)
}

// writeViewResponse writes a marshaled view body with its ETag and checks
// If-None-Match.
func writeViewResponse(w http.ResponseWriter, r *http.Request, etag string, body []byte) error {
	setETag(w, etag)
	if strings.Trim(r.Header.Get("If-None-Match"), `"`) == etag {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json")
	if hdr.Get("Cache-Control") == "" {
		hdr.Set("Cache-Control", "must-revalidate")
	}
	w.WriteHeader(200)
	w.Write(append(body, '\n'))
	return nil
}

// viewETag derives an opaque view response ETag from everything that can
// change the response body.
func viewETag(sig, view string, seq int64, rawQuery string, body map[string]any) string {
	h := md5.New()
	fmt.Fprintf(h, "%s/%s@%d?%s", sig, view, seq, rawQuery)
	if body != nil {
		raw, _ := json.Marshal(body)
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) viewQueries(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	vg, err := s.loadViewGroup(r, db, r.PathValue("ddoc"))
	if err != nil {
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
		req, err := buildViewQuery(r.URL.Query(), queryObj)
		if err != nil {
			return err
		}
		result, _, err := s.runView(r, db, vg, r.PathValue("view"), req)
		if err != nil {
			return err
		}
		results = append(results, result)
	}
	writeJSON(w, 200, map[string]any{"results": results})
	return nil
}

// runView updates the index per the update mode, executes the query, and
// shapes the response object. The returned seq is the group's last_seq
// when the update path learned it (-1 otherwise). Callers reuse it for
// the response ETag.
func (s *Server) runView(r *http.Request, db *store.DB, vg *store.ViewGroup, viewName string, req *viewRequest) (map[string]any, int64, error) {
	mapper, err := s.viewMapperFor(vg)
	if err != nil {
		return nil, -1, err
	}
	lastSeq := int64(-1)
	switch req.update {
	case "true":
		lastSeq, err = s.store.SyncViewGroup(r.Context(), db, vg, mapper)
		if err != nil {
			return nil, -1, err
		}
		req.query.LastSeqHint = lastSeq
	case "lazy":
		go func() {
			// Bounded by server lifetime, not the request.
			_ = s.store.UpdateViewGroup(s.lifetime, db, vg, mapper)
		}()
	}
	req.query.ReduceLimit = s.config.getBool("query_server_config", "reduce_limit", true)
	result, err := s.store.QueryView(r.Context(), db, vg, viewName, req.query, s.reducer)
	if err != nil {
		return nil, -1, err
	}

	rows := make([]any, 0, len(result.Rows))
	for i := range result.Rows {
		row := &result.Rows[i]
		if row.Error != "" {
			rows = append(rows, map[string]any{
				"key": row.Key, "error": row.Error, "reason": row.ErrReason,
			})
			continue
		}
		if result.Reduced {
			rows = append(rows, map[string]any{"key": row.Key, "value": row.Value})
			continue
		}
		out := map[string]any{"id": row.ID, "key": row.Key, "value": row.Value}
		if req.query.IncludeDocs {
			if row.Doc == nil {
				out["doc"] = nil
			} else {
				doc := row.Doc.JSON()
				if err := s.addAttachmentsMember(r, db, doc, row.Doc.ID, row.Doc.Rev, req.attachments); err != nil {
					return nil, -1, err
				}
				if len(row.DocConflicts) > 0 {
					doc["_conflicts"] = row.DocConflicts
				}
				out["doc"] = doc
			}
		}
		rows = append(rows, out)
	}
	response := map[string]any{"rows": rows}
	if !result.Reduced {
		response["total_rows"] = result.TotalRows
		response["offset"] = result.Offset
	}
	if req.query.UpdateSeq {
		response["update_seq"] = seqString(result.UpdateSeq)
	}
	return response, lastSeq, nil
}

// designInfo is GET /{db}/_design/{ddoc}/_info.
func (s *Server) designInfo(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	ddocName := r.PathValue("ddoc")
	vg, err := s.loadViewGroup(r, db, ddocName)
	if err != nil {
		return err
	}
	lastSeq, err := s.store.ViewGroupState(r.Context(), db, vg.Sig)
	if err != nil {
		return err
	}
	size := s.store.ViewGroupSize(r.Context(), db, vg.Sig)
	writeJSON(w, 200, map[string]any{
		"name": ddocName,
		"view_index": map[string]any{
			"signature": vg.Sig,
			"language":  vg.Language,
			// numeric, unlike the string seqs elsewhere (CouchDB shape)
			"update_seq":      lastSeq,
			"purge_seq":       0,
			"updater_running": false,
			"compact_running": false,
			"waiting_commit":  false,
			"waiting_clients": 0,
			"updates_pending": map[string]any{"minimum": 0, "preferred": 0, "total": 0},
			// couchgres's own collation implementation, versioned like
			// CouchDB reports its ICU collator.
			"collator_versions": []string{"1.0.0"},
			"sizes": map[string]any{
				"file":     size,
				"active":   size,
				"external": size,
			},
		},
	})
	return nil
}

// viewCleanup is POST /{db}/_view_cleanup. It drops index tables no live design
// doc references.
func (s *Server) viewCleanup(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	ddocs, err := s.store.DesignDocs(r.Context(), db)
	if err != nil {
		return err
	}
	live := make(map[string]bool)
	for _, ddoc := range ddocs {
		vg, err := store.ViewGroupFromDDoc(ddoc.ID, ddoc.Body)
		if err != nil || vg == nil {
			continue
		}
		live[vg.Sig] = true
	}
	if err := s.store.CleanupViews(r.Context(), db, live); err != nil {
		return err
	}
	writeJSON(w, 202, map[string]any{"ok": true})
	return nil
}

// compactDB and compactView acknowledge requests because every write is already
// durable. Storage compaction is Postgres's job.
func (s *Server) compactDB(w http.ResponseWriter, r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return couch.BadContentType("Content-Type must be application/json")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	// Storage compaction is Postgres's business. CouchDB's compaction
	// also stems revision history to _revs_limit, drops superseded bodies
	// (kept until now under keep_superseded_bodies), and garbage-collects
	// attachments no live revision references. This method does those parts.
	if err := s.store.Compact(r.Context(), db, db.RevsLimit); err != nil {
		return err
	}
	writeJSON(w, 202, map[string]any{"ok": true})
	return nil
}

func (s *Server) compactView(w http.ResponseWriter, r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return couch.BadContentType("Content-Type must be application/json")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	if _, err := s.loadViewGroup(r, db, r.PathValue("ddoc")); err != nil {
		return err
	}
	writeJSON(w, 202, map[string]any{"ok": true})
	return nil
}

// designDocs serves GET/POST /{db}/_design_docs. It is _all_docs restricted to
// design documents, with design-doc-scoped total_rows and offset.
func (s *Server) designDocsGet(w http.ResponseWriter, r *http.Request) error {
	return s.designDocsImpl(w, r, nil)
}

func (s *Server) designDocsPost(w http.ResponseWriter, r *http.Request) error {
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	return s.designDocsImpl(w, r, obj)
}

func (s *Server) designDocsImpl(w http.ResponseWriter, r *http.Request, body map[string]any) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.authorizeList(r, db); err != nil {
		return err
	}
	req, err := buildAllDocsRequest(r.URL.Query(), body)
	if err != nil {
		return err
	}

	const lo, hi = "_design/", "_design0"
	clamp := func(v *string, def string, isStart bool) *string {
		if v == nil {
			return &def
		}
		if isStart != req.params.Descending {
			if *v < lo {
				return &def
			}
		} else if *v > hi {
			return &def
		}
		return v
	}
	if req.params.Descending {
		req.params.StartKey = clamp(req.params.StartKey, hi, true)
		req.params.EndKey = clamp(req.params.EndKey, lo, false)
	} else {
		req.params.StartKey = clamp(req.params.StartKey, lo, true)
		req.params.EndKey = clamp(req.params.EndKey, hi, false)
	}

	page, err := s.runAllDocs(r, db, req)
	if err != nil {
		return err
	}
	total, err := s.store.CountDesignDocs(r.Context(), db, nil, false)
	if err != nil {
		return err
	}
	page.TotalRows = total
	if page.Offset != nil && !req.sendKeys {
		var preceding int64
		if !req.params.Descending {
			// Rows before an ascending window are ids below the start bound.
			if preceding, err = s.store.CountDesignDocs(r.Context(), db, req.params.StartKey, false); err != nil {
				return err
			}
		} else {
			// Rows before a descending window are ids above the start bound.
			upTo, err := s.store.CountDesignDocs(r.Context(), db, req.params.StartKey, true)
			if err != nil {
				return err
			}
			preceding = total - upTo
		}
		offset := preceding + req.params.Skip
		page.Offset = &offset
	}
	writeJSON(w, 200, pageJSON(page, req.params.IncludeDocs))
	return nil
}
