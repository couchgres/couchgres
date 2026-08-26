package httpapi

// Design doc functions: _show, _update, _list, and _rewrite.

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/jsengine"
	"github.com/couchgres/couchgres/internal/store"
)

// jsRequestJSON builds the CouchDB request object ddoc functions receive.
func (s *Server) jsRequestJSON(r *http.Request, db *store.DB, docid string, body []byte) (json.RawMessage, error) {
	query := make(map[string]any)
	for key, values := range r.URL.Query() {
		if len(values) > 0 {
			query[key] = values[0]
		}
	}
	headers := make(map[string]any)
	for key := range r.Header {
		headers[key] = r.Header.Get(key)
	}
	// Go moves Host out of the header map. Ddoc functions expect it.
	if r.Host != "" {
		headers["Host"] = r.Host
	}
	form := map[string]any{}
	bodyString := "undefined"
	if len(body) > 0 {
		bodyString = string(body)
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			if values, err := url.ParseQuery(bodyString); err == nil {
				for key := range values {
					form[key] = values.Get(key)
				}
			}
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
	var id any
	if docid != "" {
		id = docid
	}
	secObj, err := s.store.GetSecurity(r.Context(), db)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"info":           map[string]any{"db_name": db.Name},
		"id":             id,
		"uuid":           randomUUID(),
		"method":         r.Method,
		"path":           pathSegments(r.URL.EscapedPath()),
		"requested_path": pathSegments(r.URL.RequestURI()),
		"raw_path":       r.URL.RequestURI(),
		"query":          query,
		"headers":        headers,
		"body":           bodyString,
		"peer":           r.RemoteAddr,
		"form":           form,
		"cookie":         map[string]any{},
		"userCtx": map[string]any{
			"db": db.Name, "name": name, "roles": roles,
		},
		"secObj": secObj,
	})
}

// pathSegments splits an escaped path and unescapes each segment, so a
// db name containing %2F stays one req.path element (CouchDB semantics).
func pathSegments(escaped string) []string {
	escaped, _, _ = strings.Cut(escaped, "?")
	segments := strings.Split(strings.Trim(escaped, "/"), "/")
	for i, segment := range segments {
		if unescaped, err := url.PathUnescape(segment); err == nil {
			segments[i] = unescaped
		}
	}
	return segments
}

// writeJSResponse renders a show/update/list response object (or bare
// string) with CouchDB's defaults. defaultCT overrides the text/html
// fallback (provider-negotiated types). dropETag discards a JS-supplied
// ETag header (CouchDB owns show/list ETags).
func writeJSResponse(w http.ResponseWriter, raw json.RawMessage, defaultCode int, extraHeaders map[string]string, defaultCT string, dropETag bool) error {
	code := defaultCode
	headers := map[string]string{}
	var body []byte

	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		body = []byte(asString)
	} else {
		var resp struct {
			Code    *int              `json:"code"`
			Headers map[string]string `json:"headers"`
			Body    *string           `json:"body"`
			Base64  *string           `json:"base64"`
			JSON    json.RawMessage   `json:"json"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return couch.NewError(500, "render_error", "invalid response object from function")
		}
		if resp.Code != nil {
			code = *resp.Code
		}
		headers = resp.Headers
		switch {
		case resp.JSON != nil:
			body = resp.JSON
			if headers == nil {
				headers = map[string]string{}
			}
			if _, ok := headers["Content-Type"]; !ok {
				headers["Content-Type"] = "application/json"
			}
		case resp.Base64 != nil:
			decoded, err := base64.StdEncoding.DecodeString(*resp.Base64)
			if err != nil {
				return couch.NewError(500, "render_error", "invalid base64 response body")
			}
			body = decoded
			if headers == nil {
				headers = map[string]string{}
			}
			if _, ok := headers["Content-Type"]; !ok {
				headers["Content-Type"] = "application/binary"
			}
		case resp.Body != nil:
			body = []byte(*resp.Body)
		}
	}
	if headers == nil {
		headers = map[string]string{}
	}
	if dropETag {
		for key := range headers {
			if strings.EqualFold(key, "etag") {
				delete(headers, key)
			}
		}
	}
	if _, ok := headers["Content-Type"]; !ok {
		if defaultCT == "" {
			defaultCT = "text/html; charset=utf-8"
		}
		headers["Content-Type"] = defaultCT
	}
	for key, value := range headers {
		w.Header().Set(key, value)
	}
	for key, value := range extraHeaders {
		w.Header().Set(key, value)
	}
	w.WriteHeader(code)
	w.Write(body)
	return nil
}

// invalidZooPath answers a zoo route with no function name the way
// CouchDB does.
func invalidZooPath(_ http.ResponseWriter, _ *http.Request) error {
	return couch.NewError(404, "not_found", "Invalid path.")
}

// ddocFn fetches a named function member ("shows", "updates", ...) from a
// design doc, with CouchDB's missing-key error. The signature carries the
// ddoc revision. Zoo functions can require() any module in the ddoc, so
// contexts must not be shared across ddoc versions.
func ddocFn(ddoc *store.DocRow, member, name string) (src, sig string, err error) {
	fns, _ := ddoc.Body[member].(map[string]any)
	src, _ = fns[name].(string)
	if src == "" {
		return "", "", couch.NewError(404, "not_found", "missing json key: "+name)
	}
	sum := md5.Sum([]byte(src))
	return src, member + "-" + ddoc.Rev.String() + "-" + hex.EncodeToString(sum[:]), nil
}

// ddocJSON renders the design doc as the JSON object zoo functions see as
// `this` and require() from.
func ddocJSON(ddoc *store.DocRow) json.RawMessage {
	raw, err := json.Marshal(ddoc.JSON())
	if err != nil {
		return json.RawMessage(`null`)
	}
	return raw
}

// showHandler serves /{db}/_design/{ddoc}/_show/{fn}[/{docid}].
func (s *Server) showHandler(w http.ResponseWriter, r *http.Request) error {
	if r.Method != "GET" && r.Method != "POST" && r.Method != "HEAD" {
		return couch.MethodNotAllowed("GET,POST,HEAD")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	ddoc, err := s.store.GetDoc(r.Context(), db, "_design/"+r.PathValue("ddoc"), nil)
	if err != nil {
		return err
	}
	src, sig, err := ddocFn(ddoc, "shows", r.PathValue("fn"))
	if err != nil {
		return err
	}
	docid := r.PathValue("docid")
	docRaw := json.RawMessage(`null`)
	docRev := ""
	docMissing := docid != ""
	if docid != "" {
		if doc, err := s.store.GetDoc(r.Context(), db, docid, nil); err == nil {
			docMissing = false
			docRev = doc.Rev.String()
			if docRaw, err = json.Marshal(doc.JSON()); err != nil {
				return err
			}
		}
	}
	// The show ETag covers everything that can change the rendered body:
	// ddoc rev, doc rev, function, query, and content negotiation.
	etagSum := md5.Sum([]byte(ddoc.Rev.String() + "|" + docRev + "|" + r.PathValue("fn") +
		"|" + r.URL.RawQuery + "|" + r.Header.Get("Accept")))
	etag := hex.EncodeToString(etagSum[:])
	if strings.Trim(r.Header.Get("If-None-Match"), `"`) == etag {
		setETag(w, etag)
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	body, err := readHTTPBody(r)
	if err != nil {
		return err
	}
	reqRaw, err := s.jsRequestJSON(r, db, docid, body)
	if err != nil {
		return err
	}
	result, err := s.js.Show(r.Context(), sig, src, docRaw, reqRaw, ddocJSON(ddoc))
	if err != nil {
		var re *jsengine.RenderError
		if errors.As(err, &re) {
			return couch.NewError(re.Code, re.Name, re.Reason)
		}
		if docMissing {
			// The function failed on a null doc. CouchDB says the doc is
			// missing, not that the render failed.
			return couch.NotFound()
		}
		return couch.NewError(500, "render_error", err.Error())
	}
	extra := map[string]string{"ETag": `"` + etag + `"`, "Vary": "Accept"}
	return writeJSResponse(w, result.Resp, 200, extra, result.ContentType, true)
}

// updateHandler serves /{db}/_design/{ddoc}/_update/{fn}[/{docid}].
func (s *Server) updateHandler(w http.ResponseWriter, r *http.Request) error {
	if r.Method != "PUT" && r.Method != "POST" {
		return couch.MethodNotAllowed("PUT,POST")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	ddoc, err := s.store.GetDoc(r.Context(), db, "_design/"+r.PathValue("ddoc"), nil)
	if err != nil {
		return err
	}
	src, sig, err := ddocFn(ddoc, "updates", r.PathValue("fn"))
	if err != nil {
		return err
	}
	docid := r.PathValue("docid")
	docRaw := json.RawMessage(`null`)
	var existingRev *couch.Rev
	if docid != "" {
		if doc, err := s.store.GetDoc(r.Context(), db, docid, nil); err == nil {
			if docRaw, err = json.Marshal(doc.JSON()); err != nil {
				return err
			}
			rev := doc.Rev
			existingRev = &rev
		}
	}
	body, err := readHTTPBody(r)
	if err != nil {
		return err
	}
	reqRaw, err := s.jsRequestJSON(r, db, docid, body)
	if err != nil {
		return err
	}
	result, err := s.js.DDocCall(r.Context(), sig, src, []json.RawMessage{docRaw, reqRaw}, ddocJSON(ddoc))
	if err != nil {
		return couch.NewError(500, "render_error", err.Error())
	}
	var pair []json.RawMessage
	if err := json.Unmarshal(result, &pair); err != nil || len(pair) != 2 {
		return couch.NewError(500, "render_error", "update function did not return [doc, response]")
	}

	extraHeaders := map[string]string{}
	code := 200
	if string(pair[0]) != "null" {
		newDoc, err := couch.ParseDoc(mustDecode(pair[0]))
		if err != nil {
			return err
		}
		// A returned _id member is authoritative even when empty (CouchDB
		// rejects the empty string rather than falling back to the path).
		if returned, ok := mustDecode(pair[0]).(map[string]any); ok {
			if _, hasID := returned["_id"]; hasID {
				if err := couch.ValidateDocID(newDoc.ID); err != nil {
					return err
				}
			}
		}
		// The returned doc names itself. CouchDB never falls back to the
		// path docid for the write.
		writeID := newDoc.ID
		if writeID == "" {
			return couch.BadRequest("Document id must not be empty")
		}
		if err := s.validateDocIDForDB(db, writeID); err != nil {
			return err
		}
		if err := s.authorizeDocWrite(r, db, writeID); err != nil {
			return err
		}
		expected := existingRev
		if newDoc.Rev != nil {
			expected = newDoc.Rev
		}
		if err := s.validateDocUpdate(r, db, writeID, newDoc.Body, expected, newDoc.Deleted, newDoc.Attachments); err != nil {
			return err
		}
		atts, err := attachmentWrites(newDoc.Attachments)
		if err != nil {
			return err
		}
		// pair[0] is the doc exactly as the update function returned it.
		// the member order CouchDB's rev hash covers.
		atts = orderAttachmentWrites(atts, pair[0])
		s.compressAttachmentWrites(atts)
		rev, _, err := s.store.PutDoc(r.Context(), db, writeID, newDoc.Body, pair[0], expected, newDoc.Deleted, atts)
		if err != nil {
			return err
		}
		extraHeaders["X-Couch-Update-NewRev"] = rev.String()
		extraHeaders["X-Couch-Id"] = writeID
		code = 201
	}
	return writeJSResponse(w, pair[1], code, extraHeaders, "", false)
}

func mustDecode(raw json.RawMessage) any {
	v, err := couch.DecodeJSON(raw)
	if err != nil {
		return nil
	}
	return v
}

// listHandler serves /{db}/_design/{ddoc}/_list/{fn}/{view} and the
// cross-ddoc /{fn}/{other}/{view} form.
func (s *Server) listHandler(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case "GET", "POST", "HEAD", "OPTIONS": // OPTIONS runs the list (CouchDB behavior)
	default:
		return couch.MethodNotAllowed("GET,HEAD,POST,OPTIONS")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	ddoc, err := s.store.GetDoc(r.Context(), db, "_design/"+r.PathValue("ddoc"), nil)
	if err != nil {
		return err
	}
	src, sig, err := ddocFn(ddoc, "lists", r.PathValue("fn"))
	if err != nil {
		return err
	}
	var body map[string]any
	var rawBody []byte
	if r.Method == "POST" {
		rawBody, err = readHTTPBody(r)
		if err != nil {
			return err
		}
		if len(rawBody) > 0 {
			if v, err := couch.DecodeJSON(rawBody); err == nil {
				body, _ = v.(map[string]any)
			}
		}
	}

	var rows []json.RawMessage
	var head []byte
	if r.PathValue("view") == "_all_docs" {
		// Lists run over _all_docs too, with all_docs row shapes.
		adReq, err := buildAllDocsRequest(r.URL.Query(), body)
		if err != nil {
			return err
		}
		page, err := s.runAllDocs(r, db, adReq)
		if err != nil {
			return err
		}
		pageMap := pageJSON(page, adReq.params.IncludeDocs)
		for _, entry := range pageMap["rows"].([]any) {
			raw, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			rows = append(rows, raw)
		}
		if head, err = json.Marshal(map[string]any{
			"total_rows": pageMap["total_rows"], "offset": pageMap["offset"],
		}); err != nil {
			return err
		}
	} else {
		viewDDocName := r.PathValue("viewddoc")
		if viewDDocName == "" {
			viewDDocName = r.PathValue("ddoc")
		}
		vg, err := s.loadViewGroup(r, db, viewDDocName)
		if err != nil {
			return err
		}
		req, err := buildViewQuery(r.URL.Query(), body)
		if err != nil {
			return err
		}
		result, err := s.runViewRaw(r, db, vg, r.PathValue("view"), req)
		if err != nil {
			return err
		}

		rows = make([]json.RawMessage, 0, len(result.Rows))
		for i := range result.Rows {
			row := &result.Rows[i]
			entry := map[string]any{"key": row.Key, "value": row.Value}
			if !result.Reduced {
				entry["id"] = row.ID
				if req.query.IncludeDocs {
					if row.Doc != nil {
						entry["doc"] = row.Doc.JSON()
					} else {
						entry["doc"] = nil
					}
				}
			}
			raw, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			rows = append(rows, raw)
		}
		if head, err = json.Marshal(map[string]any{
			"total_rows": result.TotalRows, "offset": result.Offset,
		}); err != nil {
			return err
		}
	}
	reqRaw, err := s.jsRequestJSON(r, db, "", rawBody)
	if err != nil {
		return err
	}
	listResult, err := s.js.List(r.Context(), sig, src, head, reqRaw, ddocJSON(ddoc), rows)
	if err != nil {
		var re *jsengine.RenderError
		if errors.As(err, &re) {
			return couch.NewError(re.Code, re.Name, re.Reason)
		}
		return couch.NewError(500, "render_error", err.Error())
	}
	defaultCT := listResult.ContentType
	if defaultCT == "" {
		defaultCT = "application/json" // lists default to JSON, unlike shows
	}
	return writeJSResponse(w, listResult.Resp, 200, nil, defaultCT, false)
}

// runViewRaw is runView up to (and including) the store query, without
// response shaping. Lists consume the raw rows.
func (s *Server) runViewRaw(r *http.Request, db *store.DB, vg *store.ViewGroup, viewName string, req *viewRequest) (*store.ViewResult, error) {
	mapper, err := s.viewMapperFor(vg)
	if err != nil {
		return nil, err
	}
	if req.update == "true" {
		if err := s.store.UpdateViewGroup(r.Context(), db, vg, mapper); err != nil {
			return nil, err
		}
	}
	return s.store.QueryView(r.Context(), db, vg, viewName, req.query, s.reducer)
}

// Rewrites.

type rewriteDepthKey struct{}

// rewriteHandler serves /{db}/_design/{ddoc}/_rewrite[/{path...}].
// It uses array rules or a function-form rewriter, then re-dispatches through
// the router.
func (s *Server) rewriteHandler(w http.ResponseWriter, r *http.Request) error {
	depth, _ := r.Context().Value(rewriteDepthKey{}).(int)
	if depth > 16 {
		return couch.BadRequest("Exceeded rewrite recursion limit")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	ddocName := r.PathValue("ddoc")
	ddoc, err := s.store.GetDoc(r.Context(), db, "_design/"+ddocName, nil)
	if err != nil {
		return err
	}
	subPath := r.PathValue("path")

	// The body is needed twice. Rewriters read it, and the re-dispatched
	// request carries it on (unless the rewriter supplies its own).
	bodyBytes, err := readHTTPBody(r)
	if err != nil {
		return err
	}

	var rewrite *rewriteResult
	switch rules := ddoc.Body["rewrites"].(type) {
	case []any:
		target, query, err := applyRewriteRules(rules, r.Method, subPath, r.URL.Query())
		if err != nil {
			return err
		}
		rewrite = &rewriteResult{target: target, query: query}
	case string:
		rewriteRequest := s.sanitizedRewriteRequest(r)
		rewrite, err = s.applyRewriteFunction(
			rewriteRequest, db, ddoc, rules, subPath, bodyBytes)
		if err != nil {
			return err
		}
	default:
		return couch.NotFound()
	}
	if err := s.validateRewriteHeaders(rewrite.headers); err != nil {
		return err
	}
	if rewrite.direct != nil {
		// The rewriter answered directly instead of naming a target.
		return writeJSResponse(w, rewrite.direct, 200, nil, "", false)
	}

	// Resolve relative to the ddoc root. Segments are escaped so a db
	// name containing "/" survives the re-dispatch as one segment.
	base := "/" + url.PathEscape(db.Name) + "/_design/" + url.PathEscape(ddocName) + "/"
	resolved := resolveRewritePath(base, rewrite.target)
	targetMethod, err := s.authorizeRewrite(r, db.Name, resolved, rewrite.method)
	if err != nil {
		return err
	}

	// Do not re-run authentication with the visitor's Cookie, Authorization,
	// proxy-auth, or forwarding headers. The already-resolved userCtx remains
	// in the cloned context and every target handler re-runs authorization.
	r2 := s.sanitizedRewriteRequest(r)
	r2 = r2.Clone(context.WithValue(r2.Context(), rewriteDepthKey{}, depth+1))
	if unescaped, err := url.PathUnescape(resolved); err == nil && unescaped != resolved {
		r2.URL.RawPath = resolved
		r2.URL.Path = unescaped
	} else {
		r2.URL.Path = resolved
		r2.URL.RawPath = ""
	}
	r2.URL.RawQuery = rewrite.query.Encode()
	r2.RequestURI = ""
	r2.Method = targetMethod
	for key, value := range rewrite.headers {
		r2.Header.Set(key, value)
	}
	if rewrite.body != nil {
		r2.Body = io.NopCloser(strings.NewReader(*rewrite.body))
		r2.ContentLength = int64(len(*rewrite.body))
	} else {
		r2.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		r2.ContentLength = int64(len(bodyBytes))
	}
	s.router.ServeHTTP(w, r2)
	return nil
}

// applyRewriteRules finds the first matching array rule.
func applyRewriteRules(rules []any, method, subPath string, query url.Values) (string, url.Values, error) {
	segments := []string{}
	if subPath != "" {
		segments = strings.Split(subPath, "/")
	}
	for _, raw := range rules {
		rule, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if m, ok := rule["method"].(string); ok && m != "" && m != method {
			continue
		}
		from, _ := rule["from"].(string)
		bindings, ok := matchRewriteFrom(from, segments)
		if !ok {
			continue
		}
		// Query-string parameters are bindings too (path captures win).
		for key, values := range query {
			if _, taken := bindings[key]; !taken && len(values) > 0 {
				bindings[key] = values[0]
			}
		}
		to, _ := rule["to"].(string)
		target := substituteBindings(to, bindings)
		out := url.Values{}
		for key, values := range query {
			out[key] = values
		}
		if ruleQuery, ok := rule["query"].(map[string]any); ok {
			for key, value := range ruleQuery {
				if s, ok := value.(string); ok {
					out.Set(key, substituteBindings(s, bindings))
				} else if raw, err := json.Marshal(substituteQueryBindings(value, bindings)); err == nil {
					out.Set(key, string(raw))
				}
			}
		}
		return target, out, nil
	}
	return "", nil, couch.NotFound()
}

// substituteQueryBindings replaces :var placeholders inside structured rule
// query values (arrays and objects), leaving other values alone.
func substituteQueryBindings(v any, bindings map[string]string) any {
	switch t := v.(type) {
	case string:
		return substituteBindings(t, bindings)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = substituteQueryBindings(e, bindings)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = substituteQueryBindings(e, bindings)
		}
		return out
	}
	return v
}

// matchRewriteFrom matches "/a/:var/*" style patterns against the request
// segments, collecting :var bindings ("*" captures the tail).
func matchRewriteFrom(from string, segments []string) (map[string]string, bool) {
	pattern := strings.Trim(from, "/")
	var patSegments []string
	if pattern != "" {
		patSegments = strings.Split(pattern, "/")
	}
	bindings := map[string]string{}
	i := 0
	for ; i < len(patSegments); i++ {
		pat := patSegments[i]
		if pat == "*" {
			bindings["*"] = strings.Join(segments[i:], "/")
			return bindings, true
		}
		if i >= len(segments) {
			return nil, false
		}
		if strings.HasPrefix(pat, ":") {
			bindings[pat[1:]] = segments[i]
			continue
		}
		if pat != segments[i] {
			return nil, false
		}
	}
	if i != len(segments) {
		return nil, false
	}
	return bindings, true
}

func substituteBindings(s string, bindings map[string]string) string {
	segments := strings.Split(s, "/")
	for i, segment := range segments {
		if segment == "*" {
			segments[i] = bindings["*"]
		} else if strings.HasPrefix(segment, ":") {
			if value, ok := bindings[segment[1:]]; ok {
				segments[i] = value
			}
		}
	}
	return strings.Join(segments, "/")
}

// applyRewriteFunction runs a function-form rewriter.
// rewriteResult is a function-form rewriter's verdict: a re-dispatch
// target (with optional method/headers/body overrides) or, with no path,
// a direct response object.
type rewriteResult struct {
	target  string
	query   url.Values
	method  string
	headers map[string]string
	body    *string
	direct  json.RawMessage
}

func (s *Server) applyRewriteFunction(r *http.Request, db *store.DB, ddoc *store.DocRow, src, subPath string, body []byte) (*rewriteResult, error) {
	sum := md5.Sum([]byte(src))
	sig := "rewrite-" + ddoc.Rev.String() + "-" + hex.EncodeToString(sum[:])
	reqRaw, err := s.jsRequestJSON(r, db, "", body)
	if err != nil {
		return nil, err
	}
	result, err := s.js.DDocCall(r.Context(), sig, src, []json.RawMessage{reqRaw}, ddocJSON(ddoc))
	if err != nil {
		return nil, couch.NewError(500, "render_error", err.Error())
	}
	var asString string
	if string(result) != "null" && json.Unmarshal(result, &asString) == nil {
		return &rewriteResult{target: asString, query: r.URL.Query()}, nil
	}
	var resp struct {
		Path    string            `json:"path"`
		Query   map[string]any    `json:"query"`
		Method  string            `json:"method"`
		Headers map[string]string `json:"headers"`
		Body    *string           `json:"body"`
		Code    *int              `json:"code"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, couch.NotFound()
	}
	if resp.Path == "" {
		// With no target, the returned object is the response itself.
		if resp.Code != nil || resp.Body != nil {
			return &rewriteResult{direct: result, headers: resp.Headers}, nil
		}
		return nil, couch.NotFound()
	}
	query := url.Values{}
	for key, values := range r.URL.Query() {
		query[key] = values
	}
	for key, value := range resp.Query {
		if s, ok := value.(string); ok {
			query.Set(key, s)
		} else if raw, err := json.Marshal(value); err == nil {
			query.Set(key, string(raw))
		}
	}
	return &rewriteResult{
		target: resp.Path, query: query,
		method: resp.Method, headers: resp.Headers, body: resp.Body,
	}, nil
}

func (s *Server) authorizeRewrite(
	r *http.Request,
	dbName, resolvedPath, requestedMethod string,
) (string, error) {
	method := r.Method
	if requestedMethod != "" {
		if !validHTTPToken(requestedMethod) {
			return "", couch.NewError(500, "insecure_rewrite_rule", "invalid rewrite method")
		}
		method = strings.ToUpper(requestedMethod)
	}
	withinDB := rewritePathWithinDatabase(resolvedPath, dbName)
	methodChanged := method != r.Method
	if s.config.secureRewrites() {
		switch {
		case !withinDB:
			return "", couch.NewError(
				500, "insecure_rewrite_rule", "path must begin with the database")
		case methodChanged:
			return "", couch.NewError(
				500, "insecure_rewrite_rule", "rewrite method must match the request method")
		}
	} else if (!withinDB || methodChanged) && !userOf(r).IsServerAdmin() {
		return "", couch.ServerAdminRequired()
	}
	return method, nil
}

func rewritePathWithinDatabase(resolvedPath, dbName string) bool {
	unescaped, err := url.PathUnescape(resolvedPath)
	if err != nil {
		return false
	}
	cleaned := path.Clean("/" + strings.TrimPrefix(unescaped, "/"))
	dbRoot := "/" + dbName
	return cleaned == dbRoot || strings.HasPrefix(cleaned, dbRoot+"/")
}

func (s *Server) sanitizedRewriteRequest(r *http.Request) *http.Request {
	clone := r.Clone(r.Context())
	clone.Header = r.Header.Clone()
	for name := range clone.Header {
		if s.sensitiveRewriteHeader(name) {
			delete(clone.Header, name)
		}
	}
	return clone
}

func (s *Server) validateRewriteHeaders(headers map[string]string) error {
	for name, value := range headers {
		if !validHTTPToken(name) || strings.ContainsAny(value, "\r\n\x00") {
			return couch.NewError(
				500, "insecure_rewrite_rule", "rewrite contains an invalid header")
		}
		if s.sensitiveRewriteHeader(name) {
			return couch.NewError(500, "insecure_rewrite_rule",
				"rewrite may not set security-sensitive header "+http.CanonicalHeaderKey(name))
		}
	}
	return nil
}

func (s *Server) sensitiveRewriteHeader(name string) bool {
	name = strings.ToLower(name)
	if name == strings.ToLower(s.config.getOr(
		"couch_httpd_auth", "x_auth_username", "X-Auth-CouchDB-UserName")) ||
		name == strings.ToLower(s.config.getOr(
			"couch_httpd_auth", "x_auth_roles", "X-Auth-CouchDB-Roles")) ||
		name == strings.ToLower(s.config.getOr(
			"couch_httpd_auth", "x_auth_token", "X-Auth-CouchDB-Token")) ||
		name == strings.ToLower(s.config.getOr(
			"chttpd", "x_forwarded_host", "X-Forwarded-Host")) {
		return true
	}
	if strings.HasPrefix(name, "x-auth-couchdb-") ||
		strings.HasPrefix(name, "x-forwarded-") {
		return true
	}
	switch name {
	case "authorization", "cookie", "set-cookie",
		"proxy-authorization", "proxy-authenticate", "www-authenticate",
		"forwarded", "x-real-ip", "x-http-method-override",
		"host", "content-length", "connection", "keep-alive", "te",
		"trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func validHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			continue
		}
		return false
	}
	return true
}

// resolveRewritePath joins target onto base, folding "." and ".." segments.
func resolveRewritePath(base, target string) string {
	if strings.HasPrefix(target, "/") {
		return target
	}
	segments := strings.Split(strings.Trim(base, "/"), "/")
	for _, segment := range strings.Split(target, "/") {
		switch segment {
		case "", ".":
		case "..":
			if len(segments) > 0 {
				segments = segments[:len(segments)-1]
			}
		default:
			segments = append(segments, segment)
		}
	}
	return "/" + strings.Join(segments, "/")
}
