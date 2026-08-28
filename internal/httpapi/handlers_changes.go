package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

func (s *Server) changesGet(w http.ResponseWriter, r *http.Request) error {
	return s.changesImpl(w, r, nil)
}

func (s *Server) changesPost(w http.ResponseWriter, r *http.Request) error {
	raw, err := readHTTPBody(r)
	if err != nil {
		return err
	}
	// An empty body is fine (all options in the query string).
	if len(bytes.TrimSpace(raw)) == 0 {
		return s.changesImpl(w, r, nil)
	}
	body, err := couch.DecodeJSON(raw)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	return s.changesImpl(w, r, obj)
}

type changesRequest struct {
	params      store.ChangesParams
	feed        string
	heartbeat   time.Duration // 0 = none
	timeout     time.Duration
	conflicts   bool
	attachments bool // include_docs docs carry attachment data
	// filteredAttachments delays attachment loading until application-level
	// filtering has selected the rows that will actually be returned.
	filteredAttachments bool
	// filterFn, when set, decides which changes are emitted. The store query
	// then runs with IncludeDocs (filters see documents) and without a limit
	// (the limit counts post-filter rows). userIncludeDocs and userLimit
	// keep the client's originals.
	filterFn        changesFilter
	userIncludeDocs bool
	userLimit       *int64
}

func (s *Server) parseChangesRequest(r *http.Request, db *store.DB, body map[string]any) (*changesRequest, error) {
	q := r.URL.Query()
	req := &changesRequest{
		feed:    q.Get("feed"),
		timeout: 60 * time.Second,
	}
	if req.feed == "" {
		req.feed = "normal"
	}
	if req.feed == "live" { // accepted alias since CouchDB 2.0
		req.feed = "continuous"
	}
	switch req.feed {
	case "normal", "longpoll", "continuous", "eventsource":
	default:
		return nil, couch.BadRequest("Supported `feed` types: normal, continuous, longpoll, eventsource")
	}

	// EventSource reconnects resume from the Last-Event-ID header.
	since := q.Get("since")
	if req.feed == "eventsource" {
		if lastEvent := r.Header.Get("Last-Event-ID"); lastEvent != "" {
			since = lastEvent
		}
	}

	// since is 0, "now", or an opaque "N-..." seq string.
	switch {
	case since == "" || since == "0":
	case since == "now":
		seq, err := s.store.CurrentSeq(r.Context(), db)
		if err != nil {
			return nil, err
		}
		req.params.Since = seq
	default:
		numPart, _, _ := strings.Cut(since, "-")
		n, err := strconv.ParseInt(numPart, 10, 64)
		if err != nil {
			return nil, couch.BadRequest("Invalid since seq: " + since)
		}
		req.params.Since = n
	}

	var err error
	if req.params.Limit, err = nonNegParam(q, "limit"); err != nil {
		return nil, err
	}
	if req.params.Descending, err = boolParam(q, "descending", false); err != nil {
		return nil, err
	}
	if req.params.IncludeDocs, err = boolParam(q, "include_docs", false); err != nil {
		return nil, err
	}
	if req.conflicts, err = boolParam(q, "conflicts", false); err != nil {
		return nil, err
	}
	if req.attachments, err = boolParam(q, "attachments", false); err != nil {
		return nil, err
	}
	req.params.IncludeAttachments = req.params.IncludeDocs
	req.params.AttachmentData = req.attachments && req.params.IncludeDocs
	// conflicts=true only matters when docs are included (CouchDB ignores
	// it otherwise).
	req.params.Conflicts = req.conflicts && req.params.IncludeDocs
	switch style := q.Get("style"); style {
	case "", "main_only":
	case "all_docs":
		req.params.AllDocsStyle = true
	default:
		return nil, couch.BadRequest("Invalid style: " + style)
	}

	switch filter := q.Get("filter"); filter {
	case "":
	case "_doc_ids":
		ids, err := changesDocIDs(q.Get("doc_ids"), body)
		if err != nil {
			return nil, err
		}
		req.params.DocIDs = ids
	case "_design":
		req.params.DesignOnly = true
	case "_selector":
		fn, err := buildSelectorChangesFilter(body)
		if err != nil {
			return nil, err
		}
		req.filterFn = fn
	case "_view":
		fn, err := s.buildViewChangesFilter(r, db, q.Get("view"))
		if err != nil {
			return nil, err
		}
		req.filterFn = fn
	default:
		fn, err := s.buildJSChangesFilter(r, db, filter)
		if err != nil {
			return nil, err
		}
		req.filterFn = fn
	}
	if req.filterFn != nil {
		req.userIncludeDocs = req.params.IncludeDocs
		req.userLimit = req.params.Limit
		if req.params.IncludeAttachments {
			req.filteredAttachments = true
			req.params.IncludeAttachments = false
		}
		req.params.IncludeDocs = true
		req.params.Limit = nil
	}

	if raw := q.Get("heartbeat"); raw != "" {
		if raw == "true" {
			req.heartbeat = 60 * time.Second
		} else if ms, err := strconv.Atoi(raw); err != nil {
			return nil, couch.BadRequest(
				"Invalid heartbeat value. Expecting a positive integer value (in milliseconds).")
		} else if ms <= 0 {
			return nil, couch.BadRequest(
				"The heartbeat value should be a positive integer (in milliseconds).")
		} else {
			req.heartbeat = time.Duration(ms) * time.Millisecond
		}
	}
	if raw := q.Get("timeout"); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil || ms < 0 {
			return nil, couch.QueryParseError("timeout", raw)
		}
		req.timeout = time.Duration(ms) * time.Millisecond
	}
	return req, nil
}

func changesDocIDs(queryValue string, body map[string]any) ([]string, error) {
	var raw any
	if queryValue != "" {
		parsed, err := couch.DecodeJSON([]byte(queryValue))
		if err != nil {
			return nil, couch.QueryParseError("doc_ids", queryValue)
		}
		raw = parsed
	} else if body != nil {
		raw = body["doc_ids"]
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, couch.BadRequest("`doc_ids` filter requires a doc_ids array")
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			ids = append(ids, s)
		}
	}
	return ids, nil
}

func (s *Server) changesImpl(w http.ResponseWriter, r *http.Request, body map[string]any) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	req, err := s.parseChangesRequest(r, db, body)
	if err != nil {
		return err
	}

	switch req.feed {
	case "normal":
		return s.changesNormal(w, r, db, req, false)
	case "longpoll":
		return s.changesNormal(w, r, db, req, true)
	default: // continuous, eventsource
		return s.changesStream(w, r, db, req)
	}
}

// changeRowJSON is changeJSON plus the _attachments member on included
// docs (stubs, data with attachments=true, encoding via att_encoding_info).
func (s *Server) changeRowJSON(
	r *http.Request,
	c store.Change,
	req *changesRequest,
) (map[string]any, error) {
	row := changeJSON(c, req.conflicts)
	if doc, ok := row["doc"].(map[string]any); ok {
		encodingInfo, err := boolParam(r.URL.Query(), "att_encoding_info", false)
		if err != nil {
			return nil, err
		}
		if err := addAttachmentsMemberFrom(
			doc, c.Attachments, req.attachments, encodingInfo); err != nil {
			return nil, err
		}
	}
	return row, nil
}

func changeJSON(c store.Change, conflicts bool) map[string]any {
	revs := make([]map[string]string, 0, len(c.LeafRevs))
	for _, rev := range c.LeafRevs {
		revs = append(revs, map[string]string{"rev": rev.String()})
	}
	row := map[string]any{
		"seq":     seqString(c.Seq),
		"id":      c.ID,
		"changes": revs,
	}
	if c.Deleted {
		row["deleted"] = true
	}
	if c.Body != nil {
		doc := map[string]any{"_id": c.ID, "_rev": c.LeafRevs[0].String()}
		if c.Deleted {
			doc["_deleted"] = true
		}
		for k, v := range c.Body {
			doc[k] = v
		}
		if conflicts && len(c.ConflictRevs) > 0 {
			revs := make([]string, len(c.ConflictRevs))
			for i, rev := range c.ConflictRevs {
				revs[i] = rev.String()
			}
			doc["_conflicts"] = revs
		}
		row["doc"] = doc
	}
	return row
}

// changesNormal serves feed=normal and feed=longpoll (which waits for the
// first change before responding).
func (s *Server) changesNormal(w http.ResponseWriter, r *http.Request, db *store.DB, req *changesRequest, wait bool) error {
	changes, err := s.store.Changes(r.Context(), db, &req.params)
	if err != nil {
		return err
	}
	// last_seq advances over scanned changes even when the filter drops them.
	lastSeq := req.params.Since
	for _, c := range changes {
		if c.Seq > lastSeq {
			lastSeq = c.Seq
		}
	}
	filtered, err := applyChangesFilter(r.Context(), req, changes)
	if err != nil {
		return err
	}

	headersSent := false
	if wait && len(filtered) == 0 {
		// Longpoll clients see the 200 and headers right away. The body
		// follows when a change (or the timeout) arrives.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "must-revalidate")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		headersSent = true
		wake, unsubscribe := s.broker.Subscribe(db.Name)
		defer unsubscribe()
		deadline := time.NewTimer(req.timeout)
		defer deadline.Stop()
	waiting:
		for {
			// Re-check after subscribing. A write may have landed between
			// the query and the subscription.
			changes, err = s.store.Changes(r.Context(), db, &req.params)
			if err != nil {
				return err
			}
			for _, c := range changes {
				if c.Seq > lastSeq {
					lastSeq = c.Seq
				}
			}
			if filtered, err = applyChangesFilter(r.Context(), req, changes); err != nil {
				return err
			}
			if len(filtered) > 0 {
				break
			}
			select {
			case <-wake:
			case <-deadline.C:
				break waiting
			case <-r.Context().Done():
				return nil
			}
		}
	}
	exhausted := changesScanExhausted(req, len(changes))

	// A filtered feed applies the client's limit after filtering.
	if req.filterFn != nil && req.userLimit != nil && int64(len(filtered)) > *req.userLimit {
		filtered = filtered[:*req.userLimit]
		exhausted = false
		lastSeq = req.params.Since
		for _, c := range filtered {
			if c.Seq > lastSeq {
				lastSeq = c.Seq
			}
		}
	}
	if req.filteredAttachments {
		if err := s.store.AttachChangeAttachments(
			r.Context(), db, filtered, req.attachments); err != nil {
			return err
		}
	}
	rows := make([]any, 0, len(filtered))
	for _, c := range filtered {
		row, err := s.changeRowJSON(r, c, req)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	pending := int64(0)
	if !exhausted {
		pending, err = s.store.PendingAfter(r.Context(), db, lastSeq)
		if err != nil {
			if headersSent {
				return nil // status already on the wire
			}
			return err
		}
	}
	response := map[string]any{
		"results":  rows,
		"last_seq": seqString(lastSeq),
		"pending":  pending,
	}
	if headersSent {
		body, err := json.Marshal(response)
		if err != nil {
			return nil
		}
		w.Write(append(body, '\n'))
		return nil
	}
	writeJSON(w, 200, response)
	return nil
}

// changesScanExhausted reports when the store query consumed every visible
// database change, making pending known to be zero without another query.
func changesScanExhausted(req *changesRequest, scanned int) bool {
	if len(req.params.DocIDs) > 0 || req.params.DesignOnly {
		return false
	}
	return req.params.Limit == nil || int64(scanned) < *req.params.Limit
}

// changesStream serves feed=continuous (JSON lines) and feed=eventsource.
func (s *Server) changesStream(w http.ResponseWriter, r *http.Request, db *store.DB, req *changesRequest) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return couch.NewError(500, "unknown_error", "streaming unsupported")
	}
	eventsource := req.feed == "eventsource"
	if eventsource {
		w.Header().Set("Content-Type", "text/event-stream")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.Header().Set("Cache-Control", "must-revalidate")
	s.refreshStreamWriteDeadline(w)
	w.WriteHeader(200)
	// Clients (EventSource especially) need the headers before the first
	// change or heartbeat shows up.
	flusher.Flush()

	wake, unsubscribe := s.broker.Subscribe(db.Name)
	defer unsubscribe()

	emit := func(c store.Change) error {
		rowJSON, err := s.changeRowJSON(r, c, req)
		if err != nil {
			return err
		}
		row, err := marshalLine(rowJSON)
		if err != nil {
			return err
		}
		if eventsource {
			s.refreshStreamWriteDeadline(w)
			_, err = w.Write([]byte("data: " + row + "\nid: " + strconv.FormatInt(c.Seq, 10) + "\n\n"))
		} else {
			s.refreshStreamWriteDeadline(w)
			_, err = w.Write([]byte(row + "\n"))
		}
		return err
	}

	since := req.params.Since
	limit := req.params.Limit
	if req.filterFn != nil {
		limit = req.userLimit
	}
	var emitted int64
	var idle *time.Timer
	if req.heartbeat == 0 {
		idle = time.NewTimer(req.timeout)
		defer idle.Stop()
	}
	var heartbeat *time.Ticker
	if req.heartbeat > 0 {
		heartbeat = time.NewTicker(req.heartbeat)
		defer heartbeat.Stop()
	}

	for {
		params := req.params
		params.Since = since
		changes, err := s.store.Changes(r.Context(), db, &params)
		if err != nil {
			return nil // Stream already started. Drop the connection.
		}
		filtered, err := applyChangesFilter(r.Context(), req, changes)
		if err != nil {
			return nil
		}
		if limit != nil {
			remaining := *limit - emitted
			if remaining <= 0 {
				finishStream(w, eventsource, since)
				return nil
			}
			if int64(len(filtered)) > remaining {
				filtered = filtered[:remaining]
			}
		}
		if req.filteredAttachments {
			if err := s.store.AttachChangeAttachments(
				r.Context(), db, filtered, req.attachments); err != nil {
				return nil
			}
		}
		for _, c := range filtered {
			if err := emit(c); err != nil {
				return nil
			}
			since = c.Seq
			emitted++
			if limit != nil && emitted >= *limit {
				finishStream(w, eventsource, since)
				return nil
			}
		}
		// The scan consumed everything, filtered or not.
		if len(changes) > 0 && changes[len(changes)-1].Seq > since {
			since = changes[len(changes)-1].Seq
		}
		if len(changes) > 0 {
			flusher.Flush()
			if idle != nil {
				idle.Stop()
				idle.Reset(req.timeout)
			}
		}

		var idleC <-chan time.Time
		if idle != nil {
			idleC = idle.C
		}
		var heartbeatC <-chan time.Time
		if heartbeat != nil {
			heartbeatC = heartbeat.C
		}
		select {
		case <-wake:
		case <-heartbeatC:
			beat := "\n"
			if eventsource {
				beat = "event: heartbeat\ndata: \n\n"
			}
			s.refreshStreamWriteDeadline(w)
			if _, err := w.Write([]byte(beat)); err != nil {
				return nil
			}
			flusher.Flush()
		case <-idleC:
			finishStream(w, eventsource, since)
			return nil
		case <-r.Context().Done():
			return nil
		}
	}
}

func (s *Server) refreshStreamWriteDeadline(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetWriteDeadline(
		time.Now().Add(s.streamWriteTimeout))
}

func finishStream(w http.ResponseWriter, eventsource bool, lastSeq int64) {
	if eventsource {
		return
	}
	line, err := marshalLine(map[string]any{"last_seq": seqString(lastSeq), "pending": 0})
	if err == nil {
		w.Write([]byte(line + "\n"))
	}
}

func (s *Server) ensureFullCommit(w http.ResponseWriter, r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return couch.BadContentType("Content-Type must be application/json")
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	// Every write is already durable in its own transaction.
	writeJSON(w, 201, map[string]any{
		"ok":                  true,
		"instance_start_time": db.InstanceStartTime,
	})
	return nil
}
