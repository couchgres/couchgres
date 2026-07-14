package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// bulkDocs is POST /{db}/_bulk_docs: many writes, one response array,
// non-transactional across documents (CouchDB's contract).
func (s *Server) bulkDocs(w http.ResponseWriter, r *http.Request) error {
	dbName := r.PathValue("db")
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	body, rawBody, err := readRawJSONBody(r)
	if err != nil {
		return err
	}
	obj, isObj := body.(map[string]any)
	if !isObj {
		return couch.BadRequest("Request body must be a JSON object")
	}
	docsAny, present := obj["docs"]
	if !present {
		return couch.BadRequest("POST body must include `docs` parameter.")
	}
	docsRaw, ok := docsAny.([]any)
	if !ok {
		return couch.BadRequest("`docs` parameter must be an array.")
	}
	// Per-doc raw bytes preserve the body order used for rev hashes.
	var rawEnvelope struct {
		Docs []json.RawMessage `json:"docs"`
	}
	if json.Unmarshal(rawBody, &rawEnvelope) != nil ||
		len(rawEnvelope.Docs) != len(docsRaw) {
		rawEnvelope.Docs = nil
	}
	rawDocAt := func(i int) []byte {
		if rawEnvelope.Docs == nil {
			return nil
		}
		return rawEnvelope.Docs[i]
	}
	newEdits := true
	if v, present := obj["new_edits"]; present {
		b, isBool := v.(bool)
		if !isBool {
			return couch.BadRequest("`new_edits` parameter must be a boolean.")
		}
		newEdits = b
	}
	// all_or_nothing died with CouchDB 2.0. CouchDB 3.x answers 417 with one
	// not_implemented row per doc.
	if aon, _ := obj["all_or_nothing"].(bool); aon {
		rows := make([]any, 0, len(docsRaw))
		for range docsRaw {
			rows = append(rows, map[string]any{
				"error": "not_implemented", "reason": "all_or_nothing is not supported",
			})
		}
		writeJSON(w, 417, rows)
		return nil
	}

	// Malformed ids fail the whole batch (per-doc errors are for write
	// outcomes, not invalid requests).
	for _, raw := range docsRaw {
		doc, err := couch.ParseDoc(raw)
		if err != nil {
			continue // reported per-doc below
		}
		if doc.ID != "" {
			if err := s.validateDocIDForDB(db, doc.ID); err != nil {
				return err
			}
		}
	}

	results := make([]any, 0, len(docsRaw))
	report := func(id string, err error) {
		ce, ok := err.(*couch.Error)
		if !ok {
			ce = couch.NewError(500, "unknown_error", err.Error())
		}
		results = append(results, map[string]any{
			"id": id, "error": ce.Err, "reason": ce.Reason,
		})
	}

	// new_edits=true writes are deferred into one set-based store call. The
	// per-doc transaction loop made bulk ingest slow. Their result
	// slots are filled afterwards.
	var bulkWrites []store.BulkWrite
	var bulkSlots []int

	for docIdx, raw := range docsRaw {
		doc, err := couch.ParseDoc(raw)
		if err != nil {
			report("", err)
			continue
		}
		rawDoc := rawDocAt(docIdx)
		docid := doc.ID
		if docid == "" && newEdits {
			docid = randomUUID()
		}
		if docid == "" {
			report("", couch.BadRequest("new_edits=false requires _id"))
			continue
		}
		if err := s.validateDocIDForDB(db, docid); err != nil {
			report(docid, err)
			continue
		}
		if err := s.authorizeDocWrite(r, db, docid); err != nil {
			report(docid, err)
			continue
		}
		// _local docs ride through _bulk_docs into the local store
		// The write is unversioned and always wins.
		if strings.HasPrefix(docid, "_local/") {
			content := make(map[string]any, len(doc.Body))
			for k, v := range doc.Body {
				content[k] = v
			}
			local, err := s.store.PutLocalDoc(r.Context(), db, docid, content)
			if err != nil {
				report(docid, err)
				continue
			}
			if newEdits {
				results = append(results, map[string]any{
					"ok": true, "id": docid, "rev": local.Rev(),
				})
			}
			continue
		}
		if err := s.prepareUsersWrite(r, db, docid, doc.Body, doc.Deleted); err != nil {
			report(docid, err)
			continue
		}
		if db.Name == "_users" && !strings.HasPrefix(docid, "_design/") {
			rawDoc = nil // the server rewrote the body (password hashing)
		}
		atts, err := attachmentWrites(doc.Attachments)
		if err != nil {
			report(docid, err)
			continue
		}
		atts = orderAttachmentWrites(atts, rawDoc)
		s.compressAttachmentWrites(atts)

		if newEdits {
			if err := s.checkPartitionLimit(r, db, docid, doc.Deleted, doc.Body); err != nil {
				report(docid, err)
				continue
			}
			if err := s.validateDocUpdate(r, db, docid, doc.Body, doc.Rev, doc.Deleted, doc.Attachments); err != nil {
				report(docid, err)
				continue
			}
			bulkWrites = append(bulkWrites, store.BulkWrite{
				ID: docid, Body: doc.Body, RawBody: rawDoc,
				Expected: doc.Rev, Deleted: doc.Deleted, Atts: atts,
			})
			bulkSlots = append(bulkSlots, len(results))
			results = append(results, nil) // filled from the batch outcome
			continue
		}

		path := doc.RevPath()
		if len(path) == 0 {
			report(docid, couch.BadRequest("new_edits=false requires a _rev or _revisions"))
			continue
		}
		if err := s.validateDocUpdate(r, db, docid, doc.Body, &path[0], doc.Deleted, doc.Attachments); err != nil {
			report(docid, err)
			continue
		}
		if err := s.store.ForceRev(r.Context(), db, docid, doc.Body, path, doc.Deleted, atts); err != nil {
			report(docid, err)
		}
		// CouchDB reports nothing for successful new_edits=false writes.
	}

	if len(bulkWrites) > 0 {
		outcomes, err := s.store.BulkPutDocs(r.Context(), db, bulkWrites)
		if err != nil {
			return err
		}
		for i, outcome := range outcomes {
			slot := bulkSlots[i]
			if outcome.Err != nil {
				ce, ok := outcome.Err.(*couch.Error)
				if !ok {
					ce = couch.NewError(500, "unknown_error", outcome.Err.Error())
				}
				results[slot] = map[string]any{
					"id": bulkWrites[i].ID, "error": ce.Err, "reason": ce.Reason,
				}
				continue
			}
			results[slot] = map[string]any{
				"ok": true, "id": bulkWrites[i].ID, "rev": outcome.Rev.String(),
			}
		}
	}
	writeJSON(w, 201, results)
	return nil
}

// bulkGet handles POST /{db}/_bulk_get. It fetches batched multi-rev documents, the
// endpoint modern replicators prefer.
func (s *Server) bulkGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	q := r.URL.Query()
	withRevs, err := boolParam(q, "revs", false)
	if err != nil {
		return err
	}
	withAttData, err := boolParam(q, "attachments", false)
	if err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, _ := body.(map[string]any)
	docsRaw, ok := obj["docs"].([]any)
	if !ok {
		return couch.BadRequest("Missing JSON list of `docs`.")
	}

	// Winner fetches with no rev named use two batch queries. Replicators send
	// hundreds of docs per request, and per-document
	// queries would dominate the request. Rev-named entries and attachment
	// bodies keep the per-document path.
	pre := &bulkGetPrefetch{}
	var preIDs []string
	for _, raw := range docsRaw {
		spec, _ := raw.(map[string]any)
		id, _ := spec["id"].(string)
		revStr, _ := spec["rev"].(string)
		if id != "" && revStr == "" && s.validateDocIDForDB(db, id) == nil &&
			couch.ValidateDocID(id) == nil {
			preIDs = append(preIDs, id)
		}
	}
	if len(preIDs) > 0 {
		if pre.docs, err = s.store.GetDocsAny(r.Context(), db, preIDs); err != nil {
			return err
		}
		if !withAttData {
			if pre.atts, err = s.store.WinnerAttachments(r.Context(), db, preIDs); err != nil {
				return err
			}
		}
	}

	results := make([]any, 0, len(docsRaw))
	for _, raw := range docsRaw {
		spec, _ := raw.(map[string]any)
		id, _ := spec["id"].(string)
		revStr, _ := spec["rev"].(string)
		entryError := func(err error) map[string]any {
			ce, ok := err.(*couch.Error)
			if !ok {
				ce = couch.NewError(500, "unknown_error", err.Error())
			}
			rev := revStr
			if rev == "" {
				rev = "undefined"
			}
			return map[string]any{"error": map[string]any{
				"id": id, "rev": rev, "error": ce.Err, "reason": ce.Reason,
			}}
		}
		var docs []any
		if idErr := s.validateDocIDForDB(db, id); id != "" && idErr != nil {
			// Malformed ids report with a null rev, unlike fetch misses.
			ce, _ := idErr.(*couch.Error)
			docs = append(docs, map[string]any{"error": map[string]any{
				"id": id, "rev": nil, "error": ce.Err, "reason": ce.Reason,
			}})
			results = append(results, map[string]any{"id": id, "docs": docs})
			continue
		}
		row, err := s.bulkGetOne(r, db, id, revStr, withRevs, withAttData, pre)
		if err != nil {
			docs = append(docs, entryError(err))
		} else {
			docs = append(docs, map[string]any{"ok": row})
		}
		results = append(results, map[string]any{"id": id, "docs": docs})
	}
	writeJSON(w, 200, map[string]any{"results": results})
	return nil
}

// bulkGetPrefetch carries one request's batch-fetched winner rows and
// attachment metadata (atts nil = look attachments up per doc).
type bulkGetPrefetch struct {
	docs map[string]*store.DocRow
	atts map[string][]store.Attachment
}

func (s *Server) bulkGetOne(
	r *http.Request,
	db *store.DB,
	id, revStr string,
	withRevs, withAttData bool,
	pre *bulkGetPrefetch,
) (map[string]any, error) {
	if id == "" {
		return nil, couch.BadRequest("document id is required")
	}
	if err := couch.ValidateDocID(id); err != nil {
		return nil, err
	}
	var row *store.DocRow
	if revStr == "" {
		winner, ok := pre.docs[id]
		if !ok {
			return nil, couch.DocMissing()
		}
		row = winner
	} else {
		rev, err := couch.ParseRev(revStr)
		if err != nil {
			return nil, err
		}
		got, err := s.store.GetDoc(r.Context(), db, id, &rev)
		if err != nil {
			return nil, err
		}
		row = got
	}
	doc := row.JSON()
	if withRevs {
		revisions, err := s.revisionsJSON(r, db, id, row.Rev)
		if err != nil {
			return nil, err
		}
		doc["_revisions"] = revisions
	}
	if revStr == "" && pre.atts != nil {
		if atts := pre.atts[id]; len(atts) > 0 {
			encodingInfo, err := boolParam(r.URL.Query(), "att_encoding_info", false)
			if err != nil {
				return nil, err
			}
			member, err := attachmentsJSON(atts, false, encodingInfo)
			if err != nil {
				return nil, err
			}
			doc["_attachments"] = member
		}
		return doc, nil
	}
	if err := s.addAttachmentsMember(r, db, doc, id, row.Rev, withAttData); err != nil {
		return nil, err
	}
	return doc, nil
}

// revsDiff is POST /{db}/_revs_diff: which of these revs am I missing?
func (s *Server) revsDiff(w http.ResponseWriter, r *http.Request) error {
	byID, err := s.revsDiffRequest(r)
	if err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}

	out := map[string]any{}
	for id, revs := range byID {
		missing, ancestors, err := s.store.RevsDiff(r.Context(), db, id, revs)
		if err != nil {
			return err
		}
		if len(missing) == 0 {
			continue
		}
		entry := map[string]any{"missing": revStrings(missing)}
		if len(ancestors) > 0 {
			entry["possible_ancestors"] = revStrings(ancestors)
		}
		out[id] = entry
	}
	writeJSON(w, 200, out)
	return nil
}

// missingRevs is the older POST /{db}/_missing_revs shape.
func (s *Server) missingRevs(w http.ResponseWriter, r *http.Request) error {
	byID, err := s.revsDiffRequest(r)
	if err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}

	out := map[string]any{}
	for id, revs := range byID {
		missing, _, err := s.store.RevsDiff(r.Context(), db, id, revs)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			out[id] = revStrings(missing)
		}
	}
	writeJSON(w, 200, map[string]any{"missing_revs": out})
	return nil
}

func (s *Server) revsDiffRequest(r *http.Request) (map[string][]couch.Rev, error) {
	body, err := readJSONBody(r)
	if err != nil {
		return nil, err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return nil, couch.BadRequest("Request body must be a JSON object")
	}
	out := make(map[string][]couch.Rev, len(obj))
	for id, raw := range obj {
		items, ok := raw.([]any)
		if !ok {
			return nil, couch.BadRequest("Invalid rev list for " + id)
		}
		revs := make([]couch.Rev, 0, len(items))
		for _, item := range items {
			revStr, _ := item.(string)
			rev, err := couch.ParseRev(revStr)
			if err != nil {
				return nil, err
			}
			revs = append(revs, rev)
		}
		out[id] = revs
	}
	return out, nil
}

func revStrings(revs []couch.Rev) []string {
	out := make([]string, len(revs))
	for i, rev := range revs {
		out[i] = rev.String()
	}
	return out
}
