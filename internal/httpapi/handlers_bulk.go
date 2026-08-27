package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// bulkDocWrite carries one parsed document through request validation, VDU
// validation, revision preparation, and storage. Keeping it avoids parsing the
// same JSON object once for ID validation and again for the write.
type bulkDocWrite struct {
	doc  *couch.IncomingDoc
	id   string
	raw  []byte
	atts []store.AttachmentWrite
	path []couch.Rev
	err  error
}

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

	// Parse once, assign generated IDs once, and retain the raw bytes used by
	// CouchDB's revision hash. Malformed explicit IDs still fail the whole
	// batch; parse failures remain per-document outcomes.
	prepared := make([]bulkDocWrite, len(docsRaw))
	normalIDs := make([]string, 0, len(docsRaw))
	seenNormalIDs := make(map[string]bool, len(docsRaw))
	hasParsedWrite := false
	for i, raw := range docsRaw {
		doc, parseErr := couch.ParseDoc(raw)
		prepared[i] = bulkDocWrite{doc: doc, raw: rawDocAt(i), err: parseErr}
		if parseErr != nil {
			continue
		}
		if doc.ID != "" {
			if err := s.validateDocIDForDB(db, doc.ID); err != nil {
				return err
			}
		}
		prepared[i].id = doc.ID
		if prepared[i].id == "" && newEdits {
			prepared[i].id = randomUUID()
		}
		if prepared[i].id == "" {
			prepared[i].err = couch.BadRequest("new_edits=false requires _id")
			continue
		}
		hasParsedWrite = true
		if !strings.HasPrefix(prepared[i].id, "_local/") &&
			!seenNormalIDs[prepared[i].id] {
			seenNormalIDs[prepared[i].id] = true
			normalIDs = append(normalIDs, prepared[i].id)
		}
	}

	// One security read authorizes the whole batch for non-server-admins. The
	// same immutable object is passed to every VDU.
	user := userOf(r)
	var securityMap map[string]any
	var securityObj *couch.SecurityObj
	var securityErr error
	if hasParsedWrite && !user.IsServerAdmin() {
		s.vdu.securityReads.Add(1)
		securityMap, securityErr = s.store.GetSecurity(r.Context(), db)
		if securityErr == nil {
			securityObj = couch.ParseSecurity(securityMap)
		}
	}

	// VDU discovery and all database-backed validation inputs are immutable for
	// this request. _users also reuses the winner batch for its built-in rules.
	var vduSnapshot *vduRequestSnapshot
	var vduSnapshotErr error
	if len(normalIDs) > 0 && securityErr == nil {
		vduSnapshot, vduSnapshotErr = s.newVDURequestSnapshot(
			r, db, normalIDs, securityMap, db.Name == "_users")
	}

	// Complete all fallible prevalidation before applying any local,
	// replicated, or ordinary write.
	for i := range prepared {
		item := &prepared[i]
		if item.err != nil {
			continue
		}
		if securityErr != nil {
			item.err = securityErr
			continue
		}
		if !user.IsServerAdmin() {
			item.err = authorizeDocWriteWithSecurity(user, securityObj, db, item.id)
			if item.err != nil {
				continue
			}
		}
		if strings.HasPrefix(item.id, "_local/") {
			continue
		}
		if vduSnapshotErr != nil {
			item.err = vduSnapshotErr
			continue
		}
		var oldBody map[string]any
		if vduSnapshot != nil {
			if old := vduSnapshot.oldDocs[item.id]; old != nil && !old.Deleted {
				oldBody = old.Body
			}
		}
		item.err = s.prepareUsersWriteWithOld(
			r, db, item.id, item.doc.Body, item.doc.Deleted, oldBody)
		if item.err != nil {
			continue
		}
		if db.Name == "_users" && !strings.HasPrefix(item.id, "_design/") {
			item.raw = nil // the server rewrote the body (password hashing)
		}
		item.atts, item.err = attachmentWrites(item.doc.Attachments)
		if item.err != nil {
			continue
		}
		item.atts = orderAttachmentWrites(item.atts, item.raw)
		s.compressAttachmentWrites(item.atts)

		validationRev := item.doc.Rev
		if !newEdits {
			item.path = item.doc.RevPath()
			if len(item.path) == 0 {
				item.err = couch.BadRequest("new_edits=false requires a _rev or _revisions")
				continue
			}
			validationRev = &item.path[0]
		}
		item.err = s.validateDocUpdateWithSnapshot(r.Context(), vduSnapshot,
			item.id, item.doc.Body, validationRev, item.doc.Deleted, item.doc.Attachments)
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
	var bulkWrites []store.BulkWrite
	var bulkSlots []int
	for i := range prepared {
		item := &prepared[i]
		if item.err != nil {
			report(item.id, item.err)
			continue
		}
		if strings.HasPrefix(item.id, "_local/") {
			content := make(map[string]any, len(item.doc.Body))
			for k, v := range item.doc.Body {
				content[k] = v
			}
			local, err := s.store.PutLocalDoc(r.Context(), db, item.id, content)
			if err != nil {
				report(item.id, err)
				continue
			}
			if newEdits {
				results = append(results, map[string]any{
					"ok": true, "id": item.id, "rev": local.Rev(),
				})
			}
			continue
		}
		if newEdits {
			bulkWrites = append(bulkWrites, store.BulkWrite{
				ID: item.id, Body: item.doc.Body, RawBody: item.raw,
				Expected: item.doc.Rev, Deleted: item.doc.Deleted, Atts: item.atts,
			})
			bulkSlots = append(bulkSlots, len(results))
			results = append(results, nil)
			continue
		}
		if err := s.store.ForceRev(r.Context(), db, item.id, item.doc.Body,
			item.path, item.doc.Deleted, item.atts); err != nil {
			report(item.id, err)
		}
		// CouchDB reports nothing for successful new_edits=false writes.
	}

	if len(bulkWrites) > 0 {
		maxPartitionSize := int64(s.config.getInt(
			"couchdb", "max_partition_size", 10737418240))
		outcomes, err := s.store.BulkPutDocsWithLimit(
			r.Context(), db, bulkWrites, maxPartitionSize)
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
