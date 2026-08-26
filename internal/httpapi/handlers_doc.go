package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/klauspost/compress/gzip"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// The _design/{ddoc} routes re-join the full document id and share the
// document implementations.

func (s *Server) designGet(w http.ResponseWriter, r *http.Request) error {
	return s.docGetImpl(w, r, r.PathValue("db"), "_design/"+r.PathValue("ddoc"))
}

func (s *Server) designPut(w http.ResponseWriter, r *http.Request) error {
	return s.docPutImpl(w, r, r.PathValue("db"), "_design/"+r.PathValue("ddoc"))
}

func (s *Server) designDelete(w http.ResponseWriter, r *http.Request) error {
	return s.docDeleteImpl(w, r, r.PathValue("db"), "_design/"+r.PathValue("ddoc"))
}

func (s *Server) designCopyOr405(w http.ResponseWriter, r *http.Request) error {
	return s.copyOr405(w, r, r.PathValue("db"), "_design/"+r.PathValue("ddoc"))
}

func (s *Server) docGet(w http.ResponseWriter, r *http.Request) error {
	return s.docGetImpl(w, r, r.PathValue("db"), r.PathValue("docid"))
}

func (s *Server) docPut(w http.ResponseWriter, r *http.Request) error {
	return s.docPutImpl(w, r, r.PathValue("db"), r.PathValue("docid"))
}

func (s *Server) docDelete(w http.ResponseWriter, r *http.Request) error {
	return s.docDeleteImpl(w, r, r.PathValue("db"), r.PathValue("docid"))
}

func (s *Server) docCopyOr405(w http.ResponseWriter, r *http.Request) error {
	return s.copyOr405(w, r, r.PathValue("db"), r.PathValue("docid"))
}

// authorizeDocRead is the member check plus _users' per-doc exception.
// authenticated users can always read their own user doc, and nobody but a
// db admin can read anyone else's.
func (s *Server) authorizeDocRead(r *http.Request, db *store.DB, docid string) error {
	user := userOf(r)
	if user.IsServerAdmin() {
		return nil
	}
	security, err := s.security(r, db)
	if err != nil {
		return err
	}
	if db.Name == "_users" && !security.IsDBAdmin(user) {
		if !user.IsAnonymous() && docid == couch.UserDocPrefix+user.Name {
			return nil
		}
		if user.IsAnonymous() {
			return couch.MemberRequired(user)
		}
		// TODO(compat): confirm CouchDB's exact status/reason here.
		return couch.Forbidden("You may only view your own user document.")
	}
	if !security.IsDBMember(user) {
		return couch.MemberRequired(user)
	}
	return nil
}

// authorizeDocWrite is the member check plus the design-doc admin rule and
// _users' own-doc exception. The _users auth-ddoc rules run separately in
// prepareUsersWrite once the old doc is known.
func (s *Server) authorizeDocWrite(r *http.Request, db *store.DB, docid string) error {
	user := userOf(r)
	if user.IsServerAdmin() {
		return nil
	}
	security, err := s.security(r, db)
	if err != nil {
		return err
	}
	if strings.HasPrefix(docid, "_design/") {
		if !security.IsDBAdmin(user) {
			return couch.DBAdminRequired(user)
		}
		return nil
	}
	if db.Name == "_users" && !security.IsDBAdmin(user) {
		if !user.IsAnonymous() && docid == couch.UserDocPrefix+user.Name {
			return nil
		}
		if user.IsAnonymous() {
			return couch.MemberRequired(user)
		}
		return couch.Forbidden("You may only update your own user document.")
	}
	if !security.IsDBMember(user) {
		return couch.MemberRequired(user)
	}
	return nil
}

// prepareUsersWrite applies CouchDB's built-in _users validation and
// password hashing before a user-doc write.
func (s *Server) prepareUsersWrite(
	r *http.Request,
	db *store.DB,
	docid string,
	body map[string]any,
	deleting bool,
) error {
	if db.Name != "_users" ||
		strings.HasPrefix(docid, "_design/") || strings.HasPrefix(docid, "_local/") {
		return nil
	}
	var old map[string]any
	if row, err := s.store.GetDocAny(r.Context(), db, docid); err == nil && !row.Deleted {
		old = row.Body
	}
	minIterations, maxIterations := s.config.passwordIterationBounds()
	if err := couch.ValidateUserDoc(docid, body, old, deleting, userOf(r),
		minIterations, maxIterations); err != nil {
		return err
	}
	if !deleting {
		if err := s.passwordAuth.prepareUserDoc(
			r.Context(), body, s.config.passwordIterations()); err != nil {
			if r.Context().Err() != nil {
				return r.Context().Err()
			}
			return couch.BadRequest("Invalid password hashing configuration")
		}
	}
	return nil
}

func (s *Server) docGetImpl(w http.ResponseWriter, r *http.Request, dbName, docid string) error {
	if err := couch.ValidateDocID(docid); err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if err := s.authorizeDocRead(r, db, docid); err != nil {
		return err
	}
	q := r.URL.Query()

	if q.Has("open_revs") {
		return s.openRevs(w, r, db, docid)
	}

	rev, err := revParam(q, "rev")
	if err != nil {
		return err
	}
	row, err := s.store.GetDoc(r.Context(), db, docid, rev)
	if err != nil {
		return err
	}

	revString := row.Rev.String()
	// Weak validators (W/"...") also match. CouchDB ignores the marker.
	if inm := strings.TrimPrefix(r.Header.Get("If-None-Match"), "W/"); inm != "" &&
		strings.Trim(inm, `"`) == revString {
		setETag(w, revString)
		w.Header().Set("Content-Length", "0") // CouchDB sends it on 304
		w.WriteHeader(http.StatusNotModified)
		return nil
	}

	doc := row.JSON()
	if localSeq, err := boolParam(q, "local_seq", false); err != nil {
		return err
	} else if localSeq {
		doc["_local_seq"] = row.Seq
	}
	meta, err := boolParam(q, "meta", false)
	if err != nil {
		return err
	}
	if revs, err := boolParam(q, "revs", false); err != nil {
		return err
	} else if revs {
		revisions, err := s.revisionsJSON(r, db, docid, row.Rev)
		if err != nil {
			return err
		}
		doc["_revisions"] = revisions
	}
	revsInfo, err := boolParam(q, "revs_info", false)
	if err != nil {
		return err
	}
	if revsInfo || meta {
		ancestry, err := s.store.Ancestry(r.Context(), db, docid, row.Rev)
		if err != nil {
			return err
		}
		infos := make([]map[string]string, 0, len(ancestry))
		for _, entry := range ancestry {
			status := "missing"
			if entry.Available {
				status = "available"
				if entry.Deleted {
					status = "deleted"
				}
			}
			infos = append(infos, map[string]string{
				"rev":    couch.Rev{Num: entry.Num, Hash: entry.Hash}.String(),
				"status": status,
			})
		}
		doc["_revs_info"] = infos
	}

	// Conflict members: live non-winner leaves and deleted leaves. CouchDB
	// omits the members when empty.
	wantConflicts, err := boolParam(q, "conflicts", false)
	if err != nil {
		return err
	}
	wantDeletedConflicts, err := boolParam(q, "deleted_conflicts", false)
	if err != nil {
		return err
	}
	if wantConflicts || wantDeletedConflicts || meta {
		leaves, err := s.store.Leaves(r.Context(), db, docid)
		if err != nil {
			return err
		}
		var conflicts, deletedConflicts []string
		for _, leaf := range leaves {
			if leaf.Rev == row.Rev {
				continue
			}
			if leaf.Deleted {
				deletedConflicts = append(deletedConflicts, leaf.Rev.String())
			} else {
				conflicts = append(conflicts, leaf.Rev.String())
			}
		}
		if (wantConflicts || meta) && len(conflicts) > 0 {
			doc["_conflicts"] = conflicts
		}
		if (wantDeletedConflicts || meta) && len(deletedConflicts) > 0 {
			doc["_deleted_conflicts"] = deletedConflicts
		}
	}

	includeAttData, err := boolParam(q, "attachments", false)
	if err != nil {
		return err
	}
	sinceRevs, hasAttsSince, err := attsSinceParam(q)
	if err != nil {
		return err
	}
	if (includeAttData || hasAttsSince) && strings.Contains(r.Header.Get("Accept"), "multipart/related") {
		if atts, err := s.store.Attachments(r.Context(), db, docid, row.Rev); err == nil && len(atts) > 0 {
			stubPos := 0
			if hasAttsSince && !includeAttData {
				if stubPos, err = s.attsSinceRevPos(r, db, docid, row.Rev, sinceRevs); err != nil {
					return err
				}
			}
			return s.writeDocMultipartRelated(w, r, db, doc, docid, row.Rev, stubPos)
		}
	}
	if hasAttsSince && !includeAttData {
		if err := s.addAttachmentsSinceMember(r, db, doc, docid, row.Rev, sinceRevs); err != nil {
			return err
		}
	} else if err := s.addAttachmentsMember(r, db, doc, docid, row.Rev, includeAttData); err != nil {
		return err
	}

	setETag(w, revString)
	writeJSON(w, 200, doc)
	return nil
}

// revisionsJSON builds the ?revs=true _revisions member: the rev's ancestry,
// newest first (older entries may be pruned).
func (s *Server) revisionsJSON(
	r *http.Request,
	db *store.DB,
	docid string,
	head couch.Rev,
) (map[string]any, error) {
	ancestry, err := s.store.Ancestry(r.Context(), db, docid, head)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(ancestry))
	for _, entry := range ancestry {
		ids = append(ids, entry.Hash)
	}
	// CouchDB caps _revisions at revs_limit when rendering, even before
	// compaction physically stems the history.
	if db.RevsLimit > 0 && len(ids) > db.RevsLimit {
		ids = ids[:db.RevsLimit]
	}
	return map[string]any{"start": head.Num, "ids": ids}, nil
}

// attachmentsJSON builds the _attachments member for a served revision.
// Inline data is the decoded (identity) form by default. With
// att_encoding_info the stored encoded form travels along with its
// encoding/encoded_length bookkeeping. This preserves gzip
// encoding and digests end to end.
func attachmentsJSON(atts []store.Attachment, includeData, encodingInfo bool) (map[string]any, error) {
	out := make(map[string]any, len(atts))
	for _, att := range atts {
		entry := map[string]any{
			"content_type": att.ContentType,
			"revpos":       att.RevPos,
			"digest":       att.Digest,
			"length":       att.Length,
		}
		if encodingInfo && att.Encoding != "" {
			entry["encoding"] = att.Encoding
			entry["encoded_length"] = att.EncodedLength
		}
		if includeData {
			data, err := att.DecodedData()
			if err != nil {
				return nil, err
			}
			entry["data"] = base64.StdEncoding.EncodeToString(data)
			delete(entry, "length")
		} else {
			entry["stub"] = true
		}
		out[att.Name] = entry
	}
	return out, nil
}

// attsSinceParam parses ?atts_since=[...]: the ancestor revs whose
// attachments the client already has.
func attsSinceParam(q url.Values) ([]couch.Rev, bool, error) {
	raw := q.Get("atts_since")
	if raw == "" {
		return nil, false, nil
	}
	parsed, err := couch.DecodeJSON([]byte(raw))
	if err != nil {
		return nil, false, couch.BadRequest("invalid atts_since: " + raw)
	}
	items, ok := parsed.([]any)
	if !ok {
		return nil, false, couch.BadRequest("invalid atts_since: " + raw)
	}
	revs := make([]couch.Rev, 0, len(items))
	for _, item := range items {
		str, _ := item.(string)
		rev, err := couch.ParseRev(str)
		if err != nil {
			return nil, false, err
		}
		revs = append(revs, rev)
	}
	return revs, true, nil
}

// attsSinceRevPos walks the served rev's ancestry newest-first and returns
// the position of the first rev the client named in atts_since (0 when
// none is an ancestor. Attachments at or below that position travel as
// stubs, newer ones carry data.
func (s *Server) attsSinceRevPos(
	r *http.Request,
	db *store.DB,
	docid string,
	head couch.Rev,
	since []couch.Rev,
) (int, error) {
	ancestry, err := s.store.Ancestry(r.Context(), db, docid, head)
	if err != nil {
		return 0, err
	}
	for _, entry := range ancestry {
		for _, rev := range since {
			if entry.Num == rev.Num && entry.Hash == rev.Hash {
				return entry.Num, nil
			}
		}
	}
	return 0, nil
}

// splitAttsSince partitions attachments around an atts_since ancestor
// position. Attachments added after it carry data. The rest stay stubs.
func splitAttsSince(atts []store.Attachment, pos int) (newer, stubs []store.Attachment) {
	for _, att := range atts {
		if att.RevPos > pos {
			newer = append(newer, att)
		} else {
			stubs = append(stubs, att)
		}
	}
	return newer, stubs
}

// addAttachmentsSinceMember is addAttachmentsMember under ?atts_since=[...].
// attachments the named ancestors already had stay stubs, newer ones carry
// inline data.
func (s *Server) addAttachmentsSinceMember(
	r *http.Request,
	db *store.DB,
	doc map[string]any,
	docid string,
	rev couch.Rev,
	since []couch.Rev,
) error {
	encodingInfo, err := boolParam(r.URL.Query(), "att_encoding_info", false)
	if err != nil {
		return err
	}
	atts, err := s.store.AttachmentsWithData(r.Context(), db, docid, rev)
	if err != nil {
		return err
	}
	if len(atts) == 0 {
		return nil
	}
	pos, err := s.attsSinceRevPos(r, db, docid, rev, since)
	if err != nil {
		return err
	}
	newer, stubs := splitAttsSince(atts, pos)
	member, err := attachmentsJSON(newer, true, encodingInfo)
	if err != nil {
		return err
	}
	stubMember, err := attachmentsJSON(stubs, false, encodingInfo)
	if err != nil {
		return err
	}
	for name, spec := range stubMember {
		member[name] = spec
	}
	doc["_attachments"] = member
	return nil
}

// addAttachmentsMember attaches the _attachments member to a doc JSON when
// the revision has any.
func (s *Server) addAttachmentsMember(
	r *http.Request,
	db *store.DB,
	doc map[string]any,
	docid string,
	rev couch.Rev,
	includeData bool,
) error {
	encodingInfo, err := boolParam(r.URL.Query(), "att_encoding_info", false)
	if err != nil {
		return err
	}
	var atts []store.Attachment
	if includeData {
		atts, err = s.store.AttachmentsWithData(r.Context(), db, docid, rev)
	} else {
		atts, err = s.store.Attachments(r.Context(), db, docid, rev)
	}
	if err != nil {
		return err
	}
	if len(atts) == 0 {
		return nil
	}
	member, err := attachmentsJSON(atts, includeData, encodingInfo)
	if err != nil {
		return err
	}
	doc["_attachments"] = member
	return nil
}

// validateDesignDoc rejects design docs CouchDB refuses to save. Today
// that's builtin reduce names it doesn't ship.
func validateDesignDoc(body map[string]any) error {
	views, _ := body["views"].(map[string]any)
	for _, raw := range views {
		view, _ := raw.(map[string]any)
		reduce, _ := view["reduce"].(string)
		if reduce == "" || !strings.HasPrefix(reduce, "_") {
			continue
		}
		if _, resolveErr := couch.ResolveBuiltinReduce(reduce); resolveErr != nil {
			return resolveErr
		}
	}
	return nil
}

// attachmentWrites converts a parsed _attachments member into store writes.
// inline base64 data or stubs carried from the parent revision.
func attachmentWrites(member map[string]any) ([]store.AttachmentWrite, error) {
	if len(member) == 0 {
		return nil, nil
	}
	writes := make([]store.AttachmentWrite, 0, len(member))
	for name, raw := range member {
		if strings.HasPrefix(name, "_") {
			return nil, couch.BadRequest(
				"Attachment name '" + name + "' starts with prohibited character '_'")
		}
		spec, ok := raw.(map[string]any)
		if !ok {
			return nil, couch.BadRequest("Invalid attachment: " + name)
		}
		if stub, _ := spec["stub"].(bool); stub {
			write := attachmentWriteFromSpec(name, spec, nil)
			writes = append(writes, write)
			continue
		}
		if follows, _ := spec["follows"].(bool); follows {
			return nil, couch.BadRequest(
				"Attachment " + name + " uses follows outside a multipart request")
		}
		data, ok := spec["data"].(string)
		if !ok {
			return nil, couch.BadRequest("Invalid attachment data: " + name)
		}
		decoded, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, couch.BadRequest("Invalid attachment data: " + name)
		}
		writes = append(writes, attachmentWriteFromSpec(name, spec, decoded))
	}
	return writes, nil
}

// attachmentWriteFromSpec reads an _attachments entry's bookkeeping. A
// replicated entry may declare its data gzip-encoded ("encoding") along with
// the decoded "length" and a digest over the encoded bytes. All are preserved
// verbatim so replication round-trips exactly.
func attachmentWriteFromSpec(name string, spec map[string]any, data []byte) store.AttachmentWrite {
	write := store.AttachmentWrite{Name: name, Data: data}
	if ct, ok := spec["content_type"].(string); ok {
		write.ContentType = ct
	} else {
		write.ContentType = "application/octet-stream"
	}
	if revpos, ok := spec["revpos"].(json.Number); ok {
		if n, err := revpos.Int64(); err == nil {
			write.RevPos = int(n)
		}
	}
	if encoding, ok := spec["encoding"].(string); ok && encoding != "" && data != nil {
		write.Encoding = encoding
		if digest, ok := spec["digest"].(string); ok {
			write.Digest = digest
		}
		if length, ok := spec["length"].(json.Number); ok {
			if n, err := length.Int64(); err == nil {
				write.DecodedLength = n
			}
		}
		// Inlined entries omit "length". Measure the identity size so the
		// stub bookkeeping stays exact.
		if write.DecodedLength == 0 && encoding == "gzip" {
			if reader, err := gzip.NewReader(bytes.NewReader(data)); err == nil {
				if n, err := io.Copy(io.Discard, reader); err == nil {
					write.DecodedLength = n
				}
				reader.Close()
			}
		}
	}
	return write
}

// openRevs serves ?open_revs=all and ?open_revs=[...]: every requested leaf
// of the revision tree, with bodies. ?latest=true maps a requested rev to
// the leaf that descends from it. Replicators pulling attachment-bearing
// docs ask for multipart/mixed. Everyone else gets a JSON array.
func (s *Server) openRevs(w http.ResponseWriter, r *http.Request, db *store.DB, docid string) error {
	q := r.URL.Query()
	leaves, err := s.store.Leaves(r.Context(), db, docid)
	if err != nil {
		return err
	}
	withRevs, err := boolParam(q, "revs", false)
	if err != nil {
		return err
	}
	latest, err := boolParam(q, "latest", false)
	if err != nil {
		return err
	}
	includeAttData, err := boolParam(q, "attachments", false)
	if err != nil {
		return err
	}
	sinceRevs, hasAttsSince, err := attsSinceParam(q)
	if err != nil {
		return err
	}
	multipart := !wantsJSONOpenRevs(r)

	buildEntry := func(leaf store.Leaf) (openRevEntry, error) {
		row := &store.DocRow{ID: docid, Rev: leaf.Rev, Deleted: leaf.Deleted, Body: leaf.Body}
		doc := row.JSON()
		if withRevs {
			revisions, err := s.revisionsJSON(r, db, docid, leaf.Rev)
			if err != nil {
				return openRevEntry{}, err
			}
			doc["_revisions"] = revisions
		}
		entry := openRevEntry{doc: doc}
		if multipart {
			// Attachment bytes travel as multipart parts. With atts_since,
			// attachments the client's ancestors already had become stubs in
			// the doc part instead.
			atts, err := s.store.AttachmentsWithData(r.Context(), db, docid, leaf.Rev)
			if err != nil {
				return openRevEntry{}, err
			}
			if hasAttsSince && len(atts) > 0 {
				pos, err := s.attsSinceRevPos(r, db, docid, leaf.Rev, sinceRevs)
				if err != nil {
					return openRevEntry{}, err
				}
				var stubs []store.Attachment
				atts, stubs = splitAttsSince(atts, pos)
				if len(stubs) > 0 {
					member, err := attachmentsJSON(stubs, false, true)
					if err != nil {
						return openRevEntry{}, err
					}
					doc["_attachments"] = member
				}
			}
			entry.atts = atts
			return entry, nil
		}
		if hasAttsSince {
			if err := s.addAttachmentsSinceMember(r, db, doc, docid, leaf.Rev, sinceRevs); err != nil {
				return openRevEntry{}, err
			}
			return entry, nil
		}
		if err := s.addAttachmentsMember(r, db, doc, docid, leaf.Rev, includeAttData); err != nil {
			return openRevEntry{}, err
		}
		return entry, nil
	}

	raw := q.Get("open_revs")
	var entries []openRevEntry
	if raw == "all" {
		for _, leaf := range leaves {
			entry, err := buildEntry(leaf)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
	} else {
		parsed, err := couch.DecodeJSON([]byte(raw))
		if err != nil {
			return couch.BadRequest("invalid open_revs: " + raw)
		}
		items, ok := parsed.([]any)
		if !ok {
			return couch.BadRequest("invalid open_revs: " + raw)
		}
		for _, item := range items {
			revStr, _ := item.(string)
			rev, err := couch.ParseRev(revStr)
			if err != nil {
				return err
			}
			leaf, found, err := s.resolveOpenRev(r, db, docid, rev, leaves, latest)
			if err != nil {
				return err
			}
			if !found {
				entries = append(entries, openRevEntry{missingRev: revStr})
				continue
			}
			entry, err := buildEntry(leaf)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
	}

	if multipart {
		return writeOpenRevsMultipart(w, entries)
	}
	results := make([]any, 0, len(entries))
	for _, entry := range entries {
		if entry.missingRev != "" {
			results = append(results, map[string]any{"missing": entry.missingRev})
		} else {
			results = append(results, map[string]any{"ok": entry.doc})
		}
	}
	writeJSON(w, 200, results)
	return nil
}

// resolveOpenRev matches a requested rev against the tree's leaves. With
// latest=true a non-leaf rev resolves to the leaf descending from it.
func (s *Server) resolveOpenRev(
	r *http.Request,
	db *store.DB,
	docid string,
	rev couch.Rev,
	leaves []store.Leaf,
	latest bool,
) (store.Leaf, bool, error) {
	for _, leaf := range leaves {
		if leaf.Rev == rev {
			return leaf, true, nil
		}
	}
	if !latest {
		return store.Leaf{}, false, nil
	}
	for _, leaf := range leaves {
		ancestry, err := s.store.Ancestry(r.Context(), db, docid, leaf.Rev)
		if err != nil {
			return store.Leaf{}, false, err
		}
		for _, entry := range ancestry {
			if entry.Num == rev.Num && entry.Hash == rev.Hash {
				return leaf, true, nil
			}
		}
	}
	return store.Leaf{}, false, nil
}

func (s *Server) docPutImpl(w http.ResponseWriter, r *http.Request, dbName, docid string) error {
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	// A bare or empty _local path names no local doc (CouchDB wording).
	if docid == "_local" || docid == "_local/" {
		return couch.BadRequest("Invalid _local document id.")
	}
	if err := s.validateDocIDForDB(db, docid); err != nil {
		return err
	}

	// multipart/related bodies carry the doc JSON plus attachment parts
	// (how replicators push large attachments).
	var body any
	var rawDoc []byte
	var multipartParts map[string][]byte
	if boundary, ok := isMultipartRelated(r); ok {
		body, rawDoc, multipartParts, err = readDocMultipart(r.Body, boundary)
	} else {
		body, rawDoc, err = readRawJSONBody(r)
	}
	if err != nil {
		return err
	}
	doc, err := couch.ParseDoc(body)
	if err != nil {
		return err
	}
	// Rev consistency (body vs etag vs query) is checked. A body _id that
	// disagrees with the path is simply ignored (the path names the doc,
	// like CouchDB 3.x).
	if _, err := resolveRev(r, doc.Rev); err != nil {
		return err
	}
	if strings.HasPrefix(docid, "_design/") {
		if err := validateDesignDoc(doc.Body); err != nil {
			return err
		}
	}
	if err := s.authorizeDocWrite(r, db, docid); err != nil {
		return err
	}
	if err := s.prepareUsersWrite(r, db, docid, doc.Body, doc.Deleted); err != nil {
		return err
	}
	if db.Name == "_users" && !strings.HasPrefix(docid, "_design/") {
		rawDoc = nil // the server rewrote the body (password hashing)
	}
	var atts []store.AttachmentWrite
	if multipartParts != nil {
		atts, err = attachmentWritesWithParts(doc.Attachments, multipartParts)
	} else {
		atts, err = attachmentWrites(doc.Attachments)
	}
	if err != nil {
		return err
	}
	atts = orderAttachmentWrites(atts, rawDoc)
	s.compressAttachmentWrites(atts)

	// new_edits=false is a replicated write carrying its own rev path.
	if newEdits, err := boolParam(r.URL.Query(), "new_edits", true); err != nil {
		return err
	} else if !newEdits {
		path := doc.RevPath()
		if len(path) == 0 {
			// Replicators may name the rev in the query string instead.
			if qrev, err := revParam(r.URL.Query(), "rev"); err == nil && qrev != nil {
				path = []couch.Rev{*qrev}
			}
		}
		if len(path) == 0 {
			return couch.BadRequest("new_edits=false requires a _rev or _revisions")
		}
		if err := s.validateDocUpdate(r, db, docid, doc.Body, &path[0], doc.Deleted, doc.Attachments); err != nil {
			return err
		}
		if err := s.store.ForceRev(r.Context(), db, docid, doc.Body, path, doc.Deleted, atts); err != nil {
			return err
		}
		return s.writeDocSuccess(w, r, dbName, docid, path[0])
	}

	expected, err := resolveRev(r, doc.Rev)
	if err != nil {
		return err
	}
	if err := s.checkPartitionLimit(r, db, docid, doc.Deleted, doc.Body); err != nil {
		return err
	}
	if err := s.validateDocUpdate(r, db, docid, doc.Body, expected, doc.Deleted, doc.Attachments); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, docid, doc.Body, rawDoc, expected, doc.Deleted, atts)
	if err != nil {
		if batchAccepted(w, r, docid, err) {
			return nil
		}
		return err
	}
	return s.writeDocSuccess(w, r, dbName, docid, rev)
}

func (s *Server) docDeleteImpl(w http.ResponseWriter, r *http.Request, dbName, docid string) error {
	if err := couch.ValidateDocID(docid); err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if err := s.authorizeDocWrite(r, db, docid); err != nil {
		return err
	}
	if err := s.prepareUsersWrite(r, db, docid, map[string]any{}, true); err != nil {
		return err
	}
	expected, err := resolveRev(r, nil)
	if err != nil {
		return err
	}
	// Deleting a doc that was never there is 404 (rev or not). A missing
	// rev on an existing doc is the conflict.
	if _, err := s.store.GetDocAny(r.Context(), db, docid); err != nil {
		return err
	}
	if expected == nil {
		return couch.Conflict()
	}
	if err := s.validateDocUpdate(r, db, docid, map[string]any{}, expected, true, nil); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, docid, map[string]any{}, nil, expected, true, nil)
	if err != nil {
		return err
	}
	revString := rev.String()
	setETag(w, revString)
	writeJSON(w, 200, map[string]any{"ok": true, "id": docid, "rev": revString})
	return nil
}

// docPost handles POST /{db}: create a document, generating an id if absent.
func (s *Server) docPost(w http.ResponseWriter, r *http.Request) error {
	dbName := r.PathValue("db")
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if ct := r.Header.Get("Content-Type"); ct != "" &&
		!strings.HasPrefix(ct, "application/json") {
		return couch.BadContentType("Content-Type must be application/json")
	}
	body, rawDoc, err := readRawJSONBody(r)
	if err != nil {
		return err
	}
	doc, err := couch.ParseDoc(body)
	if err != nil {
		return err
	}
	docid := doc.ID
	if docid == "" {
		docid = randomUUID()
	}
	if err := s.validateDocIDForDB(db, docid); err != nil {
		return err
	}
	if err := s.authorizeDocWrite(r, db, docid); err != nil {
		return err
	}
	if err := s.prepareUsersWrite(r, db, docid, doc.Body, doc.Deleted); err != nil {
		return err
	}
	if db.Name == "_users" && !strings.HasPrefix(docid, "_design/") {
		rawDoc = nil // the server rewrote the body (password hashing)
	}
	atts, err := attachmentWrites(doc.Attachments)
	if err != nil {
		return err
	}
	atts = orderAttachmentWrites(atts, rawDoc)
	s.compressAttachmentWrites(atts)
	if err := s.checkPartitionLimit(r, db, docid, doc.Deleted, doc.Body); err != nil {
		return err
	}
	if err := s.validateDocUpdate(r, db, docid, doc.Body, doc.Rev, doc.Deleted, doc.Attachments); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, docid, doc.Body, rawDoc, doc.Rev, doc.Deleted, atts)
	if err != nil {
		if batchAccepted(w, r, docid, err) {
			return nil
		}
		return err
	}
	return s.writeDocSuccess(w, r, dbName, docid, rev)
}

func (s *Server) copyOr405(w http.ResponseWriter, r *http.Request, dbName, docid string) error {
	if r.Method == "POST" &&
		strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		return s.formDataUpdate(w, r, dbName, docid)
	}
	if r.Method != "COPY" {
		return couch.MethodNotAllowed("DELETE,GET,HEAD,PUT,COPY")
	}
	if err := couch.ValidateDocID(docid); err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}

	destination := r.Header.Get("Destination")
	if destination == "" {
		return couch.BadRequest("Destination header is mandatory for COPY.")
	}
	if strings.Contains(destination, "://") {
		return couch.BadRequest("Destination URL must be relative.")
	}
	destID := destination
	var destRev *couch.Rev
	if id, query, found := strings.Cut(destination, "?"); found {
		destID = id
		for param := range strings.SplitSeq(query, "&") {
			if raw, ok := strings.CutPrefix(param, "rev="); ok {
				rev, err := couch.ParseRev(raw)
				if err != nil {
					return err
				}
				destRev = &rev
			}
		}
	}
	if err := s.validateDocIDForDB(db, destID); err != nil {
		return err
	}

	if err := s.authorizeDocRead(r, db, docid); err != nil {
		return err
	}
	if err := s.authorizeDocWrite(r, db, destID); err != nil {
		return err
	}
	sourceRev, err := revParam(r.URL.Query(), "rev")
	if err != nil {
		return err
	}
	source, err := s.store.GetDoc(r.Context(), db, docid, sourceRev)
	if err != nil {
		return err
	}
	if err := s.prepareUsersWrite(r, db, destID, source.Body, false); err != nil {
		return err
	}
	// Attachments copy with the document.
	sourceAtts, err := s.store.AttachmentsWithData(r.Context(), db, docid, source.Rev)
	if err != nil {
		return err
	}
	var atts []store.AttachmentWrite
	for _, att := range sourceAtts {
		// Data holds the stored (possibly gzip) form. The encoding
		// bookkeeping and digest must travel with it.
		atts = append(atts, store.AttachmentWrite{
			Name:          att.Name,
			ContentType:   att.ContentType,
			Data:          att.Data,
			Encoding:      att.Encoding,
			DecodedLength: att.Length,
			Digest:        att.Digest,
		})
	}
	if err := s.validateDocUpdate(r, db, destID, source.Body, destRev, false, nil); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, destID, source.Body, nil, destRev, false, atts)
	if err != nil {
		return err
	}
	return s.writeDocSuccess(w, r, dbName, destID, rev)
}

// resolveRev merges the three places a client can put the expected rev.
// CouchDB 400s when they disagree.
// formDataUpdate is the legacy Futon form upload. It POSTs a doc as
// multipart/form-data with a _rev field and file parts named _attachments.
func (s *Server) formDataUpdate(w http.ResponseWriter, r *http.Request, dbName, docid string) error {
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if err := s.validateDocIDForDB(db, docid); err != nil {
		return err
	}
	if err := s.authorizeDocWrite(r, db, docid); err != nil {
		return err
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		return mapBodyReadErr(err, "invalid multipart/form-data body")
	}
	revStr := r.FormValue("_rev")
	rev, err := couch.ParseRev(revStr)
	if err != nil {
		return err
	}
	current, err := s.store.GetDoc(r.Context(), db, docid, &rev)
	if err != nil {
		return err
	}
	existing, err := s.store.Attachments(r.Context(), db, docid, rev)
	if err != nil {
		return err
	}
	var atts []store.AttachmentWrite
	seen := map[string]bool{}
	for _, headers := range r.MultipartForm.File["_attachments"] {
		file, err := headers.Open()
		if err != nil {
			return couch.BadRequest("could not read form attachment")
		}
		data, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return couch.BadRequest("could not read form attachment")
		}
		contentType := headers.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		atts = append(atts, store.AttachmentWrite{
			Name: headers.Filename, ContentType: contentType, Data: data,
		})
		seen[headers.Filename] = true
	}
	for _, att := range existing {
		if !seen[att.Name] {
			atts = append(atts, store.AttachmentWrite{Name: att.Name})
		}
	}
	s.compressAttachmentWrites(atts)
	if err := s.validateDocUpdate(r, db, docid, current.Body, &rev, false, nil); err != nil {
		return err
	}
	newRev, _, err := s.store.PutDoc(r.Context(), db, docid, current.Body, nil, &rev, false, atts)
	if err != nil {
		return err
	}
	return s.writeDocSuccess(w, r, dbName, docid, newRev)
}

func resolveRev(r *http.Request, bodyRev *couch.Rev) (*couch.Rev, error) {
	queryRev, err := revParam(r.URL.Query(), "rev")
	if err != nil {
		return nil, err
	}
	var etagRev *couch.Rev
	if im := r.Header.Get("If-Match"); im != "" {
		rev, err := couch.ParseRev(strings.Trim(im, `"`))
		if err != nil {
			return nil, err
		}
		etagRev = &rev
	}
	if queryRev != nil && etagRev != nil && *queryRev != *etagRev {
		return nil, couch.BadRequest("Document rev and etag have different values")
	}
	if etagRev != nil && bodyRev != nil && *etagRev != *bodyRev {
		return nil, couch.BadRequest("Document rev and etag have different values")
	}
	if queryRev != nil && bodyRev != nil && *queryRev != *bodyRev {
		return nil, couch.BadRequest(
			"Document rev from request body and query string have different values")
	}
	if queryRev != nil {
		return queryRev, nil
	}
	if etagRev != nil {
		return etagRev, nil
	}
	return bodyRev, nil
}

func (s *Server) writeDocSuccess(w http.ResponseWriter, r *http.Request, dbName, docid string, rev couch.Rev) error {
	if r.URL.Query().Get("batch") == "ok" {
		// couchgres writes synchronously. Only the response shape is batchy.
		writeJSON(w, 202, map[string]any{"ok": true, "id": docid})
		return nil
	}
	revString := rev.String()
	setETag(w, revString)
	w.Header().Set("Location", s.docURL(r, dbName, docid))
	writeJSON(w, 201, map[string]any{"ok": true, "id": docid, "rev": revString})
	return nil
}

// batchAccepted mirrors CouchDB's deferred batch=ok writes: a conflicting
// batched write is silently dropped and still acknowledged with 202.
func batchAccepted(w http.ResponseWriter, r *http.Request, docid string, err error) bool {
	if r.URL.Query().Get("batch") != "ok" {
		return false
	}
	if ce, ok := err.(*couch.Error); !ok || ce.Status != 409 {
		return false
	}
	writeJSON(w, 202, map[string]any{"ok": true, "id": docid})
	return true
}
