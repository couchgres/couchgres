package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/klauspost/compress/gzip"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// defaultCompressibleTypes is CouchDB's [attachments] compressible_types.
const defaultCompressibleTypes = "text/*, application/javascript, application/json, application/xml"

// compressAttachmentWrites gzips eligible attachment data at write time,
// like CouchDB ([attachments] compression_level 1-9, 0 disables). The
// digest is left for the store to compute over the stored (encoded) form,
// matching CouchDB's bookkeeping. Entries that arrived already encoded
// (replication) or as stubs pass through untouched.
func (s *Server) compressAttachmentWrites(atts []store.AttachmentWrite) {
	level := s.config.getInt("attachments", "compression_level", 8)
	if level <= 0 {
		return
	}
	if level > gzip.BestCompression {
		level = gzip.BestCompression
	}
	types := s.config.getOr("attachments", "compressible_types", defaultCompressibleTypes)
	for i := range atts {
		att := &atts[i]
		if att.Data == nil || att.Encoding != "" || att.Digest != "" ||
			!compressibleType(att.ContentType, types) {
			continue
		}
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, level)
		if err != nil {
			continue
		}
		zw.Write(att.Data)
		if err := zw.Close(); err != nil {
			continue
		}
		att.DecodedLength = int64(len(att.Data))
		att.Data = buf.Bytes()
		att.Encoding = "gzip"
	}
}

// compressibleType matches a content type against CouchDB's comma-separated
// compressible_types list (exact entries, type/* wildcards, or *).
func compressibleType(contentType, list string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	for _, entry := range strings.Split(list, ",") {
		entry = strings.ToLower(strings.TrimSpace(entry))
		switch {
		case entry == "":
		case entry == "*", entry == ct:
			return true
		default:
			if base, ok := strings.CutSuffix(entry, "/*"); ok && strings.HasPrefix(ct, base+"/") {
				return true
			}
		}
	}
	return false
}

// Standalone attachment endpoints: /{db}/{docid}/{attname...}
// Method dispatch happens here (the routes are method-less to avoid
// ServeMux pattern conflicts with the doc fallbacks).

func (s *Server) attachmentDispatch(w http.ResponseWriter, r *http.Request) error {
	return s.attachmentByMethod(w, r,
		r.PathValue("db"), r.PathValue("docid"), r.PathValue("attname"))
}

func (s *Server) designAttachmentDispatch(w http.ResponseWriter, r *http.Request) error {
	return s.attachmentByMethod(w, r,
		r.PathValue("db"), "_design/"+r.PathValue("ddoc"), r.PathValue("attname"))
}

func (s *Server) attachmentByMethod(w http.ResponseWriter, r *http.Request, dbName, docid, name string) error {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return s.attachmentGetImpl(w, r, dbName, docid, name)
	case http.MethodPut:
		return s.attachmentPutImpl(w, r, dbName, docid, name)
	case http.MethodDelete:
		return s.attachmentDeleteImpl(w, r, dbName, docid, name)
	default:
		return couch.MethodNotAllowed("DELETE,GET,HEAD,PUT")
	}
}

func (s *Server) attachmentGetImpl(w http.ResponseWriter, r *http.Request, dbName, docid, name string) error {
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if err := s.authorizeDocRead(r, db, docid); err != nil {
		return err
	}
	rev, err := revParam(r.URL.Query(), "rev")
	if err != nil {
		return err
	}
	var attRev couch.Rev
	if rev != nil {
		// Old revisions keep their attachment rows even after the doc
		// body is gone, like CouchDB before compaction.
		attRev = *rev
	} else {
		row, err := s.store.GetDoc(r.Context(), db, docid, nil)
		if err != nil {
			return err
		}
		attRev = row.Rev
	}
	att, err := s.store.GetAttachment(r.Context(), db, docid, attRev, name)
	if err != nil {
		return err
	}
	// Serve identity bytes regardless of how the attachment is stored.
	data, err := att.DecodedData()
	if err != nil {
		return err
	}
	size := int64(len(data))

	w.Header().Set("Content-Type", att.ContentType)
	// The ETag is the digest's base64 payload without the md5- prefix.
	etag := strings.TrimPrefix(att.Digest, "md5-")
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	if strings.Trim(strings.TrimPrefix(r.Header.Get("If-None-Match"), "W/"), `"`) == etag {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	// Backward and fully-out-of-range requests can't be satisfied.
	if unsatisfiableRange(r.Header.Get("Range"), size) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return nil
	}
	if att.Encoding == "" {
		// The stored digest covers the encoded form. Only advertise it as
		// a body checksum when the body is that form.
		w.Header().Set("Content-MD5", strings.TrimPrefix(att.Digest, "md5-"))
	}

	// Single-range requests get a 206.
	if start, end, ok := parseRange(r.Header.Get("Range"), size); ok {
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
		return nil
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(200)
	w.Write(data)
	return nil
}

// unsatisfiableRange reports a single "bytes=a-b" range that is backward
// or lies entirely past the end, which answers 416 (other malformed
// ranges fall back to a 200).
func unsatisfiableRange(header string, size int64) bool {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return false
	}
	fromStr, toStr, found := strings.Cut(spec, "-")
	if !found || fromStr == "" {
		return false
	}
	from, err := strconv.ParseInt(fromStr, 10, 64)
	if err != nil {
		return false
	}
	if from >= size {
		return true
	}
	if toStr != "" {
		if to, err := strconv.ParseInt(toStr, 10, 64); err == nil && from > to {
			return true
		}
	}
	return false
}

// parseRange handles single "bytes=a-b" ranges (suffix and open-ended too).
// Requests for the whole entity (open-ended from 0), multiple ranges, and
// malformed headers report !ok. CouchDB serves all of those as plain 200s.
func parseRange(header string, size int64) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	if spec == "0-" {
		return 0, 0, false
	}
	fromStr, toStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	if fromStr == "" { // suffix range: last N bytes
		n, err := strconv.ParseInt(toStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	start, err := strconv.ParseInt(fromStr, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end = size - 1
	if toStr != "" {
		end, err = strconv.ParseInt(toStr, 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, true
}

func (s *Server) attachmentPutImpl(w http.ResponseWriter, r *http.Request, dbName, docid, name string) error {
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
	expected, err := resolveRev(r, nil)
	if err != nil {
		return err
	}
	if strings.HasPrefix(name, "_") {
		return couch.BadRequest(
			"Attachment name '" + name + "' starts with prohibited character '_'")
	}
	data, err := readHTTPBody(r)
	if err != nil {
		return err
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// The new revision keeps the current body, carries existing attachments
	// as stubs, and adds/replaces this one first in the list, like
	// CouchDB, whose revision hash covers attachment order.
	body := map[string]any{}
	atts := []store.AttachmentWrite{{
		Name:        name,
		ContentType: contentType,
		Data:        data,
	}}
	if expected != nil {
		current, err := s.store.GetDoc(r.Context(), db, docid, expected)
		if err != nil {
			// A named rev that isn't there conflicts (CouchDB's shape).
			if ce, ok := err.(*couch.Error); ok && ce.Status == 404 {
				return couch.NewError(409, "not_found", "missing_rev")
			}
			return err
		}
		body = current.Body
		existing, err := s.store.Attachments(r.Context(), db, docid, *expected)
		if err != nil {
			return err
		}
		for _, att := range existing {
			if att.Name != name {
				atts = append(atts, store.AttachmentWrite{Name: att.Name})
			}
		}
	}
	s.compressAttachmentWrites(atts)

	if err := s.validateDocUpdate(r, db, docid, body, expected, false, nil); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, docid, body, nil, expected, false, atts)
	if err != nil {
		return err
	}
	revString := rev.String()
	setETag(w, revString)
	w.Header().Set("Location", s.docURL(r, dbName, docid+"/"+name))
	writeJSON(w, 201, map[string]any{"ok": true, "id": docid, "rev": revString})
	return nil
}

func (s *Server) attachmentDeleteImpl(w http.ResponseWriter, r *http.Request, dbName, docid, name string) error {
	db, err := s.store.GetDB(r.Context(), dbName)
	if err != nil {
		return err
	}
	if err := s.authorizeDocWrite(r, db, docid); err != nil {
		return err
	}
	expected, err := resolveRev(r, nil)
	if err != nil {
		return err
	}
	if expected == nil {
		return couch.Conflict()
	}
	current, err := s.store.GetDoc(r.Context(), db, docid, expected)
	if err != nil {
		// A named rev that isn't there conflicts (CouchDB's shape).
		if ce, ok := err.(*couch.Error); ok && ce.Status == 404 {
			return couch.NewError(409, "not_found", "missing_rev")
		}
		return err
	}
	existing, err := s.store.Attachments(r.Context(), db, docid, *expected)
	if err != nil {
		return err
	}
	found := false
	var atts []store.AttachmentWrite
	for _, att := range existing {
		if att.Name == name {
			found = true
			continue
		}
		atts = append(atts, store.AttachmentWrite{Name: att.Name})
	}
	if !found {
		return couch.NewError(404, "not_found", "Document is missing attachment")
	}
	if err := s.validateDocUpdate(r, db, docid, current.Body, expected, false, nil); err != nil {
		return err
	}
	rev, _, err := s.store.PutDoc(r.Context(), db, docid, current.Body, nil, expected, false, atts)
	if err != nil {
		return err
	}
	revString := rev.String()
	setETag(w, revString)
	writeJSON(w, 200, map[string]any{"ok": true, "id": docid, "rev": revString})
	return nil
}
