package httpapi

// CouchDB's multipart formats.
//
//   - PUT /{db}/{docid} as multipart/related. The first part is the doc
//     JSON whose _attachments entries carry follows:true. The remaining
//     parts are those attachments' bytes, in the order the entries appear
//     in the JSON. This is how replicators push large attachments.
//   - GET /{db}/{docid}?open_revs=... with Accept: multipart/mixed. One
//     part represents each requested rev. A rev with attachments becomes a nested
//     multipart/related part (doc JSON + attachment parts).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sort"
	"strconv"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// writeDocMultipartRelated answers a doc GET with ?attachments=true (or
// ?atts_since=[...]) and an Accept of multipart/related. The doc JSON
// (attachments as follows entries) is followed by one part per attachment carrying
// its stored bytes, matching CouchDB's exact part headers. Attachments at
// or below stubPos stay stubs with no part (the atts_since ancestors
// already have them). stubPos 0 sends every attachment.
func (s *Server) writeDocMultipartRelated(
	w http.ResponseWriter,
	r *http.Request,
	db *store.DB,
	doc map[string]any,
	docid string,
	rev couch.Rev,
	stubPos int,
) error {
	all, err := s.store.AttachmentsWithData(r.Context(), db, docid, rev)
	if err != nil {
		return err
	}
	atts, _ := splitAttsSince(all, stubPos)
	member := make(map[string]any, len(all))
	for _, att := range all {
		entry := map[string]any{
			"content_type": att.ContentType,
			"revpos":       att.RevPos,
			"digest":       att.Digest,
			"length":       att.Length,
		}
		if att.RevPos > stubPos {
			entry["follows"] = true
		} else {
			entry["stub"] = true
		}
		if att.Encoding != "" {
			entry["encoding"] = att.Encoding
			entry["encoded_length"] = att.EncodedLength
		}
		member[att.Name] = entry
	}
	doc["_attachments"] = member
	docJSON, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	boundary := randomUUID()
	var buf bytes.Buffer
	buf.WriteString("--" + boundary + "\r\nContent-Type: application/json\r\n\r\n")
	buf.Write(docJSON)
	for _, att := range atts {
		buf.WriteString("\r\n--" + boundary + "\r\n")
		fmt.Fprintf(&buf, "Content-Disposition: attachment; filename=%q\r\n", att.Name)
		buf.WriteString("Content-Type: " + att.ContentType + "\r\n")
		if att.Encoding != "" {
			fmt.Fprintf(&buf, "Content-Length: %d\r\nContent-Encoding: %s\r\n", att.EncodedLength, att.Encoding)
		} else {
			fmt.Fprintf(&buf, "Content-Length: %d\r\n", att.Length)
		}
		buf.WriteString("\r\n")
		buf.Write(att.Data)
	}
	buf.WriteString("\r\n--" + boundary + "--")

	w.Header().Set("Content-Type", `multipart/related; boundary="`+boundary+`"`)
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(200)
	w.Write(buf.Bytes())
	return nil
}

// isMultipartRelated reports whether the request body is a multipart/related
// document, returning its boundary.
func isMultipartRelated(r *http.Request) (string, bool) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/related" {
		return "", false
	}
	boundary := params["boundary"]
	return boundary, boundary != ""
}

// readDocMultipart parses a multipart/related document body: the decoded
// first-part JSON (including raw bytes because rev hashing needs the original member
// order) plus each follows attachment's bytes.
func readDocMultipart(body io.Reader, boundary string) (any, []byte, map[string][]byte, error) {
	// Clients may glue an epilogue straight onto the closing boundary
	// ("--b--epilogue"), which CouchDB accepts but Go's parser does not.
	// normalize the terminator.
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, nil, nil, couch.BadRequest("could not read request body")
	}
	if i := bytes.LastIndex(data, []byte("\r\n--"+boundary+"--")); i >= 0 {
		data = append(data[:i], []byte("\r\n--"+boundary+"--\r\n")...)
	}
	reader := multipart.NewReader(bytes.NewReader(data), boundary)

	first, err := reader.NextPart()
	if err != nil {
		return nil, nil, nil, couch.BadRequest("multipart body has no document part")
	}
	raw, err := io.ReadAll(first)
	if err != nil {
		return nil, nil, nil, couch.BadRequest("could not read document part")
	}
	docValue, err := couch.DecodeJSON(raw)
	if err != nil {
		return nil, nil, nil, err
	}
	ordered, err := orderedFollowsNames(raw)
	if err != nil {
		return nil, nil, nil, err
	}

	parts := make(map[string][]byte, len(ordered))
	for i := 0; ; i++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, nil, couch.BadRequest("invalid multipart body: " + err.Error())
		}
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, nil, nil, couch.BadRequest("could not read attachment part")
		}
		// Parts match _attachments follows entries by JSON order. A
		// Content-Disposition filename overrides when present.
		name := part.FileName()
		if name == "" {
			if i >= len(ordered) {
				return nil, nil, nil, couch.BadRequest("more attachment parts than follows entries")
			}
			name = ordered[i]
		}
		parts[name] = data
	}
	if len(parts) < len(ordered) {
		return nil, nil, nil, couch.BadRequest("fewer attachment parts than follows entries")
	}
	return docValue, raw, parts, nil
}

// orderedFollowsNames scans raw document JSON for the _attachments member
// and returns the names whose entries carry follows:true, in JSON order
// (Go maps don't preserve it, but part matching depends on it).
func orderedFollowsNames(raw []byte) ([]string, error) {
	return scanAttachmentNames(raw, true)
}

// orderedAttachmentNames returns every _attachments member name in JSON
// order. CouchDB keeps its attachment list in this order, which joins the
// revision hash.
func orderedAttachmentNames(raw []byte) ([]string, error) {
	return scanAttachmentNames(raw, false)
}

// orderAttachmentWrites sorts attachment writes into the raw document's
// _attachments member order so the revision hash sees them like CouchDB
// does. Without raw bytes the slice stays as given.
func orderAttachmentWrites(writes []store.AttachmentWrite, rawDoc []byte) []store.AttachmentWrite {
	if len(writes) < 2 || rawDoc == nil {
		return writes
	}
	names, err := orderedAttachmentNames(rawDoc)
	if err != nil {
		return writes
	}
	pos := make(map[string]int, len(names))
	for i, name := range names {
		pos[name] = i
	}
	sort.SliceStable(writes, func(i, j int) bool {
		return pos[writes[i].Name] < pos[writes[j].Name]
	})
	return writes
}

func scanAttachmentNames(raw []byte, followsOnly bool) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	invalid := func() error { return couch.BadRequest("invalid multipart document JSON") }

	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, invalid()
	}
	var names []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, invalid()
		}
		key, _ := keyTok.(string)
		if key != "_attachments" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, invalid()
			}
			continue
		}
		if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
			return nil, invalid()
		}
		for dec.More() {
			nameTok, err := dec.Token()
			if err != nil {
				return nil, invalid()
			}
			name, _ := nameTok.(string)
			var spec map[string]any
			if err := dec.Decode(&spec); err != nil {
				return nil, invalid()
			}
			if follows, _ := spec["follows"].(bool); follows || !followsOnly {
				names = append(names, name)
			}
		}
		if _, err := dec.Token(); err != nil { // closing }
			return nil, invalid()
		}
	}
	return names, nil
}

// attachmentWritesWithParts is attachmentWrites plus follows resolution
// from multipart body parts.
func attachmentWritesWithParts(member map[string]any, parts map[string][]byte) ([]store.AttachmentWrite, error) {
	if len(member) == 0 {
		return nil, nil
	}
	writes := make([]store.AttachmentWrite, 0, len(member))
	for name, raw := range member {
		spec, ok := raw.(map[string]any)
		if !ok {
			return nil, couch.BadRequest("Invalid attachment: " + name)
		}
		if follows, _ := spec["follows"].(bool); !follows {
			continue // inline/stub entries handled by attachmentWrites
		}
		data, ok := parts[name]
		if !ok {
			return nil, couch.BadRequest("missing multipart data for attachment " + name)
		}
		writes = append(writes, attachmentWriteFromSpec(name, spec, data))
	}
	// Non-follows entries (stubs, inline data) go through the normal path.
	remaining := make(map[string]any, len(member))
	for name, raw := range member {
		if spec, ok := raw.(map[string]any); ok {
			if follows, _ := spec["follows"].(bool); follows {
				continue
			}
		}
		remaining[name] = raw
	}
	rest, err := attachmentWrites(remaining)
	if err != nil {
		return nil, err
	}
	return append(writes, rest...), nil
}

// Responses.

// openRevEntry is one part of an open_revs response.
type openRevEntry struct {
	doc        map[string]any     // nil when missing
	missingRev string             // set when the rev is unknown
	atts       []store.Attachment // with data, for multipart parts
}

// wantsJSONOpenRevs mirrors CouchDB's open_revs content negotiation.
// multipart/mixed is the default. JSON is used only when Accept names it.
func wantsJSONOpenRevs(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/json")
}

// writeOpenRevsMultipart renders open_revs results as multipart/mixed, the
// format replicators request when pulling documents with attachments.
func writeOpenRevsMultipart(w http.ResponseWriter, entries []openRevEntry) error {
	writer := multipart.NewWriter(w)
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+writer.Boundary())
	w.WriteHeader(200)

	for _, entry := range entries {
		if entry.missingRev != "" {
			part, err := writer.CreatePart(jsonPartHeader())
			if err != nil {
				return nil
			}
			raw, _ := json.Marshal(map[string]string{"missing": entry.missingRev})
			part.Write(raw)
			continue
		}
		if len(entry.atts) == 0 {
			part, err := writer.CreatePart(jsonPartHeader())
			if err != nil {
				return nil
			}
			raw, _ := json.Marshal(entry.doc)
			part.Write(raw)
			continue
		}
		if err := writeRelatedPart(writer, entry); err != nil {
			return nil
		}
	}
	writer.Close()
	return nil
}

// writeRelatedPart nests a multipart/related document (doc JSON with
// follows:true attachments, followed by the attachment bytes) inside a part.
func writeRelatedPart(outer *multipart.Writer, entry openRevEntry) error {
	inner := multipart.NewWriter(nil) // boundary source only
	header := textproto.MIMEHeader{}
	header.Set("Content-Type", "multipart/related; boundary="+inner.Boundary())
	part, err := outer.CreatePart(header)
	if err != nil {
		return err
	}
	related := multipart.NewWriter(part)
	related.SetBoundary(inner.Boundary())
	return writeRelatedBody(related, entry)
}

// writeDocMultipartRelated renders a single document with attachment data as
// a top-level multipart/related response (GET with Accept: multipart/related).
func writeDocMultipartRelated(w http.ResponseWriter, entry openRevEntry) error {
	writer := multipart.NewWriter(w)
	w.Header().Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	w.WriteHeader(200)
	return writeRelatedBody(writer, entry)
}

func writeRelatedBody(writer *multipart.Writer, entry openRevEntry) error {
	doc := make(map[string]any, len(entry.doc))
	for k, v := range entry.doc {
		doc[k] = v
	}
	// Stub entries placed by atts_since stay alongside the follows entries.
	member := followsAttachmentsJSON(entry.atts)
	if existing, ok := doc["_attachments"].(map[string]any); ok {
		for name, spec := range existing {
			if _, taken := member[name]; !taken {
				member[name] = spec
			}
		}
	}
	doc["_attachments"] = member

	docPart, err := writer.CreatePart(jsonPartHeader())
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(doc)
	docPart.Write(raw)

	for _, att := range entry.atts {
		// Encoded attachments travel in stored form so digests survive
		// replication. Identity attachments travel as-is.
		data := att.Data
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition",
			fmt.Sprintf("attachment; filename=%q", att.Name))
		header.Set("Content-Type", att.ContentType)
		header.Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
		if att.Encoding != "" {
			header.Set("Content-Encoding", att.Encoding)
		}
		part, err := writer.CreatePart(header)
		if err != nil {
			return err
		}
		part.Write(data)
	}
	return writer.Close()
}

func followsAttachmentsJSON(atts []store.Attachment) map[string]any {
	out := make(map[string]any, len(atts))
	for _, att := range atts {
		entry := map[string]any{
			"content_type": att.ContentType,
			"revpos":       att.RevPos,
			"digest":       att.Digest,
			"length":       att.Length,
			"follows":      true,
		}
		if att.Encoding != "" {
			entry["encoding"] = att.Encoding
			entry["encoded_length"] = att.EncodedLength
		}
		out[att.Name] = entry
	}
	return out
}

func jsonPartHeader() textproto.MIMEHeader {
	header := textproto.MIMEHeader{}
	header.Set("Content-Type", "application/json")
	return header
}
