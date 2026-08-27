package replicate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"sort"
)

// Attachment-bearing documents are fetched as multipart/mixed: JSON inlines
// only decoded attachment data, but gzip-encoded attachments must travel in
// stored form or their digests change. The multipart parts carry exactly
// those bytes.

// FetchRevsMultipart pulls the given revs of one document with attachment
// parts folded back in as inline (encoded) base64 data, ready for a
// new_edits=false push.
func (p *Peer) FetchRevsMultipart(ctx context.Context, id string, revs []string) ([]map[string]any, error) {
	revsJSON, err := json.Marshal(revs)
	if err != nil {
		return nil, err
	}
	path := "/" + url.PathEscape(id) +
		"?open_revs=" + url.QueryEscape(string(revsJSON)) +
		"&revs=true&latest=true&attachments=true&att_encoding_info=true"
	req, err := p.request(ctx, "GET", path)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "multipart/mixed")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		_ = drainResponseBody(resp, p.maxResponseBytes)
		return nil, fmt.Errorf("multipart open_revs %s: HTTP %d", id, resp.StatusCode)
	}
	body, err := boundedResponseBody(resp, p.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	if mediaType == "application/json" {
		// No attachments are present. Some servers answer JSON regardless.
		return decodeOpenRevsJSON(body, p.maxResponseBytes)
	}
	if mediaType != "multipart/mixed" {
		return nil, fmt.Errorf("multipart open_revs %s: unexpected %s", id, mediaType)
	}
	if params["boundary"] == "" {
		return nil, fmt.Errorf("multipart open_revs %s: missing boundary", id)
	}

	var docs []map[string]any
	reader := multipart.NewReader(body, params["boundary"])
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		partType, partParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
		if err != nil {
			return nil, err
		}
		switch partType {
		case "application/json":
			doc, err := decodeDocPart(part, p.maxResponseBytes)
			if err != nil {
				return nil, err
			}
			if doc != nil {
				docs = append(docs, doc)
			}
		case "multipart/related":
			if partParams["boundary"] == "" {
				return nil, errors.New("multipart/related part has no boundary")
			}
			doc, err := decodeRelatedPart(part, partParams["boundary"],
				p.maxResponseBytes, p.maxAttachmentBytes)
			if err != nil {
				return nil, err
			}
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

// decodeDocPart reads a JSON part. "missing" entries return nil.
func decodeDocPart(part io.Reader, maxBytes int64) (map[string]any, error) {
	raw, err := readAllBounded(part, maxBytes, ErrResponseTooLarge)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if _, missing := doc["missing"]; missing {
		return nil, nil
	}
	return doc, nil
}

// decodeRelatedPart reads a doc + attachment-parts bundle and inlines the
// attachment bytes (in their transferred, possibly encoded form).
func decodeRelatedPart(part io.Reader, boundary string, maxDocBytes, maxAttachmentBytes int64) (map[string]any, error) {
	reader := multipart.NewReader(part, boundary)
	docPart, err := reader.NextPart()
	if err != nil {
		return nil, err
	}
	raw, err := readAllBounded(docPart, maxDocBytes, ErrResponseTooLarge)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	ordered, err := followsOrder(raw)
	if err != nil {
		return nil, err
	}

	atts, _ := doc["_attachments"].(map[string]any)
	for i := 0; ; i++ {
		attPart, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		data, err := readAllBounded(attPart, maxAttachmentBytes, ErrAttachmentTooLarge)
		if err != nil {
			return nil, err
		}
		name := attPart.FileName()
		if name == "" && i < len(ordered) {
			name = ordered[i]
		}
		spec, ok := atts[name].(map[string]any)
		if !ok {
			continue
		}
		delete(spec, "follows")
		spec["data"] = base64.StdEncoding.EncodeToString(data)
	}
	return doc, nil
}

// pushDocMultipart writes one document as a multipart/related PUT with
// new_edits=false. The JSON part declares each attachment follows:true.
// (keeping the encoding bookkeeping) and the parts carry the stored --
// possibly gzip bytes. This is how encoded attachments survive a push.
// JSON _bulk_docs would treat the encoded bytes as content.
func (p *Peer) pushDocMultipart(ctx context.Context, doc map[string]any) error {
	id, _ := doc["_id"].(string)
	atts, _ := doc["_attachments"].(map[string]any)

	// The doc part carries follows:true instead of data. Parts must appear
	// in the JSON's member order, which for a marshaled map is sorted.
	outDoc := make(map[string]any, len(doc))
	for k, v := range doc {
		outDoc[k] = v
	}
	outAtts := make(map[string]any, len(atts))
	names := make([]string, 0, len(atts))
	var parts [][]byte
	for name := range atts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		spec, _ := atts[name].(map[string]any)
		outSpec := make(map[string]any, len(spec))
		for k, v := range spec {
			outSpec[k] = v
		}
		if data, ok := outSpec["data"].(string); ok {
			raw, err := base64.StdEncoding.DecodeString(data)
			if err != nil {
				return fmt.Errorf("attachment %s of %s: %w", name, id, err)
			}
			delete(outSpec, "data")
			outSpec["follows"] = true
			parts = append(parts, raw)
		}
		outAtts[name] = outSpec
	}
	outDoc["_attachments"] = outAtts
	docJSON, err := json.Marshal(outDoc)
	if err != nil {
		return err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	docHeader := textproto.MIMEHeader{}
	docHeader.Set("Content-Type", "application/json")
	docPart, err := writer.CreatePart(docHeader)
	if err != nil {
		return err
	}
	docPart.Write(docJSON)
	for _, data := range parts {
		part, err := writer.CreatePart(textproto.MIMEHeader{})
		if err != nil {
			return err
		}
		part.Write(data)
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := p.request(ctx, "PUT", "/"+url.PathEscape(id)+"?new_edits=false")
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	req.Body = io.NopCloser(&body)
	req.ContentLength = int64(body.Len())
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := drainResponseBody(resp, p.maxResponseBytes); err != nil {
		return err
	}
	if resp.StatusCode != 201 && resp.StatusCode != 202 {
		return fmt.Errorf("multipart PUT %s: HTTP %d", id, resp.StatusCode)
	}
	return nil
}

// followsOrder lists _attachments names carrying follows:true in JSON order
// Multipart parts match positionally when filenames are absent.
func followsOrder(raw []byte) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("invalid document JSON")
	}
	var names []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if keyTok != "_attachments" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, err
			}
			continue
		}
		if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
			return nil, fmt.Errorf("invalid _attachments member")
		}
		for dec.More() {
			nameTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name, _ := nameTok.(string)
			var spec map[string]any
			if err := dec.Decode(&spec); err != nil {
				return nil, err
			}
			if follows, _ := spec["follows"].(bool); follows {
				names = append(names, name)
			}
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func decodeOpenRevsJSON(body io.Reader, maxBytes int64) ([]map[string]any, error) {
	raw, err := readAllBounded(body, maxBytes, ErrResponseTooLarge)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var entries []struct {
		OK map[string]any `json:"ok"`
	}
	if err := dec.Decode(&entries); err != nil {
		return nil, err
	}
	var docs []map[string]any
	for _, entry := range entries {
		if entry.OK != nil {
			docs = append(docs, entry.OK)
		}
	}
	return docs, nil
}
