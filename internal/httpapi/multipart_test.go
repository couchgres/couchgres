package httpapi

// Tests multipart/related PUT and multipart/mixed open_revs.

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"

	"github.com/couchgres/couchgres/internal/store"
)

func TestOpenRevsMultipartClosingBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []openRevEntry
	}{
		{name: "empty"},
		{name: "plain", entries: []openRevEntry{{doc: map[string]any{"_id": "plain"}}}},
		{name: "missing", entries: []openRevEntry{{missingRev: "1-missing"}}},
		{name: "attachment", entries: []openRevEntry{{
			doc: map[string]any{"_id": "with-att"},
			atts: []store.Attachment{{
				Name: "note.txt", ContentType: "text/plain", Length: 7,
				Data: []byte("payload"),
			}},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			if err := writeOpenRevsMultipart(rec, tc.entries); err != nil {
				t.Fatal(err)
			}
			mediaType, params, err := mime.ParseMediaType(rec.Header().Get("Content-Type"))
			if err != nil || mediaType != "multipart/mixed" || params["boundary"] == "" {
				t.Fatalf("content type: %q", rec.Header().Get("Content-Type"))
			}
			// CouchDB's replicator expects exactly "--" after parsing the
			// final boundary. Go's usual trailing CRLF causes badmatch retries.
			_, remainder, found := bytes.Cut(rec.Body.Bytes(), []byte("\r\n--"+params["boundary"]+"--"))
			if !found || len(remainder) != 0 {
				t.Fatalf("closing boundary missing or followed by extra bytes: %q", remainder)
			}
			reader := multipart.NewReader(rec.Body, params["boundary"])
			for range tc.entries {
				part, err := reader.NextPart()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, part); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := reader.NextPart(); err != io.EOF {
				t.Fatalf("end of multipart response: %v", err)
			}
		})
	}
}

func TestMultipartRelatedPut(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/mp_put", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/mp_put", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "mp_put")
	defer send(t, h, "DELETE", "/mp_put", nil, testAdminAuth, admin)

	// Build the multipart/related body a replicator would push:
	// doc JSON (follows attachments) + attachment bytes, new_edits=false.
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	docPart, _ := writer.CreatePart(textproto.MIMEHeader{
		"Content-Type": {"application/json"},
	})
	docPart.Write([]byte(`{
	  "_id": "mp-doc",
	  "_rev": "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	  "_revisions": {"start": 2, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
	  "kind": "pushed",
	  "_attachments": {
	    "first.txt": {"content_type": "text/plain", "length": 11, "follows": true, "revpos": 2},
	    "second.bin": {"content_type": "application/octet-stream", "length": 4, "follows": true, "revpos": 1}
	  }
	}`))
	part1, _ := writer.CreatePart(textproto.MIMEHeader{})
	part1.Write([]byte("first bytes"))
	part2, _ := writer.CreatePart(textproto.MIMEHeader{})
	part2.Write([]byte{0xde, 0xad, 0xbe, 0xef})
	writer.Close()

	req := httptest.NewRequest("PUT", "/mp_put/mp-doc?new_edits=false", &buf)
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("multipart put: %d %s", rec.Code, rec.Body.String())
	}

	// Both attachments landed with the right bytes and metadata.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/mp_put/mp-doc/first.txt", nil))
	if rec.Code != 200 || rec.Body.String() != "first bytes" {
		t.Fatalf("first attachment: %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/mp_put/mp-doc/second.bin", nil))
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("second attachment: %d %v", rec.Code, rec.Body.Bytes())
	}
	resp := send(t, h, "GET", "/mp_put/mp-doc", nil)
	if resp.body["_rev"] != "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("replicated rev: %+v", resp.body)
	}
	atts := resp.body["_attachments"].(map[string]any)
	if atts["first.txt"].(map[string]any)["revpos"].(float64) != 2 {
		t.Fatalf("revpos carried: %+v", atts)
	}
}

func TestMultipartMixedOpenRevs(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/mp_get", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/mp_get", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "mp_get")
	defer send(t, h, "DELETE", "/mp_get", nil, testAdminAuth, admin)

	// One doc with an attachment, one plain. (octet-stream: text/* would
	// be gzip-compressed at write and served encoded in multipart, which
	// is covered elsewhere. This test is about the multipart framing.)
	resp := send(t, h, "PUT", "/mp_get/with-att", decode(t, `{
	  "n": 1,
	  "_attachments": {"a.txt": {"content_type": "application/octet-stream", "data": "cGF5bG9hZA=="}}
	}`))
	if resp.status != 201 {
		t.Fatalf("seed doc: %+v", resp)
	}
	rev := resp.body["rev"].(string)

	req := httptest.NewRequest("GET",
		"/mp_get/with-att?open_revs=[%22"+rev+"%22]&revs=true&latest=true", nil)
	req.Header.Set("Accept", "multipart/mixed")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("open_revs multipart: %d %s", rec.Code, rec.Body.String())
	}
	mediaType, params, err := mime.ParseMediaType(rec.Header().Get("Content-Type"))
	if err != nil || mediaType != "multipart/mixed" {
		t.Fatalf("content type: %q", rec.Header().Get("Content-Type"))
	}

	outer := multipart.NewReader(rec.Body, params["boundary"])
	part, err := outer.NextPart()
	if err != nil {
		t.Fatalf("outer part: %v", err)
	}
	innerType, innerParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
	if err != nil || innerType != "multipart/related" {
		t.Fatalf("inner type: %q", part.Header.Get("Content-Type"))
	}

	inner := multipart.NewReader(part, innerParams["boundary"])
	docPart, err := inner.NextPart()
	if err != nil {
		t.Fatalf("doc part: %v", err)
	}
	raw, _ := io.ReadAll(docPart)
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("doc part json: %v (%s)", err, raw)
	}
	if doc["_rev"] != rev || doc["_revisions"] == nil {
		t.Fatalf("doc part: %+v", doc)
	}
	attMember := doc["_attachments"].(map[string]any)["a.txt"].(map[string]any)
	if attMember["follows"] != true {
		t.Fatalf("follows flag: %+v", attMember)
	}

	attPart, err := inner.NextPart()
	if err != nil {
		t.Fatalf("att part: %v", err)
	}
	if got := attPart.FileName(); got != "a.txt" {
		t.Fatalf("att filename: %q", got)
	}
	attBytes, _ := io.ReadAll(attPart)
	if string(attBytes) != "payload" {
		t.Fatalf("att bytes: %q", attBytes)
	}

	// A missing rev arrives as a JSON part.
	req = httptest.NewRequest("GET",
		"/mp_get/with-att?open_revs=[%221-00000000000000000000000000000000%22]", nil)
	req.Header.Set("Accept", "multipart/mixed")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	_, params, _ = mime.ParseMediaType(rec.Header().Get("Content-Type"))
	outer = multipart.NewReader(rec.Body, params["boundary"])
	part, err = outer.NextPart()
	if err != nil {
		t.Fatalf("missing part: %v", err)
	}
	raw, _ = io.ReadAll(part)
	if !strings.Contains(string(raw), `"missing"`) {
		t.Fatalf("missing entry: %s", raw)
	}
}

// Attachments accumulated over several revisions: multipart parts come in
// write order (revpos, not name), and ?atts_since=[...] turns attachments
// the named ancestor already had into stubs with no part.
func TestAttsSince(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/mp_since", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/mp_since", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "mp_since")
	defer send(t, h, "DELETE", "/mp_since", nil, testAdminAuth, admin)

	// rev1 is a bare doc. rev2 has z.bin. rev3 has a.bin. Name order and write order
	// disagree on purpose.
	resp := send(t, h, "PUT", "/mp_since/doc", decode(t, `{"n": 1}`))
	if resp.status != 201 {
		t.Fatalf("seed doc: %+v", resp)
	}
	putAtt := func(name, rev, body string) string {
		req := httptest.NewRequest("PUT", "/mp_since/doc/"+name+"?rev="+rev,
			strings.NewReader(body))
		req.Header.Set("Content-Type", "application/octet-stream")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 201 {
			t.Fatalf("put %s: %d %s", name, rec.Code, rec.Body.String())
		}
		var out map[string]any
		json.Unmarshal(rec.Body.Bytes(), &out)
		return out["rev"].(string)
	}
	rev2 := putAtt("z.bin", resp.body["rev"].(string), "older bytes")
	rev3 := putAtt("a.bin", rev2, "newer bytes")

	readParts := func(body io.Reader, boundary string) (map[string]any, []string) {
		t.Helper()
		reader := multipart.NewReader(body, boundary)
		docPart, err := reader.NextPart()
		if err != nil {
			t.Fatalf("doc part: %v", err)
		}
		raw, _ := io.ReadAll(docPart)
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("doc part json: %v (%s)", err, raw)
		}
		var names []string
		for {
			part, err := reader.NextPart()
			if err != nil {
				break
			}
			names = append(names, part.FileName())
		}
		return doc, names
	}

	// Full multipart/related GET: parts in write order, z.bin first.
	req := httptest.NewRequest("GET", "/mp_since/doc?attachments=true", nil)
	req.Header.Set("Accept", "multipart/related")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mediaType, params, err := mime.ParseMediaType(rec.Header().Get("Content-Type"))
	if err != nil || mediaType != "multipart/related" {
		t.Fatalf("content type: %q", rec.Header().Get("Content-Type"))
	}
	_, names := readParts(rec.Body, params["boundary"])
	if len(names) != 2 || names[0] != "z.bin" || names[1] != "a.bin" {
		t.Fatalf("part order (want write order): %v", names)
	}

	// atts_since naming rev2: z.bin is a stub without a part, a.bin follows.
	req = httptest.NewRequest("GET",
		"/mp_since/doc?open_revs=[%22"+rev3+"%22]&atts_since=[%22"+rev2+"%22]", nil)
	req.Header.Set("Accept", "multipart/mixed")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	_, params, _ = mime.ParseMediaType(rec.Header().Get("Content-Type"))
	outer := multipart.NewReader(rec.Body, params["boundary"])
	part, err := outer.NextPart()
	if err != nil {
		t.Fatalf("outer part: %v", err)
	}
	_, innerParams, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
	doc, names := readParts(part, innerParams["boundary"])
	atts := doc["_attachments"].(map[string]any)
	if atts["z.bin"].(map[string]any)["stub"] != true ||
		atts["a.bin"].(map[string]any)["follows"] != true {
		t.Fatalf("atts_since member split: %+v", atts)
	}
	if len(names) != 1 || names[0] != "a.bin" {
		t.Fatalf("atts_since parts: %v", names)
	}

	// Plain JSON GET with atts_since: inline data only for the newer one.
	resp = send(t, h, "GET", "/mp_since/doc?atts_since=[%22"+rev2+"%22]", nil)
	atts = resp.body["_attachments"].(map[string]any)
	zbin := atts["z.bin"].(map[string]any)
	abin := atts["a.bin"].(map[string]any)
	if zbin["stub"] != true || zbin["data"] != nil {
		t.Fatalf("z.bin should stay a stub: %+v", zbin)
	}
	if abin["data"] == nil || abin["stub"] != nil {
		t.Fatalf("a.bin should carry data: %+v", abin)
	}

	// A rev that is no ancestor is skipped: everything carries data.
	resp = send(t, h, "GET",
		"/mp_since/doc?atts_since=[%221-00000000000000000000000000000000%22]", nil)
	atts = resp.body["_attachments"].(map[string]any)
	if atts["z.bin"].(map[string]any)["data"] == nil ||
		atts["a.bin"].(map[string]any)["data"] == nil {
		t.Fatalf("unknown atts_since rev should send everything: %+v", atts)
	}
}
