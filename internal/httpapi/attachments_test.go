package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Attachment PUT and DELETE responses must preserve CouchDB's field order.
// Our previous map encoding returned id before ok, so clients could not recognize
// a successful write and retried it with a stale revision, producing a conflict.
func TestAttachmentWriteResponseCompatibility(t *testing.T) {
	h := testHandler(t)
	const db = "attachment_response_compat"
	send(t, h, "DELETE", "/"+db, nil, testAdminAuth, adminAuth())
	if resp := send(t, h, "PUT", "/"+db, nil, testAdminAuth, adminAuth()); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, db)
	t.Cleanup(func() { send(t, h, "DELETE", "/"+db, nil, testAdminAuth, adminAuth()) })

	for _, id := range []string{"new-doc", "folder/existing-doc"} {
		t.Run(id, func(t *testing.T) {
			path := "/" + db + "/" + url.PathEscape(id)
			rev := ""
			if id != "new-doc" {
				// Cover uploading to an existing document with saved metadata.
				resp := send(t, h, "PUT", path, map[string]any{"description": "test document"})
				if resp.status != 201 {
					t.Fatalf("save metadata: %+v", resp)
				}
				rev = resp.body["rev"].(string)
			}

			writeAttachment := func(method, name, data string, ifMatch bool) string {
				t.Helper()
				attPath := path + "/" + name
				if rev != "" && !ifMatch {
					attPath += "?rev=" + rev
				}
				req := httptest.NewRequest(method, attPath, strings.NewReader(data))
				req.Header.Set("Content-Type", "application/octet-stream")
				if ifMatch {
					req.Header.Set("If-Match", `"`+rev+`"`)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				wantStatus := http.StatusCreated
				if method == http.MethodDelete {
					wantStatus = http.StatusOK
				}
				if rec.Code != wantStatus {
					t.Fatalf("%s attachment: %d %s", method, rec.Code, rec.Body.String())
				}
				// Check the wire order before decoding to a map, which would hide
				// the response compatibility regression.
				decoder := json.NewDecoder(strings.NewReader(rec.Body.String()))
				open, _ := decoder.Token()
				key, _ := decoder.Token()
				value, _ := decoder.Token()
				if open != json.Delim('{') || key != "ok" || value != true {
					t.Fatalf("attachment response must start with ok:true: %s", rec.Body.String())
				}
				var result struct {
					OK  bool   `json:"ok"`
					ID  string `json:"id"`
					Rev string `json:"rev"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if !result.OK || result.ID != id || result.Rev == "" || result.Rev == rev {
					t.Fatalf("invalid attachment result: %+v", result)
				}
				if rec.Header().Get("ETag") != `"`+result.Rev+`"` {
					t.Fatalf("ETag does not match result: %s", rec.Header().Get("ETag"))
				}
				return result.Rev
			}

			rev = writeAttachment("PUT", "file.bin", "original content", false)
			rev = writeAttachment("PUT", "other.bin", "other content", false)
			stale := rev
			rev = writeAttachment("PUT", "file.bin", "replacement content", true)

			// A real stale-revision retry must still conflict and leave the
			// successfully uploaded replacement intact.
			req := httptest.NewRequest("PUT", path+"/file.bin?rev="+stale,
				strings.NewReader("stale content"))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusConflict {
				t.Fatalf("stale upload: %d %s", rec.Code, rec.Body.String())
			}
			for name, data := range map[string]string{
				"file.bin": "replacement content", "other.bin": "other content",
			} {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest("GET", path+"/"+name, nil))
				if rec.Code != 200 || rec.Body.String() != data {
					t.Fatalf("read %s: %d %q", name, rec.Code, rec.Body.String())
				}
			}

			rev = writeAttachment("DELETE", "file.bin", "", false)
			rev = writeAttachment("DELETE", "other.bin", "", true)
			doc := send(t, h, "GET", path, nil)
			if doc.status != 200 || doc.body["_rev"] != rev || doc.body["_attachments"] != nil {
				t.Fatalf("document after deleting attachments: %+v", doc)
			}
			if id != "new-doc" && doc.body["description"] != "test document" {
				t.Fatal("attachment updates lost document metadata")
			}
			for _, name := range []string{"file.bin", "other.bin"} {
				if resp := send(t, h, "GET", path+"/"+name, nil); resp.status != 404 {
					t.Fatalf("deleted attachment %s: %+v", name, resp)
				}
			}
		})
	}
}
