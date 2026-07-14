package httpapi

// Tests the request sequence a CouchDB replicator uses for push and pull.

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReplicationPushSequence(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/rep_push", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/rep_push", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "rep_push")
	defer send(t, h, "DELETE", "/rep_push", nil, testAdminAuth, admin)

	// 1. Replicator reads the target's checkpoint: 404 on first run.
	resp := send(t, h, "GET", "/rep_push/_local/repl-checkpoint-1", nil)
	if resp.status != 404 {
		t.Fatalf("fresh checkpoint read: %+v", resp)
	}

	// 2. _revs_diff: which revs does the target lack? (All of them.)
	remoteRev1 := "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	remoteRev2 := "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	resp = send(t, h, "POST", "/rep_push/_revs_diff",
		decode(t, `{"pushed":["`+remoteRev2+`"]}`))
	if resp.status != 200 {
		t.Fatalf("revs_diff: %+v", resp)
	}
	missing := resp.body["pushed"].(map[string]any)["missing"].([]any)
	if len(missing) != 1 || missing[0] != remoteRev2 {
		t.Fatalf("revs_diff missing: %+v", resp.body)
	}

	// 3. _bulk_docs with new_edits=false: the doc arrives with its history
	//    and an inline attachment.
	bulk := `{
	  "new_edits": false,
	  "docs": [{
	    "_id": "pushed",
	    "_rev": "` + remoteRev2 + `",
	    "_revisions": {"start": 2, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
	    "from": "remote",
	    "_attachments": {"note.txt": {"content_type": "text/plain", "data": "aGVsbG8gcmVw"}}
	  }]
	}`
	resp = send(t, h, "POST", "/rep_push/_bulk_docs", decode(t, bulk))
	if resp.status != 201 {
		t.Fatalf("bulk_docs: %+v", resp)
	}
	if resp.array == nil || len(resp.array) != 0 {
		t.Fatalf("new_edits=false success must report []: %+v", resp.array)
	}

	// The doc exists at the replicated rev with its full ancestry.
	resp = send(t, h, "GET", "/rep_push/pushed?revs=true", nil)
	if resp.status != 200 || resp.body["_rev"] != remoteRev2 {
		t.Fatalf("replicated doc: %+v", resp)
	}
	revisions := resp.body["_revisions"].(map[string]any)
	ids := revisions["ids"].([]any)
	if revisions["start"].(float64) != 2 || len(ids) != 2 {
		t.Fatalf("replicated ancestry: %+v", revisions)
	}
	atts := resp.body["_attachments"].(map[string]any)["note.txt"].(map[string]any)
	if atts["stub"] != true || atts["length"].(float64) != 9 {
		t.Fatalf("replicated attachment: %+v", atts)
	}

	// Re-push is a no-op (idempotent replication).
	resp = send(t, h, "POST", "/rep_push/_bulk_docs", decode(t, bulk))
	if resp.status != 201 || len(resp.array) != 0 {
		t.Fatalf("re-push: %+v", resp)
	}
	// revs_diff now reports nothing missing.
	resp = send(t, h, "POST", "/rep_push/_revs_diff",
		decode(t, `{"pushed":["`+remoteRev2+`","`+remoteRev1+`"]}`))
	if len(resp.body) != 0 {
		t.Fatalf("revs_diff after push: %+v", resp.body)
	}

	// 4. Conflicting branch from another "cluster member".
	conflict := `{
	  "new_edits": false,
	  "docs": [{
	    "_id": "pushed",
	    "_rev": "2-0000000000000000000000000000000c",
	    "_revisions": {"start": 2, "ids": ["0000000000000000000000000000000c", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
	    "from": "other"
	  }]
	}`
	if resp := send(t, h, "POST", "/rep_push/_bulk_docs", decode(t, conflict)); resp.status != 201 {
		t.Fatalf("conflict push: %+v", resp)
	}
	resp = send(t, h, "GET", "/rep_push/pushed?conflicts=true", nil)
	conflicts := resp.body["_conflicts"].([]any)
	if len(conflicts) != 1 || conflicts[0] != "2-0000000000000000000000000000000c" {
		t.Fatalf("conflicts member: %+v", resp.body)
	}

	// 5. Checkpoint: _ensure_full_commit + _local write.
	resp = send(t, h, "POST", "/rep_push/_ensure_full_commit", decode(t, `{}`))
	if resp.status != 201 || resp.body["ok"] != true {
		t.Fatalf("ensure_full_commit: %+v", resp)
	}
	resp = send(t, h, "PUT", "/rep_push/_local/repl-checkpoint-1",
		decode(t, `{"last_seq":"3-couchgres"}`))
	if resp.status != 201 || resp.body["rev"] != "0-1" {
		t.Fatalf("checkpoint write: %+v", resp)
	}
	resp = send(t, h, "GET", "/rep_push/_local/repl-checkpoint-1", nil)
	if resp.status != 200 || resp.body["last_seq"] != "3-couchgres" {
		t.Fatalf("checkpoint read: %+v", resp)
	}
	// Checkpoints update in place with 0-N revs.
	resp = send(t, h, "PUT", "/rep_push/_local/repl-checkpoint-1",
		decode(t, `{"last_seq":"5-couchgres"}`))
	if resp.body["rev"] != "0-2" {
		t.Fatalf("checkpoint rev counter: %+v", resp)
	}
	// Local docs never appear in _changes or _all_docs.
	resp = send(t, h, "GET", "/rep_push/_changes", nil)
	for _, row := range resp.body["results"].([]any) {
		if strings.HasPrefix(row.(map[string]any)["id"].(string), "_local/") {
			t.Fatalf("local doc leaked into changes: %+v", row)
		}
	}
}

func TestReplicationPullSequence(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/rep_pull", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/rep_pull", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "rep_pull")
	defer send(t, h, "DELETE", "/rep_pull", nil, testAdminAuth, admin)

	// Seed: a doc updated twice, a deleted doc, and a conflict.
	resp := send(t, h, "PUT", "/rep_pull/alpha", decode(t, `{"v":1}`))
	rev1 := resp.body["rev"].(string)
	resp = send(t, h, "PUT", "/rep_pull/alpha?rev="+rev1, decode(t, `{"v":2}`))
	rev2 := resp.body["rev"].(string)
	branch := `{"new_edits":false,"docs":[{"_id":"alpha",
	  "_rev":"2-ffffffffffffffffffffffffffffffff",
	  "_revisions":{"start":2,"ids":["ffffffffffffffffffffffffffffffff","` +
		strings.TrimPrefix(rev1, "1-") + `"]},"branch":true}]}`
	send(t, h, "POST", "/rep_pull/_bulk_docs", decode(t, branch))
	resp = send(t, h, "PUT", "/rep_pull/gone", decode(t, `{"bye":1}`))
	goneRev := resp.body["rev"].(string)
	send(t, h, "DELETE", "/rep_pull/gone?rev="+goneRev, nil)

	// 1. Puller reads _changes with style=all_docs.
	resp = send(t, h, "GET", "/rep_pull/_changes?style=all_docs", nil)
	if resp.status != 200 {
		t.Fatalf("changes: %+v", resp)
	}
	rows := resp.body["results"].([]any)
	byID := map[string]map[string]any{}
	for _, raw := range rows {
		row := raw.(map[string]any)
		byID[row["id"].(string)] = row
	}
	alphaChanges := byID["alpha"]["changes"].([]any)
	if len(alphaChanges) != 2 {
		t.Fatalf("alpha leaves in changes: %+v", alphaChanges)
	}
	if byID["gone"]["deleted"] != true {
		t.Fatalf("deletion in changes: %+v", byID["gone"])
	}

	// 2. Fetch each leaf with open_revs (how the replicator pulls).
	leafRevs := fmt.Sprintf(`["%s","2-ffffffffffffffffffffffffffffffff"]`,
		resp2rev(alphaChanges[0]))
	resp = send(t, h, "GET",
		"/rep_pull/alpha?open_revs="+leafRevs+"&revs=true&latest=true", nil,
		"Accept", "application/json")
	if resp.status != 200 || len(resp.array) != 2 {
		t.Fatalf("open_revs: %+v", resp)
	}
	for _, entry := range resp.array {
		doc := entry.(map[string]any)["ok"].(map[string]any)
		if doc["_revisions"] == nil {
			t.Fatalf("open_revs entry missing _revisions: %+v", doc)
		}
	}

	// 3. _bulk_get variant (modern replicators).
	resp = send(t, h, "POST", "/rep_pull/_bulk_get?revs=true", decode(t,
		`{"docs":[{"id":"alpha","rev":"`+rev2+`"},{"id":"ghost"}]}`))
	if resp.status != 200 {
		t.Fatalf("bulk_get: %+v", resp)
	}
	results := resp.body["results"].([]any)
	first := results[0].(map[string]any)["docs"].([]any)[0].(map[string]any)
	if first["ok"] == nil {
		t.Fatalf("bulk_get alpha: %+v", first)
	}
	second := results[1].(map[string]any)["docs"].([]any)[0].(map[string]any)
	if second["error"].(map[string]any)["error"] != "not_found" {
		t.Fatalf("bulk_get ghost: %+v", second)
	}
}

func resp2rev(change any) string {
	return change.(map[string]any)["rev"].(string)
}

func TestChangesFeeds(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/rep_feeds", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/rep_feeds", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "rep_feeds")
	defer send(t, h, "DELETE", "/rep_feeds", nil, testAdminAuth, admin)

	send(t, h, "PUT", "/rep_feeds/one", decode(t, `{"n":1}`))
	send(t, h, "PUT", "/rep_feeds/two", decode(t, `{"n":2}`))

	// normal: both rows, opaque last_seq, pending.
	resp := send(t, h, "GET", "/rep_feeds/_changes", nil)
	results := resp.body["results"].([]any)
	if len(results) != 2 || resp.body["pending"].(float64) != 0 {
		t.Fatalf("normal feed: %+v", resp.body)
	}
	lastSeq := resp.body["last_seq"].(string)
	if !strings.HasSuffix(lastSeq, "-couchgres") {
		t.Fatalf("last_seq shape: %q", lastSeq)
	}

	// limit + since paging.
	resp = send(t, h, "GET", "/rep_feeds/_changes?limit=1", nil)
	if len(resp.body["results"].([]any)) != 1 || resp.body["pending"].(float64) != 1 {
		t.Fatalf("limited feed: %+v", resp.body)
	}
	sinceSeq := resp.body["last_seq"].(string)
	resp = send(t, h, "GET", "/rep_feeds/_changes?since="+sinceSeq, nil)
	if len(resp.body["results"].([]any)) != 1 {
		t.Fatalf("since paging: %+v", resp.body)
	}

	// filter=_doc_ids.
	resp = send(t, h, "POST", "/rep_feeds/_changes?filter=_doc_ids",
		decode(t, `{"doc_ids":["two"]}`))
	results = resp.body["results"].([]any)
	if len(results) != 1 || results[0].(map[string]any)["id"] != "two" {
		t.Fatalf("_doc_ids filter: %+v", resp.body)
	}

	// longpoll: a concurrent write wakes the request.
	writeDone := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		send(t, h, "PUT", "/rep_feeds/three", decode(t, `{"n":3}`))
		close(writeDone)
	}()
	start := time.Now()
	resp = send(t, h, "GET", "/rep_feeds/_changes?feed=longpoll&since="+lastSeq+"&timeout=5000", nil)
	<-writeDone
	if len(resp.body["results"].([]any)) != 1 {
		t.Fatalf("longpoll results: %+v", resp.body)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("longpoll did not wake on write (took %v)", elapsed)
	}

	// continuous: JSON lines then a last_seq line at timeout.
	req := httptest.NewRequest("GET", "/rep_feeds/_changes?feed=continuous&timeout=200", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if len(lines) != 4 { // three changes + last_seq
		t.Fatalf("continuous lines: %q", lines)
	}
	var last map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatalf("continuous last line: %v", err)
	}
	if last["last_seq"] == nil {
		t.Fatalf("continuous last_seq: %+v", last)
	}

	// eventsource format.
	req = httptest.NewRequest("GET", "/rep_feeds/_changes?feed=eventsource&timeout=200", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("eventsource content type: %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "data: {") {
		t.Fatalf("eventsource body: %q", rec.Body.String())
	}
}

func TestStandaloneAttachments(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/rep_att", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/rep_att", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "rep_att")
	defer send(t, h, "DELETE", "/rep_att", nil, testAdminAuth, admin)

	// PUT attachment on a fresh doc (no rev).
	req := httptest.NewRequest("PUT", "/rep_att/doc/file.txt",
		strings.NewReader("attachment body"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("att put: %d %s", rec.Code, rec.Body.String())
	}
	var putResp map[string]any
	json.Unmarshal(rec.Body.Bytes(), &putResp)
	rev1 := putResp["rev"].(string)

	// GET whole and ranged.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/rep_att/doc/file.txt", nil))
	if rec.Code != 200 || rec.Body.String() != "attachment body" {
		t.Fatalf("att get: %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("att content type: %q", rec.Header().Get("Content-Type"))
	}
	req = httptest.NewRequest("GET", "/rep_att/doc/file.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 206 || rec.Body.String() != "attachment" {
		t.Fatalf("ranged get: %d %q", rec.Code, rec.Body.String())
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes 0-9/15" {
		t.Fatalf("content range: %q", cr)
	}

	// Document GET carries the stub. ?attachments=true inlines base64.
	resp := send(t, h, "GET", "/rep_att/doc", nil)
	stub := resp.body["_attachments"].(map[string]any)["file.txt"].(map[string]any)
	if stub["stub"] != true || stub["length"].(float64) != 15 {
		t.Fatalf("stub member: %+v", stub)
	}
	resp = send(t, h, "GET", "/rep_att/doc?attachments=true", nil)
	inline := resp.body["_attachments"].(map[string]any)["file.txt"].(map[string]any)
	if inline["data"] != "YXR0YWNobWVudCBib2R5" {
		t.Fatalf("inline data: %+v", inline)
	}

	// DELETE the attachment (needs rev), then it's gone.
	resp = send(t, h, "DELETE", "/rep_att/doc/file.txt?rev="+rev1, nil)
	if resp.status != 200 {
		t.Fatalf("att delete: %+v", resp)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/rep_att/doc/file.txt", nil))
	if rec.Code != 404 {
		t.Fatalf("deleted att get: %d", rec.Code)
	}
}
