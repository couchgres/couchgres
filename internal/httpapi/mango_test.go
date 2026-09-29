package httpapi

// Tests for _index, _find, _explain, and filter=_selector.

import (
	"fmt"
	"net/http"
	"testing"
)

func seedMangoDB(t *testing.T, h http.Handler, db string) {
	t.Helper()
	admin := adminAuth()
	send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/"+db, nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create %s: %+v", db, resp)
	}
	openSecurity(t, h, db)
	docs := []string{
		`{"_id":"u1","type":"user","name":"alice","age":31,"tags":["admin","dev"]}`,
		`{"_id":"u2","type":"user","name":"bob","age":25,"tags":["dev"]}`,
		`{"_id":"u3","type":"user","name":"carol","age":40}`,
		`{"_id":"p1","type":"post","name":"hello","author":"alice"}`,
		`{"_id":"p2","type":"post","name":"world","author":"bob"}`,
	}
	for i, doc := range docs {
		if resp := send(t, h, "POST", "/"+db, decode(t, doc), testAdminAuth, admin); resp.status != 201 {
			t.Fatalf("seed %d: %+v", i, resp)
		}
	}
}

func findIDs(t *testing.T, h http.Handler, db, body string) []string {
	t.Helper()
	resp := send(t, h, "POST", "/"+db+"/_find", decode(t, body), testAdminAuth, adminAuth())
	if resp.status != 200 {
		t.Fatalf("_find %s: %+v", body, resp)
	}
	docs, ok := resp.body["docs"].([]any)
	if !ok {
		t.Fatalf("_find %s: no docs member: %+v", body, resp.body)
	}
	ids := make([]string, len(docs))
	for i, raw := range docs {
		ids[i] = raw.(map[string]any)["_id"].(string)
	}
	return ids
}

func TestMangoFindFallbackScan(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedMangoDB(t, h, "mgfind")
	defer send(t, h, "DELETE", "/mgfind", nil, testAdminAuth, admin)

	resp := send(t, h, "POST", "/mgfind/_find", decode(t, `{"selector":{"type":"user"}}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("find: %+v", resp)
	}
	if resp.body["warning"] == nil {
		t.Fatalf("fallback scan should warn: %+v", resp.body)
	}
	if got := len(resp.body["docs"].([]any)); got != 3 {
		t.Fatalf("users: %d", got)
	}

	// Operators through the full evaluator.
	if ids := findIDs(t, h, "mgfind", `{"selector":{"age":{"$gt":25}}}`); len(ids) != 2 {
		t.Fatalf("$gt: %v", ids)
	}
	if ids := findIDs(t, h, "mgfind", `{"selector":{"tags":{"$in":["admin"]}}}`); len(ids) != 1 || ids[0] != "u1" {
		t.Fatalf("$in: %v", ids)
	}
	if ids := findIDs(t, h, "mgfind", `{"selector":{"$or":[{"name":"bob"},{"name":"carol"}]}}`); len(ids) != 2 {
		t.Fatalf("$or matches u2 and u3: %v", ids)
	}

	// fields projection.
	resp = send(t, h, "POST", "/mgfind/_find", decode(t,
		`{"selector":{"_id":"u1"},"fields":["name","age"]}`), testAdminAuth, admin)
	doc := resp.body["docs"].([]any)[0].(map[string]any)
	if doc["name"] != "alice" || doc["age"].(float64) != 31 || doc["_id"] != nil {
		t.Fatalf("projection: %+v", doc)
	}

	// Missing selector error.
	resp = send(t, h, "POST", "/mgfind/_find", decode(t, `{}`), testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "missing_required_key" {
		t.Fatalf("missing selector: %+v", resp)
	}
	// Invalid operator error.
	resp = send(t, h, "POST", "/mgfind/_find", decode(t, `{"selector":{"a":{"$nope":1}}}`), testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "invalid_operator" {
		t.Fatalf("invalid operator: %+v", resp)
	}
}

func TestMangoIndexLifecycle(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedMangoDB(t, h, "mgidx")
	defer send(t, h, "DELETE", "/mgidx", nil, testAdminAuth, admin)

	// Create.
	resp := send(t, h, "POST", "/mgidx/_index", decode(t,
		`{"index":{"fields":["type","age"]},"name":"byage","ddoc":"mgindexes"}`), testAdminAuth, admin)
	if resp.status != 200 || resp.body["result"] != "created" || resp.body["id"] != "_design/mgindexes" {
		t.Fatalf("create index: %+v", resp)
	}
	// Idempotent.
	resp = send(t, h, "POST", "/mgidx/_index", decode(t,
		`{"index":{"fields":["type","age"]},"name":"byage","ddoc":"mgindexes"}`), testAdminAuth, admin)
	if resp.body["result"] != "exists" {
		t.Fatalf("recreate index: %+v", resp)
	}
	// text type errors like a search-less CouchDB.
	resp = send(t, h, "POST", "/mgidx/_index", decode(t,
		`{"index":{"fields":["name"]},"type":"text"}`), testAdminAuth, admin)
	if resp.status != 503 {
		t.Fatalf("text index: %+v", resp)
	}

	// List: special + json index.
	resp = send(t, h, "GET", "/mgidx/_index", nil, testAdminAuth, admin)
	if int(resp.body["total_rows"].(float64)) != 2 {
		t.Fatalf("index list: %+v", resp.body)
	}
	first := resp.body["indexes"].([]any)[0].(map[string]any)
	if first["name"] != "_all_docs" || first["type"] != "special" {
		t.Fatalf("special index first: %+v", first)
	}

	// The indexed query returns sorted results without a warning.
	resp = send(t, h, "POST", "/mgidx/_find", decode(t,
		`{"selector":{"type":"user","age":{"$gte":26}},"sort":[{"type":"asc"},{"age":"asc"}]}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("indexed find: %+v", resp)
	}
	if resp.body["warning"] != nil {
		t.Fatalf("indexed find warned: %+v", resp.body)
	}
	docs := resp.body["docs"].([]any)
	if len(docs) != 2 || docs[0].(map[string]any)["_id"] != "u1" || docs[1].(map[string]any)["_id"] != "u3" {
		t.Fatalf("indexed order: %+v", docs)
	}

	// Descending sort reverses.
	resp = send(t, h, "POST", "/mgidx/_find", decode(t,
		`{"selector":{"type":"user"},"sort":[{"type":"desc"},{"age":"desc"}]}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("descending find: %+v", resp)
	}
	docs = resp.body["docs"].([]any)
	if len(docs) != 3 || docs[0].(map[string]any)["_id"] != "u3" {
		t.Fatalf("descending: %+v", docs)
	}

	// _explain names the chosen index.
	resp = send(t, h, "POST", "/mgidx/_explain", decode(t,
		`{"selector":{"type":"user","age":{"$gt":30}}}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("explain: %+v", resp)
	}
	index := resp.body["index"].(map[string]any)
	if index["ddoc"] != "_design/mgindexes" || index["type"] != "json" {
		t.Fatalf("explain index: %+v", index)
	}
	if resp.body["limit"].(float64) != 25 {
		t.Fatalf("default limit: %+v", resp.body["limit"])
	}

	// Bookmark paging through the index.
	var seen []string
	bookmark := ""
	for i := 0; i < 5; i++ {
		body := `{"selector":{"type":{"$exists":true},"age":{"$gt":0}},"limit":1,"sort":[{"type":"asc"},{"age":"asc"}]}`
		if bookmark != "" {
			body = body[:len(body)-1] + `,"bookmark":"` + bookmark + `"}`
		}
		resp := send(t, h, "POST", "/mgidx/_find", decode(t, body), testAdminAuth, admin)
		docs := resp.body["docs"].([]any)
		if len(docs) == 0 {
			break
		}
		seen = append(seen, docs[0].(map[string]any)["_id"].(string))
		bookmark = resp.body["bookmark"].(string)
	}
	if fmt.Sprint(seen) != "[u2 u1 u3]" {
		t.Fatalf("paged order: %v", seen)
	}

	// Invalid bookmark.
	resp = send(t, h, "POST", "/mgidx/_find", decode(t,
		`{"selector":{"type":"user"},"bookmark":"garbage!"}`), testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "invalid_bookmark" {
		t.Fatalf("invalid bookmark: %+v", resp)
	}

	// Sort without a usable index errors.
	resp = send(t, h, "POST", "/mgidx/_find", decode(t,
		`{"selector":{"type":"user"},"sort":[{"name":"asc"}]}`), testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "no_usable_index" {
		t.Fatalf("sort without index: %+v", resp)
	}

	// Delete the index. The query falls back to scanning with a warning.
	resp = send(t, h, "DELETE", "/mgidx/_index/_design/mgindexes/json/byage", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["ok"] != true {
		t.Fatalf("delete index: %+v", resp)
	}
	resp = send(t, h, "GET", "/mgidx/_index", nil, testAdminAuth, admin)
	if int(resp.body["total_rows"].(float64)) != 1 {
		t.Fatalf("index list after delete: %+v", resp.body)
	}
	resp = send(t, h, "POST", "/mgidx/_find", decode(t, `{"selector":{"type":"user"}}`), testAdminAuth, admin)
	if resp.body["warning"] == nil {
		t.Fatalf("fallback after delete should warn: %+v", resp.body)
	}
}

func TestChangesSelectorFilter(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedMangoDB(t, h, "mgsel")
	defer send(t, h, "DELETE", "/mgsel", nil, testAdminAuth, admin)

	resp := send(t, h, "POST", "/mgsel/_changes?filter=_selector", decode(t,
		`{"selector":{"type":"post"}}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("selector changes: %+v", resp)
	}
	results := resp.body["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("selector filter rows: %+v", results)
	}
	for _, raw := range results {
		id := raw.(map[string]any)["id"].(string)
		if id != "p1" && id != "p2" {
			t.Fatalf("wrong doc: %s", id)
		}
	}
	// Without a body: CouchDB's exact complaint.
	resp = send(t, h, "GET", "/mgsel/_changes?filter=_selector", nil, testAdminAuth, admin)
	if resp.status != 400 || resp.body["reason"] != "Selector must be specified in POST payload" {
		t.Fatalf("selector without body: %+v", resp)
	}
}

func TestSelectorReplication(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	for _, db := range []string{"selrep_src", "selrep_dst"} {
		send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	send(t, h, "PUT", "/selrep_src", nil, testAdminAuth, admin)
	defer func() {
		for _, db := range []string{"selrep_src", "selrep_dst"} {
			send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()
	send(t, h, "PUT", "/selrep_src/keep1", decode(t, `{"kind":"keep"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/selrep_src/drop1", decode(t, `{"kind":"drop"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/selrep_src/keep2", decode(t, `{"kind":"keep"}`), testAdminAuth, admin)

	body := `{"source":"selrep_src","target":"selrep_dst","create_target":true,
	  "selector":{"kind":"keep"}}`
	resp := send(t, h, "POST", "/_replicate", decode(t, body), testAdminAuth, admin)
	if resp.status != 200 || resp.body["ok"] != true {
		t.Fatalf("selector _replicate: %+v", resp)
	}
	for _, id := range []string{"keep1", "keep2"} {
		if resp := send(t, h, "GET", "/selrep_dst/"+id, nil, testAdminAuth, admin); resp.status != 200 {
			t.Fatalf("%s missing: %+v", id, resp)
		}
	}
	if resp := send(t, h, "GET", "/selrep_dst/drop1", nil, testAdminAuth, admin); resp.status != 404 {
		t.Fatalf("drop1 should not replicate: %+v", resp)
	}
}
