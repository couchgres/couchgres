package httpapi

// Tests for views, validate_doc_update, and JS changes filters.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// seedViewDB creates a public db with numbered docs and a design doc.
func seedViewDB(t *testing.T, h http.Handler, db string, ddoc string) {
	t.Helper()
	admin := adminAuth()
	send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/"+db, nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create %s: %+v", db, resp)
	}
	openSecurity(t, h, db)
	for i := 1; i <= 5; i++ {
		doc := decode(t, fmt.Sprintf(`{"kind":"n","n":%d,"tag":"%s"}`, i, tagOf(i)))
		if resp := send(t, h, "PUT", fmt.Sprintf("/%s/doc%d", db, i), doc,
			testAdminAuth, admin); resp.status != 201 {
			t.Fatalf("seed doc%d: %+v", i, resp)
		}
	}
	if ddoc != "" {
		if resp := send(t, h, "PUT", "/"+db+"/_design/vt", decode(t, ddoc),
			testAdminAuth, admin); resp.status != 201 {
			t.Fatalf("ddoc: %+v", resp)
		}
	}
}

func tagOf(i int) string {
	if i%2 == 0 {
		return "even"
	}
	return "odd"
}

const viewDDoc = `{
  "views": {
    "byn": {"map": "function(doc){ if(doc.kind==='n') emit(doc.n, doc.n*10); }"},
    "sum": {"map": "function(doc){ if(doc.kind==='n') emit(doc.tag, doc.n); }",
            "reduce": "function(keys, values, rereduce){ return sum(values); }"},
    "stats": {"map": "function(doc){ if(doc.kind==='n') emit([doc.tag, doc.n], doc.n); }",
              "reduce": "_stats"}
  }
}`

func rowKeys(t *testing.T, resp response) []any {
	t.Helper()
	rows, ok := resp.body["rows"].([]any)
	if !ok {
		t.Fatalf("no rows: %+v", resp.body)
	}
	keys := make([]any, len(rows))
	for i, raw := range rows {
		keys[i] = raw.(map[string]any)["key"]
	}
	return keys
}

func TestViewMapQuery(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedViewDB(t, h, "vmap", viewDDoc)
	defer send(t, h, "DELETE", "/vmap", nil, testAdminAuth, admin)

	resp := send(t, h, "GET", "/vmap/_design/vt/_view/byn", nil, testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("view: %+v", resp)
	}
	if resp.body["total_rows"].(float64) != 5 || resp.body["offset"].(float64) != 0 {
		t.Fatalf("totals: %+v", resp.body)
	}
	rows := resp.body["rows"].([]any)
	first := rows[0].(map[string]any)
	if first["id"] != "doc1" || first["key"].(float64) != 1 || first["value"].(float64) != 10 {
		t.Fatalf("row 0: %+v", first)
	}

	// Range with offset, limit, descending.
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn?startkey=2&endkey=4", nil, testAdminAuth, admin)
	if got := len(resp.body["rows"].([]any)); got != 3 || resp.body["offset"].(float64) != 1 {
		t.Fatalf("range: rows %d offset %v", got, resp.body["offset"])
	}
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn?descending=true&limit=2", nil, testAdminAuth, admin)
	keys := rowKeys(t, resp)
	if keys[0].(float64) != 5 || keys[1].(float64) != 4 {
		t.Fatalf("descending: %v", keys)
	}
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn?endkey=3&inclusive_end=false", nil, testAdminAuth, admin)
	if got := len(resp.body["rows"].([]any)); got != 2 {
		t.Fatalf("exclusive end: %d rows", got)
	}

	// key= and POST keys.
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn?key=3", nil, testAdminAuth, admin)
	if got := len(resp.body["rows"].([]any)); got != 1 {
		t.Fatalf("key=3: %d rows", got)
	}
	resp = send(t, h, "POST", "/vmap/_design/vt/_view/byn", decode(t, `{"keys":[5,1]}`), testAdminAuth, admin)
	keys = rowKeys(t, resp)
	if keys[0].(float64) != 5 || keys[1].(float64) != 1 {
		t.Fatalf("keys order: %v", keys)
	}

	// Incremental: a new doc appears on the next query.
	send(t, h, "PUT", "/vmap/doc6", decode(t, `{"kind":"n","n":6,"tag":"even"}`), testAdminAuth, admin)
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn", nil, testAdminAuth, admin)
	if resp.body["total_rows"].(float64) != 6 {
		t.Fatalf("incremental update: %+v", resp.body["total_rows"])
	}
	// Deletion drops the row.
	doc := send(t, h, "GET", "/vmap/doc6", nil, testAdminAuth, admin)
	send(t, h, "DELETE", "/vmap/doc6?rev="+doc.body["_rev"].(string), nil, testAdminAuth, admin)
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn", nil, testAdminAuth, admin)
	if resp.body["total_rows"].(float64) != 5 {
		t.Fatalf("deletion not reflected: %+v", resp.body["total_rows"])
	}

	// include_docs.
	resp = send(t, h, "GET", "/vmap/_design/vt/_view/byn?include_docs=true&limit=1", nil, testAdminAuth, admin)
	docObj := resp.body["rows"].([]any)[0].(map[string]any)["doc"].(map[string]any)
	if docObj["_id"] != "doc1" || docObj["n"].(float64) != 1 {
		t.Fatalf("include_docs: %+v", docObj)
	}
}

func TestViewReduce(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedViewDB(t, h, "vred", viewDDoc)
	defer send(t, h, "DELETE", "/vred", nil, testAdminAuth, admin)

	// JS reduce, whole range: 1+2+3+4+5.
	resp := send(t, h, "GET", "/vred/_design/vt/_view/sum", nil, testAdminAuth, admin)
	rows := resp.body["rows"].([]any)
	row := rows[0].(map[string]any)
	if row["key"] != nil || row["value"].(float64) != 15 {
		t.Fatalf("reduce: %+v", rows)
	}
	if _, present := resp.body["total_rows"]; present {
		t.Fatalf("reduce response should not carry total_rows: %+v", resp.body)
	}

	// group=true: evens 2+4=6, odds 1+3+5=9 ("even" < "odd" by collation).
	resp = send(t, h, "GET", "/vred/_design/vt/_view/sum?group=true", nil, testAdminAuth, admin)
	rows = resp.body["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("groups: %+v", rows)
	}
	even := rows[0].(map[string]any)
	odd := rows[1].(map[string]any)
	if even["key"] != "even" || even["value"].(float64) != 6 ||
		odd["key"] != "odd" || odd["value"].(float64) != 9 {
		t.Fatalf("grouped: %+v", rows)
	}

	// reduce=false gives map rows.
	resp = send(t, h, "GET", "/vred/_design/vt/_view/sum?reduce=false", nil, testAdminAuth, admin)
	if len(resp.body["rows"].([]any)) != 5 {
		t.Fatalf("reduce=false: %+v", resp.body)
	}

	// Builtin _stats with group_level=1 over [tag, n] keys.
	resp = send(t, h, "GET", "/vred/_design/vt/_view/stats?group_level=1", nil, testAdminAuth, admin)
	rows = resp.body["rows"].([]any)
	evenStats := rows[0].(map[string]any)
	key := evenStats["key"].([]any)
	value := evenStats["value"].(map[string]any)
	if key[0] != "even" || value["sum"].(float64) != 6 || value["count"].(float64) != 2 ||
		value["min"].(float64) != 2 || value["max"].(float64) != 4 || value["sumsqr"].(float64) != 20 {
		t.Fatalf("stats: %+v", evenStats)
	}

	// Errors: reduce on a map-only view and group on a map-only view.
	resp = send(t, h, "GET", "/vred/_design/vt/_view/byn?reduce=true", nil, testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "query_parse_error" {
		t.Fatalf("reduce on map view: %+v", resp)
	}
	resp = send(t, h, "GET", "/vred/_design/vt/_view/byn?group=true", nil, testAdminAuth, admin)
	if resp.status != 400 {
		t.Fatalf("group on map view: %+v", resp)
	}
}

func TestViewCollationOrder(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/vcoll", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/vcoll", nil, testAdminAuth, admin)
	openSecurity(t, h, "vcoll")
	defer send(t, h, "DELETE", "/vcoll", nil, testAdminAuth, admin)

	docs := `{"docs":[
	  {"_id":"d1","k":"apple"},
	  {"_id":"d2","k":10},
	  {"_id":"d3","k":null},
	  {"_id":"d4","k":["a"]},
	  {"_id":"d5","k":true},
	  {"_id":"d6","k":{"o":1}},
	  {"_id":"d7","k":"Apple"},
	  {"_id":"d8","k":2},
	  {"_id":"d9","k":false}
	]}`
	send(t, h, "POST", "/vcoll/_bulk_docs", decode(t, docs), testAdminAuth, admin)
	ddoc := `{"views":{"k":{"map":"function(doc){ if('k' in doc) emit(doc.k, null); }"}}}`
	send(t, h, "PUT", "/vcoll/_design/c", decode(t, ddoc), testAdminAuth, admin)

	resp := send(t, h, "GET", "/vcoll/_design/c/_view/k", nil, testAdminAuth, admin)
	ids := []string{}
	for _, raw := range resp.body["rows"].([]any) {
		ids = append(ids, raw.(map[string]any)["id"].(string))
	}
	// null < false < true < 2 < 10 < "apple" < "Apple" < ["a"] < {"o":1}
	want := []string{"d3", "d9", "d5", "d8", "d2", "d1", "d7", "d4", "d6"}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("collation order: got %v want %v", ids, want)
	}

	// startkey ranges follow the same order.
	resp = send(t, h, "GET", `/vcoll/_design/c/_view/k?startkey="apple"`, nil, testAdminAuth, admin)
	if got := len(resp.body["rows"].([]any)); got != 4 {
		t.Fatalf("startkey range: %d rows", got)
	}
}

func TestViewDDocChangeAndCleanup(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedViewDB(t, h, "vddoc", viewDDoc)
	defer send(t, h, "DELETE", "/vddoc", nil, testAdminAuth, admin)

	send(t, h, "GET", "/vddoc/_design/vt/_view/byn", nil, testAdminAuth, admin)

	// Changing the ddoc gives a fresh index (new signature).
	ddoc := send(t, h, "GET", "/vddoc/_design/vt", nil, testAdminAuth, admin)
	updated := `{"_rev":"` + ddoc.body["_rev"].(string) + `",
	  "views":{"byn":{"map":"function(doc){ if(doc.kind==='n' && doc.n > 2) emit(doc.n, null); }"}}}`
	if resp := send(t, h, "PUT", "/vddoc/_design/vt", decode(t, updated), testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("ddoc update: %+v", resp)
	}
	resp := send(t, h, "GET", "/vddoc/_design/vt/_view/byn", nil, testAdminAuth, admin)
	if resp.body["total_rows"].(float64) != 3 {
		t.Fatalf("new signature: %+v", resp.body)
	}

	// _view_cleanup drops the orphaned old index.
	if resp := send(t, h, "POST", "/vddoc/_view_cleanup", nil, testAdminAuth, admin); resp.status != 202 {
		t.Fatalf("cleanup: %+v", resp)
	}
	// _info reports the current signature.
	resp = send(t, h, "GET", "/vddoc/_design/vt/_info", nil, testAdminAuth, admin)
	info := resp.body["view_index"].(map[string]any)
	if info["signature"] == "" || info["language"] != "javascript" {
		t.Fatalf("_info: %+v", resp.body)
	}
	// _compact stubs answer 202.
	if resp := send(t, h, "POST", "/vddoc/_compact", decode(t, `{}`), testAdminAuth, admin); resp.status != 202 {
		t.Fatalf("compact: %+v", resp)
	}
	if resp := send(t, h, "POST", "/vddoc/_compact/vt", decode(t, `{}`), testAdminAuth, admin); resp.status != 202 {
		t.Fatalf("compact ddoc: %+v", resp)
	}
}

func TestDesignDocsListing(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedViewDB(t, h, "vlist", viewDDoc)
	defer send(t, h, "DELETE", "/vlist", nil, testAdminAuth, admin)

	resp := send(t, h, "GET", "/vlist/_design_docs", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["total_rows"].(float64) != 1 {
		t.Fatalf("_design_docs: %+v", resp)
	}
	row := resp.body["rows"].([]any)[0].(map[string]any)
	if row["id"] != "_design/vt" {
		t.Fatalf("_design_docs row: %+v", row)
	}
}

func TestValidateDocUpdate(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/vvdu", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/vvdu", nil, testAdminAuth, admin)
	openSecurity(t, h, "vvdu")
	defer send(t, h, "DELETE", "/vvdu", nil, testAdminAuth, admin)

	vdu := `{"validate_doc_update": "function(newDoc, oldDoc, userCtx, secObj){ if (newDoc._deleted) return; if (!newDoc.author) throw({forbidden: 'documents need an author'}); if (oldDoc && oldDoc.author !== newDoc.author) throw({unauthorized: 'wrong author'}); }"}`
	if resp := send(t, h, "PUT", "/vvdu/_design/rules", decode(t, vdu), testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("vdu ddoc: %+v", resp)
	}

	// Rejected without author.
	resp := send(t, h, "PUT", "/vvdu/nope", decode(t, `{"x":1}`), testAdminAuth, admin)
	if resp.status != 403 || resp.body["error"] != "forbidden" || resp.body["reason"] != "documents need an author" {
		t.Fatalf("forbidden: %+v", resp)
	}
	// Accepted with author.
	resp = send(t, h, "PUT", "/vvdu/ok", decode(t, `{"author":"rob"}`), testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("allowed write: %+v", resp)
	}
	// Author change -> unauthorized (401).
	rev := resp.body["rev"].(string)
	resp = send(t, h, "PUT", "/vvdu/ok?rev="+rev, decode(t, `{"author":"mallory"}`), testAdminAuth, admin)
	if resp.status != 401 || resp.body["error"] != "unauthorized" {
		t.Fatalf("unauthorized: %+v", resp)
	}
	// Bulk writes report per-doc validation errors.
	bulk := `{"docs":[{"_id":"b1","author":"rob"},{"_id":"b2"}]}`
	resp = send(t, h, "POST", "/vvdu/_bulk_docs", decode(t, bulk), testAdminAuth, admin)
	if resp.array[0].(map[string]any)["ok"] != true {
		t.Fatalf("bulk ok doc: %+v", resp.array)
	}
	if resp.array[1].(map[string]any)["error"] != "forbidden" {
		t.Fatalf("bulk rejected doc: %+v", resp.array)
	}
	// Deleting the ddoc lifts the rules (cache invalidation).
	ddoc := send(t, h, "GET", "/vvdu/_design/rules", nil, testAdminAuth, admin)
	send(t, h, "DELETE", "/vvdu/_design/rules?rev="+ddoc.body["_rev"].(string), nil, testAdminAuth, admin)
	resp = send(t, h, "PUT", "/vvdu/free", decode(t, `{"x":1}`), testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("rules lifted: %+v", resp)
	}
}

func TestChangesJSFilter(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/vfilt", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/vfilt", nil, testAdminAuth, admin)
	openSecurity(t, h, "vfilt")
	defer send(t, h, "DELETE", "/vfilt", nil, testAdminAuth, admin)

	ddoc := `{
	  "filters": {"bytag": "function(doc, req){ return doc.tag === req.query.tag; }"},
	  "views": {"tagged": {"map": "function(doc){ if(doc.tag) emit(doc.tag, null); }"}}
	}`
	send(t, h, "PUT", "/vfilt/_design/f", decode(t, ddoc), testAdminAuth, admin)
	send(t, h, "PUT", "/vfilt/a", decode(t, `{"tag":"keep"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/vfilt/b", decode(t, `{"tag":"drop"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/vfilt/c", decode(t, `{"tag":"keep"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/vfilt/d", decode(t, `{"other":true}`), testAdminAuth, admin)

	resp := send(t, h, "GET", "/vfilt/_changes?filter=f/bytag&tag=keep", nil, testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("filtered changes: %+v", resp)
	}
	results := resp.body["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("filtered rows: %+v", results)
	}
	for _, raw := range results {
		row := raw.(map[string]any)
		if row["id"] != "a" && row["id"] != "c" {
			t.Fatalf("wrong doc passed: %+v", row)
		}
		if _, hasDoc := row["doc"]; hasDoc {
			t.Fatalf("doc should not be included: %+v", row)
		}
	}

	// _view filter: docs the map emits for.
	resp = send(t, h, "GET", "/vfilt/_changes?filter=_view&view=f/tagged", nil, testAdminAuth, admin)
	if got := len(resp.body["results"].([]any)); got != 3 {
		t.Fatalf("_view filter: %d rows", got)
	}

	// Missing filter -> 404.
	resp = send(t, h, "GET", "/vfilt/_changes?filter=f/nope", nil, testAdminAuth, admin)
	if resp.status != 404 {
		t.Fatalf("missing filter: %+v", resp)
	}
}

// TestFilteredReplication: the replicator passes filter and query_params to
// the source's _changes, copying only matching docs.
func TestFilteredReplication(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	for _, db := range []string{"frep_src", "frep_dst"} {
		send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	send(t, h, "PUT", "/frep_src", nil, testAdminAuth, admin)
	defer func() {
		for _, db := range []string{"frep_src", "frep_dst"} {
			send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()

	ddoc := `{"filters": {"bytag": "function(doc, req){ return doc.tag === req.query.tag; }"}}`
	send(t, h, "PUT", "/frep_src/_design/f", decode(t, ddoc), testAdminAuth, admin)
	send(t, h, "PUT", "/frep_src/keep1", decode(t, `{"tag":"keep"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/frep_src/drop1", decode(t, `{"tag":"drop"}`), testAdminAuth, admin)
	send(t, h, "PUT", "/frep_src/keep2", decode(t, `{"tag":"keep"}`), testAdminAuth, admin)

	body := `{"source":"frep_src","target":"frep_dst","create_target":true,
	  "filter":"f/bytag","query_params":{"tag":"keep"}}`
	resp := send(t, h, "POST", "/_replicate", decode(t, body), testAdminAuth, admin)
	if resp.status != 200 || resp.body["ok"] != true {
		t.Fatalf("filtered _replicate: %+v", resp)
	}
	if resp := send(t, h, "GET", "/frep_dst/keep1", nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("keep1 missing: %+v", resp)
	}
	if resp := send(t, h, "GET", "/frep_dst/keep2", nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("keep2 missing: %+v", resp)
	}
	if resp := send(t, h, "GET", "/frep_dst/drop1", nil, testAdminAuth, admin); resp.status != 404 {
		t.Fatalf("drop1 should not replicate: %+v", resp)
	}
}

// The view response cache must never serve stale data: doc writes, design
// doc edits, and purge (which moves no sequence) all invalidate.
func TestViewResponseCacheFreshness(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/vcache", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/vcache", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	openSecurity(t, h, "vcache")
	defer send(t, h, "DELETE", "/vcache", nil, testAdminAuth, admin)

	for i := 1; i <= 5; i++ {
		send(t, h, "PUT", fmt.Sprintf("/vcache/d%d", i),
			decode(t, fmt.Sprintf(`{"n":%d}`, i%3)))
	}
	send(t, h, "PUT", "/vcache/_design/v", decode(t,
		`{"views":{"byn":{"map":"function(doc){ if (doc.n !== undefined) emit(doc.n, null); }"}}}`),
		testAdminAuth, admin)

	const q = "/vcache/_design/v/_view/byn?key=1&reduce=false"
	first := send(t, h, "GET", q, nil)
	if first.status != 200 || first.body["total_rows"] != float64(5) {
		t.Fatalf("first read: %+v", first.body)
	}
	rows := first.body["rows"].([]any)

	// Repeat read (cache hit) must be identical.
	second := send(t, h, "GET", q, nil)
	if len(second.body["rows"].([]any)) != len(rows) {
		t.Fatalf("cached read differs: %+v", second.body)
	}

	// A doc write invalidates via the database seq.
	resp := send(t, h, "PUT", "/vcache/d6", decode(t, `{"n":1}`))
	d6rev := resp.body["rev"].(string)
	third := send(t, h, "GET", q, nil)
	if third.body["total_rows"] != float64(6) ||
		len(third.body["rows"].([]any)) != len(rows)+1 {
		t.Fatalf("read after write served stale data: %+v", third.body)
	}

	// A design doc edit invalidates via the ddoc seq (new signature).
	ddoc := send(t, h, "GET", "/vcache/_design/v", nil)
	send(t, h, "PUT", "/vcache/_design/v", decode(t, fmt.Sprintf(
		`{"_rev":%q,"views":{"byn":{"map":"function(doc){ if (doc.n !== undefined) emit(doc.n, 7); }"}}}`,
		ddoc.body["_rev"].(string))), testAdminAuth, admin)
	fourth := send(t, h, "GET", q, nil)
	if fourth.body["rows"].([]any)[0].(map[string]any)["value"] != float64(7) {
		t.Fatalf("read after ddoc edit served stale view code: %+v", fourth.body)
	}

	// Purge moves no sequence. The handler clears the cache.
	if resp := send(t, h, "POST", "/vcache/_purge",
		decode(t, fmt.Sprintf(`{"d6":[%q]}`, d6rev)), testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("purge: %+v", resp)
	}
	fifth := send(t, h, "GET", q, nil)
	if len(fifth.body["rows"].([]any)) != len(rows) {
		t.Fatalf("read after purge served purged rows: %+v", fifth.body)
	}

	// 304 revalidation against the cached ETag.
	etag := fifth.header.Get("Etag")
	req := httptest.NewRequest("GET", q, nil)
	req.Header.Set("If-None-Match", etag)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 304 {
		t.Fatalf("If-None-Match revalidation: %d", rec.Code)
	}
}
