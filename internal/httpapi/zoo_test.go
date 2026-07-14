package httpapi

// Tests for _purge, partitioned databases, ddoc functions
// (_show/_update/_list/_rewrite), and cluster stubs.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPurge(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/purgedb", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/purgedb", nil, testAdminAuth, admin)
	defer send(t, h, "DELETE", "/purgedb", nil, testAdminAuth, admin)

	resp := send(t, h, "PUT", "/purgedb/d1", decode(t, `{"v":1}`), testAdminAuth, admin)
	rev1 := resp.body["rev"].(string)
	resp = send(t, h, "PUT", "/purgedb/d1?rev="+rev1, decode(t, `{"v":2}`), testAdminAuth, admin)
	rev2 := resp.body["rev"].(string)

	// Purging the sole leaf removes the whole document.
	resp = send(t, h, "POST", "/purgedb/_purge", decode(t, `{"d1":["`+rev2+`"]}`), testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("purge: %+v", resp)
	}
	purged := resp.body["purged"].(map[string]any)["d1"].([]any)
	if len(purged) != 1 || purged[0] != rev2 {
		t.Fatalf("purged revs: %+v", purged)
	}
	if resp := send(t, h, "GET", "/purgedb/d1", nil, testAdminAuth, admin); resp.status != 404 || resp.body["reason"] != "missing" {
		t.Fatalf("doc should be gone without a tombstone: %+v", resp)
	}
	info := send(t, h, "GET", "/purgedb", nil, testAdminAuth, admin)
	if info.body["doc_count"].(float64) != 0 || info.body["purge_seq"] == "0-couchgres" {
		t.Fatalf("db info after purge: %+v", info.body)
	}

	// Purging one branch of a conflict keeps the other.
	resp = send(t, h, "PUT", "/purgedb/c1", decode(t, `{"v":1}`), testAdminAuth, admin)
	crev := resp.body["rev"].(string)
	send(t, h, "PUT", "/purgedb/c1?rev="+crev, decode(t, `{"v":2}`), testAdminAuth, admin)
	branch := `{"new_edits":false,"docs":[{"_id":"c1",
	  "_rev":"2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	  "_revisions":{"start":2,"ids":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","` + crev[2:] + `"]},"b":true}]}`
	send(t, h, "POST", "/purgedb/_bulk_docs", decode(t, branch), testAdminAuth, admin)
	resp = send(t, h, "GET", "/purgedb/c1", nil, testAdminAuth, admin)
	winner := resp.body["_rev"].(string)
	resp = send(t, h, "POST", "/purgedb/_purge", decode(t, `{"c1":["`+winner+`"]}`), testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("branch purge: %+v", resp)
	}
	resp = send(t, h, "GET", "/purgedb/c1", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["_rev"] == winner {
		t.Fatalf("other branch should win now: %+v", resp)
	}

	// Unknown revs and docs are ignored.
	resp = send(t, h, "POST", "/purgedb/_purge",
		decode(t, `{"nope":["1-00000000000000000000000000000000"]}`), testAdminAuth, admin)
	if resp.status != 201 || len(resp.body["purged"].(map[string]any)["nope"].([]any)) != 0 {
		t.Fatalf("purge missing doc: %+v", resp)
	}

	// purged_infos_limit round-trip.
	if resp := send(t, h, "GET", "/purgedb/_purged_infos_limit", nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("get limit: %+v", resp)
	}
	if resp := send(t, h, "PUT", "/purgedb/_purged_infos_limit", decode(t, `500`), testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("put limit: %+v", resp)
	}
}

func TestPartitionedDB(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/partdb", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/partdb?partitioned=true", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create: %+v", resp)
	}
	defer send(t, h, "DELETE", "/partdb", nil, testAdminAuth, admin)

	// Doc id rules.
	resp := send(t, h, "PUT", "/partdb/noprefix", decode(t, `{}`), testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "illegal_docid" ||
		resp.body["reason"] != "Doc id must be of form partition:id" {
		t.Fatalf("bad docid: %+v", resp)
	}
	for _, id := range []string{"p1:a", "p1:b", "p2:a"} {
		n := "1"
		if strings.HasPrefix(id, "p2") {
			n = "9"
		}
		if resp := send(t, h, "PUT", "/partdb/"+id, decode(t, `{"n":`+n+`}`), testAdminAuth, admin); resp.status != 201 {
			t.Fatalf("put %s: %+v", id, resp)
		}
	}
	// Design docs need no partition prefix.
	ddoc := `{"views":{"byn":{"map":"function(d){ if(d.n) emit(d.n, null); }"}}}`
	if resp := send(t, h, "PUT", "/partdb/_design/pv", decode(t, ddoc), testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("ddoc: %+v", resp)
	}

	// props.partitioned in db info.
	resp = send(t, h, "GET", "/partdb", nil, testAdminAuth, admin)
	if resp.body["props"].(map[string]any)["partitioned"] != true {
		t.Fatalf("props: %+v", resp.body)
	}

	// Partition info + _all_docs.
	resp = send(t, h, "GET", "/partdb/_partition/p1", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["doc_count"].(float64) != 2 || resp.body["partition"] != "p1" {
		t.Fatalf("partition info: %+v", resp)
	}
	resp = send(t, h, "GET", "/partdb/_partition/p1/_all_docs", nil, testAdminAuth, admin)
	rows := resp.body["rows"].([]any)
	if resp.body["total_rows"].(float64) != 2 || len(rows) != 2 ||
		rows[0].(map[string]any)["id"] != "p1:a" {
		t.Fatalf("partition all_docs: %+v", resp.body)
	}

	// Partitioned views have partition-scoped rows. Global queries are rejected.
	resp = send(t, h, "GET", "/partdb/_partition/p1/_design/pv/_view/byn", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["total_rows"].(float64) != 2 {
		t.Fatalf("partition view: %+v", resp)
	}
	resp = send(t, h, "GET", "/partdb/_design/pv/_view/byn", nil, testAdminAuth, admin)
	if resp.status != 400 || resp.body["error"] != "query_parse_error" {
		t.Fatalf("global query on partitioned view: %+v", resp)
	}

	// A global ddoc (options.partitioned=false) serves global queries.
	global := `{"options":{"partitioned":false},"views":{"byn":{"map":"function(d){ if(d.n) emit(d.n, null); }"}}}`
	send(t, h, "PUT", "/partdb/_design/gv", decode(t, global), testAdminAuth, admin)
	resp = send(t, h, "GET", "/partdb/_design/gv/_view/byn", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["total_rows"].(float64) != 3 {
		t.Fatalf("global view: %+v", resp)
	}

	// Partition _find via fallback scan.
	resp = send(t, h, "POST", "/partdb/_partition/p1/_find", decode(t,
		`{"selector":{"n":{"$gte":0}}}`), testAdminAuth, admin)
	if resp.status != 200 || len(resp.body["docs"].([]any)) != 2 {
		t.Fatalf("partition find: %+v", resp)
	}
	// Partitioned mango index used from the partition route.
	send(t, h, "POST", "/partdb/_index", decode(t,
		`{"index":{"fields":["n"]},"name":"byn","ddoc":"pidx"}`), testAdminAuth, admin)
	resp = send(t, h, "POST", "/partdb/_partition/p2/_find", decode(t,
		`{"selector":{"n":{"$gt":0}},"sort":[{"n":"asc"}]}`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("partition indexed find: %+v", resp)
	}
	docs := resp.body["docs"].([]any)
	if len(docs) != 1 || docs[0].(map[string]any)["_id"] != "p2:a" {
		t.Fatalf("partition indexed docs: %+v", docs)
	}
	if resp.body["warning"] != nil {
		t.Fatalf("indexed partition find warned: %+v", resp.body)
	}
}

const zooDDoc = `{
  "views": {"byn": {"map": "function(d){ if(d.n) emit(d.n, d.name); }"}},
  "shows": {
    "hello": "function(doc, req){ if(!doc) return {code:404, body:'no doc'}; return {body:'Hello ' + doc.name, headers:{'Content-Type':'text/html'}}; }",
    "asjson": "function(doc, req){ return {json:{id:doc._id, q:req.query.x}}; }"
  },
  "updates": {
    "bump": "function(doc, req){ if(!doc) return [null, 'nodoc']; doc.n = doc.n + 1; return [doc, 'bumped to ' + doc.n]; }",
    "create": "function(doc, req){ var b = JSON.parse(req.body); return [{_id:req.query.id, v:b.v}, {json:{made:true}}]; }"
  },
  "lists": {
    "csv": "function(head, req){ start({headers:{'Content-Type':'text/plain'}}); send('total:' + head.total_rows + '\\n'); var row; while(row = getRow()){ send(row.key + ',' + row.value + '\\n'); } return 'done'; }"
  },
  "rewrites": [
    {"from": "/greet/:doc", "to": "_show/hello/:doc"},
    {"from": "/data/*", "to": "../../*"},
    {"from": "/first", "to": "_view/byn", "query": {"limit": "1"}}
  ]
}`

func seedZoo(t *testing.T, h http.Handler, db string) {
	t.Helper()
	admin := adminAuth()
	send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	send(t, h, "PUT", "/"+db, nil, testAdminAuth, admin)
	openSecurity(t, h, db)
	send(t, h, "PUT", "/"+db+"/d1", decode(t, `{"name":"alice","n":1}`), testAdminAuth, admin)
	send(t, h, "PUT", "/"+db+"/d2", decode(t, `{"name":"bob","n":2}`), testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/"+db+"/_design/z", decode(t, zooDDoc), testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("zoo ddoc: %+v", resp)
	}
}

func TestShow(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedZoo(t, h, "zoodb")
	defer send(t, h, "DELETE", "/zoodb", nil, testAdminAuth, admin)

	resp := sendRaw(t, h, "GET", "/zoodb/_design/z/_show/hello/d1", "", nil)
	if resp.status != 200 || resp.raw != "Hello alice" ||
		!strings.HasPrefix(resp.header.Get("Content-Type"), "text/html") {
		t.Fatalf("show: %d %q %s", resp.status, resp.raw, resp.header.Get("Content-Type"))
	}
	resp = sendRaw(t, h, "GET", "/zoodb/_design/z/_show/hello/nope", "", nil)
	if resp.status != 404 || resp.raw != "no doc" {
		t.Fatalf("show missing doc: %d %q", resp.status, resp.raw)
	}
	resp = sendRaw(t, h, "GET", "/zoodb/_design/z/_show/asjson/d1?x=42", "", nil)
	if resp.status != 200 || !strings.Contains(resp.raw, `"q":"42"`) ||
		!strings.HasPrefix(resp.header.Get("Content-Type"), "application/json") {
		t.Fatalf("show json: %d %q %s", resp.status, resp.raw, resp.header.Get("Content-Type"))
	}
	// Missing function name.
	resp = sendRaw(t, h, "GET", "/zoodb/_design/z/_show/nope/d1", "", nil)
	if resp.status != 404 || !strings.Contains(resp.raw, "missing json key: nope") {
		t.Fatalf("missing show fn: %d %q", resp.status, resp.raw)
	}
}

func TestUpdateFunction(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedZoo(t, h, "zooup")
	defer send(t, h, "DELETE", "/zooup", nil, testAdminAuth, admin)

	resp := sendRaw(t, h, "POST", "/zooup/_design/z/_update/bump/d1", "", nil)
	if resp.status != 201 || resp.raw != "bumped to 2" || resp.header.Get("X-Couch-Update-NewRev") == "" {
		t.Fatalf("update: %d %q %s", resp.status, resp.raw, resp.header.Get("X-Couch-Update-NewRev"))
	}
	doc := send(t, h, "GET", "/zooup/d1", nil, testAdminAuth, admin)
	if doc.body["n"].(float64) != 2 {
		t.Fatalf("update did not write: %+v", doc.body)
	}
	// Null doc: no write, status 200.
	resp = sendRaw(t, h, "POST", "/zooup/_design/z/_update/bump/ghost", "", nil)
	if resp.status != 200 || resp.raw != "nodoc" || resp.header.Get("X-Couch-Update-NewRev") != "" {
		t.Fatalf("update null: %d %q", resp.status, resp.raw)
	}
	// Creating through an update function.
	resp = sendRaw(t, h, "POST", "/zooup/_design/z/_update/create?id=made1", `{"v":7}`, map[string]string{"Content-Type": "application/json"})
	if resp.status != 201 || !strings.Contains(resp.raw, `"made":true`) {
		t.Fatalf("update create: %d %q", resp.status, resp.raw)
	}
	if resp := send(t, h, "GET", "/zooup/made1", nil, testAdminAuth, admin); resp.status != 200 || resp.body["v"].(float64) != 7 {
		t.Fatalf("created doc: %+v", resp)
	}
}

func TestListFunction(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedZoo(t, h, "zoolist")
	defer send(t, h, "DELETE", "/zoolist", nil, testAdminAuth, admin)

	resp := sendRaw(t, h, "GET", "/zoolist/_design/z/_list/csv/byn", "", nil)
	want := "total:2\n1,alice\n2,bob\ndone"
	if resp.status != 200 || resp.raw != want ||
		!strings.HasPrefix(resp.header.Get("Content-Type"), "text/plain") {
		t.Fatalf("list: %d %q %s", resp.status, resp.raw, resp.header.Get("Content-Type"))
	}
	// View params apply.
	resp = sendRaw(t, h, "GET", "/zoolist/_design/z/_list/csv/byn?descending=true&limit=1", "", nil)
	if resp.raw != "total:2\n2,bob\ndone" {
		t.Fatalf("list with params: %q", resp.raw)
	}
}

func TestRewrite(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	seedZoo(t, h, "zoorew")
	defer send(t, h, "DELETE", "/zoorew", nil, testAdminAuth, admin)

	// Rule with :var -> _show.
	resp := sendRaw(t, h, "GET", "/zoorew/_design/z/_rewrite/greet/d1", "", nil)
	if resp.status != 200 || resp.raw != "Hello alice" {
		t.Fatalf("rewrite show: %d %q", resp.status, resp.raw)
	}
	// Wildcard back to the db root: a plain doc read.
	resp = sendRaw(t, h, "GET", "/zoorew/_design/z/_rewrite/data/d2", "", nil)
	if resp.status != 200 || !strings.Contains(resp.raw, `"name":"bob"`) {
		t.Fatalf("rewrite wildcard: %d %q", resp.status, resp.raw)
	}
	// Query injection.
	resp = sendRaw(t, h, "GET", "/zoorew/_design/z/_rewrite/first", "", nil)
	if resp.status != 200 || !strings.Contains(resp.raw, `"total_rows":2`) ||
		strings.Count(resp.raw, `"id"`) != 1 {
		t.Fatalf("rewrite view query: %d %q", resp.status, resp.raw)
	}
	// No matching rule.
	resp = sendRaw(t, h, "GET", "/zoorew/_design/z/_rewrite/nothing/here", "", nil)
	if resp.status != 404 {
		t.Fatalf("rewrite miss: %d %q", resp.status, resp.raw)
	}
}

func TestClusterStubs(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()

	resp := send(t, h, "GET", "/_membership", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["all_nodes"].([]any)[0] != "nonode@nohost" {
		t.Fatalf("membership: %+v", resp)
	}
	resp = send(t, h, "GET", "/_cluster_setup", nil, testAdminAuth, admin)
	if resp.body["state"] != "single_node_enabled" {
		t.Fatalf("cluster_setup: %+v", resp)
	}
	resp = send(t, h, "GET", "/_reshard", nil, testAdminAuth, admin)
	if resp.body["state"] != "running" || resp.body["total"].(float64) != 0 {
		t.Fatalf("reshard: %+v", resp)
	}

	send(t, h, "DELETE", "/stubdb", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/stubdb", nil, testAdminAuth, admin)
	defer send(t, h, "DELETE", "/stubdb", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/stubdb/sd", decode(t, `{}`), testAdminAuth, admin)

	resp = send(t, h, "GET", "/stubdb/_shards", nil, testAdminAuth, admin)
	shards := resp.body["shards"].(map[string]any)
	if len(shards) != 1 || shards["00000000-ffffffff"] == nil {
		t.Fatalf("shards: %+v", resp.body)
	}
	resp = send(t, h, "GET", "/stubdb/_shards/sd", nil, testAdminAuth, admin)
	if resp.body["range"] != "00000000-ffffffff" {
		t.Fatalf("shards doc: %+v", resp.body)
	}
	resp = send(t, h, "POST", "/stubdb/_sync_shards", nil, testAdminAuth, admin)
	if resp.status != 202 || resp.body["ok"] != true {
		t.Fatalf("sync_shards: %+v", resp)
	}

	// Search endpoints answer like a search-less CouchDB.
	send(t, h, "PUT", "/stubdb/_design/s", decode(t, `{"a":1}`), testAdminAuth, admin)
	resp = send(t, h, "GET", "/stubdb/_design/s/_search/idx", nil, testAdminAuth, admin)
	if resp.status != 503 || resp.body["reason"] != "Search is not available" {
		t.Fatalf("search: %+v", resp)
	}

	// Node endpoints.
	resp = send(t, h, "GET", "/_node/_local/_versions", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["javascript_engine"].(map[string]any)["name"] != "quickjs" {
		t.Fatalf("versions: %+v", resp)
	}
	resp = send(t, h, "GET", "/_node/_local/_system", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["memory"] == nil {
		t.Fatalf("system: %+v", resp)
	}
	resp = send(t, h, "GET", "/_node/_local/_stats", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["couchdb"] == nil {
		t.Fatalf("stats: %+v", resp)
	}
}

// sendRaw performs a request keeping the raw (non-JSON) body.
type rawResponse struct {
	status int
	raw    string
	header http.Header
}

func sendRaw(t *testing.T, handler http.Handler, method, path, body string, headers map[string]string) rawResponse {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", adminAuth())
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rawResponse{status: rec.Code, raw: rec.Body.String(), header: rec.Header()}
}
