package httpapi

// Tests active replication between databases over a local HTTP listener.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// testLiveServer mounts the API on a real listener so the replicator can
// loop back to it.
func testLiveServer(t *testing.T) http.Handler {
	t.Helper()
	handler := testHandler(t)
	server, ok := handler.(*Server)
	if !ok {
		t.Fatalf("testHandler returned %T, want *Server", handler)
	}
	listener := httptest.NewServer(server)
	t.Cleanup(listener.Close)
	server.SetSelfURL(listener.URL)
	server.StartReplicatorWorker(t.Context())
	return handler
}

func waitFor(t *testing.T, what string, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func deleteDocIfExists(t *testing.T, h http.Handler, path string) {
	t.Helper()
	resp := send(t, h, "GET", path, nil, testAdminAuth, adminAuth())
	if resp.status != 200 {
		return
	}
	rev, _ := resp.body["_rev"].(string)
	if rev == "" {
		t.Fatalf("doc %s missing _rev: %+v", path, resp.body)
	}
	send(t, h, "DELETE", path+"?rev="+rev, nil, testAdminAuth, adminAuth())
}

func TestReplicateOneShot(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	for _, db := range []string{"act_src", "act_dst"} {
		send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	if resp := send(t, h, "PUT", "/act_src", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create source: %+v", resp)
	}
	defer func() {
		for _, db := range []string{"act_src", "act_dst"} {
			send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()

	// Seed: two docs, one updated, one deleted, one conflict branch, one
	// attachment. This is the full shape of a real migration.
	resp := send(t, h, "PUT", "/act_src/plain", decode(t, `{"v":1}`), testAdminAuth, admin)
	rev1 := resp.body["rev"].(string)
	send(t, h, "PUT", "/act_src/plain?rev="+rev1, decode(t, `{"v":2}`), testAdminAuth, admin)
	branch := `{"new_edits":false,"docs":[{"_id":"plain",
	  "_rev":"2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	  "_revisions":{"start":2,"ids":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","` +
		rev1[2:] + `"]},"branch":true}]}`
	send(t, h, "POST", "/act_src/_bulk_docs", decode(t, branch), testAdminAuth, admin)
	resp = send(t, h, "PUT", "/act_src/gone", decode(t, `{"x":1}`), testAdminAuth, admin)
	send(t, h, "DELETE", "/act_src/gone?rev="+resp.body["rev"].(string), nil, testAdminAuth, admin)
	send(t, h, "PUT", "/act_src/withatt", decode(t,
		`{"a":1,"_attachments":{"f.txt":{"content_type":"text/plain","data":"aGk="}}}`),
		testAdminAuth, admin)

	// One-shot replication with local db names.
	resp = send(t, h, "POST", "/_replicate", decode(t,
		`{"source":"act_src","target":"act_dst","create_target":true}`),
		testAdminAuth, admin)
	if resp.status != 200 || resp.body["ok"] != true {
		t.Fatalf("_replicate: %+v", resp)
	}
	history := resp.body["history"].([]any)[0].(map[string]any)
	if history["doc_write_failures"].(float64) != 0 || history["docs_written"].(float64) < 3 {
		t.Fatalf("history: %+v", history)
	}

	// Everything arrived: winner + conflict leaves, tombstone, attachment.
	resp = send(t, h, "GET", "/act_dst/plain?conflicts=true", nil, testAdminAuth, admin)
	if resp.status != 200 || resp.body["_conflicts"] == nil {
		t.Fatalf("replicated conflict: %+v", resp.body)
	}
	resp = send(t, h, "GET", "/act_dst/gone", nil, testAdminAuth, admin)
	if resp.status != 404 || resp.body["reason"] != "deleted" {
		t.Fatalf("replicated tombstone: %+v", resp)
	}
	resp = send(t, h, "GET", "/act_dst/withatt?attachments=true", nil, testAdminAuth, admin)
	att := resp.body["_attachments"].(map[string]any)["f.txt"].(map[string]any)
	if att["data"] != "aGk=" {
		t.Fatalf("replicated attachment: %+v", att)
	}

	// Checkpoints landed on both ends. Re-running writes nothing new.
	resp = send(t, h, "GET", "/act_dst/_local_docs", nil, testAdminAuth, admin)
	if int(resp.body["total_rows"].(float64)) != 1 {
		t.Fatalf("target checkpoint: %+v", resp.body)
	}
	resp = send(t, h, "POST", "/_replicate", decode(t,
		`{"source":"act_src","target":"act_dst"}`), testAdminAuth, admin)
	history = resp.body["history"].([]any)[0].(map[string]any)
	if history["docs_written"].(float64) != 0 {
		t.Fatalf("re-run should be a no-op: %+v", history)
	}
}

func TestReplicateContinuousAndCancel(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	for _, db := range []string{"cont_src", "cont_dst"} {
		send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	send(t, h, "PUT", "/cont_src", nil, testAdminAuth, admin)
	defer func() {
		for _, db := range []string{"cont_src", "cont_dst"} {
			send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()

	body := `{"source":"cont_src","target":"cont_dst","create_target":true,"continuous":true}`
	resp := send(t, h, "POST", "/_replicate", decode(t, body), testAdminAuth, admin)
	if resp.status != 202 || resp.body["_local_id"] == nil {
		t.Fatalf("continuous start: %+v", resp)
	}

	// A write to the source shows up on the target without another call.
	send(t, h, "PUT", "/cont_src/live", decode(t, `{"n":1}`), testAdminAuth, admin)
	waitFor(t, "continuous propagation", 10*time.Second, func() bool {
		return send(t, h, "GET", "/cont_dst/live", nil, testAdminAuth, admin).status == 200
	})

	// The job is visible while running.
	resp = send(t, h, "GET", "/_scheduler/jobs", nil, testAdminAuth, admin)
	if int(resp.body["total_rows"].(float64)) != 1 {
		t.Fatalf("scheduler jobs: %+v", resp.body)
	}
	resp = send(t, h, "GET", "/_active_tasks", nil, testAdminAuth, admin)
	task := resp.array[0].(map[string]any)
	if task["type"] != "replication" || task["continuous"] != true {
		t.Fatalf("active task: %+v", task)
	}

	// Cancel stops it.
	cancel := `{"source":"cont_src","target":"cont_dst","create_target":true,"continuous":true,"cancel":true}`
	resp = send(t, h, "POST", "/_replicate", decode(t, cancel), testAdminAuth, admin)
	if resp.status != 200 || resp.body["ok"] != true {
		t.Fatalf("cancel: %+v", resp)
	}
	waitFor(t, "job cancellation", 10*time.Second, func() bool {
		resp := send(t, h, "GET", "/_scheduler/jobs", nil, testAdminAuth, admin)
		return int(resp.body["total_rows"].(float64)) == 0
	})
}

func TestReplicatorDBLifecycle(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	for _, db := range []string{"repdoc_src", "repdoc_dst"} {
		send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	send(t, h, "DELETE", "/_replicator/byddoc", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/repdoc_src", nil, testAdminAuth, admin)
	send(t, h, "PUT", "/repdoc_src/payload", decode(t, `{"data":42}`), testAdminAuth, admin)
	defer func() {
		for _, db := range []string{"repdoc_src", "repdoc_dst"} {
			send(t, h, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()

	// A _replicator document triggers the job and records its lifecycle.
	resp := send(t, h, "PUT", "/_replicator/byddoc", decode(t,
		`{"source":"repdoc_src","target":"repdoc_dst","create_target":true}`),
		testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("replicator doc: %+v", resp)
	}

	waitFor(t, "replication completion", 15*time.Second, func() bool {
		resp := send(t, h, "GET", "/_replicator/byddoc", nil, testAdminAuth, admin)
		state, _ := resp.body["_replication_state"].(string)
		return state == "completed"
	})
	resp = send(t, h, "GET", "/_replicator/byddoc", nil, testAdminAuth, admin)
	if resp.body["_replication_id"] == nil || resp.body["_replication_state_time"] == nil {
		t.Fatalf("lifecycle members: %+v", resp.body)
	}
	if resp := send(t, h, "GET", "/repdoc_dst/payload", nil, testAdminAuth, admin); resp.status != 200 {
		t.Fatalf("doc-driven replication result: %+v", resp)
	}

	// _scheduler/docs reports the doc with its state.
	resp = send(t, h, "GET", "/_scheduler/docs", nil, testAdminAuth, admin)
	docs := resp.body["docs"].([]any)
	found := false
	for _, raw := range docs {
		doc := raw.(map[string]any)
		if doc["doc_id"] == "byddoc" && doc["state"] == "completed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("scheduler docs: %+v", docs)
	}

	// Cleanup the replicator doc.
	resp = send(t, h, "GET", "/_replicator/byddoc", nil, testAdminAuth, admin)
	rev := resp.body["_rev"].(string)
	send(t, h, "DELETE", "/_replicator/byddoc?rev="+rev, nil, testAdminAuth, admin)
}

func TestReplicatorWorkerSingletonAcrossServers(t *testing.T) {
	stA := testHTTPStore(t)
	stB := testHTTPStore(t)
	serverA := testServer(t, stA)
	serverB := testServer(t, stB)

	listenerA := httptest.NewServer(serverA)
	t.Cleanup(listenerA.Close)
	serverA.SetSelfURL(listenerA.URL)
	listenerB := httptest.NewServer(serverB)
	t.Cleanup(listenerB.Close)
	serverB.SetSelfURL(listenerB.URL)

	hA := http.Handler(serverA)
	hB := http.Handler(serverB)
	admin := adminAuth()
	for _, db := range []string{"singleton_src", "singleton_dst"} {
		send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin)
	}
	deleteDocIfExists(t, hA, "/_replicator/singleton")
	if resp := send(t, hA, "PUT", "/singleton_src", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create source: %+v", resp)
	}
	defer func() {
		deleteDocIfExists(t, hA, "/_replicator/singleton")
		for _, db := range []string{"singleton_src", "singleton_dst"} {
			send(t, hA, "DELETE", "/"+db, nil, testAdminAuth, admin)
		}
	}()

	workerCtxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	workerCtxB, cancelB := context.WithCancel(t.Context())
	defer cancelB()
	serverA.StartReplicatorWorker(workerCtxA)
	serverB.StartReplicatorWorker(workerCtxB)

	resp := send(t, hA, "PUT", "/_replicator/singleton", decode(t,
		`{"source":"singleton_src","target":"singleton_dst","create_target":true,"continuous":true}`),
		testAdminAuth, admin)
	if resp.status != 201 {
		t.Fatalf("replicator doc: %+v", resp)
	}

	jobCount := func(h http.Handler) int {
		resp := send(t, h, "GET", "/_scheduler/jobs", nil, testAdminAuth, admin)
		if resp.status != 200 {
			t.Fatalf("_scheduler/jobs: %+v", resp)
		}
		return int(resp.body["total_rows"].(float64))
	}
	activeCount := func(h http.Handler) int {
		resp := send(t, h, "GET", "/_active_tasks", nil, testAdminAuth, admin)
		if resp.status != 200 {
			t.Fatalf("_active_tasks: %+v", resp)
		}
		return len(resp.array)
	}

	holder := ""
	waitFor(t, "one singleton worker", 15*time.Second, func() bool {
		a, b := jobCount(hA), jobCount(hB)
		if a+b > 1 {
			t.Fatalf("duplicate _replicator jobs: serverA=%d serverB=%d", a, b)
		}
		switch {
		case a == 1 && b == 0:
			holder = "A"
			return true
		case a == 0 && b == 1:
			holder = "B"
			return true
		default:
			return false
		}
	})
	if send(t, hA, "GET", "/", nil).status != 200 || send(t, hB, "GET", "/", nil).status != 200 {
		t.Fatal("both servers should keep serving HTTP")
	}
	if holder == "A" && activeCount(hB) != 0 {
		t.Fatal("non-holder server B reported active replication work")
	}
	if holder == "B" && activeCount(hA) != 0 {
		t.Fatal("non-holder server A reported active replication work")
	}

	send(t, hA, "PUT", "/singleton_src/first", decode(t, `{"n":1}`), testAdminAuth, admin)
	waitFor(t, "first singleton replication", 15*time.Second, func() bool {
		return send(t, hA, "GET", "/singleton_dst/first", nil, testAdminAuth, admin).status == 200
	})

	if holder == "A" {
		cancelA()
	} else {
		cancelB()
	}
	waitFor(t, "standby worker takeover", 15*time.Second, func() bool {
		a, b := jobCount(hA), jobCount(hB)
		if a+b > 1 {
			t.Fatalf("duplicate _replicator jobs after failover: serverA=%d serverB=%d", a, b)
		}
		if holder == "A" {
			return a == 0 && b == 1
		}
		return a == 1 && b == 0
	})

	send(t, hA, "PUT", "/singleton_src/second", decode(t, `{"n":2}`), testAdminAuth, admin)
	waitFor(t, "post-failover singleton replication", 15*time.Second, func() bool {
		return send(t, hA, "GET", "/singleton_dst/second", nil, testAdminAuth, admin).status == 200
	})
}

func TestDBUpdatesFeed(t *testing.T) {
	h := testLiveServer(t)
	admin := adminAuth()
	send(t, h, "DELETE", "/updates_probe", nil, testAdminAuth, admin)

	resp := send(t, h, "GET", "/_db_updates", nil, testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("_db_updates: %+v", resp)
	}
	before := len(resp.body["results"].([]any))

	send(t, h, "PUT", "/updates_probe", nil, testAdminAuth, admin)
	defer send(t, h, "DELETE", "/updates_probe", nil, testAdminAuth, admin)

	resp = send(t, h, "GET", "/_db_updates", nil, testAdminAuth, admin)
	results := resp.body["results"].([]any)
	if len(results) <= before {
		t.Fatalf("created event missing: %d -> %d", before, len(results))
	}
	last := results[len(results)-1].(map[string]any)
	if last["db_name"] != "updates_probe" || last["type"] != "created" {
		t.Fatalf("created event: %+v", last)
	}
	if resp := send(t, h, "GET", "/_db_updates", nil); resp.status != 401 {
		t.Fatalf("anonymous _db_updates: %+v", resp)
	}
}
