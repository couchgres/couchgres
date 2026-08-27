package jsengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func testPool(t *testing.T) *Pool {
	t.Helper()
	p := NewPool(2, 2*time.Second)
	t.Cleanup(p.Close)
	return p
}

func raw(s string) json.RawMessage { return json.RawMessage(s) }

func TestMapDocs(t *testing.T) {
	p := testPool(t)
	fns := []string{
		`function(doc) { emit(doc.name, doc.n); }`,
		`function(doc) { if (doc.n > 1) { emit([doc.name, doc.n], null); emit("extra", 1); } }`,
	}
	docs := []json.RawMessage{
		raw(`{"_id":"a","name":"alice","n":1}`),
		raw(`{"_id":"b","name":"bob","n":2}`),
	}
	out, err := p.MapDocs(context.Background(), "sig1", fns, nil, docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || len(out[0]) != 2 {
		t.Fatalf("shape: %+v", out)
	}
	if string(out[0][0][0].Key) != `"alice"` || string(out[0][0][0].Value) != `1` {
		t.Errorf("doc0 fn0: %+v", out[0][0])
	}
	if len(out[0][1]) != 0 {
		t.Errorf("doc0 fn1 should emit nothing: %+v", out[0][1])
	}
	if len(out[1][1]) != 2 || string(out[1][1][0].Key) != `["bob",2]` {
		t.Errorf("doc1 fn1: %+v", out[1][1])
	}
}

// A filter (or validate/show) call for a signature must not leave the
// worker with a cached context lacking the view functions: a later map
// call through the same context would silently emit nothing and build an
// empty index (found by CouchDB's Elixir suite running concurrently with
// the compatibility checker).
func TestMapAfterFilterSameSig(t *testing.T) {
	p := NewPool(1, 2*time.Second) // one worker: both calls share a context
	t.Cleanup(p.Close)
	docs := []json.RawMessage{raw(`{"_id":"a","n":1}`)}
	hits, err := p.FilterDocs(context.Background(), "shared-sig",
		`function(doc, req) { return true; }`, docs, nil, nil)
	if err != nil || len(hits) != 1 || !hits[0] {
		t.Fatalf("filter: %v %v", hits, err)
	}
	out, err := p.MapDocs(context.Background(), "shared-sig",
		[]string{`function(doc) { emit(doc._id, doc.n); }`}, nil, docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || len(out[0]) != 1 || len(out[0][0]) != 1 {
		t.Fatalf("map emitted nothing through the filter-created context: %+v", out)
	}
}

func TestMapErrorSkipsDoc(t *testing.T) {
	p := testPool(t)
	fns := []string{`function(doc) { if (doc.explode) throw new Error("boom"); emit(doc._id, null); }`}
	docs := []json.RawMessage{
		raw(`{"_id":"ok"}`),
		raw(`{"_id":"bad","explode":true}`),
		raw(`{"_id":"ok2"}`),
	}
	out, err := p.MapDocs(context.Background(), "sig2", fns, nil, docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out[0][0]) != 1 || len(out[1][0]) != 0 || len(out[2][0]) != 1 {
		t.Fatalf("throwing doc should be skipped: %+v", out)
	}
}

func TestMapUndefinedKeyBecomesNull(t *testing.T) {
	p := testPool(t)
	fns := []string{`function(doc) { emit(doc.missing, doc.alsoMissing); }`}
	out, err := p.MapDocs(context.Background(), "sig3", fns, nil,
		[]json.RawMessage{raw(`{"_id":"x"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0][0][0].Key) != `null` || string(out[0][0][0].Value) != `null` {
		t.Errorf("undefined should become null: %+v", out[0][0][0])
	}
}

func TestRequireViewsLib(t *testing.T) {
	p := testPool(t)
	lib := map[string]any{
		"views": map[string]any{
			"lib": map[string]any{
				"fmt":   `exports.shout = function(s) { return s.toUpperCase(); };`,
				"chain": `var f = require('views/lib/fmt'); exports.go = function(s){ return f.shout(s) + '!'; };`,
			},
		},
	}
	fns := []string{`function(doc) { var c = require('views/lib/chain'); emit(c.go(doc.name), null); }`}
	out, err := p.MapDocs(context.Background(), "sig4", fns, lib,
		[]json.RawMessage{raw(`{"name":"quiet"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0][0][0].Key) != `"QUIET!"` {
		t.Errorf("require chain: %s", out[0][0][0].Key)
	}
}

func TestReduceGroups(t *testing.T) {
	p := testPool(t)
	fn := `function(keys, values, rereduce) { return sum(values); }`
	groups := []ReduceGroup{
		{Keys: []json.RawMessage{raw(`["a","d1"]`), raw(`["a","d2"]`)}, Values: []json.RawMessage{raw(`1`), raw(`2`)}},
		{Keys: []json.RawMessage{raw(`["b","d3"]`)}, Values: []json.RawMessage{raw(`10`)}},
	}
	out, err := p.ReduceGroups(context.Background(), "sig5", fn, groups, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0]) != `3` || string(out[1]) != `10` {
		t.Errorf("reduce: %s %s", out[0], out[1])
	}

	// Rereduce path.
	out, err = p.ReduceGroups(context.Background(), "sig5", fn,
		[]ReduceGroup{{Values: []json.RawMessage{raw(`3`), raw(`10`)}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0]) != `13` {
		t.Errorf("rereduce: %s", out[0])
	}
}

func TestReduceErrorSurfaces(t *testing.T) {
	p := testPool(t)
	_, err := p.ReduceGroups(context.Background(), "sig6",
		`function() { throw new Error("reduce broke"); }`,
		[]ReduceGroup{{Values: []json.RawMessage{raw(`1`)}}}, false)
	if err == nil {
		t.Fatal("reduce error should surface")
	}
}

func TestFilterDocs(t *testing.T) {
	p := testPool(t)
	fn := `function(doc, req) { return doc.type === req.query.want; }`
	docs := []json.RawMessage{
		raw(`{"_id":"a","type":"post"}`),
		raw(`{"_id":"b","type":"comment"}`),
	}
	req := raw(`{"query":{"want":"post"}}`)
	out, err := p.FilterDocs(context.Background(), "sig7", fn, docs, req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !out[0] || out[1] {
		t.Errorf("filter: %v", out)
	}
}

func TestValidate(t *testing.T) {
	p := testPool(t)
	fn := `function(newDoc, oldDoc, userCtx, secObj) {
		if (newDoc.bad) throw({forbidden: "no bad docs"});
		if (userCtx.name !== "admin" && newDoc.adminOnly) throw({unauthorized: "admins only"});
	}`
	ctx := context.Background()
	userCtx := raw(`{"name":"bob","roles":[]}`)
	sec := raw(`{}`)

	if err := p.Validate(ctx, "sig8", fn, raw(`{"ok":true}`), nil, userCtx, sec, nil); err != nil {
		t.Fatalf("clean doc: %v", err)
	}
	err := p.Validate(ctx, "sig8", fn, raw(`{"bad":true}`), nil, userCtx, sec, nil)
	var verr *ValidationError
	if !errors.As(err, &verr) || verr.Kind != "forbidden" || verr.Message != "no bad docs" {
		t.Fatalf("forbidden: %v", err)
	}
	err = p.Validate(ctx, "sig8", fn, raw(`{"adminOnly":true}`), nil, userCtx, sec, nil)
	if !errors.As(err, &verr) || verr.Kind != "unauthorized" {
		t.Fatalf("unauthorized: %v", err)
	}
	// A plain throw maps to forbidden with the stringified error.
	err = p.Validate(ctx, "sig8", `function(){ throw "just no"; }`, raw(`{}`), nil, userCtx, sec, nil)
	if !errors.As(err, &verr) || verr.Kind != "forbidden" || verr.Message != "just no" {
		t.Fatalf("string throw: %v", err)
	}
}

func TestTimeoutInterruptsScript(t *testing.T) {
	p := NewPool(1, 200*time.Millisecond)
	defer p.Close()
	ctx := context.Background()
	_, err := p.MapDocs(ctx, "sigloop", []string{`function(doc) { while(true) {} }`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	// The worker must survive. The interrupted VM stays usable and the
	// same worker serves new signatures.
	out, err := p.MapDocs(ctx, "sigafter", []string{`function(doc) { emit(1, null); }`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if err != nil || len(out[0][0]) != 1 {
		t.Fatalf("pool did not recover: %v %+v", err, out)
	}
}

// Cancelling the caller's context must interrupt the running script so the
// worker is freed promptly instead of waiting for the pool's wall-clock timeout
// (which under load of many timed-out view builds would stall the pool).
func TestCancelInterruptsWorker(t *testing.T) {
	p := NewPool(1, 5*time.Second)
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := p.MapDocs(ctx, "sigcancel",
			[]string{`function(doc) { while(true) {} }`}, nil,
			[]json.RawMessage{raw(`{}`)})
		errCh <- err
	}()

	// Give the infinite loop time to start on the single worker.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MapDocs did not return after cancel")
	}

	// Without interrupt propagation the worker would stay busy until the 5s
	// timeout. A short deadline here fails the test if that still happens.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	out, err := p.MapDocs(ctx2, "sigaftercancel",
		[]string{`function(doc) { emit(1, null); }`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if err != nil || len(out[0][0]) != 1 {
		t.Fatalf("pool did not recover promptly after cancel: %v %+v", err, out)
	}
}

// A VM older than the timeout must still run busy (but finite) scripts.
// modernc.org/quickjs only re-arms its eval deadline in Eval, not Call. Without
// the pool's re-arm step, this call is spuriously interrupted as
// soon as the script runs long enough for the interrupt handler to fire
// (Found via CouchDB's Elixir suite: view builds failing on cached
// signature VMs created more than a timeout earlier).
func TestCallOnAgedVM(t *testing.T) {
	// The timeout must comfortably cover the busy loop even under -race
	// (which slows the translated interpreter considerably).
	p := NewPool(1, 2*time.Second)
	defer p.Close()
	fns := []string{`function(doc){ var s = 0; for (var i = 0; i < 200000; i++) s += i; emit(s, null); }`}
	docs := []json.RawMessage{raw(`{"_id":"a"}`)}
	if _, err := p.MapDocs(context.Background(), "sigaged", fns, nil, docs); err != nil {
		t.Fatalf("first map: %v", err)
	}
	time.Sleep(2500 * time.Millisecond) // age the cached VM past the timeout
	out, err := p.MapDocs(context.Background(), "sigaged", fns, nil, docs)
	if err != nil {
		t.Fatalf("map on aged VM: %v", err)
	}
	if len(out[0][0]) != 1 {
		t.Fatalf("aged VM emitted nothing: %+v", out)
	}
}

func TestCompileErrorSurfaces(t *testing.T) {
	p := testPool(t)
	_, err := p.MapDocs(context.Background(), "sigsyntax",
		[]string{`function(doc) { this is not javascript`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if err == nil {
		t.Fatal("syntax error should surface")
	}
}

func TestBigBatch(t *testing.T) {
	p := testPool(t)
	docs := make([]json.RawMessage, 500)
	for i := range docs {
		docs[i] = raw(`{"n":` + json.Number(itoa(i)).String() + `}`)
	}
	out, err := p.MapDocs(context.Background(), "sigbig",
		[]string{`function(doc) { emit(doc.n, doc.n * 2); }`}, nil, docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 500 || string(out[499][0][0].Value) != `998` {
		t.Fatalf("big batch: %d rows, last %s", len(out), out[499][0][0].Value)
	}
}

func TestResourceCeilingsCannotBeWidened(t *testing.T) {
	p := NewPoolWithLimits(100, 2*time.Second, Limits{
		MaxCachedContexts:    defaultMaxCachedContexts + 1,
		MaxCachedSourceBytes: defaultMaxCachedSourceBytes + 1,
		MaxMemoryBytes:       defaultMaxMemoryBytes + 1,
		MaxOutputBytes:       defaultMaxOutputBytes + 1,
		MaxEmitRows:          defaultMaxEmitRows + 1,
	})
	defer p.Close()
	if p.Stats().Workers != defaultMaxWorkers {
		t.Fatalf("worker ceiling widened: %+v", p.Stats())
	}
	if p.limits != (Limits{
		MaxCachedContexts:    defaultMaxCachedContexts,
		MaxCachedSourceBytes: defaultMaxCachedSourceBytes,
		MaxMemoryBytes:       defaultMaxMemoryBytes,
		MaxOutputBytes:       defaultMaxOutputBytes,
		MaxEmitRows:          defaultMaxEmitRows,
	}) {
		t.Fatalf("resource ceilings widened: %+v", p.limits)
	}
}

func TestContextCacheEvictsLeastRecentlyUsed(t *testing.T) {
	p := NewPoolWithLimits(1, 2*time.Second, Limits{MaxCachedContexts: 2})
	defer p.Close()
	doc := []json.RawMessage{raw(`{"_id":"a"}`)}
	mapSig := func(sig string) {
		t.Helper()
		if _, err := p.MapDocs(context.Background(), sig,
			[]string{`function(doc) { emit(doc._id, null); }`}, nil, doc); err != nil {
			t.Fatalf("map %s: %v", sig, err)
		}
	}

	mapSig("one")
	mapSig("two")
	mapSig("one") // make two the least-recently-used context
	mapSig("three")

	stats := p.Stats()
	if stats.CachedContexts != 2 || stats.CacheHits != 1 ||
		stats.CacheMisses != 3 || stats.CacheEvictions != 1 {
		t.Fatalf("cache stats after eviction: %+v", stats)
	}
	if err := p.exec(context.Background(), func(w *worker) error {
		_, hasOne := w.contexts["one"]
		_, hasTwo := w.contexts["two"]
		_, hasThree := w.contexts["three"]
		if !hasOne || hasTwo || !hasThree {
			return fmt.Errorf("wrong LRU survivors: one=%t two=%t three=%t",
				hasOne, hasTwo, hasThree)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOversizedSourceIsNotCached(t *testing.T) {
	p := NewPoolWithLimits(1, 2*time.Second, Limits{MaxCachedSourceBytes: 32})
	defer p.Close()
	fn := `function(doc) { emit(doc._id, "source larger than cache budget"); }`
	docs := []json.RawMessage{raw(`{"_id":"a"}`)}
	for i := 0; i < 2; i++ {
		if _, err := p.MapDocs(context.Background(), "large-source", []string{fn}, nil, docs); err != nil {
			t.Fatal(err)
		}
	}
	stats := p.Stats()
	if stats.CachedContexts != 0 || stats.CachedSourceBytes != 0 || stats.CacheMisses != 2 {
		t.Fatalf("oversized context was retained: %+v", stats)
	}
}

func TestEmitRowLimit(t *testing.T) {
	p := NewPoolWithLimits(1, 2*time.Second, Limits{MaxEmitRows: 2})
	defer p.Close()
	_, err := p.MapDocs(context.Background(), "too-many-emits",
		[]string{`function(doc) {
			__maxEmits = 1000000;
			__emitCount = 0;
			emit = function() {};
			emit(1, null); emit(2, null); emit(3, null);
		}`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("want ErrOutputLimit, got %v", err)
	}
	if stats := p.Stats(); stats.OutputLimits != 1 {
		t.Fatalf("output-limit stats: %+v", stats)
	}

	// A resource-limit exception must not poison the worker.
	out, err := p.MapDocs(context.Background(), "after-emit-limit",
		[]string{`function(doc) { emit(1, null); }`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if err != nil || len(out[0][0]) != 1 {
		t.Fatalf("pool did not recover after emit limit: %v %+v", err, out)
	}
}

func TestSerializedOutputLimitCoversReduce(t *testing.T) {
	p := NewPoolWithLimits(1, 2*time.Second, Limits{MaxOutputBytes: 64})
	defer p.Close()
	_, err := p.ReduceGroups(context.Background(), "large-reduce",
		`function(keys, values) { return Array(200).join("x"); }`,
		[]ReduceGroup{{Values: []json.RawMessage{raw(`1`)}}}, false)
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("want ErrOutputLimit, got %v", err)
	}
}

func TestQuickJSMemoryLimit(t *testing.T) {
	p := NewPoolWithLimits(1, 5*time.Second, Limits{MaxMemoryBytes: 2 << 20})
	defer p.Close()
	_, err := p.DDocCall(context.Background(), "memory-limit",
		`function(doc) {
			var values = [];
			while (true) values.push({n: values.length});
		}`, []json.RawMessage{raw(`{}`)}, nil)
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("want ErrMemoryLimit, got %v", err)
	}
	stats := p.Stats()
	if stats.MemoryLimits != 1 || stats.CachedContexts != 0 {
		t.Fatalf("memory-limit stats/context cleanup: %+v", stats)
	}

	out, err := p.MapDocs(context.Background(), "after-memory-limit",
		[]string{`function(doc) { emit(1, null); }`}, nil,
		[]json.RawMessage{raw(`{}`)})
	if err != nil || len(out[0][0]) != 1 {
		t.Fatalf("pool did not recover after memory limit: %v %+v", err, out)
	}
}

func itoa(n int) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}
