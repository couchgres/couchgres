// Package jsengine runs design-document JavaScript (map, reduce, filters,
// validate_doc_update) on a fixed pool of QuickJS virtual machines. It uses
// the same engine CouchDB 3.5 ships, as a pure-Go translation
// (modernc.org/quickjs). Each worker goroutine owns one VM per design-doc
// signature. A wall-clock timeout interrupts runaway scripts. Unlike V8
// isolates, an interrupted VM stays usable.
package jsengine

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/quickjs"
)

// Emit is one emitted view row with raw JSON key and value.
type Emit struct {
	Key   json.RawMessage
	Value json.RawMessage
}

// ReduceGroup is one group's input to a reduce function.
type ReduceGroup struct {
	Keys   []json.RawMessage // [key, docid] pairs. Nil when rereducing.
	Values []json.RawMessage
}

// ValidationError is a validate_doc_update rejection.
type ValidationError struct {
	Kind    string // "forbidden" or "unauthorized"
	Message string
}

func (e *ValidationError) Error() string { return e.Kind + ": " + e.Message }

const (
	defaultMaxWorkers           = 4
	defaultMaxCachedContexts    = 4
	defaultMaxCachedSourceBytes = 8 << 20
	defaultMaxMemoryBytes       = 64 << 20
	defaultMaxOutputBytes       = 16 << 20
	defaultMaxEmitRows          = 100_000
	outputLimitMarker           = "__couchgres_output_limit__"
)

var (
	// ErrTimeout reports a script exceeding the wall-clock limit.
	ErrTimeout = errors.New("JavaScript execution timed out")
	// ErrClosed reports work submitted after pool shutdown has begun.
	ErrClosed = errors.New("JavaScript pool is closed")
	// ErrMemoryLimit reports a script exhausting its QuickJS heap budget.
	ErrMemoryLimit = errors.New("JavaScript memory limit exceeded")
	// ErrOutputLimit reports a script producing too many bytes or emitted rows.
	ErrOutputLimit = errors.New("JavaScript output limit exceeded")
)

// Limits bounds persistent worker state and the work produced by one call.
// Zero values select the secure defaults used by NewPool.
type Limits struct {
	MaxCachedContexts    int
	MaxCachedSourceBytes int
	MaxMemoryBytes       int
	MaxOutputBytes       int
	MaxEmitRows          int
}

func (l Limits) withDefaults() Limits {
	l.MaxCachedContexts = secureLimit(l.MaxCachedContexts, defaultMaxCachedContexts)
	l.MaxCachedSourceBytes = secureLimit(l.MaxCachedSourceBytes, defaultMaxCachedSourceBytes)
	l.MaxMemoryBytes = secureLimit(l.MaxMemoryBytes, defaultMaxMemoryBytes)
	l.MaxOutputBytes = secureLimit(l.MaxOutputBytes, defaultMaxOutputBytes)
	l.MaxEmitRows = secureLimit(l.MaxEmitRows, defaultMaxEmitRows)
	return l
}

func secureLimit(value, ceiling int) int {
	if value <= 0 || value > ceiling {
		return ceiling
	}
	return value
}

// Stats is a point-in-time snapshot of JavaScript pool activity.
type Stats struct {
	Workers           int
	Queued            int64
	Active            int64
	Calls             uint64
	CachedContexts    int64
	CachedSourceBytes int64
	CacheHits         uint64
	CacheMisses       uint64
	CacheEvictions    uint64
	Timeouts          uint64
	MemoryLimits      uint64
	OutputLimits      uint64
}

type poolStats struct {
	queued            atomic.Int64
	active            atomic.Int64
	calls             atomic.Uint64
	cachedContexts    atomic.Int64
	cachedSourceBytes atomic.Int64
	cacheHits         atomic.Uint64
	cacheMisses       atomic.Uint64
	cacheEvictions    atomic.Uint64
	timeouts          atomic.Uint64
	memoryLimits      atomic.Uint64
	outputLimits      atomic.Uint64
}

type Pool struct {
	jobs       chan func(*worker)
	wg         sync.WaitGroup
	timeout    time.Duration
	workers    int
	limits     Limits
	stats      poolStats
	stateMu    sync.Mutex
	closing    bool
	submitters sync.WaitGroup
	closeOnce  sync.Once
	stopping   chan struct{}
	stopped    chan struct{}
}

// NewPool starts size workers, capped at four. Zero uses the smaller of
// GOMAXPROCS and four so the aggregate VM-memory ceiling remains bounded on
// large hosts. timeout bounds each script call and is analogous to CouchDB's
// os_process_timeout. Zero defaults to 5s.
func NewPool(size int, timeout time.Duration) *Pool {
	return NewPoolWithLimits(size, timeout, Limits{})
}

// NewPoolWithLimits is NewPool with explicit resource limits. The hard defaults
// remain ceilings; focused tests and embedded users can only select tighter
// budgets.
func NewPoolWithLimits(size int, timeout time.Duration, limits Limits) *Pool {
	if size <= 0 {
		size = min(runtime.GOMAXPROCS(0), defaultMaxWorkers)
	} else {
		size = min(size, defaultMaxWorkers)
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	limits = limits.withDefaults()
	p := &Pool{
		jobs:     make(chan func(*worker)),
		timeout:  timeout,
		workers:  size,
		limits:   limits,
		stopping: make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			w := &worker{
				timeout:  timeout,
				limits:   limits,
				stats:    &p.stats,
				contexts: make(map[string]*workerContext),
			}
			defer w.dispose()
			for job := range p.jobs {
				job(w)
			}
		}()
	}
	return p
}

// Stats returns current gauges and cumulative counters for this pool.
func (p *Pool) Stats() Stats {
	return Stats{
		Workers:           p.workers,
		Queued:            p.stats.queued.Load(),
		Active:            p.stats.active.Load(),
		Calls:             p.stats.calls.Load(),
		CachedContexts:    p.stats.cachedContexts.Load(),
		CachedSourceBytes: p.stats.cachedSourceBytes.Load(),
		CacheHits:         p.stats.cacheHits.Load(),
		CacheMisses:       p.stats.cacheMisses.Load(),
		CacheEvictions:    p.stats.cacheEvictions.Load(),
		Timeouts:          p.stats.timeouts.Load(),
		MemoryLimits:      p.stats.memoryLimits.Load(),
		OutputLimits:      p.stats.outputLimits.Load(),
	}
}

// Close stops accepting work, waits for admitted submissions and active jobs,
// and then frees the VMs. It is safe to call concurrently and repeatedly.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		p.stateMu.Lock()
		p.closing = true
		close(p.stopping)
		p.stateMu.Unlock()

		// No submitter can be added after closing becomes true. Waiting here
		// therefore guarantees that closing jobs cannot race a channel send.
		p.submitters.Wait()
		close(p.jobs)
		p.wg.Wait()
		close(p.stopped)
	})
	<-p.stopped
}

func (p *Pool) beginSubmit() bool {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.closing {
		return false
	}
	p.submitters.Add(1)
	return true
}

// exec runs fn on a worker and respects ctx while waiting for a free one and
// while the job runs. Cancellation interrupts the active QuickJS evaluation
// so the worker is freed promptly instead of running until the pool timeout.
func (p *Pool) exec(ctx context.Context, fn func(*worker) error) error {
	if !p.beginSubmit() {
		return ErrClosed
	}
	defer p.submitters.Done()

	done := make(chan error, 1)
	job := func(w *worker) {
		p.stats.queued.Add(-1)
		if err := ctx.Err(); err != nil {
			done <- err
			return
		}
		p.stats.calls.Add(1)
		p.stats.active.Add(1)
		defer p.stats.active.Add(-1)
		done <- fn(w)
	}
	p.stats.queued.Add(1)
	select {
	case p.jobs <- job:
	case <-p.stopping:
		p.stats.queued.Add(-1)
		return ErrClosed
	case <-ctx.Done():
		p.stats.queued.Add(-1)
		return ctx.Err()
	}
	select {
	case err := <-done:
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MapDocs folds docs through the design doc's map functions. The result is
// indexed [doc][function][emit row]. sig keys the per-worker context cache.
// fns and lib describe the design doc (lib is the require() tree).
func (p *Pool) MapDocs(ctx context.Context, sig string, fns []string, lib map[string]any, docs []json.RawMessage) ([][][]Emit, error) {
	payload, err := json.Marshal(docs)
	if err != nil {
		return nil, err
	}
	var raw [][][][2]json.RawMessage
	err = p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, fns, lib, 0)
		if err != nil {
			return err
		}
		defer release()
		out, err := w.call(ctx, c, string(payload), "__map")
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(out), &raw)
	})
	if err != nil {
		return nil, err
	}
	result := make([][][]Emit, len(raw))
	for d, perView := range raw {
		result[d] = make([][]Emit, len(perView))
		for v, rows := range perView {
			emits := make([]Emit, len(rows))
			for i, row := range rows {
				emits[i] = Emit{Key: row[0], Value: row[1]}
			}
			result[d][v] = emits
		}
	}
	return result, nil
}

// ReduceGroups runs one reduce function over many groups in a single call,
// returning one value per group.
func (p *Pool) ReduceGroups(ctx context.Context, sig, fn string, groups []ReduceGroup, rereduce bool) ([]json.RawMessage, error) {
	type groupJSON struct {
		Keys   []json.RawMessage `json:"keys"`
		Values []json.RawMessage `json:"values"`
	}
	gj := make([]groupJSON, len(groups))
	for i, g := range groups {
		gj[i] = groupJSON(g)
	}
	payload, err := json.Marshal(map[string]any{
		"src": fn, "groups": gj, "rereduce": rereduce,
	})
	if err != nil {
		return nil, err
	}
	var out []json.RawMessage
	err = p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, nil, nil, len(fn))
		if err != nil {
			return err
		}
		defer release()
		res, err := w.call(ctx, c, string(payload), "__reduce")
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(res), &out)
	})
	if err != nil {
		return nil, err
	}
	if len(out) != len(groups) {
		return nil, fmt.Errorf("reduce returned %d values for %d groups", len(out), len(groups))
	}
	return out, nil
}

// FilterDocs runs a changes filter over docs. req is the CouchDB request
// object the filter receives as its second argument.
func (p *Pool) FilterDocs(ctx context.Context, sig, fn string, docs []json.RawMessage, req, ddoc json.RawMessage) ([]bool, error) {
	if req == nil {
		req = json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(map[string]any{
		"src": fn, "docs": docs, "req": req, "ddoc": ddoc,
	})
	if err != nil {
		return nil, err
	}
	var out []bool
	err = p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, nil, nil, len(fn)+len(ddoc))
		if err != nil {
			return err
		}
		defer release()
		res, err := w.call(ctx, c, string(payload), "__filter")
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(res), &out)
	})
	if err != nil {
		return nil, err
	}
	if len(out) != len(docs) {
		return nil, fmt.Errorf("filter returned %d results for %d docs", len(out), len(docs))
	}
	return out, nil
}

// DDocCall runs a plain design-doc function (update, function-form
// rewrite) with the given arguments, returning its JSON result.
func (p *Pool) DDocCall(ctx context.Context, sig, fn string, args []json.RawMessage, ddoc json.RawMessage) (json.RawMessage, error) {
	payload, err := json.Marshal(map[string]any{"src": fn, "args": args, "ddoc": ddoc})
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	err = p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, nil, nil, len(fn)+len(ddoc))
		if err != nil {
			return err
		}
		defer release()
		res, err := w.call(ctx, c, string(payload), "__ddocfn")
		if err != nil {
			return err
		}
		out = json.RawMessage(res)
		return nil
	})
	return out, err
}

// RenderError is a structured error a show/list raised (bad format
// option, no acceptable provider).
type RenderError struct {
	Code   int    `json:"code"`
	Name   string `json:"error"`
	Reason string `json:"reason"`
}

func (e *RenderError) Error() string { return e.Name + ": " + e.Reason }

// RenderResult is a show or list function's output: the assembled
// response object and the provider-negotiated content type ("" when no
// provides() ran).
type RenderResult struct {
	Resp        json.RawMessage `json:"resp"`
	ContentType string          `json:"ct"`
	Err         *RenderError    `json:"err"`
}

// Show runs a show function (send/start/provides supported).
func (p *Pool) Show(ctx context.Context, sig, fn string, doc, req, ddoc json.RawMessage) (*RenderResult, error) {
	payload, err := json.Marshal(map[string]any{"src": fn, "doc": doc, "req": req, "ddoc": ddoc})
	if err != nil {
		return nil, err
	}
	return p.render(ctx, sig, payload, "__show", len(fn)+len(ddoc))
}

// List runs a list function over pre-materialized view rows.
func (p *Pool) List(ctx context.Context, sig, fn string, head, req, ddoc json.RawMessage, rows []json.RawMessage) (*RenderResult, error) {
	if rows == nil {
		rows = []json.RawMessage{}
	}
	payload, err := json.Marshal(map[string]any{
		"src": fn, "head": head, "req": req, "rows": rows, "ddoc": ddoc,
	})
	if err != nil {
		return nil, err
	}
	return p.render(ctx, sig, payload, "__list", len(fn)+len(ddoc))
}

func (p *Pool) render(ctx context.Context, sig string, payload []byte, fn string, sourceBytes int) (*RenderResult, error) {
	var out RenderResult
	err := p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, nil, nil, sourceBytes)
		if err != nil {
			return err
		}
		defer release()
		res, err := w.call(ctx, c, string(payload), fn)
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(res), &out)
	})
	if err != nil {
		return nil, err
	}
	if out.Err != nil {
		return nil, out.Err
	}
	return &out, nil
}

// Validate runs a validate_doc_update function. A rejection comes back as
// *ValidationError. Nil means the write is allowed.
func (p *Pool) Validate(ctx context.Context, sig, fn string, newDoc, oldDoc, userCtx, secObj, ddoc json.RawMessage) error {
	if oldDoc == nil {
		oldDoc = json.RawMessage(`null`)
	}
	payload, err := json.Marshal(map[string]any{
		"src": fn, "newDoc": newDoc, "oldDoc": oldDoc,
		"userCtx": userCtx, "secObj": secObj, "ddoc": ddoc,
	})
	if err != nil {
		return err
	}
	return p.exec(ctx, func(w *worker) error {
		c, release, err := w.context(ctx, sig, nil, nil, len(fn)+len(ddoc))
		if err != nil {
			return err
		}
		defer release()
		res, err := w.call(ctx, c, string(payload), "__validate")
		if err != nil {
			return err
		}
		var verdict struct {
			Forbidden    *string `json:"forbidden"`
			Unauthorized *string `json:"unauthorized"`
		}
		if err := json.Unmarshal([]byte(res), &verdict); err != nil {
			return err
		}
		if verdict.Unauthorized != nil {
			return &ValidationError{Kind: "unauthorized", Message: *verdict.Unauthorized}
		}
		if verdict.Forbidden != nil {
			return &ValidationError{Kind: "forbidden", Message: *verdict.Forbidden}
		}
		return nil
	})
}

// worker owns one VM per design-doc signature. All methods run on the
// worker's goroutine only (VMs are not concurrency-safe).
type worker struct {
	contexts          map[string]*workerContext
	lru               list.List
	cachedSourceBytes int
	timeout           time.Duration
	limits            Limits
	stats             *poolStats
}

// workerContext is a cached per-signature VM. Contexts are created by
// whichever call touches the signature first. A filter or validate call
// makes one without the view functions. Whether views are installed is
// tracked separately and installation happens when a map call needs it.
type workerContext struct {
	sig            string
	vm             *quickjs.VM
	viewsInstalled bool
	sourceBytes    int
	cached         bool
	closed         bool
	lruElement     *list.Element
}

func (w *worker) dispose() {
	for w.lru.Len() > 0 {
		w.closeContext(w.lru.Back().Value.(*workerContext))
	}
	w.contexts = nil
}

// context returns the cached VM for sig, creating it on first use and
// installing the ddoc's view functions the first time a caller passes them.
// The returned release closes contexts that are deliberately not cached
// because their source is larger than the cache's byte budget.
func (w *worker) context(
	ctx context.Context,
	sig string,
	fns []string,
	lib map[string]any,
	extraSourceBytes int,
) (*workerContext, func(), error) {
	entry, hit := w.contexts[sig]
	installViews := (len(fns) > 0 || lib != nil) && (!hit || !entry.viewsInstalled)
	var installPayload []byte
	if installViews {
		var err error
		installPayload, err = json.Marshal(map[string]any{"views": fns, "lib": lib})
		if err != nil {
			return nil, nil, err
		}
	}
	sourceBytes := max(1, len(sig)+extraSourceBytes+len(installPayload))

	if hit {
		w.stats.cacheHits.Add(1)
		w.lru.MoveToFront(entry.lruElement)
		desiredBytes := max(entry.sourceBytes, sourceBytes)
		if desiredBytes > entry.sourceBytes {
			delta := desiredBytes - entry.sourceBytes
			if desiredBytes > w.limits.MaxCachedSourceBytes ||
				!w.makeRoom(entry, 0, delta) {
				// Keep using the VM for this call, but stop retaining it.
				w.stats.cacheEvictions.Add(1)
				w.detachContext(entry)
			} else {
				entry.sourceBytes = desiredBytes
				w.cachedSourceBytes += delta
				w.stats.cachedSourceBytes.Add(int64(delta))
			}
		}
	} else {
		w.stats.cacheMisses.Add(1)
		cache := sourceBytes <= w.limits.MaxCachedSourceBytes &&
			w.makeRoom(nil, 1, sourceBytes)
		var err error
		entry, err = w.newContext(sig, sourceBytes)
		if err != nil {
			return nil, nil, err
		}
		if cache {
			w.cacheContext(entry)
		}
	}

	if installViews {
		if _, err := w.call(ctx, entry, string(installPayload), "__install"); err != nil {
			// The VM may hold half-installed state. Drop it so the next
			// call starts clean instead of serving empty maps.
			w.closeContext(entry)
			return nil, nil, fmt.Errorf("compiling design doc functions: %w", err)
		}
		entry.viewsInstalled = true
	}
	return entry, func() {
		if !entry.cached {
			w.closeContext(entry)
		}
	}, nil
}

func (w *worker) newContext(sig string, sourceBytes int) (*workerContext, error) {
	vm, err := quickjs.NewVM()
	if err != nil {
		return nil, err
	}
	entry := &workerContext{sig: sig, vm: vm, sourceBytes: sourceBytes}
	vm.SetMemoryLimit(uintptr(w.limits.MaxMemoryBytes))
	vm.SetGCThreshold(uintptr(w.limits.MaxMemoryBytes / 2))
	if err := vm.SetEvalTimeout(w.timeout); err != nil {
		w.closeContext(entry)
		return nil, err
	}
	if _, err := vm.Eval(runtimeJS, quickjs.EvalGlobal); err != nil {
		mapped := w.vmError(context.Background(), entry, err)
		w.closeContext(entry)
		return nil, fmt.Errorf("installing view-server runtime: %w", mapped)
	}
	if _, err := vm.Call("__configureCouchgresRuntime", w.limits.MaxEmitRows); err != nil {
		mapped := w.vmError(context.Background(), entry, err)
		w.closeContext(entry)
		return nil, fmt.Errorf("configuring view-server runtime: %w", mapped)
	}
	if _, err := vm.Eval("delete globalThis.__configureCouchgresRuntime", quickjs.EvalGlobal); err != nil {
		mapped := w.vmError(context.Background(), entry, err)
		w.closeContext(entry)
		return nil, fmt.Errorf("sealing view-server runtime: %w", mapped)
	}
	return entry, nil
}

func (w *worker) cacheContext(entry *workerContext) {
	entry.cached = true
	entry.lruElement = w.lru.PushFront(entry)
	w.contexts[entry.sig] = entry
	w.cachedSourceBytes += entry.sourceBytes
	w.stats.cachedContexts.Add(1)
	w.stats.cachedSourceBytes.Add(int64(entry.sourceBytes))
}

func (w *worker) detachContext(entry *workerContext) {
	if !entry.cached {
		return
	}
	delete(w.contexts, entry.sig)
	w.lru.Remove(entry.lruElement)
	entry.lruElement = nil
	entry.cached = false
	w.cachedSourceBytes -= entry.sourceBytes
	w.stats.cachedContexts.Add(-1)
	w.stats.cachedSourceBytes.Add(-int64(entry.sourceBytes))
}

func (w *worker) closeContext(entry *workerContext) {
	if entry == nil || entry.closed {
		return
	}
	w.detachContext(entry)
	_ = entry.vm.Close()
	entry.closed = true
}

// makeRoom evicts least-recently-used contexts until adding the requested
// entry/source bytes fits. keep is the current context while its charge grows.
func (w *worker) makeRoom(keep *workerContext, entries, sourceBytes int) bool {
	for len(w.contexts)+entries > w.limits.MaxCachedContexts ||
		w.cachedSourceBytes+sourceBytes > w.limits.MaxCachedSourceBytes {
		candidate := w.lru.Back()
		if candidate != nil && candidate.Value.(*workerContext) == keep {
			candidate = candidate.Prev()
		}
		if candidate == nil {
			return false
		}
		w.stats.cacheEvictions.Add(1)
		w.closeContext(candidate.Value.(*workerContext))
	}
	return true
}

// call invokes a runtime entry point (__map, __reduce, etc.) with the
// payload as its argument and returns the script's string result. A timed-out
// or cancelled call interrupts the VM, which stays usable afterward (no
// isolate rebuild, unlike V8).
func (w *worker) call(ctx context.Context, c *workerContext, payload, fn string) (string, error) {
	// modernc.org/quickjs only re-arms its eval timeout in Eval, not Call, so
	// run a no-op Eval before every Call on a VM that may have been idle.
	if _, err := c.vm.Eval("0", quickjs.EvalGlobal); err != nil {
		return "", w.vmError(ctx, c, err)
	}
	// Arm after re-arm so configureInterrupt cannot clear a pending cancel.
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			c.vm.Interrupt()
		case <-stop:
		}
	}()
	// Interrupt may still be running after stop closes. Join it before the VM
	// can be reused or freed, including vmError's memory-limit cleanup below.
	stopInterrupt := func() {
		close(stop)
		<-stopped
	}
	if err := ctx.Err(); err != nil {
		stopInterrupt()
		return "", err
	}
	out, err := c.vm.Call(fn, payload)
	stopInterrupt()
	if err != nil {
		return "", w.vmError(ctx, c, err)
	}
	s, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("runtime %s returned %T, want string", fn, out)
	}
	if len(s) > w.limits.MaxOutputBytes {
		w.stats.outputLimits.Add(1)
		return "", fmt.Errorf("%w: %d bytes exceeds %d", ErrOutputLimit,
			len(s), w.limits.MaxOutputBytes)
	}
	return s, nil
}

func (w *worker) vmError(ctx context.Context, entry *workerContext, err error) error {
	mapped := scriptError(err)
	switch {
	case errors.Is(mapped, ErrTimeout):
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.stats.timeouts.Add(1)
	case errors.Is(mapped, ErrMemoryLimit):
		w.stats.memoryLimits.Add(1)
		// An OOM can leave arbitrary persistent global state. Rebuild rather
		// than returning that VM to the cache.
		w.closeContext(entry)
	case errors.Is(mapped, ErrOutputLimit):
		w.stats.outputLimits.Add(1)
	}
	return mapped
}

// scriptError maps a QuickJS evaluation error. The interrupt handler's
// exception means the wall-clock timeout fired. Anything else is a script
// error surfaced with its message.
func scriptError(err error) error {
	msg := err.Error()
	if strings.Contains(msg, "InternalError: interrupted") {
		return ErrTimeout
	}
	if strings.Contains(msg, outputLimitMarker) {
		return fmt.Errorf("%w: emitted-row limit reached", ErrOutputLimit)
	}
	lower := strings.ToLower(msg)
	if msg == "null" || strings.Contains(lower, "out of memory") ||
		strings.Contains(lower, "string too long") {
		return fmt.Errorf("%w: QuickJS heap exhausted", ErrMemoryLimit)
	}
	return fmt.Errorf("JavaScript error: %s", msg)
}
