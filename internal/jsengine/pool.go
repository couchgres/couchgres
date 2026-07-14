// Package jsengine runs design-document JavaScript (map, reduce, filters,
// validate_doc_update) on a fixed pool of QuickJS virtual machines. It uses
// the same engine CouchDB 3.5 ships, as a pure-Go translation
// (modernc.org/quickjs). Each worker goroutine owns one VM per design-doc
// signature. A wall-clock timeout interrupts runaway scripts. Unlike V8
// isolates, an interrupted VM stays usable.
package jsengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
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

// ErrTimeout reports a script exceeding the wall-clock limit.
var ErrTimeout = errors.New("JavaScript execution timed out")

type Pool struct {
	jobs    chan func(*worker)
	wg      sync.WaitGroup
	timeout time.Duration
}

// NewPool starts size workers (0 = GOMAXPROCS). timeout bounds each script
// call. It is analogous to CouchDB's os_process_timeout. Zero defaults to 5s.
func NewPool(size int, timeout time.Duration) *Pool {
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	p := &Pool{
		jobs:    make(chan func(*worker)),
		timeout: timeout,
	}
	for i := 0; i < size; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			w := &worker{
				timeout:  timeout,
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

// Close stops the workers and frees the VMs.
func (p *Pool) Close() {
	close(p.jobs)
	p.wg.Wait()
}

// exec runs fn on a worker and respects ctx while waiting for a free one and
// while the job runs. Cancellation interrupts the active QuickJS evaluation
// so the worker is freed promptly instead of running until the pool timeout.
func (p *Pool) exec(ctx context.Context, fn func(*worker) error) error {
	done := make(chan error, 1)
	job := func(w *worker) {
		if err := ctx.Err(); err != nil {
			done <- err
			return
		}
		done <- fn(w)
	}
	select {
	case p.jobs <- job:
	case <-ctx.Done():
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
		c, err := w.context(ctx, sig, fns, lib)
		if err != nil {
			return err
		}
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
		gj[i] = groupJSON{Keys: g.Keys, Values: g.Values}
	}
	payload, err := json.Marshal(map[string]any{
		"src": fn, "groups": gj, "rereduce": rereduce,
	})
	if err != nil {
		return nil, err
	}
	var out []json.RawMessage
	err = p.exec(ctx, func(w *worker) error {
		c, err := w.context(ctx, sig, nil, nil)
		if err != nil {
			return err
		}
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
		c, err := w.context(ctx, sig, nil, nil)
		if err != nil {
			return err
		}
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
		c, err := w.context(ctx, sig, nil, nil)
		if err != nil {
			return err
		}
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
	return p.render(ctx, sig, payload, "__show")
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
	return p.render(ctx, sig, payload, "__list")
}

func (p *Pool) render(ctx context.Context, sig string, payload []byte, fn string) (*RenderResult, error) {
	var out RenderResult
	err := p.exec(ctx, func(w *worker) error {
		c, err := w.context(ctx, sig, nil, nil)
		if err != nil {
			return err
		}
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
		c, err := w.context(ctx, sig, nil, nil)
		if err != nil {
			return err
		}
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
	contexts map[string]*workerContext
	timeout  time.Duration
}

// workerContext is a cached per-signature VM. Contexts are created by
// whichever call touches the signature first. A filter or validate call
// makes one without the view functions. Whether views are installed is
// tracked separately and installation happens when a map call needs it.
type workerContext struct {
	vm             *quickjs.VM
	viewsInstalled bool
}

func (w *worker) dispose() {
	for _, c := range w.contexts {
		c.vm.Close()
	}
	w.contexts = nil
}

// context returns the cached VM for sig, creating it on first use and
// installing the ddoc's view functions the first time a caller passes them.
func (w *worker) context(ctx context.Context, sig string, fns []string, lib map[string]any) (*workerContext, error) {
	entry, ok := w.contexts[sig]
	if !ok {
		vm, err := quickjs.NewVM()
		if err != nil {
			return nil, err
		}
		if err := vm.SetEvalTimeout(w.timeout); err != nil {
			vm.Close()
			return nil, err
		}
		if _, err := vm.Eval(runtimeJS, quickjs.EvalGlobal); err != nil {
			vm.Close()
			return nil, fmt.Errorf("installing view-server runtime: %w", err)
		}
		entry = &workerContext{vm: vm}
		w.contexts[sig] = entry
	}
	if (len(fns) > 0 || lib != nil) && !entry.viewsInstalled {
		payload, err := json.Marshal(map[string]any{"views": fns, "lib": lib})
		if err != nil {
			return nil, err
		}
		if _, err := w.call(ctx, entry, string(payload), "__install"); err != nil {
			// The VM may hold half-installed state. Drop it so the next
			// call starts clean instead of serving empty maps.
			entry.vm.Close()
			delete(w.contexts, sig)
			return nil, fmt.Errorf("compiling design doc functions: %w", err)
		}
		entry.viewsInstalled = true
	}
	return entry, nil
}

// call invokes a runtime entry point (__map, __reduce, etc.) with the
// payload as its argument and returns the script's string result. A timed-out
// or cancelled call interrupts the VM, which stays usable afterward (no
// isolate rebuild, unlike V8).
func (w *worker) call(ctx context.Context, c *workerContext, payload, fn string) (string, error) {
	// modernc.org/quickjs only re-arms its eval timeout in Eval, not Call, so
	// run a no-op Eval before every Call on a VM that may have been idle.
	if _, err := c.vm.Eval("0", quickjs.EvalGlobal); err != nil {
		return "", scriptError(err)
	}
	// Arm after re-arm so configureInterrupt cannot clear a pending cancel.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			c.vm.Interrupt()
		case <-stop:
		}
	}()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	out, err := c.vm.Call(fn, payload)
	if err != nil {
		return "", scriptError(err)
	}
	s, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("runtime %s returned %T, want string", fn, out)
	}
	return s, nil
}

// scriptError maps a QuickJS evaluation error. The interrupt handler's
// exception means the wall-clock timeout fired. Anything else is a script
// error surfaced with its message.
func scriptError(err error) error {
	msg := err.Error()
	if strings.Contains(msg, "InternalError: interrupted") {
		return ErrTimeout
	}
	return fmt.Errorf("JavaScript error: %s", msg)
}
