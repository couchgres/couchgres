package replicate

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// Tracks running replication jobs from POST /_replicate and from
// _replicator documents.
type Scheduler struct {
	store  *store.Store
	broker *store.Broker

	mu                   sync.Mutex
	jobs                 map[string]*Job // by replication id
	selfBase             string          // this server's own base URL for local db names
	cookie               func() string   // mints an admin session for loopback requests
	allowPrivateNetworks bool            // permits remote RFC1918 and IPv6 ULA peers
}

// Job is one replication, running or finished.
type Job struct {
	ID        string
	DocID     string // _replicator doc id, "" for transient jobs
	Options   Options
	SourceURL string // credential-free, for status endpoints
	TargetURL string
	StartTime time.Time
	Stats     *Stats
	cancel    context.CancelFunc
	done      chan struct{}

	mu     sync.Mutex
	state  string // running | completed | failed | cancelled
	err    error
	result *Result
}

func (j *Job) setState(state string, err error, result *Result) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state = state
	j.err = err
	j.result = result
}

func (j *Job) State() (state string, result *Result, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state, j.result, j.err
}

func NewScheduler(st *store.Store, broker *store.Broker) *Scheduler {
	return &Scheduler{
		store:                st,
		broker:               broker,
		jobs:                 make(map[string]*Job),
		cookie:               func() string { return "" },
		allowPrivateNetworks: true,
	}
}

// SetSelf wires the loopback endpoint: the server's own URL and a cookie
// minter for admin credentials.
func (s *Scheduler) SetSelf(base string, cookie func() string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.selfBase = strings.TrimRight(base, "/")
	s.cookie = cookie
}

// SetAllowPrivateNetworks controls whether URL-form replication endpoints may
// resolve to RFC1918 and IPv6 ULA addresses (allowed by default). Loopback,
// link-local, and other special-use addresses remain blocked. Local database
// names use the trusted self URL regardless of this setting.
func (s *Scheduler) SetAllowPrivateNetworks(allow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowPrivateNetworks = allow
}

// resolve turns a replication endpoint (URL or local db name) into a Peer.
func (s *Scheduler) resolve(endpoint string) (*Peer, error) {
	s.mu.Lock()
	base, cookie := s.selfBase, s.cookie
	allowPrivateNetworks := s.allowPrivateNetworks
	s.mu.Unlock()
	if strings.Contains(endpoint, "://") {
		return newPeer(endpoint, peerConfig{allowPrivateNetworks: allowPrivateNetworks})
	}
	if base == "" {
		return nil, couch.NewError(500, "unknown_error",
			"local replication endpoints are not available before startup completes")
	}
	peer, err := newPeer(base+"/"+endpoint, peerConfig{trustedSelf: true})
	if err != nil {
		return nil, err
	}
	return peer.WithCookie(cookie()), nil
}

// Launch starts a job unless one with the same id is already running.
func (s *Scheduler) Launch(ctx context.Context, o Options, docID string) (*Job, bool, error) {
	source, err := s.resolve(o.Source)
	if err != nil {
		return nil, false, err
	}
	target, err := s.resolve(o.Target)
	if err != nil {
		return nil, false, err
	}

	id := o.ID()
	s.mu.Lock()
	if existing, ok := s.jobs[id]; ok {
		if state, _, _ := existing.State(); state == "running" {
			s.mu.Unlock()
			return existing, false, nil
		}
	}
	jobCtx, cancel := context.WithCancel(ctx)
	job := &Job{
		ID:        id,
		DocID:     docID,
		Options:   o,
		SourceURL: source.URL(),
		TargetURL: target.URL(),
		StartTime: time.Now(),
		Stats:     &Stats{},
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	job.state = "running"
	s.jobs[id] = job
	s.mu.Unlock()

	go func() {
		defer close(job.done)
		result, err := Run(jobCtx, source, target, o, job.Stats)
		switch {
		case err != nil && jobCtx.Err() != nil:
			job.setState("cancelled", nil, nil)
		case err != nil:
			slog.Warn("replication failed", "id", id, "error", err)
			job.setState("failed", err, nil)
		case jobCtx.Err() != nil:
			job.setState("cancelled", nil, result)
		default:
			job.setState("completed", nil, result)
		}
		s.prune(job)
	}()
	return job, true, nil
}

// prune removes a finished job from the map if it is still the current entry.
func (s *Scheduler) prune(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.jobs[job.ID]; ok && cur == job {
		delete(s.jobs, job.ID)
	}
}

// Wait blocks until the job finishes (one-shot _replicate responses).
func (j *Job) Wait(ctx context.Context) {
	select {
	case <-j.done:
	case <-ctx.Done():
	}
}

// Cancel stops the job matching the given options.
func (s *Scheduler) Cancel(o Options) (*Job, bool) {
	id := o.ID()
	s.mu.Lock()
	job, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	job.cancel()
	return job, true
}

// Jobs returns the jobs still tracked by the scheduler.
func (s *Scheduler) Jobs() []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		out = append(out, job)
	}
	return out
}

// ---------------------------------------------------------------------------
// The _replicator database: docs become jobs, and their lifecycle is written
// back as _replication_state fields.
// ---------------------------------------------------------------------------

// Watch reconciles _replicator docs with running jobs until ctx ends.
func (s *Scheduler) Watch(ctx context.Context) {
	defer s.stopDocJobs()
	wake, unsubscribe := s.broker.Subscribe("_replicator")
	defer unsubscribe()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := s.reconcile(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("replicator reconcile", "error", err)
		}
		select {
		case <-wake:
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// WatchSingleton runs Watch only while this process owns the PostgreSQL
// advisory lock for the durable _replicator worker.
func (s *Scheduler) WatchSingleton(ctx context.Context) {
	for ctx.Err() == nil {
		acquired, err := s.store.RunWithReplicatorLock(ctx, s.Watch)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Warn("replicator worker lock", "error", err)
		}
		if !acquired {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
			continue
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
}

func (s *Scheduler) stopDocJobs() {
	s.mu.Lock()
	jobs := make([]*Job, 0)
	for _, job := range s.jobs {
		if job.DocID != "" {
			if state, _, _ := job.State(); state == "running" {
				job.cancel()
			}
			jobs = append(jobs, job)
		}
	}
	s.mu.Unlock()
	for _, job := range jobs {
		job.Wait(context.Background())
	}
}

func (s *Scheduler) reconcile(ctx context.Context) error {
	db, err := s.store.GetDB(ctx, "_replicator")
	if err != nil {
		return err
	}
	page, err := s.store.AllDocs(ctx, db, &store.AllDocsParams{IncludeDocs: true})
	if err != nil {
		return err
	}

	live := make(map[string]bool, len(page.Rows))
	for _, row := range page.Rows {
		if strings.HasPrefix(row.ID, "_design/") {
			continue
		}
		o, parseErr := optionsFromDoc(row.Body)
		state, _ := row.Body["_replication_state"].(string)
		if parseErr != nil {
			if state != "failed" {
				s.writeBack(ctx, db, row.ID, "failed", parseErr.Error(), "")
			}
			continue
		}
		live[o.ID()] = true
		if state == "completed" || state == "failed" {
			continue
		}
		job, started, err := s.Launch(ctx, o, row.ID)
		if err != nil {
			s.writeBack(ctx, db, row.ID, "failed", err.Error(), "")
			continue
		}
		if started {
			s.writeBack(ctx, db, row.ID, "triggered", "", job.ID)
			go s.finishDoc(ctx, db, row.ID, job)
		}
	}

	// Deleted docs cancel their jobs.
	s.mu.Lock()
	for id, job := range s.jobs {
		if job.DocID != "" && !live[id] {
			if state, _, _ := job.State(); state == "running" {
				job.cancel()
			}
		}
	}
	s.mu.Unlock()
	return nil
}

// finishDoc records the terminal state of a doc-driven job. Continuous jobs
// stay "triggered" until cancelled or failed.
func (s *Scheduler) finishDoc(ctx context.Context, db *store.DB, docID string, job *Job) {
	job.Wait(ctx)
	state, _, jobErr := job.State()
	switch state {
	case "completed":
		s.writeBack(ctx, db, docID, "completed", "", job.ID)
	case "failed":
		s.writeBack(ctx, db, docID, "failed", jobErr.Error(), job.ID)
	}
}

// writeBack updates _replication_state members on a _replicator doc.
func (s *Scheduler) writeBack(ctx context.Context, db *store.DB, docID, state, reason, replicationID string) {
	row, err := s.store.GetDocAny(ctx, db, docID)
	if err != nil || row.Deleted {
		return
	}
	if current, _ := row.Body["_replication_state"].(string); current == state {
		return
	}
	body := row.Body
	body["_replication_state"] = state
	body["_replication_state_time"] = time.Now().UTC().Format(time.RFC3339)
	if replicationID != "" {
		body["_replication_id"] = replicationID
	}
	if reason != "" {
		body["_replication_state_reason"] = reason
	} else {
		delete(body, "_replication_state_reason")
	}
	if _, _, err := s.store.PutDoc(ctx, db, docID, body, nil, &row.Rev, false, nil); err != nil {
		// A concurrent edit wins. The next reconcile pass settles it.
		slog.Debug("replicator write-back skipped", "doc", docID, "error", err)
	}
}

// ParseOptions reads a _replicate body or _replicator document.
func ParseOptions(body map[string]any) (Options, error) {
	return optionsFromDoc(body)
}

// optionsFromDoc validates a _replicator document.
func optionsFromDoc(body map[string]any) (Options, error) {
	o := Options{}
	var err error
	if o.Source, err = endpointString(body["source"]); err != nil {
		return o, err
	}
	if o.Target, err = endpointString(body["target"]); err != nil {
		return o, err
	}
	o.Continuous, _ = body["continuous"].(bool)
	o.CreateTarget, _ = body["create_target"].(bool)
	if ids, ok := body["doc_ids"].([]any); ok {
		for _, id := range ids {
			if s, ok := id.(string); ok {
				o.DocIDs = append(o.DocIDs, s)
			}
		}
	}
	o.Filter, _ = body["filter"].(string)
	if params, ok := body["query_params"].(map[string]any); ok {
		o.QueryParams = params
	}
	if selector, ok := body["selector"].(map[string]any); ok {
		o.Selector = selector
	}
	return o, nil
}

// endpointString accepts "url" or {"url": ...} forms.
func endpointString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		if t != "" {
			return t, nil
		}
	case map[string]any:
		if u, ok := t["url"].(string); ok && u != "" {
			return u, nil
		}
	}
	return "", couch.BadRequest("replication documents require `source` and `target`")
}
