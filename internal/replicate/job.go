package replicate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Options describe one replication (the _replicate body / _replicator doc).
type Options struct {
	Source       string
	Target       string
	Continuous   bool
	CreateTarget bool
	DocIDs       []string
	Filter       string         // "ddoc/name" or "_view" etc., passed to the source
	QueryParams  map[string]any // extra query parameters the filter sees
	Selector     map[string]any // selector-filtered replication (filter=_selector)
}

// ID derives the checkpoint identity for a replication. It only needs to be
// stable per (source, target, options) on this server. Each replicator owns
// its checkpoints, so CouchDB's own derivation is not required.
func (o Options) ID() string {
	parts := []string{o.Source, o.Target}
	if o.Continuous {
		parts = append(parts, "continuous")
	}
	if len(o.DocIDs) > 0 {
		ids := append([]string(nil), o.DocIDs...)
		sort.Strings(ids)
		parts = append(parts, strings.Join(ids, ","))
	}
	if o.Filter != "" {
		parts = append(parts, "filter="+o.Filter)
		if raw, err := json.Marshal(o.QueryParams); err == nil && o.QueryParams != nil {
			parts = append(parts, string(raw))
		}
	}
	if o.Selector != nil {
		if raw, err := json.Marshal(o.Selector); err == nil {
			parts = append(parts, "selector="+string(raw))
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}

// changesQuery renders the filter options as _changes query parameters plus
// the POST body a selector filter requires.
func (o Options) changesQuery() (url.Values, map[string]any) {
	if o.Selector != nil {
		return url.Values{"filter": []string{"_selector"}},
			map[string]any{"selector": o.Selector}
	}
	if o.Filter == "" {
		return nil, nil
	}
	extra := url.Values{"filter": []string{o.Filter}}
	for key, value := range o.QueryParams {
		if s, ok := value.(string); ok {
			extra.Set(key, s)
		} else if raw, err := json.Marshal(value); err == nil {
			extra.Set(key, string(raw))
		}
	}
	return extra, nil
}

// Stats are live counters, updated atomically so status endpoints can read
// them while the job runs.
type Stats struct {
	DocsRead         atomic.Int64
	DocsWritten      atomic.Int64
	DocWriteFailures atomic.Int64
	MissingRevsFound atomic.Int64
	RevsChecked      atomic.Int64
	CheckpointedSeq  atomic.Value // string
	ChangesPending   atomic.Int64
}

func (s *Stats) Snapshot() map[string]any {
	checkpointed, _ := s.CheckpointedSeq.Load().(string)
	return map[string]any{
		"docs_read":               s.DocsRead.Load(),
		"docs_written":            s.DocsWritten.Load(),
		"doc_write_failures":      s.DocWriteFailures.Load(),
		"missing_revisions_found": s.MissingRevsFound.Load(),
		"revisions_checked":       s.RevsChecked.Load(),
		"through_seq":             checkpointed,
		"changes_pending":         s.ChangesPending.Load(),
	}
}

// Result summarizes a finished (or checkpointed) run for the _replicate
// response shape.
type Result struct {
	SessionID     string
	SourceLastSeq string
	StartTime     time.Time
	EndTime       time.Time
	Stats         *Stats
}

func (r *Result) History() map[string]any {
	return map[string]any{
		"session_id":         r.SessionID,
		"start_time":         r.StartTime.UTC().Format(time.RFC1123),
		"end_time":           r.EndTime.UTC().Format(time.RFC1123),
		"start_last_seq":     0,
		"end_last_seq":       r.SourceLastSeq,
		"recorded_seq":       r.SourceLastSeq,
		"docs_read":          r.Stats.DocsRead.Load(),
		"docs_written":       r.Stats.DocsWritten.Load(),
		"doc_write_failures": r.Stats.DocWriteFailures.Load(),
		"missing_checked":    r.Stats.RevsChecked.Load(),
		"missing_found":      r.Stats.MissingRevsFound.Load(),
	}
}

const batchSize = 100

// Run drives one replication until the source is drained (one-shot) or the
// context ends (continuous).
func Run(ctx context.Context, source, target *Peer, o Options, stats *Stats) (*Result, error) {
	result := &Result{
		SessionID: newSessionID(),
		StartTime: time.Now(),
		Stats:     stats,
	}

	if o.CreateTarget {
		if err := target.Create(ctx); err != nil {
			return nil, err
		}
	}

	if len(o.DocIDs) > 0 {
		if err := replicateDocIDs(ctx, source, target, o.DocIDs, stats); err != nil {
			return nil, err
		}
		result.EndTime = time.Now()
		return result, checkpoint(ctx, source, target, o.ID(), result)
	}

	since := ""
	if cp, err := target.GetCheckpoint(ctx, o.ID()); err == nil && cp != nil {
		since = cp.LastSeq
	}

	feed := "normal"
	extra, changesBody := o.changesQuery()
	for {
		page, err := source.Changes(ctx, since, batchSize, feed, 25*time.Second, extra, changesBody)
		if err != nil {
			if ctx.Err() != nil {
				break // cancelled: report what was checkpointed
			}
			return nil, err
		}
		stats.ChangesPending.Store(page.Pending)

		if len(page.Results) > 0 {
			if err := copyBatch(ctx, source, target, page.Results, stats); err != nil {
				return nil, err
			}
		}
		if page.LastSeq != "" {
			since = page.LastSeq
		}
		result.SourceLastSeq = since
		result.EndTime = time.Now()
		if err := checkpoint(ctx, source, target, o.ID(), result); err != nil && ctx.Err() == nil {
			return nil, err
		}
		stats.CheckpointedSeq.Store(since)

		if !o.Continuous && page.Pending == 0 {
			break
		}
		if o.Continuous {
			feed = "longpoll"
			if ctx.Err() != nil {
				break
			}
		}
	}
	result.EndTime = time.Now()
	return result, nil
}

// copyBatch is the heart of the protocol: revs_diff -> fetch -> push.
func copyBatch(ctx context.Context, source, target *Peer, changes []Change, stats *Stats) error {
	byID := make(map[string][]string, len(changes))
	for _, change := range changes {
		byID[change.ID] = append(byID[change.ID], change.Revs...)
		stats.RevsChecked.Add(int64(len(change.Revs)))
	}
	missing, err := target.RevsDiff(ctx, byID)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}
	for _, revs := range missing {
		stats.MissingRevsFound.Add(int64(len(revs)))
	}
	docs, err := source.FetchRevs(ctx, missing)
	if err != nil {
		return err
	}
	stats.DocsRead.Add(int64(len(docs)))
	failures, err := target.PushDocs(ctx, docs)
	if err != nil {
		return err
	}
	stats.DocWriteFailures.Add(failures)
	stats.DocsWritten.Add(int64(len(docs)) - failures)
	return nil
}

// replicateDocIDs copies every leaf of the named documents.
func replicateDocIDs(ctx context.Context, source, target *Peer, ids []string, stats *Stats) error {
	for _, id := range ids {
		leaves, err := source.OpenRevs(ctx, id)
		if err != nil {
			return err
		}
		if len(leaves) == 0 {
			continue
		}
		stats.DocsRead.Add(int64(len(leaves)))
		failures, err := target.PushDocs(ctx, leaves)
		if err != nil {
			return err
		}
		stats.DocWriteFailures.Add(failures)
		stats.DocsWritten.Add(int64(len(leaves)) - failures)
	}
	return nil
}

// checkpoint records progress on both ends. The target also gets an
// _ensure_full_commit per the replication protocol.
func checkpoint(ctx context.Context, source, target *Peer, replicationID string, result *Result) error {
	if err := target.EnsureFullCommit(ctx); err != nil {
		return err
	}
	record := &Checkpoint{
		SessionID: result.SessionID,
		LastSeq:   result.SourceLastSeq,
		History:   []map[string]any{result.History()},
	}
	if err := target.PutCheckpoint(ctx, replicationID, record); err != nil {
		return err
	}
	// The source checkpoint is best-effort (the source may be read-only).
	_ = source.PutCheckpoint(ctx, replicationID, record)
	return nil
}

func newSessionID() string {
	sum := sha256.Sum256([]byte(time.Now().String()))
	return hex.EncodeToString(sum[:16])
}
