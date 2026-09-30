package httpapi

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/replicate"
	"github.com/couchgres/couchgres/internal/store"
)

// replicateNow is POST /_replicate: transient replications, run by this
// server acting as a CouchDB replicator.
func (s *Server) replicateNow(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	options, err := replicate.ParseOptions(obj)
	if err != nil {
		return err
	}

	if cancel, _ := obj["cancel"].(bool); cancel {
		job, found := s.scheduler.Cancel(options)
		if !found {
			return couch.NewError(404, "not_found", "unknown replication")
		}
		writeJSON(w, 200, map[string]any{"ok": true, "_local_id": job.ID})
		return nil
	}

	job, _, err := s.scheduler.Launch(s.lifetime, options, "")
	if err != nil {
		return err
	}
	if options.Continuous {
		writeJSON(w, 202, map[string]any{"ok": true, "_local_id": job.ID})
		return nil
	}

	job.Wait(r.Context())
	state, result, jobErr := job.State()
	switch state {
	case "completed":
		writeJSON(w, 200, map[string]any{
			"ok":                     true,
			"session_id":             result.SessionID,
			"source_last_seq":        result.SourceLastSeq,
			"replication_id_version": 4,
			"history":                []any{result.History()},
		})
		return nil
	case "failed":
		if ce, ok := jobErr.(*couch.Error); ok {
			return ce
		}
		return couch.NewError(500, "replication_error", jobErr.Error())
	default: // cancelled or client gave up waiting
		writeJSON(w, 200, map[string]any{"ok": true, "_local_id": job.ID})
		return nil
	}
}

// schedulerJobs is GET /_scheduler/jobs: currently running jobs.
func (s *Server) schedulerJobs(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	jobs := s.scheduler.Jobs()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].StartTime.Before(jobs[j].StartTime) })
	rows := make([]any, 0, len(jobs))
	for _, job := range jobs {
		state, _, _ := job.State()
		if state != "running" {
			continue
		}
		entry := map[string]any{
			"id":         job.ID,
			"pid":        "<0.1.0>",
			"node":       "nonode@nohost",
			"database":   nil,
			"doc_id":     nil,
			"source":     job.SourceURL,
			"target":     job.TargetURL,
			"user":       nil,
			"info":       job.Stats.Snapshot(),
			"start_time": job.StartTime.UTC().Format(time.RFC1123),
			"history": []any{map[string]any{
				"type":      "started",
				"timestamp": job.StartTime.UTC().Format(time.RFC1123),
			}},
		}
		if job.DocID != "" {
			entry["database"] = "_replicator"
			entry["doc_id"] = job.DocID
		}
		rows = append(rows, entry)
	}
	writeJSON(w, 200, map[string]any{
		"total_rows": len(rows), "offset": 0, "jobs": rows,
	})
	return nil
}

// schedulerDocs is GET /_scheduler/docs: the _replicator documents with
// their lifecycle states.
func (s *Server) schedulerDocs(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	db, err := s.store.GetDB(r.Context(), "_replicator")
	if err != nil {
		return err
	}
	page, err := s.store.AllDocs(r.Context(), db, &store.AllDocsParams{IncludeDocs: true})
	if err != nil {
		return err
	}
	byDoc := make(map[string]*replicate.Job)
	for _, job := range s.scheduler.Jobs() {
		if job.DocID != "" {
			byDoc[job.DocID] = job
		}
	}
	rows := make([]any, 0, len(page.Rows))
	for _, row := range page.Rows {
		if strings.HasPrefix(row.ID, "_design/") {
			continue
		}
		state, _ := row.Body["_replication_state"].(string)
		if state == "" {
			state = "initializing"
		} else if state == "triggered" {
			state = "running"
		}
		entry := map[string]any{
			"database":     "_replicator",
			"doc_id":       row.ID,
			"id":           row.Body["_replication_id"],
			"node":         "nonode@nohost",
			"state":        state,
			"error_count":  0,
			"last_updated": row.Body["_replication_state_time"],
			"start_time":   row.Body["_replication_state_time"],
			"source":       endpointForDisplay(row.Body["source"]),
			"target":       endpointForDisplay(row.Body["target"]),
			"info":         nil,
		}
		if job, ok := byDoc[row.ID]; ok {
			entry["info"] = job.Stats.Snapshot()
			entry["source"] = job.SourceURL
			entry["target"] = job.TargetURL
		}
		rows = append(rows, entry)
	}
	writeJSON(w, 200, map[string]any{
		"total_rows": len(rows), "offset": 0, "docs": rows,
	})
	return nil
}

// endpointForDisplay strips credentials from a source/target member.
func endpointForDisplay(v any) any {
	raw := ""
	switch t := v.(type) {
	case string:
		raw = t
	case map[string]any:
		raw, _ = t["url"].(string)
	}
	if raw == "" {
		return v
	}
	if parsed, err := url.Parse(raw); err == nil && parsed.IsAbs() && parsed.Host != "" {
		parsed.User = nil
		return strings.TrimRight(parsed.String(), "/") + "/"
	}
	return raw
}

// activeTasks is GET /_active_tasks: running replications (view builds and
// purges join in later phases).
func (s *Server) activeTasks(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	tasks := make([]any, 0)
	for _, job := range s.scheduler.Jobs() {
		state, _, _ := job.State()
		if state != "running" {
			continue
		}
		task := map[string]any{
			"type":           "replication",
			"replication_id": job.ID,
			"continuous":     job.Options.Continuous,
			"source":         job.SourceURL,
			"target":         job.TargetURL,
			"started_on":     job.StartTime.Unix(),
			"updated_on":     time.Now().Unix(),
			"pid":            "<0.1.0>",
			"node":           "nonode@nohost",
			"user":           nil,
			"database":       nil,
			"doc_id":         nil,
		}
		for k, v := range job.Stats.Snapshot() {
			task[k] = v
		}
		if job.DocID != "" {
			task["database"] = "_replicator"
			task["doc_id"] = job.DocID
		}
		tasks = append(tasks, task)
	}
	writeJSON(w, 200, tasks)
	return nil
}

// dbUpdates is GET /_db_updates: the global database activity feed.
// Created and deleted events are persisted. Per-write "updated" events are
// delivered live and not stored. This is a documented divergence.
func (s *Server) dbUpdates(w http.ResponseWriter, r *http.Request) error {
	if err := s.requireServerAdmin(r); err != nil {
		return err
	}
	q := r.URL.Query()
	feed := q.Get("feed")
	if feed == "" {
		feed = "normal"
	}
	since := int64(0)
	if raw := q.Get("since"); raw != "" && raw != "now" {
		numPart, _, _ := strings.Cut(raw, "-")
		if n, err := strconv.ParseInt(numPart, 10, 64); err == nil {
			since = n
		}
	}

	events, err := s.store.DBEventsSince(r.Context(), since)
	if err != nil {
		return err
	}
	if feed == "normal" || len(events) > 0 {
		writeDBUpdates(w, events, since)
		return nil
	}

	// longpoll: wait for any activity, then re-read.
	timeout := 60 * time.Second
	if raw := q.Get("timeout"); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms >= 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	wake, unsubscribe := s.broker.SubscribeAll()
	defer unsubscribe()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		events, err = s.store.DBEventsSince(r.Context(), since)
		if err != nil {
			return err
		}
		if len(events) > 0 {
			break
		}
		select {
		case <-wake:
		case <-deadline.C:
			writeDBUpdates(w, nil, since)
			return nil
		case <-r.Context().Done():
			return nil
		}
	}
	writeDBUpdates(w, events, since)
	return nil
}

func writeDBUpdates(w http.ResponseWriter, events []store.DBEvent, since int64) {
	lastSeq := since
	rows := make([]any, 0, len(events))
	for _, event := range events {
		rows = append(rows, map[string]any{
			"db_name": event.Name,
			"type":    event.Type,
			"seq":     seqString(event.Seq),
		})
		lastSeq = event.Seq
	}
	writeJSON(w, 200, map[string]any{
		"results":  rows,
		"last_seq": seqString(lastSeq),
	})
}
