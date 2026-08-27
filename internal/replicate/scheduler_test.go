package replicate

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerPrunesTerminalJobs(t *testing.T) {
	s := NewScheduler(nil, nil)
	s.SetSelf("http://127.0.0.1:1", func() string { return "" })

	job, started, err := s.Launch(context.Background(), Options{
		Source: "prune_src",
		Target: "prune_dst",
	}, "")
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if !started {
		t.Fatal("expected a new job")
	}
	if n := len(s.Jobs()); n != 1 {
		t.Fatalf("running jobs: got %d, want 1", n)
	}

	job.Wait(context.Background())
	state, _, _ := job.State()
	if state == "running" {
		t.Fatal("job still running after Wait")
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(s.Jobs()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("terminal job still in map: state=%s jobs=%d", state, len(s.Jobs()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSchedulerPruneDoesNotClobberReplacement(t *testing.T) {
	s := NewScheduler(nil, nil)
	old := &Job{ID: "same", done: make(chan struct{})}
	old.state = "failed"
	close(old.done)
	s.mu.Lock()
	s.jobs["same"] = old
	s.mu.Unlock()

	replacement := &Job{ID: "same", done: make(chan struct{})}
	replacement.state = "running"
	s.mu.Lock()
	s.jobs["same"] = replacement
	s.mu.Unlock()

	s.prune(old)
	jobs := s.Jobs()
	if len(jobs) != 1 || jobs[0] != replacement {
		t.Fatalf("pruned replacement: %+v", jobs)
	}
}

func TestSchedulerPrivateNetworkPolicyKeepsLocalNames(t *testing.T) {
	s := NewScheduler(nil, nil)
	s.SetSelf("http://127.0.0.1:5984", func() string { return "cookie" })

	local, err := s.resolve("local_db")
	if err != nil {
		t.Fatalf("local database name: %v", err)
	}
	if local.cookie != "cookie" {
		t.Fatal("local database did not receive the self-authentication cookie")
	}
	if _, err := s.resolve("http://127.0.0.1:5984/remote_db"); err == nil {
		t.Fatal("URL-form private endpoint was allowed by default")
	}

	s.SetAllowPrivateNetworks(true)
	if _, err := s.resolve("http://127.0.0.1:5984/remote_db"); err != nil {
		t.Fatalf("private endpoint opt-in: %v", err)
	}
}
