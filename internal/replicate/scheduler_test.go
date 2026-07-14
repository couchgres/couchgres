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
