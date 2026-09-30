package replicate

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestSchedulerPublicNetworkOptInKeepsLocalNames(t *testing.T) {
	s := NewScheduler(nil, nil)
	self := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer self.Close()
	s.SetSelf(self.URL, func() string { return "cookie" })
	if _, err := s.resolve("http://10.0.0.1:5984/remote_db"); err != nil {
		t.Fatalf("private peer should be allowed by default: %v", err)
	}
	if _, err := s.resolve(self.URL + "/remote_db"); err == nil {
		t.Fatal("URL-form loopback endpoint was allowed")
	}

	local, err := s.resolve("local_db")
	if err != nil {
		t.Fatalf("local database name: %v", err)
	}
	if local.cookie != "cookie" {
		t.Fatal("local database did not receive the self-authentication cookie")
	}
	if exists, err := local.Exists(t.Context()); err != nil || !exists {
		t.Fatalf("local database request: exists=%t err=%v", exists, err)
	}

	for _, endpoint := range []string{"http://8.8.8.8/db", "http://[2606:4700:4700::1111]/db"} {
		s.SetAllowPublicNetworks(false)
		if _, err := s.resolve(endpoint); err == nil {
			t.Fatalf("public peer %q was allowed without opt-in", endpoint)
		}
		s.SetAllowPublicNetworks(true)
		if _, err := s.resolve(endpoint); err != nil {
			t.Fatalf("public peer %q with opt-in: %v", endpoint, err)
		}
	}
	if _, err := s.resolve(self.URL + "/remote_db"); err == nil {
		t.Fatal("public network opt-in allowed a loopback endpoint")
	}
}
