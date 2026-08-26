package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

func testPasswordPolicy() passwordPolicy {
	return passwordPolicy{
		minIterations: 1,
		maxIterations: 100,
		lockoutMode:   "enforce",
		lockoutLimit:  3,
		lockoutPeriod: time.Minute,
		lockoutMax:    10,
	}
}

func TestPasswordAuthenticatorLockout(t *testing.T) {
	hash, err := couch.HashUserPasswordChecked("correct", 10)
	if err != nil {
		t.Fatal(err)
	}
	auth := newPasswordAuthenticatorWithLimit(1)
	request := httptest.NewRequest("GET", "/", nil)
	request.RemoteAddr = "192.0.2.10:1234"
	policy := testPasswordPolicy()

	for attempt := 1; attempt <= policy.lockoutLimit; attempt++ {
		err := auth.verify(request, "alice", "wrong", &hash, policy)
		ce, ok := err.(*couch.Error)
		if !ok {
			t.Fatalf("attempt %d error: %v", attempt, err)
		}
		want := 401
		if attempt == policy.lockoutLimit {
			want = 403
		}
		if ce.Status != want {
			t.Fatalf("attempt %d status=%d want=%d", attempt, ce.Status, want)
		}
	}
	if err := auth.verify(request, "alice", "correct", &hash, policy); err == nil {
		t.Fatal("locked username/source pair authenticated")
	}

	// Lockouts are scoped to the username and direct peer IP.
	request.RemoteAddr = "192.0.2.11:1234"
	if err := auth.verify(request, "alice", "correct", &hash, policy); err != nil {
		t.Fatalf("different source remained locked: %v", err)
	}
}

func TestPasswordAuthenticatorSuccessClearsFailures(t *testing.T) {
	hash, err := couch.HashUserPasswordChecked("correct", 10)
	if err != nil {
		t.Fatal(err)
	}
	auth := newPasswordAuthenticatorWithLimit(1)
	request := httptest.NewRequest("GET", "/", nil)
	policy := testPasswordPolicy()

	if err := auth.verify(request, "alice", "wrong", &hash, policy); err == nil {
		t.Fatal("wrong password accepted")
	}
	if err := auth.verify(request, "alice", "correct", &hash, policy); err != nil {
		t.Fatalf("correct password rejected: %v", err)
	}
	for attempt := 1; attempt < policy.lockoutLimit; attempt++ {
		err := auth.verify(request, "alice", "wrong", &hash, policy)
		if ce, _ := err.(*couch.Error); ce == nil || ce.Status != 401 {
			t.Fatalf("post-success attempt %d: %v", attempt, err)
		}
	}
}

func TestPasswordAuthenticatorEnforcesConfiguredRange(t *testing.T) {
	hash, err := couch.HashUserPasswordChecked("correct", 10)
	if err != nil {
		t.Fatal(err)
	}
	auth := newPasswordAuthenticatorWithLimit(1)
	request := httptest.NewRequest("GET", "/", nil)
	policy := testPasswordPolicy()
	policy.maxIterations = 5
	policy.lockoutMode = "off"

	err = auth.verify(request, "alice", "correct", &hash, policy)
	if ce, _ := err.(*couch.Error); ce == nil || ce.Status != 401 {
		t.Fatalf("out-of-policy hash error: %v", err)
	}
}

func TestPasswordAuthenticatorWaitRespectsContext(t *testing.T) {
	hash, err := couch.HashUserPasswordChecked("correct", 10)
	if err != nil {
		t.Fatal(err)
	}
	auth := newPasswordAuthenticatorWithLimit(1)
	auth.slots <- struct{}{} // Simulate the sole verifier already doing work.
	defer auth.release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	policy := testPasswordPolicy()
	policy.lockoutMode = "off"
	if err := auth.verify(request, "alice", "correct", &hash, policy); !errors.Is(err, context.Canceled) {
		t.Fatalf("verify error=%v, want context cancellation", err)
	}
	body := map[string]any{"password": "new-password"}
	if err := auth.prepareUserDoc(ctx, body, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("user hash error=%v, want context cancellation", err)
	}
	if body["password"] != "new-password" {
		t.Fatal("cancelled user hash mutated the document")
	}
	if _, err := auth.hashAdminPassword(ctx, "new-password", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("admin hash error=%v, want context cancellation", err)
	}
}

func TestAuthLockoutMemoryBound(t *testing.T) {
	lockouts := authLockouts{entries: make(map[authLockoutKey]authLockoutEntry)}
	now := time.Now()
	for i := 0; i < 20; i++ {
		lockouts.failed(authLockoutKey{
			username: fmt.Sprintf("user-%d", i),
			source:   "192.0.2.1",
		}, now, 5, time.Minute, 3)
	}
	if len(lockouts.entries) > 3 {
		t.Fatalf("tracked %d lockouts, want at most 3", len(lockouts.entries))
	}
}

func TestPasswordIterationConfigValidation(t *testing.T) {
	cache := &configCache{tree: map[string]map[string]string{
		"couch_httpd_auth": {
			"iterations":     "10",
			"min_iterations": "1",
			"max_iterations": "100",
		},
	}}
	if got := cache.passwordIterations(); got != 10 {
		t.Fatalf("passwordIterations=%d want=10", got)
	}
	if err := cache.validatePasswordIterationChange("iterations", "101"); err == nil {
		t.Fatal("iterations above configured maximum accepted")
	}
	if err := cache.validatePasswordIterationChange(
		"max_iterations", fmt.Sprint(couch.MaxPasswordIterations+1)); err == nil {
		t.Fatal("maximum above process safety ceiling accepted")
	}
	if err := cache.validatePasswordIterationChange("min_iterations", "101"); err == nil {
		t.Fatal("minimum above maximum accepted")
	}
}
