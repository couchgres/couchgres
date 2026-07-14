package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRunForUsesMinimumWindow(t *testing.T) {
	const minimum = 20 * time.Millisecond
	count, elapsed := runFor(minimum, func(int) {})
	if count == 0 {
		t.Fatal("runFor completed no operations")
	}
	if elapsed < minimum {
		t.Fatalf("runFor elapsed %s, want at least %s", elapsed, minimum)
	}
}

func TestRunConcurrentForUsesMinimumWindow(t *testing.T) {
	const (
		minimum = 20 * time.Millisecond
		workers = 4
	)
	var calls atomic.Int64
	count, elapsed := runConcurrentFor(minimum, workers, func(int, int64) {
		calls.Add(1)
	})
	if count == 0 {
		t.Fatal("runConcurrentFor completed no operations")
	}
	if int64(count) != calls.Load() {
		t.Fatalf("reported %d operations, callback observed %d", count, calls.Load())
	}
	if elapsed < minimum {
		t.Fatalf("runConcurrentFor elapsed %s, want at least %s", elapsed, minimum)
	}
}
