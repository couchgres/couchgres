package httpapi

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

const maxPasswordVerifierConcurrency = 4

type passwordPolicy struct {
	minIterations int
	maxIterations int
	lockoutMode   string
	lockoutLimit  int
	lockoutPeriod time.Duration
	lockoutMax    int
}

// passwordAuthenticator bounds expensive derivations and tracks CouchDB-style
// username+client-IP lockouts. Validation in couch.VerifyPassword is still the
// final safety boundary so no caller can bypass the work-factor ceiling.
type passwordAuthenticator struct {
	slots    chan struct{}
	lockouts authLockouts
}

func newPasswordAuthenticator() *passwordAuthenticator {
	size := min(runtime.GOMAXPROCS(0), maxPasswordVerifierConcurrency)
	return newPasswordAuthenticatorWithLimit(max(size, 1))
}

func newPasswordAuthenticatorWithLimit(limit int) *passwordAuthenticator {
	return &passwordAuthenticator{
		slots: make(chan struct{}, max(limit, 1)),
		lockouts: authLockouts{
			entries: make(map[authLockoutKey]authLockoutEntry),
		},
	}
}

func (a *passwordAuthenticator) verify(
	r *http.Request,
	username, password string,
	hash *couch.HashedPassword,
	policy passwordPolicy,
) error {
	key := authLockoutKey{username: username, source: requestSourceIP(r)}
	wasLocked := policy.lockoutMode != "off" &&
		a.lockouts.locked(key, time.Now(), policy.lockoutLimit)
	if wasLocked {
		if policy.lockoutMode == "enforce" {
			return couch.Forbidden("Account is temporarily locked.")
		}
		slog.Warn("authentication lockout threshold reached",
			"username", username, "source", key.source)
	}

	verified := false
	if hash != nil && hash.Validate(policy.minIterations, policy.maxIterations) == nil {
		if err := a.acquire(r.Context()); err != nil {
			return err
		}
		verified = couch.VerifyPassword(password, hash)
		a.release()
	}
	if verified {
		a.lockouts.succeeded(key)
		return nil
	}

	if policy.lockoutMode != "off" && a.lockouts.failed(
		key, time.Now(), policy.lockoutLimit, policy.lockoutPeriod, policy.lockoutMax) {
		if policy.lockoutMode == "enforce" {
			return couch.Forbidden("Account is temporarily locked.")
		}
		if !wasLocked {
			slog.Warn("authentication lockout threshold reached",
				"username", username, "source", key.source)
		}
	}
	return couch.Unauthorized("Name or password is incorrect.")
}

func (a *passwordAuthenticator) prepareUserDoc(
	ctx context.Context,
	body map[string]any,
	iterations int,
) error {
	if _, hasPlaintext := body["password"].(string); !hasPlaintext {
		return couch.PrepareUserDoc(body, iterations)
	}
	if err := a.acquire(ctx); err != nil {
		return err
	}
	defer a.release()
	return couch.PrepareUserDoc(body, iterations)
}

func (a *passwordAuthenticator) hashAdminPassword(
	ctx context.Context,
	password string,
	iterations int,
) (string, error) {
	// Pre-hashed formats only need structural validation, not a verifier slot.
	if strings.HasPrefix(password, "-pbkdf2") || strings.HasPrefix(password, "-hashed-") {
		return couch.HashAdminPasswordChecked(password, iterations)
	}
	if err := a.acquire(ctx); err != nil {
		return "", err
	}
	defer a.release()
	return couch.HashAdminPasswordChecked(password, iterations)
}

func (a *passwordAuthenticator) acquire(ctx context.Context) error {
	select {
	case a.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *passwordAuthenticator) release() {
	<-a.slots
}

func requestSourceIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

type authLockoutKey struct {
	username string
	source   string
}

type authLockoutEntry struct {
	failures int
	expires  time.Time
}

type authLockouts struct {
	mu      sync.Mutex
	entries map[authLockoutKey]authLockoutEntry
}

func (l *authLockouts) locked(key authLockoutKey, now time.Time, threshold int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[key]
	if !ok {
		return false
	}
	if !now.Before(entry.expires) {
		delete(l.entries, key)
		return false
	}
	return entry.failures >= threshold
}

func (l *authLockouts) failed(
	key authLockoutKey,
	now time.Time,
	threshold int,
	period time.Duration,
	maxEntries int,
) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, exists := l.entries[key]
	if !now.Before(entry.expires) {
		entry = authLockoutEntry{expires: now.Add(period)}
	}
	entry.failures++
	if !exists && len(l.entries) >= maxEntries {
		l.evict(now)
	}
	if entry.failures > threshold {
		entry.failures = threshold
	}
	l.entries[key] = entry
	return entry.failures >= threshold
}

func (l *authLockouts) succeeded(key authLockoutKey) {
	l.mu.Lock()
	delete(l.entries, key)
	l.mu.Unlock()
}

func (l *authLockouts) evict(now time.Time) {
	for key, entry := range l.entries {
		if !now.Before(entry.expires) {
			delete(l.entries, key)
		}
	}
	if len(l.entries) == 0 {
		return
	}
	// Keep memory strictly bounded even when every tracked failure is live.
	for key := range l.entries {
		delete(l.entries, key)
		return
	}
}
