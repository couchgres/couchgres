package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
)

func TestRequestBodyLimitDefaults(t *testing.T) {
	c := &configCache{tree: make(map[string]map[string]string)}
	limits := c.requestBodyLimits()
	if limits.global != 64<<20 || limits.small != 64<<10 ||
		limits.query != 1<<20 || limits.document != 16<<20 ||
		limits.bulk != 64<<20 || limits.attachment != 64<<20 {
		t.Fatalf("defaults: %+v", limits)
	}

	c.insert("chttpd", "max_http_request_size", "1024")
	if got := c.requestBodyLimits().forClass(bodyDocument); got != 1024 {
		t.Fatalf("global limit did not clamp document limit: %d", got)
	}
	c.insert("chttpd", "max_http_request_size", "4294967296")
	if got := c.requestBodyLimits().global; got != defaultMaxHTTPRequestSize {
		t.Fatalf("unsafe stored limit did not fall back: %d", got)
	}
}

func TestRequestBodyClassification(t *testing.T) {
	tests := []struct {
		method, pattern string
		want            requestBodyClass
	}{
		{http.MethodPost, "POST /_session", bodySmall},
		{http.MethodPut, "PUT /_node/_local/_config/{section}/{key}", bodySmall},
		{http.MethodPost, "POST /{db}/_find", bodyQuery},
		{http.MethodPost, "POST /{db}/_bulk_docs", bodyBulk},
		{http.MethodPut, "PUT /{db}/{docid}", bodyDocument},
		{http.MethodPost, "/{db}/{docid}", bodyDocument},
		{http.MethodPut, "/{db}/{docid}/{attname...}", bodyAttachment},
		{http.MethodGet, "/{db}/{docid}/{attname...}", bodyQuery},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(tt.method, "/", nil)
		if got := classifyRequestBody(r, tt.pattern); got != tt.want {
			t.Errorf("%s %s: got %d, want %d", tt.method, tt.pattern, got, tt.want)
		}
	}
}

func TestProtectRequestBodyRejectsDeclaredAndStreamingBodies(t *testing.T) {
	s := &Server{
		config: &configCache{tree: map[string]map[string]string{
			"couchgres_httpd": {"max_small_request_size": "8"},
		}},
		bodyLimiter: newRequestBodyLimiter(),
	}

	t.Run("content_length", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/_session", strings.NewReader("123456789"))
		w := httptest.NewRecorder()
		if release, ok := s.protectRequestBody(w, r, "POST /_session"); ok {
			release()
			t.Fatal("oversized body was accepted")
		}
		assertTooLarge(t, w)
	})

	t.Run("unknown_length", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/_session", strings.NewReader("123456789"))
		r.ContentLength = -1
		w := httptest.NewRecorder()
		release, ok := s.protectRequestBody(w, r, "POST /_session")
		if !ok {
			t.Fatal("unknown-length body rejected before read")
		}
		defer release()
		if _, err := readHTTPBody(r); err == nil {
			t.Fatal("streaming oversized body was accepted")
		} else if ce, ok := err.(*couch.Error); !ok || ce.Status != 413 {
			t.Fatalf("error: %v", err)
		}
	})
}

func TestProtectRequestBodyRejectsBusyUploadSlots(t *testing.T) {
	limiter := &requestBodyLimiter{
		global:       make(chan struct{}, 1),
		perPrincipal: 1,
		principals:   make(map[string]*principalBodyLimit),
	}
	release, err := limiter.acquire(context.Background(), "ip:192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	s := &Server{
		config:      &configCache{tree: make(map[string]map[string]string)},
		bodyLimiter: limiter,
	}
	r := httptest.NewRequest(http.MethodPut, "/db/doc", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	if gotRelease, ok := s.protectRequestBody(w, r, "PUT /{db}/{docid}"); ok {
		gotRelease()
		t.Fatal("busy upload slot was accepted")
	}
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q body=%s",
			w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

func TestValidateRequestBodySizeChange(t *testing.T) {
	for _, value := range []string{"0", "-1", "nope", "268435457"} {
		if err := validateRequestBodySizeChange(
			"chttpd", "max_http_request_size", value); err == nil {
			t.Errorf("accepted global size %q", value)
		}
	}
	if err := validateRequestBodySizeChange(
		"couchgres_httpd", "max_attachment_request_size", "1048576"); err != nil {
		t.Fatalf("valid attachment size: %v", err)
	}
}

func TestRequestBodyLimiterIsolatesPrincipalsAndCleansUp(t *testing.T) {
	l := &requestBodyLimiter{
		global:       make(chan struct{}, 2),
		perPrincipal: 1,
		principals:   make(map[string]*principalBodyLimit),
	}
	releaseA, err := l.acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	releaseB, err := l.acquire(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := l.acquire(context.Background(), "a"); !errors.Is(err, errRequestBodyBusy) {
		t.Fatalf("same-principal acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.acquire(ctx, "c"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire: %v", err)
	}
	releaseA()
	releaseB()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.principals) != 0 {
		t.Fatalf("principal entries leaked: %d", len(l.principals))
	}
}
