package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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
	if got := (requestBodyLimits{
		global: 256 << 20, document: 16 << 20, bulk: 64 << 20, attachment: 128 << 20,
	}).maxUpload(); got != 128<<20 {
		t.Fatalf("maximum upload limit: %d", got)
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
	limiter := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          defaultRequestBodyReservation,
		principalBudgetBytes: defaultRequestBodyReservation,
	})
	release, err := limiter.acquire(context.Background(), "ip:192.0.2.1",
		defaultRequestBodyReservation)
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
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          2 * unit,
		principalBudgetBytes: unit,
	})
	releaseA, err := l.acquire(context.Background(), "a", unit)
	if err != nil {
		t.Fatal(err)
	}
	releaseB, err := l.acquire(context.Background(), "b", unit)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := l.acquire(context.Background(), "a", unit); !errors.Is(err, errRequestBodyBusy) {
		t.Fatalf("same-principal acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.acquire(ctx, "c", unit); !errors.Is(err, context.Canceled) {
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

func TestRequestBodyReservationWeightsKnownAndStreamingBodies(t *testing.T) {
	const limit = int64(16 << 20)
	tests := []struct {
		name             string
		contentLength    int64
		transferEncoding []string
		want             int64
	}{
		{name: "empty", contentLength: 0, want: 64 << 10},
		{name: "one_kib", contentLength: 1 << 10, want: 64 << 10},
		{name: "sixty_four_kib", contentLength: 64 << 10, want: 64 << 10},
		{name: "one_mib", contentLength: 1 << 20, want: 1 << 20},
		{name: "rounded", contentLength: (1 << 20) + 1, want: (1 << 20) + (64 << 10)},
		{name: "unknown", contentLength: -1, want: limit},
		{name: "chunked", contentLength: 1, transferEncoding: []string{"chunked"}, want: limit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "/db/doc", strings.NewReader("{}"))
			r.ContentLength = tt.contentLength
			r.TransferEncoding = tt.transferEncoding
			if got := requestBodyReservation(r, limit); got != tt.want {
				t.Fatalf("reservation=%d, want %d", got, tt.want)
			}
		})
	}
}

func TestRequestBodyLimiterAllowsConcurrentSmallWrites(t *testing.T) {
	l := newRequestBodyLimiter()
	releases := make([]func(), 0, 16)
	for i := 0; i < 16; i++ {
		release, err := l.acquire(context.Background(), "same-principal",
			defaultRequestBodyReservation)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	stats := l.stats()
	if stats.ActiveRequests != 16 || stats.ActiveBytes != 16*defaultRequestBodyReservation {
		t.Fatalf("active stats: %+v", stats)
	}
	for _, release := range releases {
		release()
	}
	releases[0]() // Handler cleanup remains safe if another path already released.
	if stats := l.stats(); stats.ActiveRequests != 0 || stats.ActiveBytes != 0 {
		t.Fatalf("released stats: %+v", stats)
	}
}

func TestRequestBodyLimiterFitsRaisedRequestCeiling(t *testing.T) {
	c := &configCache{tree: map[string]map[string]string{
		"chttpd": {
			"max_http_request_size": strconv.FormatInt(hardMaxHTTPRequestSize, 10),
		},
		"couchgres_httpd": {
			"max_attachment_request_size": strconv.FormatInt(hardMaxHTTPRequestSize, 10),
		},
	}}
	l := newRequestBodyLimiterWithConfig(
		defaultRequestBodyLimiterConfig(c.requestBodyLimits().maxUpload()))
	release, err := l.acquire(context.Background(), "principal", hardMaxHTTPRequestSize)
	if err != nil {
		t.Fatalf("largest permitted body was rejected: %v", err)
	}
	release()
	stats := l.stats()
	if stats.BudgetBytes != hardMaxHTTPRequestSize ||
		stats.PrincipalBudgetBytes != hardMaxHTTPRequestSize {
		t.Fatalf("raised-ceiling stats: %+v", stats)
	}
}

func TestRequestBodyLimiterQueuesThenWakes(t *testing.T) {
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          unit,
		principalBudgetBytes: unit,
		queueLimit:           2,
		principalQueueLimit:  2,
		queueTimeout:         time.Second,
	})
	first, err := l.acquire(context.Background(), "a", unit)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		release func()
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		release, err := l.acquire(context.Background(), "a", unit)
		resultCh <- result{release: release, err: err}
	}()
	waitForQueuedBodyRequests(t, l, 1)
	first()
	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatal(got.err)
		}
		got.release()
	case <-time.After(time.Second):
		t.Fatal("queued acquire did not wake")
	}
	stats := l.stats()
	if stats.WaitedRequests != 1 || stats.RejectedRequests != 0 || stats.QueuedRequests != 0 {
		t.Fatalf("queue stats: %+v", stats)
	}
}

func TestRequestBodyLimiterDoesNotHeadOfLineBlockOtherPrincipals(t *testing.T) {
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          3 * unit,
		principalBudgetBytes: 2 * unit,
		queueLimit:           4,
		principalQueueLimit:  2,
		queueTimeout:         time.Second,
	})
	releaseA, err := l.acquire(context.Background(), "a", 2*unit)
	if err != nil {
		t.Fatal(err)
	}
	aResult := make(chan func(), 1)
	go func() {
		release, err := l.acquire(context.Background(), "a", unit)
		if err != nil {
			aResult <- nil
			return
		}
		aResult <- release
	}()
	waitForQueuedBodyRequests(t, l, 1)

	// Principal B can use the remaining global capacity even though A is at
	// the front of the queue and has exhausted its own share.
	releaseB, err := l.acquire(context.Background(), "b", unit)
	if err != nil {
		t.Fatalf("other principal was blocked: %v", err)
	}
	releaseA()
	select {
	case release := <-aResult:
		if release == nil {
			t.Fatal("queued principal failed")
		}
		release()
	case <-time.After(time.Second):
		t.Fatal("queued principal did not wake")
	}
	releaseB()
}

func TestRequestBodyLimiterPreservesGlobalFIFO(t *testing.T) {
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          4 * unit,
		principalBudgetBytes: 4 * unit,
		queueLimit:           4,
		principalQueueLimit:  2,
		queueTimeout:         time.Second,
	})
	active, err := l.acquire(context.Background(), "active", 3*unit)
	if err != nil {
		t.Fatal(err)
	}
	defer active()
	largeResult := make(chan func(), 1)
	go func() {
		release, _ := l.acquire(context.Background(), "large", 2*unit)
		largeResult <- release
	}()
	waitForQueuedBodyRequests(t, l, 1)
	smallResult := make(chan func(), 1)
	go func() {
		release, _ := l.acquire(context.Background(), "small", unit)
		smallResult <- release
	}()
	waitForQueuedBodyRequests(t, l, 2)
	select {
	case release := <-smallResult:
		if release != nil {
			release()
		}
		t.Fatal("small request bypassed an earlier globally blocked request")
	case <-time.After(10 * time.Millisecond):
	}
	active()
	for name, resultCh := range map[string]<-chan func(){
		"large": largeResult,
		"small": smallResult,
	} {
		select {
		case release := <-resultCh:
			if release == nil {
				t.Fatalf("%s request failed", name)
			}
			release()
		case <-time.After(time.Second):
			t.Fatalf("%s request did not wake", name)
		}
	}
}

func TestRequestBodyLimiterBoundsQueueAndHandlesCancellation(t *testing.T) {
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          unit,
		principalBudgetBytes: unit,
		queueLimit:           1,
		principalQueueLimit:  1,
		queueTimeout:         time.Second,
	})
	release, err := l.acquire(context.Background(), "active", unit)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := l.acquire(ctx, "queued", unit)
		resultCh <- err
	}()
	waitForQueuedBodyRequests(t, l, 1)
	if _, err := l.acquire(context.Background(), "overflow", unit); !errors.Is(err, errRequestBodyBusy) {
		t.Fatalf("queue overflow: %v", err)
	}
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled acquire did not return")
	}
	stats := l.stats()
	if stats.RejectedRequests != 1 || stats.CanceledRequests != 1 || stats.QueuedRequests != 0 {
		t.Fatalf("cancellation stats: %+v", stats)
	}
}

func TestRequestBodyLimiterTimesOutQueuedRequests(t *testing.T) {
	unit := defaultRequestBodyReservation
	l := newRequestBodyLimiterWithConfig(requestBodyLimiterConfig{
		budgetBytes:          unit,
		principalBudgetBytes: unit,
		queueLimit:           1,
		principalQueueLimit:  1,
		queueTimeout:         5 * time.Millisecond,
	})
	release, err := l.acquire(context.Background(), "active", unit)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := l.acquire(context.Background(), "queued", unit); !errors.Is(err, errRequestBodyBusy) {
		t.Fatalf("timed-out acquire: %v", err)
	}
	stats := l.stats()
	if stats.TimedOutRequests != 1 || stats.RejectedRequests != 1 ||
		stats.QueuedRequests != 0 || stats.MaxQueuedRequests != 1 {
		t.Fatalf("timeout stats: %+v", stats)
	}
}

func waitForQueuedBodyRequests(t *testing.T, l *requestBodyLimiter, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if l.stats().QueuedRequests == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued requests=%d, want %d", l.stats().QueuedRequests, want)
}
