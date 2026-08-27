package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

const (
	defaultMaxHTTPRequestSize = int64(64 << 20)
	defaultMaxSmallBodySize   = int64(64 << 10)
	defaultMaxQueryBodySize   = int64(1 << 20)
	defaultMaxDocumentSize    = int64(16 << 20)
	defaultMaxBulkBodySize    = int64(64 << 20)
	defaultMaxAttachmentSize  = int64(64 << 20)
	hardMaxHTTPRequestSize    = int64(256 << 20)

	defaultRequestBodyBudget       = int64(256 << 20)
	defaultPrincipalBodyBudget     = int64(64 << 20)
	defaultRequestBodyReservation  = int64(64 << 10)
	defaultRequestBodyQueueLimit   = 128
	defaultPrincipalBodyQueueLimit = 32
	defaultRequestBodyQueueTimeout = 250 * time.Millisecond
)

type requestBodyClass uint8

const (
	bodyQuery requestBodyClass = iota
	bodySmall
	bodyDocument
	bodyBulk
	bodyAttachment
)

func (c requestBodyClass) expensive() bool {
	return c == bodyDocument || c == bodyBulk || c == bodyAttachment
}

type requestBodyLimits struct {
	global     int64
	small      int64
	query      int64
	document   int64
	bulk       int64
	attachment int64
}

func (c *configCache) requestBodyLimits() requestBodyLimits {
	return requestBodyLimits{
		global: c.bodySize("chttpd", "max_http_request_size",
			defaultMaxHTTPRequestSize),
		small: c.bodySize("couchgres_httpd", "max_small_request_size",
			defaultMaxSmallBodySize),
		query: c.bodySize("couchgres_httpd", "max_query_request_size",
			defaultMaxQueryBodySize),
		document: c.bodySize("couchgres_httpd", "max_document_request_size",
			defaultMaxDocumentSize),
		bulk: c.bodySize("couchgres_httpd", "max_bulk_request_size",
			defaultMaxBulkBodySize),
		attachment: c.bodySize("couchgres_httpd", "max_attachment_request_size",
			defaultMaxAttachmentSize),
	}
}

func (c *configCache) bodySize(section, key string, fallback int64) int64 {
	value, ok := c.get(section, key)
	if !ok {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 1 || n > hardMaxHTTPRequestSize {
		return fallback
	}
	return n
}

func validateRequestBodySizeChange(section, key, value string) error {
	if !isRequestBodySizeSetting(section, key) {
		return nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%s must be an integer", key)
	}
	if n < 1 || n > hardMaxHTTPRequestSize {
		return fmt.Errorf("%s must be between 1 and %d", key, hardMaxHTTPRequestSize)
	}
	return nil
}

func isRequestBodySizeSetting(section, key string) bool {
	if section == "chttpd" && key == "max_http_request_size" {
		return true
	}
	if section != "couchgres_httpd" {
		return false
	}
	switch key {
	case "max_small_request_size", "max_query_request_size",
		"max_document_request_size", "max_bulk_request_size",
		"max_attachment_request_size":
		return true
	default:
		return false
	}
}

func (l requestBodyLimits) forClass(class requestBodyClass) int64 {
	limit := l.query
	switch class {
	case bodySmall:
		limit = l.small
	case bodyDocument:
		limit = l.document
	case bodyBulk:
		limit = l.bulk
	case bodyAttachment:
		limit = l.attachment
	}
	if l.global < limit {
		return l.global
	}
	return limit
}

func (l requestBodyLimits) maxUpload() int64 {
	return max(l.forClass(bodyDocument), l.forClass(bodyBulk),
		l.forClass(bodyAttachment))
}

func classifyRequestBody(r *http.Request, pattern string) requestBodyClass {
	switch pattern {
	case "POST /_session",
		"POST /_cluster_setup",
		"PUT /_node/_local/_config/{section}/{key}",
		"PUT /_node/nonode@nohost/_config/{section}/{key}":
		return bodySmall
	case "POST /{db}/_bulk_docs":
		return bodyBulk
	case "POST /{db}",
		"PUT /{db}/{docid}",
		"PUT /{db}/_design/{ddoc}",
		"/{db}/{docid}",
		"/{db}/_design/{ddoc}":
		return bodyDocument
	case "/{db}/{docid}/{attname...}",
		"/{db}/_design/{ddoc}/{attname...}":
		if r.Method == http.MethodPut {
			return bodyAttachment
		}
	}
	return bodyQuery
}

// protectRequestBody applies the tighter of the global and route-class limit.
// Expensive upload routes also share byte-weighted global and per-principal
// admission budgets.
func (s *Server) protectRequestBody(
	w http.ResponseWriter,
	r *http.Request,
	pattern string,
) (func(), bool) {
	class := classifyRequestBody(r, pattern)
	limit := s.config.requestBodyLimits().forClass(class)
	if r.ContentLength > limit {
		writeError(w, requestTooLarge())
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if !class.expensive() {
		return func() {}, true
	}
	reservation := requestBodyReservation(r, limit)
	release, err := s.bodyLimiter.acquire(
		r.Context(), requestPrincipal(r), reservation)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			w.Header().Set("Retry-After", "1")
			writeError(w, couch.NewError(http.StatusServiceUnavailable,
				"service_unavailable", "too many concurrent upload requests"))
		}
		return nil, false
	}
	return release, true
}

func requestTooLarge() error {
	return couch.NewError(413, "too_large", "the request entity is too large")
}

func requestPrincipal(r *http.Request) string {
	if user := userOf(r); !user.IsAnonymous() {
		return "user:" + user.Name
	}
	return "ip:" + requestSourceIP(r)
}

type principalBodyLimit struct {
	activeBytes    int64
	activeRequests int64
	queuedRequests int64
}

type requestBodyWaiter struct {
	principal string
	bytes     int64
	queuedAt  time.Time
	ready     chan struct{}
	queued    bool
	granted   bool
}

type requestBodyLimiterConfig struct {
	budgetBytes          int64
	principalBudgetBytes int64
	queueLimit           int
	principalQueueLimit  int
	queueTimeout         time.Duration
}

type requestBodyLimiterStats struct {
	BudgetBytes          int64
	PrincipalBudgetBytes int64
	ReservationBytes     int64
	ActiveBytes          int64
	ActiveRequests       int64
	QueueLimit           int64
	PrincipalQueueLimit  int64
	QueueTimeoutMillis   int64
	QueuedRequests       int64
	MaxQueuedRequests    int64
	AdmittedRequests     uint64
	WaitedRequests       uint64
	RejectedRequests     uint64
	TimedOutRequests     uint64
	CanceledRequests     uint64
	QueueWaitMicros      uint64
}

// requestBodyLimiter bounds aggregate upload buffering and prevents one
// authenticated principal or anonymous source from taking the process budget.
// Waiters are globally FIFO, except that a principal at its own byte limit is
// skipped so it cannot head-of-line block other principals.
type requestBodyLimiter struct {
	mu                sync.Mutex
	config            requestBodyLimiterConfig
	activeBytes       int64
	activeRequests    int64
	queuedRequests    int64
	maxQueuedRequests int64
	admittedRequests  uint64
	waitedRequests    uint64
	rejectedRequests  uint64
	timedOutRequests  uint64
	canceledRequests  uint64
	queueWaitMicros   uint64
	principals        map[string]*principalBodyLimit
	waiters           []*requestBodyWaiter
}

var errRequestBodyBusy = errors.New("too many concurrent request bodies")

func newRequestBodyLimiter() *requestBodyLimiter {
	return newRequestBodyLimiterWithConfig(
		defaultRequestBodyLimiterConfig(defaultMaxHTTPRequestSize))
}

func defaultRequestBodyLimiterConfig(maxUploadBytes int64) requestBodyLimiterConfig {
	principalBudget := defaultPrincipalBodyBudget
	if maxUploadBytes > principalBudget {
		// Any body permitted by an effective upload-route limit must fit by
		// itself. Raising that limit intentionally reduces per-principal
		// concurrency for large bodies rather than making it unusable.
		principalBudget = maxUploadBytes
	}
	return requestBodyLimiterConfig{
		budgetBytes:          defaultRequestBodyBudget,
		principalBudgetBytes: principalBudget,
		queueLimit:           defaultRequestBodyQueueLimit,
		principalQueueLimit:  defaultPrincipalBodyQueueLimit,
		queueTimeout:         defaultRequestBodyQueueTimeout,
	}
}

func newRequestBodyLimiterWithConfig(config requestBodyLimiterConfig) *requestBodyLimiter {
	l := &requestBodyLimiter{principals: make(map[string]*principalBodyLimit)}
	l.configure(config)
	return l
}

func (l *requestBodyLimiter) configure(config requestBodyLimiterConfig) {
	if config.budgetBytes < 1 {
		config.budgetBytes = 1
	}
	if config.principalBudgetBytes < 1 {
		config.principalBudgetBytes = 1
	}
	if config.principalBudgetBytes > config.budgetBytes {
		config.principalBudgetBytes = config.budgetBytes
	}
	if config.queueLimit < 0 {
		config.queueLimit = 0
	}
	if config.principalQueueLimit < 0 {
		config.principalQueueLimit = 0
	}
	if config.principalQueueLimit > config.queueLimit {
		config.principalQueueLimit = config.queueLimit
	}
	if config.queueTimeout < 0 {
		config.queueTimeout = 0
	}
	l.mu.Lock()
	l.config = config
	l.dispatchLocked()
	l.mu.Unlock()
}

func requestBodyReservation(r *http.Request, limit int64) int64 {
	bytes := r.ContentLength
	if bytes < 0 || len(r.TransferEncoding) > 0 {
		// An unknown-length stream could reach the route ceiling, so account
		// for all of it before a handler starts reading.
		bytes = limit
	}
	if bytes < defaultRequestBodyReservation {
		bytes = defaultRequestBodyReservation
	}
	if remainder := bytes % defaultRequestBodyReservation; remainder != 0 {
		bytes += defaultRequestBodyReservation - remainder
	}
	return bytes
}

func (l *requestBodyLimiter) acquire(
	ctx context.Context,
	principal string,
	bytes int64,
) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bytes < 1 {
		bytes = 1
	}
	l.mu.Lock()
	if bytes > l.config.budgetBytes || bytes > l.config.principalBudgetBytes {
		l.rejectedRequests++
		l.mu.Unlock()
		return nil, errRequestBodyBusy
	}
	p := l.principalLocked(principal)
	if len(l.waiters) == 0 && l.canGrantLocked(p, bytes) {
		l.grantLocked(p, bytes, false, 0)
		l.mu.Unlock()
		return l.releaseFunc(principal, bytes), nil
	}
	if int(l.queuedRequests) >= l.config.queueLimit ||
		int(p.queuedRequests) >= l.config.principalQueueLimit ||
		l.config.queueTimeout == 0 {
		l.rejectedRequests++
		l.cleanupPrincipalLocked(principal, p)
		l.mu.Unlock()
		return nil, errRequestBodyBusy
	}
	waiter := &requestBodyWaiter{
		principal: principal,
		bytes:     bytes,
		queuedAt:  time.Now(),
		ready:     make(chan struct{}),
		queued:    true,
	}
	l.waiters = append(l.waiters, waiter)
	p.queuedRequests++
	l.queuedRequests++
	if l.queuedRequests > l.maxQueuedRequests {
		l.maxQueuedRequests = l.queuedRequests
	}
	l.dispatchLocked()
	if waiter.granted {
		l.mu.Unlock()
		return l.releaseFunc(principal, bytes), nil
	}
	timeout := l.config.queueTimeout
	l.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			l.release(principal, bytes)
			return nil, err
		}
		return l.releaseFunc(principal, bytes), nil
	case <-ctx.Done():
		if l.cancelWaiter(waiter, false) {
			l.release(principal, bytes)
		}
		return nil, ctx.Err()
	case <-timer.C:
		if l.cancelWaiter(waiter, true) {
			return l.releaseFunc(principal, bytes), nil
		}
		return nil, errRequestBodyBusy
	}
}

func (l *requestBodyLimiter) principalLocked(principal string) *principalBodyLimit {
	p := l.principals[principal]
	if p == nil {
		p = &principalBodyLimit{}
		l.principals[principal] = p
	}
	return p
}

func (l *requestBodyLimiter) canGrantLocked(p *principalBodyLimit, bytes int64) bool {
	return l.activeBytes+bytes <= l.config.budgetBytes &&
		p.activeBytes+bytes <= l.config.principalBudgetBytes
}

func (l *requestBodyLimiter) grantLocked(
	p *principalBodyLimit,
	bytes int64,
	waited bool,
	wait time.Duration,
) {
	l.activeBytes += bytes
	l.activeRequests++
	p.activeBytes += bytes
	p.activeRequests++
	l.admittedRequests++
	if waited {
		l.waitedRequests++
		l.queueWaitMicros += uint64(wait.Microseconds())
	}
}

func (l *requestBodyLimiter) dispatchLocked() {
	for len(l.waiters) > 0 {
		granted := false
		for i, waiter := range l.waiters {
			if l.activeBytes+waiter.bytes > l.config.budgetBytes {
				// Preserve global FIFO when byte pressure is the reason a
				// request cannot proceed, preventing large-body starvation.
				return
			}
			p := l.principals[waiter.principal]
			if p.activeBytes+waiter.bytes > l.config.principalBudgetBytes {
				// A saturated principal must not block a different one.
				continue
			}
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			waiter.queued = false
			waiter.granted = true
			p.queuedRequests--
			l.queuedRequests--
			l.grantLocked(p, waiter.bytes, true, time.Since(waiter.queuedAt))
			close(waiter.ready)
			granted = true
			break
		}
		if !granted {
			return
		}
	}
}

// cancelWaiter returns true when the waiter won the grant race and therefore
// owns a reservation that its caller must release.
func (l *requestBodyLimiter) cancelWaiter(waiter *requestBodyWaiter, timedOut bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if waiter.granted {
		return true
	}
	if !waiter.queued {
		return false
	}
	for i, queued := range l.waiters {
		if queued == waiter {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			break
		}
	}
	waiter.queued = false
	p := l.principals[waiter.principal]
	p.queuedRequests--
	l.queuedRequests--
	if timedOut {
		l.timedOutRequests++
		l.rejectedRequests++
	} else {
		l.canceledRequests++
	}
	l.queueWaitMicros += uint64(time.Since(waiter.queuedAt).Microseconds())
	l.cleanupPrincipalLocked(waiter.principal, p)
	l.dispatchLocked()
	return false
}

func (l *requestBodyLimiter) releaseFunc(principal string, bytes int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() { l.release(principal, bytes) })
	}
}

func (l *requestBodyLimiter) release(principal string, bytes int64) {
	l.mu.Lock()
	p := l.principals[principal]
	if p != nil {
		p.activeBytes -= bytes
		p.activeRequests--
	}
	l.activeBytes -= bytes
	l.activeRequests--
	l.cleanupPrincipalLocked(principal, p)
	l.dispatchLocked()
	l.mu.Unlock()
}

func (l *requestBodyLimiter) cleanupPrincipalLocked(
	principal string,
	p *principalBodyLimit,
) {
	if p != nil && p.activeRequests == 0 && p.queuedRequests == 0 {
		delete(l.principals, principal)
	}
}

func (l *requestBodyLimiter) stats() requestBodyLimiterStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return requestBodyLimiterStats{
		BudgetBytes:          l.config.budgetBytes,
		PrincipalBudgetBytes: l.config.principalBudgetBytes,
		ReservationBytes:     defaultRequestBodyReservation,
		ActiveBytes:          l.activeBytes,
		ActiveRequests:       l.activeRequests,
		QueueLimit:           int64(l.config.queueLimit),
		PrincipalQueueLimit:  int64(l.config.principalQueueLimit),
		QueueTimeoutMillis:   l.config.queueTimeout.Milliseconds(),
		QueuedRequests:       l.queuedRequests,
		MaxQueuedRequests:    l.maxQueuedRequests,
		AdmittedRequests:     l.admittedRequests,
		WaitedRequests:       l.waitedRequests,
		RejectedRequests:     l.rejectedRequests,
		TimedOutRequests:     l.timedOutRequests,
		CanceledRequests:     l.canceledRequests,
		QueueWaitMicros:      l.queueWaitMicros,
	}
}
