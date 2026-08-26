package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"

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
// Expensive upload routes also share global and per-principal work slots.
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
	if !class.expensive() || (r.ContentLength == 0 && r.TransferEncoding == nil) {
		return func() {}, true
	}
	release, err := s.bodyLimiter.acquire(r.Context(), requestPrincipal(r))
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
	slots chan struct{}
	refs  int
}

// requestBodyLimiter bounds aggregate upload buffering and prevents one
// authenticated principal or anonymous source from taking every global slot.
type requestBodyLimiter struct {
	global       chan struct{}
	perPrincipal int
	mu           sync.Mutex
	principals   map[string]*principalBodyLimit
}

var errRequestBodyBusy = errors.New("too many concurrent request bodies")

func newRequestBodyLimiter() *requestBodyLimiter {
	global := runtime.GOMAXPROCS(0)
	if global > 4 {
		global = 4
	}
	if global < 1 {
		global = 1
	}
	perPrincipal := 2
	if global == 1 {
		perPrincipal = 1
	}
	return &requestBodyLimiter{
		global:       make(chan struct{}, global),
		perPrincipal: perPrincipal,
		principals:   make(map[string]*principalBodyLimit),
	}
}

func (l *requestBodyLimiter) acquire(ctx context.Context, principal string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	p := l.principals[principal]
	if p == nil {
		p = &principalBodyLimit{slots: make(chan struct{}, l.perPrincipal)}
		l.principals[principal] = p
	}
	p.refs++
	l.mu.Unlock()

	cleanup := func() {
		l.mu.Lock()
		p.refs--
		if p.refs == 0 {
			delete(l.principals, principal)
		}
		l.mu.Unlock()
	}
	select {
	case p.slots <- struct{}{}:
	default:
		cleanup()
		return nil, errRequestBodyBusy
	}
	select {
	case l.global <- struct{}{}:
	default:
		<-p.slots
		cleanup()
		return nil, errRequestBodyBusy
	}
	return func() {
		<-l.global
		<-p.slots
		cleanup()
	}, nil
}
