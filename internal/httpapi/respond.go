package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
)

// couchHeaders stamps the headers CouchDB sets on every response. Clients
// are known to sniff Server for version detection. It also strips a
// trailing slash. CouchDB treats /db/ as /db, and its replicator relies
// on that.
func couchHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := w.Header()
		hdr.Set("Server", "CouchDB/"+couchDBVersion+" (Erlang OTP/26)")
		// A client-supplied request ID is echoed, like CouchDB's nonce.
		reqID := r.Header.Get("X-Couch-Request-ID")
		if reqID == "" {
			reqID = requestID()
		}
		hdr.Set("X-Couch-Request-ID", reqID)
		hdr.Set("X-CouchDB-Body-Time", "0")
		if len(r.URL.Path) > 1 && strings.HasSuffix(r.URL.Path, "/") {
			r.URL.Path = strings.TrimRight(r.URL.Path, "/")
			r.URL.RawPath = strings.TrimRight(r.URL.RawPath, "/")
		}
		next.ServeHTTP(w, r)
	})
}

// decodePlusToSpace mirrors mochiweb's path unquoting. A literal '+'
// anywhere in the path decodes to a space (an escaped %2B never does).
// CouchDB gates it on config chttpd/decode_plus_to_space, default true.
func (s *Server) decodePlusToSpace(r *http.Request) {
	esc := r.URL.EscapedPath()
	if !strings.Contains(esc, "+") || !s.config.getBool("chttpd", "decode_plus_to_space", true) {
		return
	}
	esc = strings.ReplaceAll(esc, "+", "%20")
	if p, err := url.PathUnescape(esc); err == nil {
		r.URL.RawPath = esc
		r.URL.Path = p
	}
}

func requestID() string {
	b := make([]byte, 5)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// writeJSON sends a CouchDB-style JSON response. It uses a compact body with a
// trailing newline and Cache-Control: must-revalidate.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshaling response", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json")
	if hdr.Get("Cache-Control") == "" {
		hdr.Set("Cache-Control", "must-revalidate")
	}
	w.WriteHeader(status)
	w.Write(append(body, '\n'))
}

func writeError(w http.ResponseWriter, err error) {
	if ce, ok := err.(*couch.Error); ok {
		writeJSON(w, ce.Status, ce)
		return
	}
	slog.Error("internal error", "error", err)
	writeJSON(w, 500, &couch.Error{Err: "unknown_error", Reason: err.Error()})
}

// marshalLine renders one compact JSON value for streaming feeds.
func marshalLine(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func setETag(w http.ResponseWriter, rev string) {
	w.Header().Set("ETag", `"`+rev+`"`)
}

// externalHost is the host clients reach us at. It uses the forwarded-host
// header (name configurable as chttpd/x_forwarded_host).
func (s *Server) externalHost(r *http.Request) string {
	headerName := s.config.getOr("chttpd", "x_forwarded_host", "X-Forwarded-Host")
	if forwarded := r.Header.Get(headerName); forwarded != "" {
		return forwarded
	}
	if r.Host != "" {
		return r.Host
	}
	return "localhost:5984"
}

// docURL builds the Location header value for a created document. Docids
// are escaped per path segment (slashes in _design/-style ids stay).
func (s *Server) docURL(r *http.Request, db, docid string) string {
	segments := strings.Split(docid, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return "http://" + s.externalHost(r) + "/" + db + "/" + strings.Join(segments, "/")
}

// validJSONPCallback mirrors CouchDB's callback validation. It allows word chars
// plus the characters needed for property paths.
func validJSONPCallback(cb string) bool {
	for _, c := range cb {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '$', c == '.', c == '[', c == ']':
		default:
			return false
		}
	}
	return true
}

// jsonpWriter wraps JSON responses in a callback ("/* CouchDB */cb(...);")
// when ?callback= is present and chttpd/allow_jsonp is on. Non-JSON
// responses pass through untouched.
type jsonpWriter struct {
	http.ResponseWriter
	callback    string
	wrap        bool
	wroteHeader bool
	started     bool
}

func (jw *jsonpWriter) WriteHeader(status int) {
	if jw.wroteHeader {
		return
	}
	jw.wroteHeader = true
	hdr := jw.Header()
	if ct := hdr.Get("Content-Type"); strings.HasPrefix(ct, "application/json") {
		jw.wrap = true
		hdr.Set("Content-Type", "application/javascript")
		hdr.Del("Content-Length")
	}
	jw.ResponseWriter.WriteHeader(status)
}

func (jw *jsonpWriter) Write(p []byte) (int, error) {
	if !jw.wroteHeader {
		jw.WriteHeader(http.StatusOK)
	}
	if jw.wrap && !jw.started {
		jw.started = true
		if _, err := jw.ResponseWriter.Write([]byte("/* CouchDB */" + jw.callback + "(")); err != nil {
			return 0, err
		}
	}
	return jw.ResponseWriter.Write(p)
}

func (jw *jsonpWriter) finish() {
	if jw.wrap && jw.started {
		jw.ResponseWriter.Write([]byte(");\n"))
	}
}

// Flush keeps streaming feeds (changes longpoll/continuous) working
// through the wrapper.
func (jw *jsonpWriter) Flush() {
	if f, ok := jw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
