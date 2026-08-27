// Package replicate implements the CouchDB replication algorithm with
// couchgres as the driver. It reads the source's changes, asks the target which
// revs it lacks, copy them with new_edits=false, checkpoint on both ends.
// Every endpoint, including couchgres itself, is accessed over HTTP.
package replicate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
)

// Peer is one replication endpoint (a database URL).
type Peer struct {
	base               string // scheme://host/db, credentials stripped
	auth               string // Authorization header value, if the URL carried userinfo
	cookie             string // AuthSession value for loopback requests
	client             *http.Client
	maxResponseBytes   int64
	maxAttachmentBytes int64
}

// NewPeer parses a database URL. Credentials in the URL become a Basic
// Authorization header (and are never reported back to clients).
func NewPeer(rawURL string) (*Peer, error) {
	return newPeer(rawURL, peerConfig{})
}

func newPeer(rawURL string, cfg peerConfig) (*Peer, error) {
	cfg = cfg.withDefaults()
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, couch.BadRequest("Invalid replication endpoint")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	peer := &Peer{
		client:             newPeerHTTPClient(cfg),
		maxResponseBytes:   cfg.maxResponseBytes,
		maxAttachmentBytes: cfg.maxAttachmentBytes,
	}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		cred := parsed.User.Username() + ":" + password
		peer.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
		parsed.User = nil
	}
	if err := validatePeerURL(parsed, cfg.allowPrivateNetworks); err != nil {
		return nil, couch.BadRequest("Invalid replication endpoint: " + err.Error())
	}
	peer.base = strings.TrimRight(parsed.String(), "/")
	return peer, nil
}

// WithCookie authenticates loopback requests with a session cookie.
func (p *Peer) WithCookie(cookie string) *Peer {
	p.cookie = cookie
	return p
}

// URL is the credential-free endpoint, as reported in statuses (CouchDB
// appends a trailing slash).
func (p *Peer) URL() string {
	return p.base + "/"
}

// request builds an authenticated request without executing it.
func (p *Peer) request(ctx context.Context, method, path string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, p.base+path, nil)
	if err != nil {
		return nil, err
	}
	if p.auth != "" {
		req.Header.Set("Authorization", p.auth)
	}
	if p.cookie != "" {
		req.Header.Set("Cookie", "AuthSession="+p.cookie)
	}
	return req, nil
}

func (p *Peer) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.base+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if p.auth != "" {
		req.Header.Set("Authorization", p.auth)
	}
	if p.cookie != "" {
		req.Header.Set("Cookie", "AuthSession="+p.cookie)
	}
	return p.client.Do(req)
}

// rpc runs a JSON request. out may be nil. Statuses outside okStatuses
// become CouchDB-shaped errors.
func (p *Peer) rpc(ctx context.Context, method, path string, body, out any, okStatuses ...int) (int, error) {
	resp, err := p.do(ctx, method, path, body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := readResponseBody(resp, p.maxResponseBytes)
	if err != nil {
		return resp.StatusCode, err
	}
	ok := len(okStatuses) == 0 && resp.StatusCode < 400
	for _, status := range okStatuses {
		if resp.StatusCode == status {
			ok = true
		}
	}
	if !ok {
		var ce couch.Error
		if json.Unmarshal(raw, &ce) == nil && ce.Err != "" {
			ce.Status = resp.StatusCode
			return resp.StatusCode, &ce
		}
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if out != nil {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: invalid JSON response: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

// Exists checks the database. Create makes it (create_target).
func (p *Peer) Exists(ctx context.Context) (bool, error) {
	status, err := p.rpc(ctx, "GET", "", nil, nil, 200, 404)
	if err != nil {
		return false, err
	}
	return status == 200, nil
}

func (p *Peer) Create(ctx context.Context) error {
	_, err := p.rpc(ctx, "PUT", "", nil, nil, 201, 412)
	return err
}

// Change rows as the replicator needs them.
type Change struct {
	Seq     string
	ID      string
	Deleted bool
	Revs    []string
}

type ChangesPage struct {
	Results []Change
	LastSeq string
	Pending int64
}

// Changes reads one batch of the source feed. feed is "normal" or
// "longpoll" (continuous replication follows with longpoll batches). extra
// carries filter parameters. A non-nil body switches to POST, which is how a
// _selector filter travels).
func (p *Peer) Changes(ctx context.Context, since string, limit int, feed string, timeout time.Duration, extra url.Values, body map[string]any) (*ChangesPage, error) {
	path := fmt.Sprintf("/_changes?style=all_docs&feed=%s&limit=%d&timeout=%d",
		feed, limit, timeout.Milliseconds())
	if since != "" {
		path += "&since=" + url.QueryEscape(since)
	}
	if len(extra) > 0 {
		path += "&" + extra.Encode()
	}
	// A typed nil map must not reach rpc's `any` body parameter.
	method := "GET"
	var payload any
	if body != nil {
		method = "POST"
		payload = body
	}
	var raw struct {
		Results []struct {
			Seq     json.RawMessage `json:"seq"`
			ID      string          `json:"id"`
			Deleted bool            `json:"deleted"`
			Changes []struct {
				Rev string `json:"rev"`
			} `json:"changes"`
		} `json:"results"`
		LastSeq json.RawMessage `json:"last_seq"`
		Pending int64           `json:"pending"`
	}
	if _, err := p.rpc(ctx, method, path, payload, &raw); err != nil {
		return nil, err
	}
	page := &ChangesPage{LastSeq: seqString(raw.LastSeq), Pending: raw.Pending}
	for _, row := range raw.Results {
		change := Change{Seq: seqString(row.Seq), ID: row.ID, Deleted: row.Deleted}
		for _, c := range row.Changes {
			change.Revs = append(change.Revs, c.Rev)
		}
		page.Results = append(page.Results, change)
	}
	return page, nil
}

// seqString renders an opaque seq (string in 2.x+, number in 1.x) as text.
func seqString(raw json.RawMessage) string {
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return asString
	}
	return strings.TrimSpace(string(raw))
}

// RevsDiff asks which of the listed revs the peer is missing.
func (p *Peer) RevsDiff(ctx context.Context, revs map[string][]string) (map[string][]string, error) {
	if len(revs) == 0 {
		return nil, nil
	}
	var raw map[string]struct {
		Missing []string `json:"missing"`
	}
	if _, err := p.rpc(ctx, "POST", "/_revs_diff", revs, &raw); err != nil {
		return nil, err
	}
	missing := make(map[string][]string, len(raw))
	for id, entry := range raw {
		if len(entry.Missing) > 0 {
			missing[id] = entry.Missing
		}
	}
	return missing, nil
}

// FetchRevs pulls full documents (ancestry + inline attachments) for the
// given id->revs map via _bulk_get.
func (p *Peer) FetchRevs(ctx context.Context, missing map[string][]string) ([]map[string]any, error) {
	type docReq struct {
		ID  string `json:"id"`
		Rev string `json:"rev"`
	}
	var reqs []docReq
	for id, revs := range missing {
		for _, rev := range revs {
			reqs = append(reqs, docReq{ID: id, Rev: rev})
		}
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	var raw struct {
		Results []struct {
			Docs []struct {
				OK    map[string]any `json:"ok"`
				Error map[string]any `json:"error"`
			} `json:"docs"`
		} `json:"results"`
	}
	_, err := p.rpc(ctx, "POST", "/_bulk_get?revs=true&attachments=true&att_encoding_info=true",
		map[string]any{"docs": reqs}, &raw)
	if err != nil {
		return nil, err
	}
	var docs []map[string]any
	needEncoded := make(map[string][]string)
	for _, result := range raw.Results {
		for _, entry := range result.Docs {
			if entry.OK == nil {
				continue
			}
			// JSON inlines only decoded attachment data. Encoded (gzip)
			// attachments must be refetched as multipart so their stored
			// bytes, and therefore digests, survive the copy.
			if hasEncodedAttachments(entry.OK) {
				id, _ := entry.OK["_id"].(string)
				rev, _ := entry.OK["_rev"].(string)
				if id != "" && rev != "" {
					needEncoded[id] = append(needEncoded[id], rev)
					continue
				}
			}
			docs = append(docs, entry.OK)
		}
	}
	for id, revs := range needEncoded {
		fetched, err := p.FetchRevsMultipart(ctx, id, revs)
		if err != nil {
			return nil, err
		}
		docs = append(docs, fetched...)
	}
	return docs, nil
}

func hasEncodedAttachments(doc map[string]any) bool {
	atts, _ := doc["_attachments"].(map[string]any)
	for _, raw := range atts {
		if spec, ok := raw.(map[string]any); ok {
			if encoding, _ := spec["encoding"].(string); encoding != "" {
				return true
			}
		}
	}
	return false
}

// OpenRevs fetches every leaf of one document (doc_ids replication).
func (p *Peer) OpenRevs(ctx context.Context, id string) ([]map[string]any, error) {
	path := "/" + url.PathEscape(id) + "?open_revs=all&revs=true&attachments=true&att_encoding_info=true&latest=true"
	var raw []struct {
		OK map[string]any `json:"ok"`
	}
	status, err := p.rpc(ctx, "GET", path, nil, &raw, 200, 404)
	if err != nil || status == 404 {
		return nil, err
	}
	var docs []map[string]any
	for _, entry := range raw {
		if entry.OK != nil {
			docs = append(docs, entry.OK)
		}
	}
	return docs, nil
}

// PushDocs writes replicated documents with new_edits=false. The response
// lists only failures. Documents carrying encoded (gzip-stored) attachment
// data go one at a time as multipart/related PUTs. JSON _bulk_docs treats
// inline data as content and would store the gzip bytes verbatim.
func (p *Peer) PushDocs(ctx context.Context, docs []map[string]any) (failures int64, err error) {
	var plain []map[string]any
	for _, doc := range docs {
		if hasEncodedAttachments(doc) {
			if err := p.pushDocMultipart(ctx, doc); err != nil {
				failures++
			}
			continue
		}
		plain = append(plain, doc)
	}
	if len(plain) == 0 {
		return failures, nil
	}
	var raw []any
	_, err = p.rpc(ctx, "POST", "/_bulk_docs",
		map[string]any{"docs": plain, "new_edits": false}, &raw, 201)
	if err != nil {
		return failures, err
	}
	return failures + int64(len(raw)), nil
}

// Checkpoints: _local documents on both ends.

type Checkpoint struct {
	SessionID string           `json:"session_id"`
	LastSeq   string           `json:"source_last_seq"`
	History   []map[string]any `json:"history"`
}

func (p *Peer) GetCheckpoint(ctx context.Context, replicationID string) (*Checkpoint, error) {
	var checkpoint Checkpoint
	status, err := p.rpc(ctx, "GET", "/_local/"+replicationID, nil, &checkpoint, 200, 404)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, nil
	}
	return &checkpoint, nil
}

func (p *Peer) PutCheckpoint(ctx context.Context, replicationID string, checkpoint *Checkpoint) error {
	_, err := p.rpc(ctx, "PUT", "/_local/"+replicationID, checkpoint, nil, 201)
	return err
}

func (p *Peer) EnsureFullCommit(ctx context.Context) error {
	_, err := p.rpc(ctx, "POST", "/_ensure_full_commit", map[string]any{}, nil, 201)
	return err
}
