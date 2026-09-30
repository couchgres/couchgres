package replicate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNewPeerValidatesEndpointAndStripsCredentials(t *testing.T) {
	peer, err := NewPeer("https://alice:secret@example.com/db")
	if err != nil {
		t.Fatal(err)
	}
	if peer.URL() != "https://example.com/db/" {
		t.Fatalf("credential-free URL: %q", peer.URL())
	}
	if peer.auth == "" {
		t.Fatal("URL credentials were not converted to an authorization header")
	}

	for _, endpoint := range []string{
		"http://10.0.0.1/db", "http://172.16.0.1/db", "http://192.168.0.1/db",
		"http://[fd00::1]/db", "http://[::ffff:10.0.0.1]/db",
	} {
		if _, err := NewPeer(endpoint); err != nil {
			t.Errorf("private peer %q: %v", endpoint, err)
		}
	}

	invalid := []string{
		"ftp://example.com/db",
		"file:///tmp/db",
		"http://127.0.0.1/db",
		"http://169.254.169.254/db",
		"http://0.0.0.1/db",
		"http://100.64.0.1/db",
		"http://[::1]/db",
		"http://[::ffff:127.0.0.1]/db",
		"http://[fe80::1]/db",
		"http://[fec0::1]/db",
		"http://192.0.2.1/db",
		"https://example.com/db#fragment",
		"https://example.com/db?redirect=http://127.0.0.1",
	}
	for _, endpoint := range invalid {
		if _, err := NewPeer(endpoint); err == nil {
			t.Errorf("NewPeer(%q) succeeded", endpoint)
		}
	}

	_, err = NewPeer("ftp://alice:do-not-log@example.com/db")
	if err == nil || strings.Contains(err.Error(), "do-not-log") {
		t.Fatalf("endpoint error exposed credentials: %v", err)
	}
}

func TestPeerBlocksLoopbackDNSAtDialTime(t *testing.T) {
	peer, err := newPeer("http://localhost:1/db", peerConfig{allowPublicNetworks: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.Exists(context.Background())
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("localhost request: want ErrBlockedAddress, got %v", err)
	}
}

func TestPeerTransportLimits(t *testing.T) {
	peer, err := NewPeer("http://10.0.0.1:5984/db")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := peer.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type: %T", peer.client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("replication transport honors an environment proxy")
	}
	if transport.TLSHandshakeTimeout != 10*time.Second ||
		transport.ResponseHeaderTimeout != 45*time.Second ||
		transport.IdleConnTimeout != 90*time.Second ||
		transport.MaxResponseHeaderBytes != 1<<20 {
		t.Fatalf("transport limits: %+v", transport)
	}
}

func TestPeerRedirectPolicy(t *testing.T) {
	check := checkPeerRedirect(peerConfig{allowPublicNetworks: true})
	previous := &http.Request{URL: mustURL(t, "https://example.com/db")}

	public := &http.Request{URL: mustURL(t, "https://8.8.8.8/db"), Header: make(http.Header)}
	if err := checkPeerRedirect(peerConfig{})(public, []*http.Request{previous}); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("public redirect without opt-in: %v", err)
	}
	if err := check(public, []*http.Request{previous}); err != nil {
		t.Fatalf("public redirect with opt-in: %v", err)
	}

	for _, endpoint := range []string{"https://127.0.0.1/db", "https://169.254.169.254/db"} {
		blocked := &http.Request{URL: mustURL(t, endpoint), Header: make(http.Header)}
		if err := check(blocked, []*http.Request{previous}); !errors.Is(err, ErrBlockedAddress) {
			t.Fatalf("special-use redirect %q: %v", endpoint, err)
		}
	}

	downgrade := &http.Request{URL: mustURL(t, "http://example.com/db"), Header: make(http.Header)}
	if err := check(downgrade, []*http.Request{previous}); err == nil ||
		!strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("HTTPS downgrade: %v", err)
	}

	crossOrigin := &http.Request{
		URL: mustURL(t, "https://10.0.0.1/db"),
		Header: http.Header{
			"Authorization":       {"Basic secret"},
			"Cookie":              {"AuthSession=secret"},
			"Proxy-Authorization": {"Basic proxy-secret"},
		},
	}
	if err := check(crossOrigin, []*http.Request{previous}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
		if crossOrigin.Header.Get(name) != "" {
			t.Errorf("cross-origin redirect retained %s", name)
		}
	}

	credentialURL := &http.Request{
		URL:    mustURL(t, "https://alice:redirect-secret@example.com/db"),
		Header: make(http.Header),
	}
	if err := check(credentialURL, []*http.Request{previous}); err == nil {
		t.Fatal("credential-bearing redirect was allowed")
	}
	if strings.Contains(credentialURL.URL.String(), "redirect-secret") {
		t.Fatalf("rejected redirect retained credentials: %s", credentialURL.URL)
	}

	selfCheck := checkPeerRedirect(peerConfig{trustedSelf: true})
	self := &http.Request{URL: mustURL(t, "http://127.0.0.1:5984/db")}
	redirect := &http.Request{URL: mustURL(t, "http://127.0.0.1:5984/db/")}
	if err := selfCheck(redirect, []*http.Request{self}); err != nil {
		t.Fatalf("same-origin local redirect: %v", err)
	}
	redirect.URL = mustURL(t, "http://127.0.0.1:8000/db")
	if err := selfCheck(redirect, []*http.Request{self}); err == nil {
		t.Fatal("local redirect escaped the trusted self origin")
	}
}

func TestRPCRejectsOversizedResponse(t *testing.T) {
	peer, err := newPeer("https://example.com/db", peerConfig{maxResponseBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	peer.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: -1,
			Body:          io.NopCloser(strings.NewReader(strings.Repeat("x", 33))),
			Header:        make(http.Header),
		}, nil
	})
	if _, err := peer.rpc(context.Background(), "GET", "", nil, nil); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized response: %v", err)
	}
}

func TestResponseLimitAllowsExactSize(t *testing.T) {
	raw, err := readAllBounded(strings.NewReader("1234"), 4, ErrResponseTooLarge)
	if err != nil || string(raw) != "1234" {
		t.Fatalf("exact-sized response: raw=%q err=%v", raw, err)
	}
	if _, err := readAllBounded(strings.NewReader("12345"), 4, ErrResponseTooLarge); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("one-byte oversized response: %v", err)
	}
}

func TestMultipartAttachmentLimit(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	docHeader := make(textproto.MIMEHeader)
	docHeader.Set("Content-Type", "application/json")
	docPart, err := writer.CreatePart(docHeader)
	if err != nil {
		t.Fatal(err)
	}
	docPart.Write([]byte(`{"_attachments":{"a.bin":{"follows":true}}}`))
	attHeader := make(textproto.MIMEHeader)
	attHeader.Set("Content-Disposition", `attachment; filename="a.bin"`)
	attPart, err := writer.CreatePart(attHeader)
	if err != nil {
		t.Fatal(err)
	}
	attPart.Write([]byte("12345"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = decodeRelatedPart(&body, writer.Boundary(), 1024, 4)
	if !errors.Is(err, ErrAttachmentTooLarge) {
		t.Fatalf("oversized attachment: %v", err)
	}
}

func TestMultipartResponseLimit(t *testing.T) {
	peer, err := newPeer("https://example.com/db", peerConfig{maxResponseBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	peer.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, _ := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json"}})
		part.Write([]byte(`{"missing":"1-a"}`))
		writer.Close()
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: -1,
			Body:          io.NopCloser(bytes.NewReader(body.Bytes())),
			Header: http.Header{
				"Content-Type": {"multipart/mixed; boundary=" + writer.Boundary()},
			},
		}, nil
	})
	_, err = peer.FetchRevsMultipart(context.Background(), "doc", []string{"1-a"})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized multipart response: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
