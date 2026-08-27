package httpapi

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/couchgres/couchgres/internal/couch"
)

func TestProxyAuthenticationRequiresSecretByDefault(t *testing.T) {
	const secret = "proxy-shared-secret"
	server := proxyTestServer(map[string]string{"secret": secret})
	request := proxyTestRequest("192.0.2.10:4321")
	request.Header.Set("X-Auth-CouchDB-Token", proxyToken("alice", secret))
	user, err := server.proxyUser(request)
	if err != nil {
		t.Fatal(err)
	}
	if user.Name != "alice" || user.Authenticated != "proxy" ||
		len(user.Roles) != 1 || user.Roles[0] != "_admin" {
		t.Fatalf("signed proxy user: %+v", user)
	}

	request.Header.Set("X-Auth-CouchDB-Token", "incorrect")
	if _, err := server.proxyUser(request); couchStatus(err) != 401 {
		t.Fatalf("incorrect proxy token: %v", err)
	}

	// An absent secret must not silently become a known empty HMAC key.
	server = proxyTestServer(nil)
	request.Header.Set("X-Auth-CouchDB-Token", proxyToken("alice", ""))
	if _, err := server.proxyUser(request); couchStatus(err) != 401 ||
		!strings.Contains(err.Error(), "not configured securely") {
		t.Fatalf("empty proxy secret: %v", err)
	}
}

func TestUnsignedProxyAuthenticationRequiresAcknowledgementAndTrustedSource(t *testing.T) {
	secureUnsigned := map[string]string{
		"proxy_use_secret":             "false",
		"proxy_allow_insecure_headers": "true",
		"proxy_trusted_cidrs":          "192.0.2.0/24, 2001:db8::/32",
	}
	server := proxyTestServer(secureUnsigned)

	request := proxyTestRequest("192.0.2.10:4321")
	user, err := server.proxyUser(request)
	if err != nil || user == nil || user.Name != "alice" {
		t.Fatalf("trusted IPv4 proxy: user=%+v err=%v", user, err)
	}

	request = proxyTestRequest("[2001:db8::10]:4321")
	if user, err := server.proxyUser(request); err != nil || user == nil {
		t.Fatalf("trusted IPv6 proxy: user=%+v err=%v", user, err)
	}

	// Forwarded headers never establish trust; only the direct socket peer does.
	request = proxyTestRequest("198.51.100.10:4321")
	request.Header.Set("X-Forwarded-For", "192.0.2.10")
	if _, err := server.proxyUser(request); couchStatus(err) != 401 {
		t.Fatalf("spoofed forwarded source: %v", err)
	}

	for name, settings := range map[string]map[string]string{
		"missing acknowledgement": {
			"proxy_use_secret":    "false",
			"proxy_trusted_cidrs": "192.0.2.0/24",
		},
		"missing trusted CIDRs": {
			"proxy_use_secret":             "false",
			"proxy_allow_insecure_headers": "true",
		},
		"invalid trusted CIDRs": {
			"proxy_use_secret":             "false",
			"proxy_allow_insecure_headers": "true",
			"proxy_trusted_cidrs":          "not-a-cidr",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := proxyTestServer(settings).proxyUser(
				proxyTestRequest("192.0.2.10:4321")); couchStatus(err) != 401 {
				t.Fatalf("unsafe unsigned configuration: %v", err)
			}
		})
	}
}

func TestParseProxyTrustedCIDRs(t *testing.T) {
	prefixes, err := parseProxyTrustedCIDRs(
		"192.0.2.9/24, 2001:db8::1/32\n::ffff:198.51.100.0/120")
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixes) != 3 || prefixes[0].String() != "192.0.2.0/24" ||
		prefixes[1].String() != "2001:db8::/32" ||
		prefixes[2].String() != "198.51.100.0/24" {
		t.Fatalf("normalized prefixes: %v", prefixes)
	}
	if _, err := parseProxyTrustedCIDRs("192.0.2.1"); err == nil {
		t.Fatal("address without CIDR mask was accepted")
	}
	tooMany := strings.Repeat("192.0.2.0/24,", maxProxyTrustedCIDRs+1)
	if _, err := parseProxyTrustedCIDRs(tooMany); err == nil {
		t.Fatal("oversized trusted CIDR list was accepted")
	}
}

func TestProxyAuthConfigValidation(t *testing.T) {
	for _, change := range []struct {
		section string
		key     string
		value   string
	}{
		{"chttpd_auth", "proxy_authentication", "sometimes"},
		{"couch_httpd_auth", "proxy_use_secret", "0"},
		{"couch_httpd_auth", "proxy_allow_insecure_headers", "yes"},
		{"couch_httpd_auth", "proxy_trusted_cidrs", "192.0.2.1"},
	} {
		if err := validateProxyAuthConfigChange(change.section, change.key, change.value); err == nil {
			t.Errorf("accepted invalid change: %+v", change)
		}
	}
	for _, change := range []struct {
		section string
		key     string
		value   string
	}{
		{"chttpd_auth", "proxy_authentication", "true"},
		{"couch_httpd_auth", "proxy_use_secret", "false"},
		{"couch_httpd_auth", "proxy_allow_insecure_headers", "true"},
		{"couch_httpd_auth", "proxy_trusted_cidrs", "192.0.2.0/24, 2001:db8::/32"},
	} {
		if err := validateProxyAuthConfigChange(change.section, change.key, change.value); err != nil {
			t.Errorf("rejected valid change %+v: %v", change, err)
		}
	}
}

func proxyTestServer(settings map[string]string) *Server {
	tree := map[string]map[string]string{"couch_httpd_auth": {}}
	for key, value := range settings {
		tree["couch_httpd_auth"][key] = value
	}
	return &Server{config: &configCache{tree: tree}}
}

func proxyTestRequest(remoteAddr string) *http.Request {
	request := httptest.NewRequest("GET", "/_session", nil)
	request.RemoteAddr = remoteAddr
	request.Header.Set("X-Auth-CouchDB-UserName", "alice")
	request.Header.Set("X-Auth-CouchDB-Roles", "_admin")
	return request
}

func proxyToken(name, secret string) string {
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(name))
	return hex.EncodeToString(mac.Sum(nil))
}

func couchStatus(err error) int {
	if ce, ok := err.(*couch.Error); ok {
		return ce.Status
	}
	return 0
}
