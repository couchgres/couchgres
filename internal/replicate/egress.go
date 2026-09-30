package replicate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const (
	defaultMaxResponseBytes   int64 = 64 << 20
	defaultMaxAttachmentBytes int64 = 64 << 20
)

var (
	// ErrBlockedAddress reports a remote endpoint that resolves to an address
	// Couchgres does not permit replication to reach.
	ErrBlockedAddress = errors.New("replication endpoint resolves to a blocked address")

	// Prefixes which netip considers global unicast but which are not suitable
	// remote replication destinations. Method-based checks below cover loopback,
	// link-local, multicast, and unspecified ranges.
	specialUsePrefixes = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),      // this network
		netip.MustParsePrefix("100.64.0.0/10"),  // shared address space
		netip.MustParsePrefix("192.0.0.0/24"),   // IETF protocol assignments
		netip.MustParsePrefix("192.0.2.0/24"),   // documentation
		netip.MustParsePrefix("192.88.99.0/24"), // deprecated 6to4 relay anycast
		netip.MustParsePrefix("198.18.0.0/15"),  // benchmarking
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("240.0.0.0/4"),  // reserved
		netip.MustParsePrefix("64:ff9b::/96"), // NAT64 well-known prefix
		netip.MustParsePrefix("64:ff9b:1::/48"),
		netip.MustParsePrefix("100::/64"),  // discard-only
		netip.MustParsePrefix("2001::/23"), // IETF protocol assignments
		netip.MustParsePrefix("2002::/16"), // 6to4
		netip.MustParsePrefix("3fff::/20"), // documentation
		netip.MustParsePrefix("5f00::/16"), // segment-routing SIDs
		netip.MustParsePrefix("fec0::/10"), // deprecated site-local
	}
)

type peerConfig struct {
	allowPublicNetworks bool
	trustedSelf         bool // only for local database names using the server's self URL
	maxResponseBytes    int64
	maxAttachmentBytes  int64
}

func (c peerConfig) withDefaults() peerConfig {
	if c.maxResponseBytes <= 0 || c.maxResponseBytes > defaultMaxResponseBytes {
		c.maxResponseBytes = defaultMaxResponseBytes
	}
	if c.maxAttachmentBytes <= 0 || c.maxAttachmentBytes > defaultMaxAttachmentBytes {
		c.maxAttachmentBytes = defaultMaxAttachmentBytes
	}
	return c
}

func validatePeerURL(u *url.URL, cfg peerConfig) error {
	if u == nil || u.Opaque != "" || u.Host == "" || u.Hostname() == "" {
		return errors.New("endpoint must be an absolute URL")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return errors.New("endpoint scheme must be http or https")
	}
	if u.Fragment != "" {
		return errors.New("endpoint must not contain a fragment")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return errors.New("endpoint must not contain a query string")
	}
	if u.User != nil {
		return errors.New("redirect endpoint must not contain credentials")
	}
	if cfg.trustedSelf {
		return nil
	}
	if addr, err := netip.ParseAddr(u.Hostname()); err == nil && blockedAddress(addr, cfg.allowPublicNetworks) {
		return ErrBlockedAddress
	}
	return nil
}

func blockedAddress(addr netip.Addr, allowPublicNetworks bool) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() ||
		addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return true
	}
	for _, prefix := range specialUsePrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return !addr.IsPrivate() && !allowPublicNetworks
}

func newPeerHTTPClient(cfg peerConfig) *http.Client {
	dialer := &peerDialer{
		dialer: net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		},
		resolver:            net.DefaultResolver,
		allowPublicNetworks: cfg.allowPublicNetworks,
		trustedSelf:         cfg.trustedSelf,
	}
	transport := &http.Transport{
		// Deliberately do not use ProxyFromEnvironment. An HTTP proxy would make
		// the proxy, rather than the validated peer, the dial target and could
		// therefore bypass the destination policy.
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           16,
		MaxIdleConnsPerHost:    4,
		MaxConnsPerHost:        8,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  45 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
	}
	return &http.Client{
		Transport:     transport,
		Timeout:       5 * time.Minute,
		CheckRedirect: checkPeerRedirect(cfg),
	}
}

type peerDialer struct {
	dialer              net.Dialer
	resolver            *net.Resolver
	allowPublicNetworks bool
	trustedSelf         bool
}

func (d *peerDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.trustedSelf {
		return d.dialer.DialContext(ctx, network, address)
	}
	dialCtx, cancel := context.WithTimeout(ctx, d.dialer.Timeout)
	defer cancel()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid replication address: %w", err)
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if blockedAddress(addr, d.allowPublicNetworks) {
			return nil, fmt.Errorf("%w: %s", ErrBlockedAddress, host)
		}
		return d.dialer.DialContext(dialCtx, network, address)
	}

	addrs, err := d.resolver.LookupNetIP(dialCtx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolving replication endpoint %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolving replication endpoint %s: no addresses", host)
	}
	for _, addr := range addrs {
		if blockedAddress(addr, d.allowPublicNetworks) {
			return nil, fmt.Errorf("%w: %s", ErrBlockedAddress, host)
		}
	}

	var dialErrors []error
	for _, addr := range addrs {
		if network == "tcp4" && !addr.Is4() || network == "tcp6" && !addr.Is6() {
			continue
		}
		conn, err := d.dialer.DialContext(dialCtx, network,
			net.JoinHostPort(addr.String(), port))
		if err == nil {
			return conn, nil
		}
		dialErrors = append(dialErrors, err)
	}
	if len(dialErrors) == 0 {
		return nil, fmt.Errorf("replication endpoint %s has no address for %s", host, network)
	}
	return nil, fmt.Errorf("dialing replication endpoint %s: %w", host, errors.Join(dialErrors...))
}

func checkPeerRedirect(cfg peerConfig) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		// The client wraps redirect failures with req.URL. Remove rejected
		// credential/query material first so it cannot leak through job status or
		// logs supplied by a peer's Location header.
		if req.URL.User != nil {
			req.URL.User = nil
			return errors.New("unsafe replication redirect: credentials are not allowed")
		}
		if req.URL.RawQuery != "" || req.URL.ForceQuery {
			req.URL.RawQuery = ""
			req.URL.ForceQuery = false
			return errors.New("unsafe replication redirect: query strings are not allowed")
		}
		if req.URL.Fragment != "" {
			req.URL.Fragment = ""
			return errors.New("unsafe replication redirect: fragments are not allowed")
		}
		// A local peer's address-policy exception must not follow a redirect
		// away from the server's configured self endpoint.
		if cfg.trustedSelf && !sameOrigin(via[0].URL, req.URL) {
			return errors.New("unsafe replication redirect: local endpoint changed origin")
		}
		if err := validatePeerURL(req.URL, cfg); err != nil {
			return fmt.Errorf("unsafe replication redirect: %w", err)
		}
		previous := via[len(via)-1].URL
		if strings.EqualFold(previous.Scheme, "https") && strings.EqualFold(req.URL.Scheme, "http") {
			return errors.New("unsafe replication redirect: HTTPS downgrade")
		}
		if !sameOrigin(previous, req.URL) {
			req.Header.Del("Authorization")
			req.Header.Del("Cookie")
			req.Header.Del("Proxy-Authorization")
		}
		return nil
	}
}

func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "443"
	}
	return "80"
}
