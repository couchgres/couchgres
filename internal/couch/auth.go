package couch

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/pbkdf2"
)

const (
	// DefaultIterations is the pbkdf2-sha256 iteration count CouchDB 3.4+ uses.
	DefaultIterations = 600_000

	// Password iteration limits are a process-level safety boundary. Runtime
	// min_iterations/max_iterations settings may narrow this range, but may not
	// expand it: PBKDF2 runs synchronously and an attacker-controlled work factor
	// must never be able to monopolize a server indefinitely.
	MinPasswordIterations = 1
	MaxPasswordIterations = 1_200_000

	maxPasswordSaltBytes = 256
)

var ErrInvalidPasswordHash = errors.New("invalid password hash")

// HashedPassword holds the pbkdf2 fields as stored in _users docs.
type HashedPassword struct {
	// Scheme is "pbkdf2" (sha1, legacy) or "pbkdf2:sha256".
	Scheme     string
	DerivedKey string
	Salt       string
	Iterations int
}

// Validate checks both the stored hash shape and the configured work-factor
// range before any password derivation begins.
func (h *HashedPassword) Validate(minIterations, maxIterations int) error {
	if h == nil {
		return ErrInvalidPasswordHash
	}
	if err := ValidatePasswordIterationRange(minIterations, maxIterations); err != nil {
		return err
	}
	if h.Iterations < minIterations || h.Iterations > maxIterations {
		return fmt.Errorf("%w: iterations must be between %d and %d",
			ErrInvalidPasswordHash, minIterations, maxIterations)
	}
	derivedBytes := 0
	switch h.Scheme {
	case "pbkdf2":
		derivedBytes = sha1.Size
	case "pbkdf2:sha256":
		derivedBytes = sha256.Size
	default:
		return fmt.Errorf("%w: unsupported scheme", ErrInvalidPasswordHash)
	}
	if len(h.DerivedKey) != hex.EncodedLen(derivedBytes) {
		return fmt.Errorf("%w: invalid derived key length", ErrInvalidPasswordHash)
	}
	if _, err := hex.DecodeString(h.DerivedKey); err != nil {
		return fmt.Errorf("%w: invalid derived key encoding", ErrInvalidPasswordHash)
	}
	if h.Salt == "" || len(h.Salt) > maxPasswordSaltBytes {
		return fmt.Errorf("%w: invalid salt length", ErrInvalidPasswordHash)
	}
	return nil
}

// ValidatePasswordIterationRange validates configured PBKDF2 bounds. The hard
// maximum cannot be raised through CouchDB's runtime configuration API.
func ValidatePasswordIterationRange(minIterations, maxIterations int) error {
	if minIterations < MinPasswordIterations ||
		maxIterations < minIterations ||
		maxIterations > MaxPasswordIterations {
		return fmt.Errorf("password iterations must satisfy %d <= min <= max <= %d",
			MinPasswordIterations, MaxPasswordIterations)
	}
	return nil
}

// HashUserPassword hashes a plaintext password the way CouchDB 3.4+ does.
// Invalid work factors return a zero hash without doing any derivation. Request
// paths that need an actionable error use HashUserPasswordChecked.
func HashUserPassword(password string, iterations int) HashedPassword {
	h, _ := HashUserPasswordChecked(password, iterations)
	return h
}

func HashUserPasswordChecked(password string, iterations int) (HashedPassword, error) {
	if err := ValidatePasswordIterationRange(iterations, iterations); err != nil {
		return HashedPassword{}, err
	}
	salt := randomHexString(16)
	return HashedPassword{
		Scheme:     "pbkdf2:sha256",
		DerivedKey: deriveSHA256(password, salt, iterations),
		Salt:       salt,
		Iterations: iterations,
	}, nil
}

// VerifyPassword checks a password against _users-doc style fields,
// supporting both the legacy sha1 scheme and pbkdf2:sha256.
func VerifyPassword(password string, h *HashedPassword) bool {
	if err := h.Validate(MinPasswordIterations, MaxPasswordIterations); err != nil {
		return false
	}
	var computed string
	switch h.Scheme {
	case "pbkdf2":
		computed = deriveSHA1(password, h.Salt, h.Iterations)
	case "pbkdf2:sha256":
		computed = deriveSHA256(password, h.Salt, h.Iterations)
	default:
		return false
	}
	return subtle.ConstantTimeCompare([]byte(computed), []byte(h.DerivedKey)) == 1
}

// HashAdminPassword produces CouchDB's config-stored admin format.
// "-pbkdf2:sha256-<derived_key>,<salt>,<iterations>". Already-hashed values
// pass through unchanged after validation.
// Invalid input returns an empty value without performing password derivation.
// Configuration request paths use HashAdminPasswordChecked to report the error.
func HashAdminPassword(password string, iterations int) string {
	stored, _ := HashAdminPasswordChecked(password, iterations)
	return stored
}

func HashAdminPasswordChecked(password string, iterations int) (string, error) {
	if strings.HasPrefix(password, "-pbkdf2") {
		if _, ok := ParseAdminPassword(password); !ok {
			return "", fmt.Errorf("%w: malformed administrator hash", ErrInvalidPasswordHash)
		}
		return password, nil
	}
	// CouchDB's deprecated single-SHA1 administrator hashes have no attacker-
	// controlled work factor. Preserve the existing pass-through behavior.
	if strings.HasPrefix(password, "-hashed-") {
		if !validLegacyAdminPassword(password) {
			return "", fmt.Errorf("%w: malformed legacy administrator hash", ErrInvalidPasswordHash)
		}
		return password, nil
	}
	h, err := HashUserPasswordChecked(password, iterations)
	if err != nil {
		return "", err
	}
	return "-pbkdf2:sha256-" + h.DerivedKey + "," + h.Salt + "," +
		strconv.Itoa(h.Iterations), nil
}

func validLegacyAdminPassword(stored string) bool {
	rest, ok := strings.CutPrefix(stored, "-hashed-")
	if !ok {
		return false
	}
	derived, salt, ok := strings.Cut(rest, ",")
	if !ok || strings.ContainsRune(salt, ',') ||
		len(derived) != hex.EncodedLen(sha1.Size) ||
		salt == "" || len(salt) > maxPasswordSaltBytes {
		return false
	}
	_, err := hex.DecodeString(derived)
	return err == nil
}

// ParseAdminPassword reads a stored admin value. It accepts the sha256 form above or
// the legacy "-pbkdf2-<derived_key>,<salt>,<iterations>" (sha1).
func ParseAdminPassword(stored string) (*HashedPassword, bool) {
	scheme := ""
	rest := ""
	if r, ok := strings.CutPrefix(stored, "-pbkdf2:sha256-"); ok {
		scheme, rest = "pbkdf2:sha256", r
	} else if r, ok := strings.CutPrefix(stored, "-pbkdf2-"); ok {
		scheme, rest = "pbkdf2", r
	} else {
		return nil, false
	}
	parts := strings.Split(rest, ",")
	if len(parts) != 3 {
		return nil, false
	}
	iterations, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, false
	}
	h := &HashedPassword{
		Scheme:     scheme,
		DerivedKey: parts[0],
		Salt:       parts[1],
		Iterations: iterations,
	}
	if err := h.Validate(MinPasswordIterations, MaxPasswordIterations); err != nil {
		return nil, false
	}
	return h, true
}

func VerifyAdminPassword(password, stored string) bool {
	h, ok := ParseAdminPassword(stored)
	return ok && VerifyPassword(password, h)
}

// ---------------------------------------------------------------------------
// AuthSession cookies.
//
// Layout: base64url_nopad("name:timestamp_hex:mac_hex"), MAC = HMAC-SHA1
// keyed with server secret + user salt over "name:timestamp_hex", matching
// CouchDB's construction (a password change rotates the salt and thereby
// invalidates sessions), though not its exact byte encoding, which nothing
// external can validate anyway.
// ---------------------------------------------------------------------------

// SessionCookie is a decoded (not yet verified) AuthSession cookie.
type SessionCookie struct {
	Name     string
	IssuedAt int64 // unix seconds
	mac      string
}

func EncodeSessionCookie(secret, salt, name string, issuedAt int64) string {
	payload := name + ":" + strconv.FormatInt(issuedAt, 16)
	token := payload + ":" + sessionMAC(secret, salt, payload)
	return base64.RawURLEncoding.EncodeToString([]byte(token))
}

// DecodeSessionCookie splits a cookie into its claimed parts. The caller
// must verify it with Verify once the user's salt is known.
func DecodeSessionCookie(token string) (*SessionCookie, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, false
	}
	// Tampered tokens decode to arbitrary bytes. Only clean UTF-8 without
	// control characters can be a real cookie.
	text := string(raw)
	if !utf8.ValidString(text) || strings.ContainsFunc(text, func(r rune) bool {
		return r < 0x20
	}) {
		return nil, false
	}
	rest, mac, ok := cutLast(text, ':')
	if !ok {
		return nil, false
	}
	name, tsHex, ok := cutLast(rest, ':')
	if !ok {
		return nil, false
	}
	issuedAt, err := strconv.ParseInt(tsHex, 16, 64)
	if err != nil {
		return nil, false
	}
	return &SessionCookie{Name: name, IssuedAt: issuedAt, mac: mac}, true
}

func (c *SessionCookie) Verify(secret, salt string) bool {
	payload := c.Name + ":" + strconv.FormatInt(c.IssuedAt, 16)
	expected := sessionMAC(secret, salt, payload)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(c.mac)) == 1
}

func sessionMAC(secret, salt, payload string) string {
	mac := hmac.New(sha1.New, []byte(secret+salt))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func cutLast(s string, sep byte) (before, after string, found bool) {
	i := strings.LastIndexByte(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+1:], true
}

func deriveSHA256(password, salt string, iterations int) string {
	return hex.EncodeToString(
		pbkdf2.Key([]byte(password), []byte(salt), iterations, 32, sha256.New))
}

func deriveSHA1(password, salt string, iterations int) string {
	return hex.EncodeToString(
		pbkdf2.Key([]byte(password), []byte(salt), iterations, 20, sha1.New))
}

func randomHexString(bytes int) string {
	b := make([]byte, bytes)
	rand.Read(b)
	return hex.EncodeToString(b)
}
