package couch

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAdminPasswordRoundTrip(t *testing.T) {
	stored, err := HashAdminPasswordChecked("s3cret", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyAdminPassword("s3cret", stored) {
		t.Fatal("correct password rejected")
	}
	if VerifyAdminPassword("wrong", stored) {
		t.Fatal("wrong password accepted")
	}
	passthrough, err := HashAdminPasswordChecked(stored, 10)
	if err != nil || passthrough != stored {
		t.Fatal("already-hashed value must pass through")
	}
}

func TestLegacySHA1Verify(t *testing.T) {
	// The format CouchDB <3.4 wrote: -pbkdf2-<sha1 dk>,<salt>,<iterations>
	dk := deriveSHA1("pass", "abcd", 10)
	stored := "-pbkdf2-" + dk + ",abcd,10"
	if !VerifyAdminPassword("pass", stored) {
		t.Fatal("legacy sha1 password rejected")
	}
	if VerifyAdminPassword("nope", stored) {
		t.Fatal("wrong legacy password accepted")
	}
}

func TestUserPasswordHashing(t *testing.T) {
	h, err := HashUserPasswordChecked("hunter2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if h.Scheme != "pbkdf2:sha256" {
		t.Fatalf("scheme: %s", h.Scheme)
	}
	if !VerifyPassword("hunter2", &h) || VerifyPassword("hunter3", &h) {
		t.Fatal("verification broken")
	}
}

func TestSessionCookieRoundTrip(t *testing.T) {
	issued := time.Now().Unix()
	token := EncodeSessionCookie("secret", "salt", "alice:with:colons", issued)
	cookie, ok := DecodeSessionCookie(token)
	if !ok || cookie.Name != "alice:with:colons" || cookie.IssuedAt != issued {
		t.Fatalf("decode: %+v %v", cookie, ok)
	}
	if !cookie.Verify("secret", "salt") {
		t.Fatal("valid cookie rejected")
	}
	if cookie.Verify("secret", "othersalt") || cookie.Verify("other", "salt") {
		t.Fatal("forged cookie accepted")
	}
	if _, ok := DecodeSessionCookie("!!notbase64!!"); ok {
		t.Fatal("garbage decoded")
	}
}

func TestSecurityObjChecks(t *testing.T) {
	ctx := func(name string, roles ...string) *UserCtx {
		return &UserCtx{Name: name, Roles: roles, Authenticated: "default"}
	}

	public := ParseSecurity(map[string]any{})
	if !public.IsDBMember(Anonymous()) {
		t.Fatal("empty security must be public")
	}
	if public.IsDBAdmin(ctx("bob")) {
		t.Fatal("bob is not an admin of a public db")
	}
	if !public.IsDBAdmin(ctx("root", "_admin")) {
		t.Fatal("server admin must be db admin")
	}

	sec := ParseSecurity(map[string]any{
		"admins":  map[string]any{"names": []any{"alice"}, "roles": []any{"ops"}},
		"members": map[string]any{"names": []any{"bob"}, "roles": []any{"readers"}},
	})
	if sec.IsDBMember(Anonymous()) {
		t.Fatal("anonymous must not be a member")
	}
	if !sec.IsDBMember(ctx("bob")) || !sec.IsDBMember(ctx("carol", "readers")) {
		t.Fatal("member by name/role broken")
	}
	if sec.IsDBMember(ctx("carol", "writers")) {
		t.Fatal("non-member accepted")
	}
	if !sec.IsDBMember(ctx("alice")) {
		t.Fatal("db admins are implicitly members")
	}
	if !sec.IsDBAdmin(ctx("dave", "ops")) || sec.IsDBAdmin(ctx("bob")) {
		t.Fatal("db admin by role broken")
	}

	if MemberRequired(Anonymous()).Status != 401 {
		t.Fatal("anonymous rejection must be 401")
	}
	if MemberRequired(ctx("bob")).Status != 403 {
		t.Fatal("named rejection must be 403")
	}
}

func TestValidateSecurity(t *testing.T) {
	ok := map[string]any{"members": map[string]any{"names": []any{"a"}}}
	if err := ValidateSecurity(ok); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []any{
		[]any{},
		map[string]any{"admins": "nope"},
		map[string]any{"members": map[string]any{"names": []any{1}}},
	} {
		if err := ValidateSecurity(bad); err == nil {
			t.Fatalf("ValidateSecurity(%v) accepted", bad)
		}
	}
}

func TestValidateUserDoc(t *testing.T) {
	admin := &UserCtx{Name: "root", Roles: []string{"_admin"}}
	bob := &UserCtx{Name: "bob"}
	doc := func(name string, roles ...any) map[string]any {
		return map[string]any{"type": "user", "name": name, "roles": roles}
	}

	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), nil, false,
		admin, MinPasswordIterations, MaxPasswordIterations); err != nil {
		t.Fatalf("admin create: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:alice", doc("bob"), nil, false,
		admin, MinPasswordIterations, MaxPasswordIterations); err == nil {
		t.Fatal("docid/name mismatch accepted")
	}
	badType := doc("bob")
	badType["type"] = "person"
	if err := ValidateUserDoc("org.couchdb.user:bob", badType, nil, false,
		admin, MinPasswordIterations, MaxPasswordIterations); err == nil {
		t.Fatal("wrong type accepted")
	}
	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "_admin"), nil, false,
		admin, MinPasswordIterations, MaxPasswordIterations); err == nil {
		t.Fatal("system role accepted")
	}

	// Non-admin rules.
	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob"), nil, false,
		bob, MinPasswordIterations, MaxPasswordIterations); err != nil {
		t.Fatalf("self signup: %v", err)
	}
	err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), nil, false,
		bob, MinPasswordIterations, MaxPasswordIterations)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "Only _admin may set roles" {
		t.Fatalf("role grant on create: %v", err)
	}
	err = ValidateUserDoc("org.couchdb.user:alice", doc("alice"), nil, false,
		bob, MinPasswordIterations, MaxPasswordIterations)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "You may only update your own user document." {
		t.Fatalf("foreign doc: %v", err)
	}
	err = ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), doc("bob"), false,
		bob, MinPasswordIterations, MaxPasswordIterations)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "Only _admin may edit roles" {
		t.Fatalf("role escalation: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:bob", nil, doc("bob"), true,
		bob, MinPasswordIterations, MaxPasswordIterations); err != nil {
		t.Fatalf("self delete: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:alice", nil, doc("alice"), true,
		bob, MinPasswordIterations, MaxPasswordIterations); err == nil {
		t.Fatal("foreign delete accepted")
	}
}

func TestPrepareUserDoc(t *testing.T) {
	body := map[string]any{"type": "user", "name": "bob", "roles": []any{},
		"password": "hunter2"}
	if err := PrepareUserDoc(body, 10); err != nil {
		t.Fatal(err)
	}
	if _, still := body["password"]; still {
		t.Fatal("plaintext password kept")
	}
	record, ok := UserRecordFromDoc(body)
	if !ok || record.Password == nil {
		t.Fatalf("record: %+v %v", record, ok)
	}
	if !VerifyPassword("hunter2", record.Password) {
		t.Fatal("hashed password does not verify")
	}
}

func TestPasswordHashValidation(t *testing.T) {
	valid, err := HashUserPasswordChecked("hunter2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := valid.Validate(1, 100); err != nil {
		t.Fatalf("valid hash rejected: %v", err)
	}

	tests := []struct {
		name string
		hash HashedPassword
	}{
		{"unknown scheme", HashedPassword{"bcrypt", valid.DerivedKey, valid.Salt, 10}},
		{"zero iterations", HashedPassword{valid.Scheme, valid.DerivedKey, valid.Salt, 0}},
		{"excessive iterations", HashedPassword{valid.Scheme, valid.DerivedKey, valid.Salt, MaxPasswordIterations + 1}},
		{"short key", HashedPassword{valid.Scheme, "00", valid.Salt, 10}},
		{"non-hex key", HashedPassword{valid.Scheme, strings.Repeat("z", 64), valid.Salt, 10}},
		{"empty salt", HashedPassword{valid.Scheme, valid.DerivedKey, "", 10}},
		{"oversized salt", HashedPassword{valid.Scheme, valid.DerivedKey, strings.Repeat("s", maxPasswordSaltBytes+1), 10}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.hash.Validate(MinPasswordIterations, MaxPasswordIterations); err == nil {
				t.Fatal("invalid hash accepted")
			}
			if VerifyPassword("hunter2", &tc.hash) {
				t.Fatal("invalid hash verified")
			}
		})
	}

	if _, err := HashUserPasswordChecked("pw", MaxPasswordIterations+1); err == nil {
		t.Fatal("unsafe hashing work factor accepted")
	}
	if got := HashUserPassword("pw", MaxPasswordIterations+1); got != (HashedPassword{}) {
		t.Fatal("unchecked hashing API did not fail closed")
	}
	badAdmin := "-pbkdf2:sha256-" + valid.DerivedKey + "," + valid.Salt + "," +
		strconv.Itoa(MaxPasswordIterations+1)
	if _, ok := ParseAdminPassword(badAdmin); ok {
		t.Fatal("unsafe administrator hash accepted")
	}
	if _, err := HashAdminPasswordChecked(badAdmin, 10); err == nil {
		t.Fatal("unsafe administrator hash passed through")
	}
	if got := HashAdminPassword(badAdmin, 10); got != "" {
		t.Fatal("unchecked administrator hashing API did not fail closed")
	}
	if _, err := HashAdminPasswordChecked("-hashed-not-a-valid-hash", 10); err == nil {
		t.Fatal("malformed legacy administrator hash passed through")
	}
}

func TestValidateUserPasswordHash(t *testing.T) {
	admin := &UserCtx{Name: "root", Roles: []string{"_admin"}}
	base := func() map[string]any {
		return map[string]any{"type": "user", "name": "bob", "roles": []any{}}
	}
	valid, err := HashUserPasswordChecked("pw", 10)
	if err != nil {
		t.Fatal(err)
	}

	doc := base()
	doc["password_scheme"] = valid.Scheme
	doc["derived_key"] = valid.DerivedKey
	doc["salt"] = valid.Salt
	doc["iterations"] = MaxPasswordIterations + 1
	if err := ValidateUserDoc(UserDocPrefix+"bob", doc, nil, false, admin,
		MinPasswordIterations, MaxPasswordIterations); err == nil {
		t.Fatal("excessive user work factor accepted")
	}
	record, ok := UserRecordFromDoc(doc)
	if !ok || record.Password != nil {
		t.Fatalf("unsafe stored record became authenticatable: %+v", record)
	}

	// A plaintext password wins over stale hash members and is safely replaced.
	doc["password"] = "new-password"
	if err := ValidateUserDoc(UserDocPrefix+"bob", doc, nil, false, admin,
		MinPasswordIterations, MaxPasswordIterations); err != nil {
		t.Fatalf("plaintext replacement rejected: %v", err)
	}
}
