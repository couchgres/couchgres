package couch

import (
	"testing"
	"time"
)

func TestAdminPasswordRoundTrip(t *testing.T) {
	stored := HashAdminPassword("s3cret", 10)
	if !VerifyAdminPassword("s3cret", stored) {
		t.Fatal("correct password rejected")
	}
	if VerifyAdminPassword("wrong", stored) {
		t.Fatal("wrong password accepted")
	}
	if HashAdminPassword(stored, 10) != stored {
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
	h := HashUserPassword("hunter2", 10)
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

	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), nil, false, admin); err != nil {
		t.Fatalf("admin create: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:alice", doc("bob"), nil, false, admin); err == nil {
		t.Fatal("docid/name mismatch accepted")
	}
	badType := doc("bob")
	badType["type"] = "person"
	if err := ValidateUserDoc("org.couchdb.user:bob", badType, nil, false, admin); err == nil {
		t.Fatal("wrong type accepted")
	}
	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "_admin"), nil, false, admin); err == nil {
		t.Fatal("system role accepted")
	}

	// Non-admin rules.
	if err := ValidateUserDoc("org.couchdb.user:bob", doc("bob"), nil, false, bob); err != nil {
		t.Fatalf("self signup: %v", err)
	}
	err := ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), nil, false, bob)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "Only _admin may set roles" {
		t.Fatalf("role grant on create: %v", err)
	}
	err = ValidateUserDoc("org.couchdb.user:alice", doc("alice"), nil, false, bob)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "You may only update your own user document." {
		t.Fatalf("foreign doc: %v", err)
	}
	err = ValidateUserDoc("org.couchdb.user:bob", doc("bob", "staff"), doc("bob"), false, bob)
	if ce, _ := err.(*Error); ce == nil || ce.Reason != "Only _admin may edit roles" {
		t.Fatalf("role escalation: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:bob", nil, doc("bob"), true, bob); err != nil {
		t.Fatalf("self delete: %v", err)
	}
	if err := ValidateUserDoc("org.couchdb.user:alice", nil, doc("alice"), true, bob); err == nil {
		t.Fatal("foreign delete accepted")
	}
}

func TestPrepareUserDoc(t *testing.T) {
	body := map[string]any{"type": "user", "name": "bob", "roles": []any{},
		"password": "hunter2"}
	PrepareUserDoc(body, 10)
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
