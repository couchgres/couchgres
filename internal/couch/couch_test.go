package couch

import (
	"encoding/json"
	"testing"
)

func TestValidateDBName(t *testing.T) {
	valid := []string{"mydb", "a-z0_$()+/-9", "_users", "_replicator"}
	for _, name := range valid {
		if err := ValidateDBName(name); err != nil {
			t.Errorf("ValidateDBName(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{"MyDb", "9db", "_other", "", "db!"}
	for _, name := range invalid {
		if err := ValidateDBName(name); err == nil {
			t.Errorf("ValidateDBName(%q) = nil, want error", name)
		}
	}
}

func TestValidateDocID(t *testing.T) {
	for _, id := range []string{"plain", "_design/foo", "_local/foo"} {
		if err := ValidateDocID(id); err != nil {
			t.Errorf("ValidateDocID(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range []string{"_users", ""} {
		if err := ValidateDocID(id); err == nil {
			t.Errorf("ValidateDocID(%q) = nil, want error", id)
		}
	}
}

func TestParseRev(t *testing.T) {
	rev, err := ParseRev("3-abc123")
	if err != nil || rev.Num != 3 || rev.String() != "3-abc123" {
		t.Fatalf("ParseRev round trip failed: %v %v", rev, err)
	}
	for _, bad := range []string{"nope", "0-abc", "1-", "x-abc", "1-ha sh"} {
		if _, err := ParseRev(bad); err == nil {
			t.Errorf("ParseRev(%q) = nil error, want failure", bad)
		}
	}
}

func TestNextRev(t *testing.T) {
	r1, err := NextRev(false, nil, []byte("{}"), nil)
	if err != nil {
		t.Fatal(err)
	}
	r1b, _ := NextRev(false, nil, []byte("{}"), nil)
	if r1 != r1b {
		t.Fatal("rev generation is not deterministic")
	}
	if r1.Num != 1 || len(r1.Hash) != 32 {
		t.Fatalf("unexpected first rev: %v", r1)
	}
	r2, _ := NextRev(false, &r1, []byte("{}"), nil)
	if r2.Num != 2 || r2.Hash == r1.Hash {
		t.Fatalf("unexpected chained rev: %v", r2)
	}
	del, _ := NextRev(true, &r1, []byte("{}"), nil)
	if del.Hash == r2.Hash {
		t.Fatal("deleted flag must affect the hash")
	}
	// Member order changes the hash (CouchDB semantics: the body is hashed
	// as sent, not canonicalized).
	ab, _ := NextRev(false, nil, []byte(`{"a":1,"b":2}`), nil)
	ba, _ := NextRev(false, nil, []byte(`{"b":2,"a":1}`), nil)
	if ab.Hash == ba.Hash {
		t.Fatal("member order must affect the hash")
	}
}

func mustDecode(t *testing.T, s string) any {
	t.Helper()
	v, err := DecodeJSON([]byte(s))
	if err != nil {
		t.Fatalf("DecodeJSON(%q): %v", s, err)
	}
	return v
}

func TestParseDocSplitsSpecialMembers(t *testing.T) {
	v := mustDecode(t, `{"_id":"a","_rev":"1-abc","_deleted":false,"x":1,"y":{"nested":true}}`)
	doc, err := ParseDoc(v)
	if err != nil {
		t.Fatal(err)
	}
	if doc.ID != "a" || doc.Rev == nil || doc.Rev.Num != 1 || doc.Deleted {
		t.Fatalf("metadata wrong: %+v", doc)
	}
	if len(doc.Body) != 2 {
		t.Fatalf("body should have 2 members, got %v", doc.Body)
	}
	if _, hasUnderscore := doc.Body["_id"]; hasUnderscore {
		t.Fatal("underscore member leaked into body")
	}
}

func TestParseDocRejections(t *testing.T) {
	_, err := ParseDoc(mustDecode(t, `{"_zing":1}`))
	var ce *Error
	if !errorAs(err, &ce) || ce.Err != "doc_validation" ||
		ce.Reason != "Bad special document member: _zing" {
		t.Fatalf("unknown special member: got %v", err)
	}

	for _, body := range []string{
		`{"a":"b\u0000c"}`,
		`{"a":["ok",{"deep":"\u0000"}]}`,
		`{"a\u0000b":1}`,
		`[1,2]`,
		`"str"`,
	} {
		if _, err := ParseDoc(mustDecode(t, body)); err == nil {
			t.Errorf("ParseDoc(%s) accepted, want rejection", body)
		}
	}
}

func TestDecodeJSONNumbersAndGarbage(t *testing.T) {
	v := mustDecode(t, `{"big":18446744073709551615,"dec":1.10}`)
	obj := v.(map[string]any)
	if obj["big"].(json.Number).String() != "18446744073709551615" {
		t.Fatal("large integer literal not preserved")
	}
	if obj["dec"].(json.Number).String() != "1.10" {
		t.Fatal("decimal literal not preserved")
	}
	canonical := CanonicalBody(obj)
	if string(canonical) != `{"big":18446744073709551615,"dec":1.10}` {
		t.Fatalf("canonical form unexpected: %s", canonical)
	}
	if _, err := DecodeJSON([]byte(`{"a":1} trailing`)); err == nil {
		t.Fatal("trailing garbage accepted")
	}
	if _, err := DecodeJSON([]byte(`{bad`)); err == nil {
		t.Fatal("malformed JSON accepted")
	}
}

// errorAs avoids importing errors in every assertion.
func errorAs(err error, target **Error) bool {
	if e, ok := err.(*Error); ok {
		*target = e
		return true
	}
	return false
}
