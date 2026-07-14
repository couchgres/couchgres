package couch

import (
	"crypto/md5"
	"encoding/base64"
	"testing"
)

// Frozen vectors from CouchDB 3.5.2. TestLiveCouchRevIDs in
// revid_couchdb_test.go generates more. These run without a live server.
func TestNewRevIDVectors(t *testing.T) {
	rev := func(s string) *Rev {
		r, err := ParseRev(s)
		if err != nil {
			t.Fatal(err)
		}
		return &r
	}
	digest := func(data string) string {
		sum := md5.Sum([]byte(data))
		return "md5-" + base64.StdEncoding.EncodeToString(sum[:])
	}
	atts := []RevAtt{
		{Name: "z.bin", ContentType: "application/x-zed", Digest: digest("zed bytes")},
		{Name: "a.bin", ContentType: "application/x-aay", Digest: digest("aay bytes")},
	}

	cases := []struct {
		name    string
		deleted bool
		parent  *Rev
		rawDoc  string
		atts    []RevAtt
		want    string
	}{
		{"empty doc", false, nil, `{}`, nil,
			"967a00dff5e02add41819138abb3284d"},
		{"empty doc rev2", false, rev("1-967a00dff5e02add41819138abb3284d"), `{}`, nil,
			"7051cbe5c8faecd085a3fa619e6e6337"},
		{"tombstone", true, rev("2-7051cbe5c8faecd085a3fa619e6e6337"), `{}`, nil,
			"7379b9e515b161226c6559d90c4dc49f"},
		{"unicode keys + empty arrays", false, nil,
			`{"ünïcode":[],"x-9":[],"a":255}`, nil,
			"652e5812d67fdab021796340dd12c990"},
		{"string-ext array + escapes + int32 boundary", false, nil,
			`{"zulu":[3,56,225],"b":"line\nbreak","ünïcode":2147483648}`, nil,
			"75ded617a58ebe02d5480ffa49374b06"},
		{"attachments in doc order", false, nil,
			`{"kind":"att-doc","_attachments":{"z.bin":{"content_type":"application/x-zed","data":"emVkIGJ5dGVz"},"a.bin":{"content_type":"application/x-aay","data":"YWF5IGJ5dGVz"}}}`,
			atts, "90f9593018325165de53ad9a5b480bc1"},
		{"attachment stubs carry digests", false, rev("1-90f9593018325165de53ad9a5b480bc1"),
			`{"_rev":"1-90f9593018325165de53ad9a5b480bc1","kind":"att-doc-v2","_attachments":{"z.bin":{"content_type":"application/x-zed","stub":true},"a.bin":{"content_type":"application/x-aay","stub":true}}}`,
			atts, "3e19fcaf0afd45d8f7068fde7f11398e"},
	}
	for _, tc := range cases {
		got, err := NewRevID(tc.deleted, tc.parent, []byte(tc.rawDoc), tc.atts)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
