package collate

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestLiveCouchDBOrdering compares view row order from a real CouchDB against
// sorting by this package's encoding. Runs only when COUCHGRES_LIVE_COUCH
// names a CouchDB base URL (with credentials).
func TestLiveCouchDBOrdering(t *testing.T) {
	base := os.Getenv("COUCHGRES_LIVE_COUCH")
	if base == "" {
		t.Skip("set COUCHGRES_LIVE_COUCH=http://admin:secret@127.0.0.1:5985 to run against live CouchDB")
	}
	couch := newCouchClient(t, base)
	const db = "collate_check"
	couch.request("DELETE", "/"+db, nil, 200, 404)
	couch.request("PUT", "/"+db, nil, 201)
	defer couch.request("DELETE", "/"+db, nil, 200)

	keys := testKeys(t)

	// Doc ids are fixed-width ASCII digits so the view's doc-id tiebreak
	// orders them identically under ICU and byte comparison.
	docs := make([]any, 0, len(keys)+1)
	for i, raw := range keys {
		docs = append(docs, map[string]any{
			"_id": fmt.Sprintf("d%05d", i),
			"k":   json.RawMessage(raw),
		})
	}
	docs = append(docs, map[string]any{
		"_id":   "_design/check",
		"views": map[string]any{"byk": map[string]any{"map": "function(doc){ if(doc.k !== undefined) emit(doc.k, null); }"}},
	})
	couch.request("POST", "/"+db+"/_bulk_docs", map[string]any{"docs": docs}, 201)

	var view struct {
		Rows []struct {
			ID  string          `json:"id"`
			Key json.RawMessage `json:"key"`
		} `json:"rows"`
	}
	couch.requestInto("GET", "/"+db+"/_design/check/_view/byk", nil, &view, 200)
	if len(view.Rows) != len(keys) {
		t.Fatalf("view rows: got %d, want %d", len(view.Rows), len(keys))
	}

	// Expected order: sort ids by (encoded key, id).
	type entry struct {
		id  string
		enc []byte
		raw string
	}
	expected := make([]entry, len(keys))
	for i, raw := range keys {
		enc, err := Key([]byte(raw))
		if err != nil {
			t.Fatalf("Key(%s): %v", raw, err)
		}
		expected[i] = entry{id: fmt.Sprintf("d%05d", i), enc: enc, raw: raw}
	}
	sort.SliceStable(expected, func(i, j int) bool {
		if c := bytes.Compare(expected[i].enc, expected[j].enc); c != 0 {
			return c < 0
		}
		return expected[i].id < expected[j].id
	})

	divergences := 0
	for i, row := range view.Rows {
		if row.ID != expected[i].id {
			divergences++
			if divergences <= 10 {
				t.Errorf("row %d: couchdb has %s (key %s), encoding predicts %s (key %s)",
					i, row.ID, compactJSON(row.Key), expected[i].id, expected[i].raw)
			}
		}
	}
	if divergences > 0 {
		t.Fatalf("%d/%d rows diverge from CouchDB's ordering", divergences, len(view.Rows))
	}
	t.Logf("collation: %d keys ordered identically to CouchDB", len(keys))
}

// testKeys builds the key set: hand-picked edge cases plus seeded random
// values from the same generator the unit tests use.
func testKeys(t *testing.T) []string {
	t.Helper()
	handPicked := []string{
		`null`, `false`, `true`,
		`0`, `-0.0`, `1`, `-1`, `0.5`, `-0.5`, `3`, `3.0000001`,
		`1e-300`, `-1e-300`, `1e300`, `-1e300`, `123456789012345`,
		`10`, `2`, `-100`, `0.0001`,
		`""`, `" "`, `"  "`, `"a"`, `"A"`, `"aa"`, `"aA"`, `"Aa"`, `"AA"`,
		`"b"`, `"B"`, `"ba"`, `"bb"`, `"b b"`, `"b-b"`, `"b_b"`, `"bB"`,
		`"apple"`, `"Apple"`, `"APPLE"`, `"applesauce"`, `"apple sauce"`,
		`"1"`, `"10"`, `"2"`, `"#1"`, `"~tilde"`, `"!bang"`,
		`"café"`, `"café"`, `"cafz"`, `"cafés"`,
		`"ß"`, `"ss"`, `"s"`, `"sz"`,
		`"Ärger"`, `"ärger"`, `"arger"`, `"azger"`,
		`"Zürich"`, `"zurich"`, `"zz"`,
		`"日本語"`, `"日本"`, `"中文"`, `"한국어"`,
		`"русский"`, `"Русский"`, `"абв"`,
		`"emoji 😀"`, `"emoji"`, `"émoji"`,
		`"tab\there"`, `"line\nbreak"`, `"quote\"inside"`, `"back\\slash"`,
		`[]`, `[null]`, `[false]`, `[true]`, `[0]`, `[1]`, `["a"]`, `["b"]`,
		`["a",""]`, `["a","b"]`, `["ab"]`, `["b","c"]`, `["b","c","a"]`,
		`["b","d"]`, `["b","d","e"]`, `[[]]`, `[[""]]`, `[[],[]]`,
		`[1,2]`, `[1,10]`, `[1,"a"]`, `[1,[]]`, `[1,{}]`,
		`{}`, `{"a":1}`, `{"a":2}`, `{"b":1}`, `{"b":2}`,
		`{"b":2,"a":1}`, `{"b":2,"c":2}`, `{"a":{"b":1}}`, `{"a":[1]}`,
	}
	keys := append([]string(nil), handPicked...)
	r := rand.New(rand.NewSource(20260711))
	for i := 0; i < 400; i++ {
		v := randomValue(r, 0)
		raw := toJSON(t, v)
		keys = append(keys, string(raw))
	}
	return keys
}

func compactJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if json.Compact(&buf, raw) != nil {
		return string(raw)
	}
	return buf.String()
}

// couchClient is a minimal authenticated CouchDB HTTP client.
type couchClient struct {
	t    *testing.T
	base string
	auth string
}

func newCouchClient(t *testing.T, rawURL string) *couchClient {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("COUCHGRES_LIVE_COUCH: %v", err)
	}
	c := &couchClient{t: t}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		cred := parsed.User.Username() + ":" + password
		c.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
		parsed.User = nil
	}
	c.base = strings.TrimRight(parsed.String(), "/")
	return c
}

func (c *couchClient) request(method, path string, body any, okStatuses ...int) []byte {
	c.t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	for _, status := range okStatuses {
		if resp.StatusCode == status {
			return buf.Bytes()
		}
	}
	c.t.Fatalf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, buf.String())
	return nil
}

func (c *couchClient) requestInto(method, path string, body, out any, okStatuses ...int) {
	c.t.Helper()
	raw := c.request(method, path, body, okStatuses...)
	if err := json.Unmarshal(raw, out); err != nil {
		c.t.Fatalf("%s %s: invalid JSON: %v", method, path, err)
	}
}
