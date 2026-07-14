package couch

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Compares NewRevID against revs minted by a real CouchDB. There's no
// written spec for the hash, so a live server is the reference. Runs when
// COUCHGRES_LIVE_COUCH names a CouchDB base URL (with credentials).
func TestLiveCouchRevIDs(t *testing.T) {
	base := os.Getenv("COUCHGRES_LIVE_COUCH")
	if base == "" {
		t.Skip("set COUCHGRES_LIVE_COUCH=http://admin:secret@127.0.0.1:5985 to run against live CouchDB")
	}
	c := newLiveCouchClient(t, base)
	const db = "revid_check"
	c.request("DELETE", "/"+db, "", 200, 404)
	c.request("PUT", "/"+db, "", 201)
	defer c.request("DELETE", "/"+db, "", 200)

	// Fixed bodies covering every external-term-format branch: integer
	// width boundaries, the all-bytes-array STRING_EXT quirk, floats,
	// unicode, nesting, member order.
	fixed := []string{
		`{}`,
		`{"a":1}`,
		`{"a":0,"b":255,"c":256,"d":-1,"e":-256,"f":65535,"g":2147483647,"h":-2147483648,"i":2147483648,"j":-2147483649,"k":9223372036854775807,"l":-9223372036854775808}`,
		`{"big":123456789012345678901234567890,"negbig":-123456789012345678901234567890}`,
		`{"f":1.0,"g":-3.5,"h":1e2,"i":1.5e-8,"j":1.7976931348623157e308,"k":5e-324,"l":-0.0,"m":0.1}`,
		`{"s":"plain","t":"café — ß 中文 🚀","u":"line\nbreak\t\"quoted\"","v":""}`,
		`{"n":null,"t":true,"f":false}`,
		`{"bytes":[0,1,2,255],"notbytes":[0,256],"mixed":[1,"two",3.0,null,true],"empty":[],"nested":[[1,2],[3]]}`,
		`{"obj":{"z":1,"a":2},"emptyobj":{},"deep":{"l1":{"l2":{"l3":[{"x":1}]}}}}`,
		`{"z":1,"a":2,"m":3}`,
		`{"a":2,"z":1,"m":3}`,
	}

	seed := int64(4242)
	if env := os.Getenv("REVID_GATE_SEED"); env != "" {
		parsed, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			t.Fatalf("REVID_GATE_SEED: %v", err)
		}
		seed = parsed
	}
	t.Logf("fuzz seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	bodies := fixed
	for i := 0; i < 150; i++ {
		bodies = append(bodies, fuzzDocJSON(rng))
	}

	for i, body := range bodies {
		docid := fmt.Sprintf("doc-%03d", i)

		// First revision.
		rev1 := c.putDoc(db, docid, "", body)
		checkRev(t, rev1, false, nil, body, nil, "rev1 of %s (%s)", docid, body)

		// Second revision: same body, parent rev1 (exercises OldStart +
		// the 16-byte OldRev binary).
		parent1, _ := ParseRev(rev1)
		rev2 := c.putDoc(db, docid, rev1, body)
		checkRev(t, rev2, false, &parent1, body, nil, "rev2 of %s (%s)", docid, body)

		// Tombstone: DELETE writes deleted=true with an empty body.
		parent2, _ := ParseRev(rev2)
		rev3 := c.deleteDoc(db, docid, rev2)
		checkRev(t, rev3, true, &parent2, "{}", nil, "tombstone of %s", docid)
	}

	// Attachments join the hash as {Name, Type, Md5} triples in document
	// order (application/x-* stays identity-encoded, so digests are over
	// the bytes we sent). z-before-a order proves it is JSON order, not
	// name order.
	attBody := func(rev string) string {
		revMember := ""
		if rev != "" {
			revMember = `"_rev":"` + rev + `",`
		}
		return `{` + revMember + `"kind":"att-doc","_attachments":{` +
			`"z.bin":{"content_type":"application/x-zed","data":"` +
			base64.StdEncoding.EncodeToString([]byte("zed bytes")) + `"},` +
			`"a.bin":{"content_type":"application/x-aay","data":"` +
			base64.StdEncoding.EncodeToString([]byte("aay bytes")) + `"}}}`
	}
	digest := func(data string) string {
		sum := md5.Sum([]byte(data))
		return "md5-" + base64.StdEncoding.EncodeToString(sum[:])
	}
	atts := []RevAtt{
		{Name: "z.bin", ContentType: "application/x-zed", Digest: digest("zed bytes")},
		{Name: "a.bin", ContentType: "application/x-aay", Digest: digest("aay bytes")},
	}
	rev1 := c.putDoc(db, "att-doc", "", attBody(""))
	checkRev(t, rev1, false, nil, attBody(""), atts, "attachment doc rev1")

	// Stub update: attachments carried by stub keep their digests.
	parent1, _ := ParseRev(rev1)
	stubBody := `{"_rev":"` + rev1 + `","kind":"att-doc-v2","_attachments":{` +
		`"z.bin":{"content_type":"application/x-zed","stub":true},` +
		`"a.bin":{"content_type":"application/x-aay","stub":true}}}`
	rev2 := c.putDoc(db, "att-doc", "", stubBody)
	checkRev(t, rev2, false, &parent1, stubBody, atts, "attachment stub rev2")

	// Standalone attachment PUT: CouchDB prepends the new attachment to
	// the stored list and hashes the unchanged body.
	parent2, _ := ParseRev(rev2)
	resp := c.request("PUT", "/"+db+"/att-doc/n.bin?rev="+rev2, "new bytes", 201)
	var out struct {
		Rev string `json:"rev"`
	}
	json.Unmarshal(resp, &out)
	standaloneAtts := append([]RevAtt{
		{Name: "n.bin", ContentType: "application/octet-stream", Digest: digest("new bytes")},
	}, atts...)
	checkRev(t, out.Rev, false, &parent2, `{"kind":"att-doc-v2"}`, standaloneAtts,
		"standalone attachment PUT rev3")
}

func checkRev(t *testing.T, got string, deleted bool, parent *Rev, rawDoc string,
	atts []RevAtt, format string, args ...any) {
	t.Helper()
	want, err := NewRevID(deleted, parent, []byte(rawDoc), atts)
	if err != nil {
		t.Fatalf("NewRevID for "+format+": %v", append(args, err)...)
	}
	rev, err := ParseRev(got)
	if err != nil {
		t.Fatalf("couch rev %q for "+format+": %v", append([]any{got}, append(args, err)...)...)
	}
	if rev.Hash != want {
		t.Errorf("rev mismatch for "+format+": couch %s, ours %s", append(args, got, want)...)
	}
}

// fuzzDocJSON builds a random document as raw JSON text so member order is
// under our control.
func fuzzDocJSON(rng *rand.Rand) string {
	var b strings.Builder
	fuzzObject(rng, &b, 0)
	return b.String()
}

func fuzzObject(rng *rand.Rand, b *strings.Builder, depth int) {
	b.WriteByte('{')
	n := rng.Intn(6)
	seen := map[string]bool{}
	wrote := 0
	for i := 0; i < n; i++ {
		key := fuzzKey(rng)
		if seen[key] { // jiffy keeps duplicates. JSONB does not, so skip them.
			continue
		}
		seen[key] = true
		if wrote > 0 {
			b.WriteByte(',')
		}
		wrote++
		raw, _ := json.Marshal(key)
		b.Write(raw)
		b.WriteByte(':')
		fuzzValue(rng, b, depth+1)
	}
	b.WriteByte('}')
}

func fuzzKey(rng *rand.Rand) string {
	keys := []string{"a", "b", "zulu", "key with space", "ünïcode", "中文键", "x-9", "UPPER"}
	return keys[rng.Intn(len(keys))]
}

func fuzzValue(rng *rand.Rand, b *strings.Builder, depth int) {
	choices := 8
	if depth >= 3 {
		choices = 6 // no more nesting
	}
	switch rng.Intn(choices) {
	case 0:
		ints := []string{"0", "1", "42", "255", "256", "-1", "-255", "-256",
			"65536", "2147483647", "2147483648", "-2147483648", "-2147483649",
			"9223372036854775807", "98765432109876543210987654321"}
		b.WriteString(ints[rng.Intn(len(ints))])
	case 1:
		floats := []string{"0.5", "-0.5", "1.0", "3.14159", "1e3", "-2.5e-4",
			"1.7976931348623157e308", "5e-324", "123456.789"}
		b.WriteString(floats[rng.Intn(len(floats))])
	case 2:
		strs := []string{"", "plain", "with \"quotes\"", "tab\there",
			"café", "中文文本", "emoji 🎉", "back\\slash", "line\nbreak"}
		raw, _ := json.Marshal(strs[rng.Intn(len(strs))])
		b.Write(raw)
	case 3:
		b.WriteString([]string{"null", "true", "false"}[rng.Intn(3)])
	case 4: // array of small ints: the STRING_EXT encoding
		b.WriteByte('[')
		n := rng.Intn(6)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(b, "%d", rng.Intn(256))
		}
		b.WriteByte(']')
	case 5:
		b.WriteString([]string{"[]", "{}"}[rng.Intn(2)])
	case 6: // mixed array
		b.WriteByte('[')
		n := 1 + rng.Intn(4)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			fuzzValue(rng, b, depth+1)
		}
		b.WriteByte(']')
	case 7:
		fuzzObject(rng, b, depth+1)
	}
}

// ---------------------------------------------------------------------------
// Minimal HTTP client for a live CouchDB instance.
// ---------------------------------------------------------------------------

type liveCouchClient struct {
	t    *testing.T
	base string
	auth string
}

func newLiveCouchClient(t *testing.T, rawURL string) *liveCouchClient {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("COUCHGRES_LIVE_COUCH: %v", err)
	}
	c := &liveCouchClient{t: t}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		cred := parsed.User.Username() + ":" + password
		c.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
		parsed.User = nil
	}
	c.base = strings.TrimRight(parsed.String(), "/")
	return c
}

func (c *liveCouchClient) request(method, path, body string, okStatuses ...int) []byte {
	c.t.Helper()
	req, err := http.NewRequest(method, c.base+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	if body != "" && !strings.Contains(path, "/att-doc/") {
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
	for _, ok := range okStatuses {
		if resp.StatusCode == ok {
			return buf.Bytes()
		}
	}
	c.t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, buf.String())
	return nil
}

func (c *liveCouchClient) putDoc(db, docid, parentRev, body string) string {
	c.t.Helper()
	path := "/" + db + "/" + docid
	if parentRev != "" {
		path += "?rev=" + parentRev
	}
	raw := c.request("PUT", path, body, 201)
	var out struct {
		Rev string `json:"rev"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Rev == "" {
		c.t.Fatalf("put %s: %s", docid, raw)
	}
	return out.Rev
}

func (c *liveCouchClient) deleteDoc(db, docid, rev string) string {
	c.t.Helper()
	raw := c.request("DELETE", "/"+db+"/"+docid+"?rev="+rev, "", 200)
	var out struct {
		Rev string `json:"rev"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Rev == "" {
		c.t.Fatalf("delete %s: %s", docid, raw)
	}
	return out.Rev
}
