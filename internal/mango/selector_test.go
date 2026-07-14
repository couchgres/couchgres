package mango

import (
	"encoding/json"
	"strings"
	"testing"
)

func parseJSON(t *testing.T, raw string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("bad JSON %s: %v", raw, err)
	}
	return v
}

func sel(t *testing.T, raw string) *Selector {
	t.Helper()
	s, err := Parse(parseJSON(t, raw))
	if err != nil {
		t.Fatalf("Parse(%s): %v", raw, err)
	}
	return s
}

// The doc set mirrors the live-CouchDB probe fixtures so every expectation
// below was verified against 3.5.2.
var probeDocs = map[string]string{
	"arr1":   `{"_id":"arr1","tags":["x","y"],"n":1}`,
	"arr2":   `{"_id":"arr2","tags":"x","n":2}`,
	"arr3":   `{"_id":"arr3","tags":["y"],"n":3}`,
	"scalar": `{"_id":"scalar","n":5,"name":"bob"}`,
	"nested": `{"_id":"nested","a":{"b":{"c":7}}}`,
}

func matchIDs(t *testing.T, selector string) map[string]bool {
	t.Helper()
	s := sel(t, selector)
	out := make(map[string]bool)
	for id, raw := range probeDocs {
		if s.Matches(parseJSON(t, raw)) {
			out[id] = true
		}
	}
	return out
}

func expect(t *testing.T, selector string, want ...string) {
	t.Helper()
	got := matchIDs(t, selector)
	wantSet := make(map[string]bool, len(want))
	for _, id := range want {
		wantSet[id] = true
	}
	for id := range got {
		if !wantSet[id] {
			t.Errorf("%s: unexpectedly matched %s", selector, id)
		}
	}
	for id := range wantSet {
		if !got[id] {
			t.Errorf("%s: should match %s", selector, id)
		}
	}
}

func TestSelectorAgainstProbeFixtures(t *testing.T) {
	expect(t, `{}`, "arr1", "arr2", "arr3", "scalar", "nested")
	// $eq is whole-value, no array element fallback.
	expect(t, `{"tags":"x"}`, "arr2")
	expect(t, `{"tags":["x","y"]}`, "arr1")
	// $in / $nin have element semantics on array fields.
	expect(t, `{"tags":{"$in":["x"]}}`, "arr1", "arr2")
	expect(t, `{"$and":[{"tags":{"$exists":true}},{"tags":{"$nin":["x"]}}]}`, "arr3")
	// Nested selectors and dotted paths are equivalent.
	expect(t, `{"a":{"b":{"c":7}}}`, "nested")
	expect(t, `{"a.b.c":7}`, "nested")
	expect(t, `{"n":{"$gt":3}}`, "scalar")
	expect(t, `{"n":{"$gte":3}}`, "arr3", "scalar")
	expect(t, `{"n":{"$lt":2}}`, "arr1")
	expect(t, `{"n":{"$ne":2}}`, "arr1", "arr3", "scalar")
	// Missing fields fail every operator except $exists:false, including $not.
	expect(t, `{"name":{"$exists":false},"n":{"$gt":0}}`, "arr1", "arr2", "arr3")
	expect(t, `{"n":{"$not":{"$gt":2}}}`, "arr1", "arr2")
	expect(t, `{"$nor":[{"n":1},{"n":2}],"n":{"$exists":true}}`, "arr3", "scalar")
	expect(t, `{"tags":{"$elemMatch":{"$eq":"y"}}}`, "arr1", "arr3")
	expect(t, `{"n":{"$elemMatch":{"$gt":0}}}`) // scalars never $elemMatch
	expect(t, `{"tags":{"$allMatch":{"$in":["x","y"]}}}`, "arr1", "arr3")
	expect(t, `{"a":{"$keyMapMatch":{"$eq":"b"}}}`, "nested")
	expect(t, `{"tags":{"$all":["x","y"]}}`, "arr1")
	expect(t, `{"tags":{"$all":["y"]}}`, "arr1", "arr3")
	expect(t, `{"tags":{"$size":2}}`, "arr1")
	expect(t, `{"n":{"$mod":[2,1]}}`, "arr1", "arr3", "scalar")
	expect(t, `{"name":{"$regex":"^b.b$"}}`, "scalar")
	expect(t, `{"n":{"$regex":"5"}}`) // $regex only matches strings
	expect(t, `{"$or":[{"n":1},{"n":5}]}`, "arr1", "scalar")
	expect(t, `{"tags":{"$ne":"x"}}`, "arr1", "arr3")
	expect(t, `{"n":{"$type":"number"}}`, "arr1", "arr2", "arr3", "scalar")
	expect(t, `{"a":{"$type":"object"}}`, "nested")
	expect(t, `{"n":{"$gt":1,"$lt":5}}`, "arr2", "arr3")
}

func TestSelectorErrors(t *testing.T) {
	cases := []struct {
		raw       string
		wantError string
	}{
		{`{"n":{"$bogus":1}}`, "invalid_operator"},
		{`{"n":{"$mod":[2.5,0]}}`, "bad_arg"},
		{`{"n":{"$mod":[2]}}`, "bad_arg"},
		{`{"n":{"$in":5}}`, "bad_arg"},
		{`{"$and":5}`, "bad_arg"},
	}
	for _, c := range cases {
		_, err := Parse(parseJSON(t, c.raw))
		if err == nil {
			t.Errorf("Parse(%s): expected error", c.raw)
			continue
		}
		if got := err.Error(); !containsStr(got, c.wantError) {
			t.Errorf("Parse(%s): error %q, want %q", c.raw, got, c.wantError)
		}
	}
	if _, err := Parse(json.Number("5")); err == nil ||
		!containsStr(err.Error(), "invalid_selector_json") {
		t.Errorf("non-object selector: %v", err)
	}
}

func containsStr(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

func TestFieldRanges(t *testing.T) {
	s := sel(t, `{"a":1,"b":{"$gt":2,"$lte":10},"$or":[{"c":3}],"d.e":{"$exists":true}}`)
	ranges := s.FieldRanges()
	if !ranges["a"].HasEq || ranges["a"].Eq.(json.Number) != "1" {
		t.Errorf("a: %+v", ranges["a"])
	}
	b := ranges["b"]
	if b.Low == nil || b.Low.Inclusive || b.High == nil || !b.High.Inclusive {
		t.Errorf("b: %+v", b)
	}
	if _, ok := ranges["c"]; ok {
		t.Errorf("$or must not contribute constraints")
	}
	if _, ok := ranges["d.e"]; !ok {
		t.Errorf("$exists true should constrain d.e")
	}
}

func TestProject(t *testing.T) {
	doc := parseJSON(t, `{"_id":"x","a":{"b":{"c":7},"z":1},"top":2}`)
	out := Project(doc, []string{"a.b.c", "_id", "missing"})
	raw, _ := json.Marshal(out)
	if string(raw) != `{"_id":"x","a":{"b":{"c":7}}}` {
		t.Errorf("Project: %s", raw)
	}
}
