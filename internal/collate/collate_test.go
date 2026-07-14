package collate

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"sort"
	"strconv"
	"testing"
)

// compareRef is a direct implementation of CouchDB's collation rules,
// used to check that the memcomparable encoding orders identically.
func compareRef(a, b any) int {
	ra, rb := rank(a), rank(b)
	if ra != rb {
		return ra - rb
	}
	switch av := a.(type) {
	case nil:
		return 0
	case bool:
		return 0 // same rank means same value for false/false, true/true
	case json.Number:
		fa, _ := strconv.ParseFloat(av.String(), 64)
		fb, _ := strconv.ParseFloat(b.(json.Number).String(), 64)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}
		return 0
	case string:
		return CompareStrings(av, b.(string))
	case []any:
		bv := b.([]any)
		for i := 0; i < len(av) && i < len(bv); i++ {
			if c := compareRef(av[i], bv[i]); c != 0 {
				return c
			}
		}
		return len(av) - len(bv)
	case []member: // ordered object
		bv := b.([]member)
		for i := 0; i < len(av) && i < len(bv); i++ {
			if c := CompareStrings(av[i].key, bv[i].key); c != 0 {
				return c
			}
			if c := compareRef(av[i].value, bv[i].value); c != 0 {
				return c
			}
		}
		return len(av) - len(bv)
	}
	return 0
}

type member struct {
	key   string
	value any
}

func rank(v any) int {
	switch t := v.(type) {
	case nil:
		return 0
	case bool:
		if t {
			return 2
		}
		return 1
	case json.Number:
		return 3
	case string:
		return 4
	case []any:
		return 5
	case []member:
		return 6
	}
	return 7
}

// toJSON renders a test value (including ordered objects) as JSON text.
func toJSON(t *testing.T, v any) []byte {
	t.Helper()
	switch tv := v.(type) {
	case []member:
		var buf bytes.Buffer
		buf.WriteByte('{')
		for i, m := range tv {
			if i > 0 {
				buf.WriteByte(',')
			}
			k, _ := json.Marshal(m.key)
			buf.Write(k)
			buf.WriteByte(':')
			buf.Write(toJSON(t, m.value))
		}
		buf.WriteByte('}')
		return buf.Bytes()
	case []any:
		var buf bytes.Buffer
		buf.WriteByte('[')
		for i, e := range tv {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.Write(toJSON(t, e))
		}
		buf.WriteByte(']')
		return buf.Bytes()
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %v: %v", v, err)
		}
		return raw
	}
}

func num(s string) json.Number { return json.Number(s) }

// TestDocumentedOrder is the ordering example from the CouchDB view
// collation documentation.
func TestDocumentedOrder(t *testing.T) {
	ordered := []any{
		nil,
		false,
		true,
		num("1"),
		num("2"),
		num("3.0"),
		num("4"),
		"a",
		"A",
		"aa",
		"b",
		"B",
		"ba",
		"bb",
		[]any{"a"},
		[]any{"b"},
		[]any{"b", "c"},
		[]any{"b", "c", "a"},
		[]any{"b", "d"},
		[]any{"b", "d", "e"},
		[]member{{"a", num("1")}},
		[]member{{"a", num("2")}},
		[]member{{"b", num("1")}},
		[]member{{"b", num("2")}},
		[]member{{"b", num("2")}, {"a", num("1")}},
		[]member{{"b", num("2")}, {"c", num("2")}},
	}
	keys := make([][]byte, len(ordered))
	for i, v := range ordered {
		k, err := Key(toJSON(t, v))
		if err != nil {
			t.Fatalf("Key(%s): %v", toJSON(t, v), err)
		}
		keys[i] = k
	}
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Errorf("%s should sort before %s", toJSON(t, ordered[i-1]), toJSON(t, ordered[i]))
		}
	}
}

func TestNumberEdges(t *testing.T) {
	ordered := []string{
		"-1.7976931348623157e308", "-1000000", "-3.5", "-1", "-0.0001",
		"0", "1e-300", "0.5", "1", "2", "10", "1000000000000000000000",
		"1.7976931348623157e308",
	}
	var prev []byte
	for _, s := range ordered {
		k, err := Key([]byte(s))
		if err != nil {
			t.Fatalf("Key(%s): %v", s, err)
		}
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Errorf("number ordering broke at %s", s)
		}
		prev = k
	}
	zero, _ := Key([]byte("0"))
	negZero, _ := Key([]byte("-0.0"))
	if !bytes.Equal(zero, negZero) {
		t.Errorf("-0 and 0 should encode identically")
	}
}

func TestStringPrefixesAndEscapes(t *testing.T) {
	// A string must sort before its extensions, and strings containing
	// low bytes must not confuse element boundaries inside arrays.
	pairs := [][2]any{
		{"a", "ab"},
		{"", "a"},
		{[]any{"a"}, []any{"a", ""}},
		{[]any{"a", "b"}, []any{"ab"}}, // "a" < "ab" decides element 1
		{[]any{}, []any{nil}},
	}
	for _, pair := range pairs {
		ka, err := Key(toJSON(t, pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		kb, err := Key(toJSON(t, pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(ka, kb) >= 0 {
			t.Errorf("%s should sort before %s", toJSON(t, pair[0]), toJSON(t, pair[1]))
		}
	}
}

// randomValue generates arbitrary JSON values for the agreement test.
func randomValue(r *rand.Rand, depth int) any {
	strings := []string{
		"", "a", "A", "aa", "b", "apple", "Apple", "APPLE", "banana",
		"Ärger", "ärger", "zebra", "Zürich", "abc123", "ABC 123",
		"日本語", "русский", "emoji 😀", "café", "café", " ", "  ",
		"tab\there", "line\nbreak", "quote\"inside", "back\\slash",
		"control", "mixedCASE", "ümlaut", "ss", "ß",
	}
	numbers := []string{
		"0", "1", "-1", "0.5", "-0.5", "3.14159", "100", "-100",
		"1e10", "-1e10", "1e-10", "123456789012345", "0.0001",
	}
	switch n := r.Intn(14); {
	case n == 0:
		return nil
	case n == 1:
		return false
	case n == 2:
		return true
	case n < 6:
		return num(numbers[r.Intn(len(numbers))])
	case n < 10:
		return strings[r.Intn(len(strings))]
	case n < 13 && depth < 3:
		size := r.Intn(4)
		arr := make([]any, size)
		for i := range arr {
			arr[i] = randomValue(r, depth+1)
		}
		return arr
	case depth < 3:
		size := r.Intn(3)
		obj := make([]member, size)
		for i := range obj {
			obj[i] = member{strings[r.Intn(len(strings))], randomValue(r, depth+1)}
		}
		return obj
	default:
		return num(numbers[r.Intn(len(numbers))])
	}
}

// TestEncodingAgreesWithComparator: bytes.Compare(Key(a), Key(b)) must have
// the same sign as the reference comparator for arbitrary value pairs.
func TestEncodingAgreesWithComparator(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 20000; i++ {
		a := randomValue(r, 0)
		b := randomValue(r, 0)
		ja, jb := toJSON(t, a), toJSON(t, b)
		ka, err := Key(ja)
		if err != nil {
			t.Fatalf("Key(%s): %v", ja, err)
		}
		kb, err := Key(jb)
		if err != nil {
			t.Fatalf("Key(%s): %v", jb, err)
		}
		want := sign(compareRef(a, b))
		got := sign(bytes.Compare(ka, kb))
		if want != got {
			t.Fatalf("disagreement: %s vs %s: comparator %d, encoding %d",
				ja, jb, want, got)
		}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

func TestTruncateKey(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{`["a","b","c"]`, 1, `["a"]`},
		{`["a","b","c"]`, 2, `["a","b"]`},
		{`["a","b","c"]`, 5, `["a","b","c"]`},
		{`["a"]`, 0, `[]`},
		{`"scalar"`, 1, `"scalar"`},
		{`["a","b"]`, -1, `["a","b"]`},
	}
	for _, c := range cases {
		got, err := TruncateKey([]byte(c.in), c.n)
		if err != nil {
			t.Fatalf("TruncateKey(%s, %d): %v", c.in, c.n, err)
		}
		if string(got) != c.want {
			t.Errorf("TruncateKey(%s, %d) = %s, want %s", c.in, c.n, got, c.want)
		}
	}
}

// TestSortStability: sorting many encoded keys yields the comparator's order.
func TestSortStability(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	values := make([]any, 300)
	for i := range values {
		values[i] = randomValue(r, 0)
	}
	byEncoding := make([]any, len(values))
	copy(byEncoding, values)
	keyOf := func(v any) []byte {
		k, err := Key(toJSON(t, v))
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	sort.SliceStable(byEncoding, func(i, j int) bool {
		return bytes.Compare(keyOf(byEncoding[i]), keyOf(byEncoding[j])) < 0
	})
	byRef := make([]any, len(values))
	copy(byRef, values)
	sort.SliceStable(byRef, func(i, j int) bool {
		return compareRef(byRef[i], byRef[j]) < 0
	})
	for i := range byRef {
		if compareRef(byRef[i], byEncoding[i]) != 0 {
			t.Fatalf("order diverges at %d: %s vs %s",
				i, toJSON(t, byRef[i]), toJSON(t, byEncoding[i]))
		}
	}
}
