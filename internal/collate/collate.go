// Package collate encodes JSON view keys into memcomparable byte strings.
// bytes.Compare on two encoded keys reproduces CouchDB's view collation
// (null < false < true < numbers < strings < arrays < objects, strings by
// ICU root collation). Every startkey/endkey/group query then becomes a
// plain btree range scan.
package collate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// Type tags, one byte each, in CouchDB collation order. 0x00 is reserved as
// the terminator for arrays/objects (every tag sorts above it, so a shorter
// array sorts before its extensions).
const (
	tagNull   = 0x02
	tagFalse  = 0x03
	tagTrue   = 0x04
	tagNumber = 0x05
	tagString = 0x06
	tagArray  = 0x07
	tagObject = 0x08

	terminator = 0x00
)

// collators are pooled because a Collator (with its Buffer) is not safe for
// concurrent use.
var collators = sync.Pool{
	New: func() any {
		return &collatorState{c: collate.New(language.Und)}
	},
}

type collatorState struct {
	c   *collate.Collator
	buf collate.Buffer
}

// sortKey returns the ICU-root sort key of s. The result is only valid until
// the state's next use.
func (cs *collatorState) sortKey(s string) []byte {
	cs.buf.Reset()
	return cs.c.KeyFromString(&cs.buf, s)
}

// Key encodes a JSON value (raw JSON text) into its memcomparable form.
// Object member order is preserved, matching CouchDB's order-sensitive
// object comparison.
func Key(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	cs := collators.Get().(*collatorState)
	defer collators.Put(cs)
	var out []byte
	if err := encodeValue(dec, cs, &out); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return out, nil
}

// KeyOf encodes an already-decoded JSON value (as produced by
// couch.DecodeJSON: nil, bool, json.Number, string, []any,
// map[string]any). Map member order is not recoverable, so members are
// encoded in Go's marshal order (sorted). Raw-text callers should prefer Key.
func KeyOf(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Key(raw)
}

func encodeValue(dec *json.Decoder, cs *collatorState, out *[]byte) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	return encodeToken(dec, cs, out, tok)
}

func encodeToken(dec *json.Decoder, cs *collatorState, out *[]byte, tok json.Token) error {
	switch t := tok.(type) {
	case nil:
		*out = append(*out, tagNull)
	case bool:
		if t {
			*out = append(*out, tagTrue)
		} else {
			*out = append(*out, tagFalse)
		}
	case json.Number:
		f, err := strconv.ParseFloat(t.String(), 64)
		if err != nil {
			return fmt.Errorf("number %q out of range", t.String())
		}
		*out = append(*out, tagNumber)
		appendFloat(out, f)
	case string:
		encodeString(cs, out, t)
	case json.Delim:
		switch t {
		case '[':
			*out = append(*out, tagArray)
			for dec.More() {
				if err := encodeValue(dec, cs, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing ]
				return err
			}
			*out = append(*out, terminator)
		case '{':
			*out = append(*out, tagObject)
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return err
				}
				name, ok := keyTok.(string)
				if !ok {
					return fmt.Errorf("invalid object key token %v", keyTok)
				}
				encodeString(cs, out, name)
				if err := encodeValue(dec, cs, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing }
				return err
			}
			*out = append(*out, terminator)
		default:
			return fmt.Errorf("unexpected delimiter %v", t)
		}
	default:
		return fmt.Errorf("unexpected JSON token %T", tok)
	}
	return nil
}

// encodeString appends the string tag, the escaped ICU sort key, and a
// terminator. Sort keys may contain 0x00, so 0x00 escapes to 0x00 0xFF and
// the element ends with 0x00 0x00. A prefix string therefore sorts before
// its extensions, and element boundaries stay unambiguous inside arrays.
func encodeString(cs *collatorState, out *[]byte, s string) {
	*out = append(*out, tagString)
	for _, b := range cs.sortKey(s) {
		if b == 0x00 {
			*out = append(*out, 0x00, 0xFF)
		} else {
			*out = append(*out, b)
		}
	}
	*out = append(*out, 0x00, 0x00)
}

// appendFloat writes an order-preserving 8-byte encoding of f. Positive
// numbers get the sign bit set. Negative numbers are bit-inverted, so
// unsigned byte order equals numeric order. -0 normalizes to 0.
func appendFloat(out *[]byte, f float64) {
	if f == 0 {
		f = 0 // collapse -0
	}
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(bits >> (56 - 8*i))
	}
	*out = append(*out, b[:]...)
}

// CompareStrings exposes the underlying string collation (used by tests and
// collate_test's reference comparator).
func CompareStrings(a, b string) int {
	cs := collators.Get().(*collatorState)
	defer collators.Put(cs)
	return cs.c.CompareString(a, b)
}

// TruncateKey returns the raw JSON of key truncated for group_level=n.
// array keys keep their first n elements, everything else is unchanged.
// n < 0 means no truncation (group=true).
func TruncateKey(raw []byte, n int) ([]byte, error) {
	if n < 0 {
		return raw, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return raw, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	if _, err := dec.Token(); err != nil { // opening [
		return nil, err
	}
	elems := make([]json.RawMessage, 0, n)
	for dec.More() && len(elems) < n {
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			return nil, err
		}
		elems = append(elems, elem)
	}
	out, err := json.Marshal(elems)
	if err != nil {
		return nil, err
	}
	return out, nil
}
