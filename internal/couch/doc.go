package couch

import (
	"bytes"
	"encoding/json"
	"strings"
)

// IncomingDoc is a validated client-supplied document with metadata split out.
// body stripped of every underscore member (stored JSONB never contains
// them).
type IncomingDoc struct {
	ID          string // "" when absent
	Rev         *Rev
	Deleted     bool
	Attachments map[string]any
	Body        map[string]any
	// Revisions is the _revisions member (rev path, newest first), used by
	// replicated writes (new_edits=false).
	Revisions []Rev
}

// RevPath resolves the revision path for a new_edits=false write, newest
// first. It uses the _revisions member when present, or just the _rev.
func (d *IncomingDoc) RevPath() []Rev {
	if len(d.Revisions) > 0 {
		return d.Revisions
	}
	if d.Rev != nil {
		return []Rev{*d.Rev}
	}
	return nil
}

// allowedSpecial lists the underscore members a client may send.
var allowedSpecial = map[string]bool{
	"_id": true, "_rev": true, "_deleted": true,
	"_attachments": true, "_revisions": true,
}

// ParseDoc validates and dissects a decoded document body.
func ParseDoc(v any) (*IncomingDoc, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, BadRequest("Document must be a JSON object")
	}
	if err := rejectNUL(v); err != nil {
		return nil, err
	}
	doc := &IncomingDoc{Body: make(map[string]any, len(obj))}
	for k, val := range obj {
		if !strings.HasPrefix(k, "_") {
			doc.Body[k] = val
			continue
		}
		if !allowedSpecial[k] {
			return nil, DocValidation("Bad special document member: " + k)
		}
		switch k {
		case "_id":
			s, ok := val.(string)
			if !ok {
				return nil, NewError(400, "illegal_docid", "Document id must be a string")
			}
			doc.ID = s
		case "_rev":
			s, ok := val.(string)
			if !ok {
				return nil, BadRequest("Invalid rev format")
			}
			rev, err := ParseRev(s)
			if err != nil {
				return nil, err
			}
			doc.Rev = &rev
		case "_deleted":
			b, _ := val.(bool)
			doc.Deleted = b
		case "_attachments":
			atts, ok := val.(map[string]any)
			if !ok {
				return nil, BadRequest("_attachments must be an object")
			}
			doc.Attachments = atts
		case "_revisions":
			revisions, err := parseRevisions(val)
			if err != nil {
				return nil, err
			}
			doc.Revisions = revisions
		}
	}
	return doc, nil
}

// parseRevisions reads a _revisions member ({"start": N, "ids": [...]}) into
// a rev path, newest first.
func parseRevisions(v any) ([]Rev, error) {
	invalid := func() error { return BadRequest("Invalid _revisions member") }
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, invalid()
	}
	start, ok := obj["start"].(json.Number)
	if !ok {
		return nil, invalid()
	}
	startNum, err := start.Int64()
	if err != nil || startNum < 1 {
		return nil, invalid()
	}
	items, ok := obj["ids"].([]any)
	if !ok || len(items) == 0 || int64(len(items)) > startNum {
		return nil, invalid()
	}
	revisions := make([]Rev, 0, len(items))
	for i, item := range items {
		hash, ok := item.(string)
		if !ok || hash == "" {
			return nil, invalid()
		}
		revisions = append(revisions, Rev{Num: int(startNum) - i, Hash: hash})
	}
	return revisions, nil
}

// rejectNUL refuses NUL characters in strings and object keys. JSONB cannot
// store them (documented divergence from CouchDB, which accepts them).
func rejectNUL(v any) error {
	switch t := v.(type) {
	case string:
		if strings.ContainsRune(t, 0) {
			return BadRequest("Strings may not contain NUL (\\u0000) characters")
		}
	case []any:
		for _, item := range t {
			if err := rejectNUL(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for k, item := range t {
			if strings.ContainsRune(k, 0) {
				return BadRequest("Strings may not contain NUL (\\u0000) characters")
			}
			if err := rejectNUL(item); err != nil {
				return err
			}
		}
	}
	return nil
}

// CanonicalBody serializes a body map deterministically for rev hashing.
// encoding/json sorts map keys and json.Number marshals as its original
// literal, so plain Marshal is already canonical.
func CanonicalBody(body map[string]any) []byte {
	b, err := json.Marshal(body)
	if err != nil {
		// Bodies come from DecodeJSON and contain only JSON-representable
		// values. Marshal cannot fail on them.
		panic("couch: unmarshalable document body: " + err.Error())
	}
	return b
}

// DecodeJSON decodes one JSON value with number-literal preservation
// (json.Number) and rejects trailing garbage.
func DecodeJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, InvalidJSON()
	}
	if dec.More() {
		return nil, InvalidJSON()
	}
	return v, nil
}
