package httpapi

import (
	"encoding/json"
	"net/url"
	"strconv"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

func boolParam(q url.Values, name string, def bool) (bool, error) {
	if !q.Has(name) {
		return def, nil
	}
	switch q.Get(name) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, couch.QueryParseError(name, q.Get(name))
}

func nonNegParam(q url.Values, name string) (*int64, error) {
	if !q.Has(name) {
		return nil, nil
	}
	n, err := strconv.ParseInt(q.Get(name), 10, 64)
	if err != nil || n < 0 {
		return nil, couch.QueryParseError(name, q.Get(name))
	}
	return &n, nil
}

func revParam(q url.Values, name string) (*couch.Rev, error) {
	if !q.Has(name) {
		return nil, nil
	}
	rev, err := couch.ParseRev(q.Get(name))
	if err != nil {
		return nil, err
	}
	return &rev, nil
}

// keyPos places a JSON key relative to doc ids (strings) in CouchDB's view
// collation: null/bool/number sort before all strings, arrays/objects after.
type keyPos int

const (
	keyLow keyPos = iota
	keyStr
	keyHigh
)

func keyPosOf(v any) (keyPos, string) {
	switch t := v.(type) {
	case string:
		return keyStr, t
	case nil, bool, json.Number:
		return keyLow, ""
	default: // []any, map[string]any
		return keyHigh, ""
	}
}

// allDocsRequest is a parsed _all_docs query (GET params, POST body, or one
// entry of /queries).
type allDocsRequest struct {
	params   store.AllDocsParams
	keys     []string
	sendKeys bool
	// Bounds that exclude every string key short-circuit to an empty page.
	alwaysEmpty bool
}

// buildAllDocsRequest merges query-string parameters (JSON-encoded where
// applicable) with an optional POST body. On a collision, the query string
// wins (CouchDB: "query takes precedence").
func buildAllDocsRequest(q url.Values, body map[string]any) (*allDocsRequest, error) {
	getValue := func(names ...string) (any, bool, error) {
		for _, name := range names {
			if q.Has(name) {
				v, err := couch.DecodeJSON([]byte(q.Get(name)))
				if err != nil {
					// CouchDB shape for undecodable JSON query params.
					return nil, false, couch.BadRequest("invalid UTF-8 JSON")
				}
				return v, true, nil
			}
		}
		for _, name := range names {
			if body != nil {
				if v, ok := body[name]; ok {
					return v, true, nil
				}
			}
		}
		return nil, false, nil
	}
	getBool := func(name string, def bool) (bool, error) {
		if !q.Has(name) && body != nil {
			if v, ok := body[name]; ok {
				b, isBool := v.(bool)
				if !isBool {
					return false, couch.QueryParseError(name, "not a boolean")
				}
				return b, nil
			}
		}
		return boolParam(q, name, def)
	}
	getInt := func(name string) (*int64, error) {
		if !q.Has(name) && body != nil {
			if v, ok := body[name]; ok {
				num, isNum := v.(json.Number)
				if isNum {
					if n, err := num.Int64(); err == nil && n >= 0 {
						return &n, nil
					}
				}
				return nil, couch.QueryParseError(name, "not a non-negative integer")
			}
		}
		return nonNegParam(q, name)
	}

	req := &allDocsRequest{}
	var err error
	if req.params.InclusiveEnd, err = getBool("inclusive_end", true); err != nil {
		return nil, err
	}
	if req.params.Descending, err = getBool("descending", false); err != nil {
		return nil, err
	}
	if req.params.IncludeDocs, err = getBool("include_docs", false); err != nil {
		return nil, err
	}
	if req.params.Conflicts, err = getBool("conflicts", false); err != nil {
		return nil, err
	}
	if req.params.UpdateSeq, err = getBool("update_seq", false); err != nil {
		return nil, err
	}
	if req.params.Limit, err = getInt("limit"); err != nil {
		return nil, err
	}
	skip, err := getInt("skip")
	if err != nil {
		return nil, err
	}
	if skip != nil {
		req.params.Skip = *skip
	}
	descending := req.params.Descending

	if key, ok, err := getValue("key"); err != nil {
		return nil, err
	} else if ok {
		// ?key= is sugar for an exact single-key range.
		if pos, s := keyPosOf(key); pos == keyStr {
			req.params.StartKey = &s
			req.params.EndKey = &s
			req.params.InclusiveEnd = true
		} else {
			req.alwaysEmpty = true
		}
	} else {
		if v, ok, err := getValue("startkey", "start_key"); err != nil {
			return nil, err
		} else if ok {
			switch pos, s := keyPosOf(v); {
			case pos == keyStr:
				req.params.StartKey = &s
			// Iteration starts before (asc) / after (desc) all ids.
			case pos == keyLow && !descending, pos == keyHigh && descending:
			default:
				req.alwaysEmpty = true
			}
		}
		if v, ok, err := getValue("endkey", "end_key"); err != nil {
			return nil, err
		} else if ok {
			switch pos, s := keyPosOf(v); {
			case pos == keyStr:
				req.params.EndKey = &s
			// Iteration ends after (asc) / before (desc) all ids.
			case pos == keyHigh && !descending, pos == keyLow && descending:
			default:
				req.alwaysEmpty = true
			}
		}
	}

	if v, ok, err := getValue("keys"); err != nil {
		return nil, err
	} else if ok {
		items, isArray := v.([]any)
		if !isArray {
			// CouchDB words the error by where the value came from.
			if !q.Has("keys") {
				return nil, couch.BadRequest("`keys` body member must be an array.")
			}
			return nil, couch.BadRequest("`keys` parameter must be an array.")
		}
		req.sendKeys = true
		for _, item := range items {
			if s, isStr := item.(string); isStr {
				req.keys = append(req.keys, s)
			} else {
				// Non-string keys can never match a doc id. Stringify so the
				// row reports not_found.
				raw, _ := json.Marshal(item)
				req.keys = append(req.keys, string(raw))
			}
		}
	}
	return req, nil
}
