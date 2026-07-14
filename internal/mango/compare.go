// Package mango implements CouchDB's Mango selector language. It parses and
// matches selectors, then extracts constraints for the index planner. Matching
// remains the source of truth because CouchDB index-scans and filters in memory.
package mango

import (
	"encoding/json"
	"sort"
	"strconv"

	"github.com/couchgres/couchgres/internal/collate"
)

// typeRank orders JSON types per CouchDB collation.
// null < false < true < number < string < array < object.
func typeRank(v any) int {
	switch t := v.(type) {
	case nil:
		return 0
	case bool:
		if t {
			return 2
		}
		return 1
	case json.Number, float64, int, int64:
		return 3
	case string:
		return 4
	case []any:
		return 5
	case map[string]any:
		return 6
	}
	return 7
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(t.String(), 64)
		return f, err == nil
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

// Compare orders two decoded JSON values by CouchDB collation. Object
// members compare in sorted-key order. Decoding loses the original order, but
// selectors comparing whole objects are rare enough for this to hold.
func Compare(a, b any) int {
	ra, rb := typeRank(a), typeRank(b)
	if ra != rb {
		return ra - rb
	}
	switch ra {
	case 0, 1, 2: // null, false, true: rank decided it
		return 0
	case 3:
		fa, _ := toFloat(a)
		fb, _ := toFloat(b)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}
		return 0
	case 4:
		return collate.CompareStrings(a.(string), b.(string))
	case 5:
		aa, ba := a.([]any), b.([]any)
		for i := 0; i < len(aa) && i < len(ba); i++ {
			if c := Compare(aa[i], ba[i]); c != 0 {
				return c
			}
		}
		return len(aa) - len(ba)
	case 6:
		am, bm := a.(map[string]any), b.(map[string]any)
		aKeys := sortedKeys(am)
		bKeys := sortedKeys(bm)
		for i := 0; i < len(aKeys) && i < len(bKeys); i++ {
			if c := collate.CompareStrings(aKeys[i], bKeys[i]); c != 0 {
				return c
			}
			if c := Compare(am[aKeys[i]], bm[bKeys[i]]); c != 0 {
				return c
			}
		}
		return len(aKeys) - len(bKeys)
	}
	return 0
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
