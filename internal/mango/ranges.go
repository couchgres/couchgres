package mango

import "strings"

// Bound is one end of a field range constraint.
type Bound struct {
	Value     any
	Inclusive bool
}

// FieldRange is the tightest index-scannable constraint on one field that
// the top-level $and structure guarantees. The evaluator remains the source
// of truth. Ranges only narrow the scan.
type FieldRange struct {
	Eq    any
	HasEq bool
	Low   *Bound
	High  *Bound
}

// FieldRanges extracts per-field constraints from the selector's top-level
// conjunction. $or and related operators contribute nothing because post-filtering handles
// them).
func (s *Selector) FieldRanges() map[string]*FieldRange {
	out := make(map[string]*FieldRange)
	collectRanges(s.root, out)
	return out
}

// ConstrainedFields reports the fields FieldRanges knows something about.
func (s *Selector) ConstrainedFields() map[string]bool {
	ranges := s.FieldRanges()
	out := make(map[string]bool, len(ranges))
	for field := range ranges {
		out[field] = true
	}
	return out
}

func collectRanges(n node, out map[string]*FieldRange) {
	switch t := n.(type) {
	case andNode:
		for _, sub := range t {
			collectRanges(sub, out)
		}
	case fieldNode:
		field := strings.Join(t.path, ".")
		collectCond(t.cond, field, out)
	}
}

func collectCond(c cond, field string, out map[string]*FieldRange) {
	fr := out[field]
	ensure := func() *FieldRange {
		if fr == nil {
			fr = &FieldRange{}
			out[field] = fr
		}
		return fr
	}
	switch t := c.(type) {
	case opEq:
		r := ensure()
		r.Eq = t.arg
		r.HasEq = true
	case opCmp:
		r := ensure()
		switch t.op {
		case "$gt":
			tighterLow(r, &Bound{Value: t.arg})
		case "$gte":
			tighterLow(r, &Bound{Value: t.arg, Inclusive: true})
		case "$lt":
			tighterHigh(r, &Bound{Value: t.arg})
		case "$lte":
			tighterHigh(r, &Bound{Value: t.arg, Inclusive: true})
		}
	case opExists:
		if t.want {
			ensure() // constrains existence: enough to make an index usable
		}
	case condAnd:
		for _, sub := range t.subs {
			collectCond(sub, field, out)
		}
	}
}

func tighterLow(r *FieldRange, b *Bound) {
	if r.Low == nil || Compare(b.Value, r.Low.Value) > 0 ||
		(Compare(b.Value, r.Low.Value) == 0 && !b.Inclusive) {
		r.Low = b
	}
}

func tighterHigh(r *FieldRange, b *Bound) {
	if r.High == nil || Compare(b.Value, r.High.Value) < 0 ||
		(Compare(b.Value, r.High.Value) == 0 && !b.Inclusive) {
		r.High = b
	}
}

// Lookup resolves a dotted field path against a document.
func Lookup(doc map[string]any, field string) (any, bool) {
	return lookupPath(doc, strings.Split(field, "."))
}

// Normalize rewrites a selector into CouchDB's canonical form. Literals
// become {"$eq": v}, nested selectors flatten to dotted paths, and multiple
// top-level clauses wrap in {"$and": [...]}. This is the form _explain echoes and
// index definitions store).
func Normalize(obj map[string]any) map[string]any {
	clauses := normalizeClauses("", obj)
	if len(clauses) == 1 {
		return clauses[0]
	}
	items := make([]any, len(clauses))
	for i, c := range clauses {
		items[i] = c
	}
	return map[string]any{"$and": items}
}

func normalizeClauses(prefix string, obj map[string]any) []map[string]any {
	var out []map[string]any
	for _, key := range sortedKeys(obj) {
		value := obj[key]
		if strings.HasPrefix(key, "$") {
			out = append(out, map[string]any{key: normalizeCombination(key, value)})
			continue
		}
		field := key
		if prefix != "" {
			field = prefix + "." + key
		}
		if sub, ok := value.(map[string]any); ok && len(sub) > 0 && !hasOperator(sub) {
			out = append(out, normalizeClauses(field, sub)...)
			continue
		}
		out = append(out, map[string]any{field: normalizeCond(value)})
	}
	return out
}

func normalizeCombination(op string, value any) any {
	switch op {
	case "$and", "$or", "$nor":
		items, ok := value.([]any)
		if !ok {
			return value
		}
		normalized := make([]any, len(items))
		for i, item := range items {
			if obj, ok := item.(map[string]any); ok {
				normalized[i] = Normalize(obj)
			} else {
				normalized[i] = item
			}
		}
		return normalized
	case "$not":
		if obj, ok := value.(map[string]any); ok {
			return Normalize(obj)
		}
	}
	return value
}

func normalizeCond(value any) any {
	obj, ok := value.(map[string]any)
	if !ok || !hasOperator(obj) {
		return map[string]any{"$eq": value}
	}
	return obj
}

func hasOperator(obj map[string]any) bool {
	for key := range obj {
		if strings.HasPrefix(key, "$") {
			return true
		}
	}
	return false
}

// Project copies only the named field paths from doc, rebuilding nesting
// (CouchDB's `fields` option). Missing paths are skipped.
func Project(doc map[string]any, fields []string) map[string]any {
	out := make(map[string]any)
	for _, field := range fields {
		path := strings.Split(field, ".")
		value, found := lookupPath(doc, path)
		if !found {
			continue
		}
		target := out
		for _, segment := range path[:len(path)-1] {
			next, ok := target[segment].(map[string]any)
			if !ok {
				next = make(map[string]any)
				target[segment] = next
			}
			target = next
		}
		target[path[len(path)-1]] = value
	}
	return out
}
