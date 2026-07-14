package mango

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
)

// Selector is a parsed Mango selector.
type Selector struct {
	root node
	// Raw is the selector as given (echoed by _explain and used in
	// replication ids).
	Raw map[string]any
}

// Parse validates and compiles a selector. Errors carry CouchDB's exact
// error/reason shapes.
func Parse(v any) (*Selector, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		raw, _ := json.Marshal(v)
		return nil, couch.NewError(400, "invalid_selector_json",
			"Selector must be a JSON object, not: "+string(raw))
	}
	root, err := parseSelector(obj)
	if err != nil {
		return nil, err
	}
	return &Selector{root: root, Raw: obj}, nil
}

// Matches reports whether the document satisfies the selector.
func (s *Selector) Matches(doc map[string]any) bool {
	return s.root.matches(doc)
}

// node is a selector clause evaluated against a whole document.
type node interface {
	matches(doc map[string]any) bool
}

type andNode []node

func (n andNode) matches(doc map[string]any) bool {
	for _, sub := range n {
		if !sub.matches(doc) {
			return false
		}
	}
	return true
}

type orNode []node

func (n orNode) matches(doc map[string]any) bool {
	for _, sub := range n {
		if sub.matches(doc) {
			return true
		}
	}
	return false
}

type notNode struct{ sub node }

func (n notNode) matches(doc map[string]any) bool { return !n.sub.matches(doc) }

type norNode []node

func (n norNode) matches(doc map[string]any) bool { return !orNode(n).matches(doc) }

// fieldNode binds a condition to a document field path. A missing path
// fails every condition except $exists:false. CouchDB also treats
// {"$not": {"$gt": 2}} does not match a document lacking the field).
type fieldNode struct {
	path []string
	cond cond
}

func (n fieldNode) matches(doc map[string]any) bool {
	value, found := lookupPath(doc, n.path)
	if !found {
		return n.cond.matchMissing()
	}
	return n.cond.match(value)
}

// lookupPath walks a dotted field path.
func lookupPath(doc map[string]any, path []string) (any, bool) {
	var current any = doc
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, present := obj[segment]
		if !present {
			return nil, false
		}
		current = next
	}
	return current, true
}

// cond is an operator condition on one field value.
type cond interface {
	match(v any) bool
	// matchMissing is the verdict when the field is absent.
	matchMissing() bool
}

// missingFalse is the default missing-field verdict.
type missingFalse struct{}

func (missingFalse) matchMissing() bool { return false }

type opCmp struct {
	missingFalse
	op  string // "$lt" "$lte" "$gt" "$gte"
	arg any
}

func (o opCmp) match(v any) bool {
	c := Compare(v, o.arg)
	switch o.op {
	case "$lt":
		return c < 0
	case "$lte":
		return c <= 0
	case "$gt":
		return c > 0
	}
	return c >= 0 // $gte
}

type opEq struct {
	missingFalse
	arg any
}

func (o opEq) match(v any) bool { return Compare(v, o.arg) == 0 }

type opNe struct {
	missingFalse
	arg any
}

func (o opNe) match(v any) bool { return Compare(v, o.arg) != 0 }

// opIn matches when the value equals any listed item. For array values, it
// matches when any element does (verified against 3.5.2). $eq has no such
// element fallback.
type opIn struct {
	missingFalse
	items []any
	neg   bool // $nin
}

func (o opIn) match(v any) bool {
	found := false
	for _, item := range o.items {
		if Compare(v, item) == 0 {
			found = true
			break
		}
		if arr, ok := v.([]any); ok {
			for _, elem := range arr {
				if Compare(elem, item) == 0 {
					found = true
					break
				}
			}
		}
		if found {
			break
		}
	}
	return found != o.neg
}

type opExists struct{ want bool }

func (o opExists) match(v any) bool   { return o.want }
func (o opExists) matchMissing() bool { return !o.want }

type opType struct {
	missingFalse
	want string
}

func (o opType) match(v any) bool {
	switch typeRank(v) {
	case 0:
		return o.want == "null"
	case 1, 2:
		return o.want == "boolean"
	case 3:
		return o.want == "number"
	case 4:
		return o.want == "string"
	case 5:
		return o.want == "array"
	default:
		return o.want == "object"
	}
}

type opSize struct {
	missingFalse
	want int64
}

func (o opSize) match(v any) bool {
	arr, ok := v.([]any)
	return ok && int64(len(arr)) == o.want
}

type opMod struct {
	missingFalse
	divisor, remainder int64
}

func (o opMod) match(v any) bool {
	num, ok := v.(json.Number)
	if !ok {
		return false
	}
	i, err := num.Int64()
	if err != nil {
		return false // non-integer values never match $mod
	}
	return i%o.divisor == o.remainder
}

type opRegex struct {
	missingFalse
	re *regexp.Regexp
}

func (o opRegex) match(v any) bool {
	s, ok := v.(string)
	return ok && o.re.MatchString(s)
}

// opAll: the value is an array containing every listed element.
type opAll struct {
	missingFalse
	items []any
}

func (o opAll) match(v any) bool {
	arr, ok := v.([]any)
	if !ok {
		return false
	}
	for _, item := range o.items {
		found := false
		for _, elem := range arr {
			if Compare(elem, item) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// opElemMatch / opAllMatch apply a sub-condition to array elements.
type opElemMatch struct {
	missingFalse
	sub cond
	all bool // $allMatch
}

func (o opElemMatch) match(v any) bool {
	arr, ok := v.([]any)
	if !ok {
		return false
	}
	for _, elem := range arr {
		if o.sub.match(elem) != o.all {
			return !o.all
		}
	}
	return o.all
}

// opKeyMapMatch applies a sub-condition to an object's keys.
type opKeyMapMatch struct {
	missingFalse
	sub cond
}

func (o opKeyMapMatch) match(v any) bool {
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	for key := range obj {
		if o.sub.match(key) {
			return true
		}
	}
	return false
}

// Condition combinators (inside an operator object, still bound to the
// field). A missing field defeats them regardless of structure, matching
// CouchDB, where $not on a missing field is false, not true.
type condAnd struct {
	missingFalse
	subs []cond
}

func (c condAnd) match(v any) bool {
	for _, sub := range c.subs {
		if !sub.match(v) {
			return false
		}
	}
	return true
}

type condOr struct {
	missingFalse
	subs []cond
}

func (c condOr) match(v any) bool {
	for _, sub := range c.subs {
		if sub.match(v) {
			return true
		}
	}
	return false
}

type condNot struct {
	missingFalse
	sub cond
}

func (c condNot) match(v any) bool { return !c.sub.match(v) }

// selectorCond wraps a full selector as a condition (for $elemMatch over
// object elements).
type selectorCond struct {
	missingFalse
	sel node
}

func (c selectorCond) match(v any) bool {
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	return c.sel.matches(obj)
}

// --- parsing ---------------------------------------------------------------

func invalidOperator(op string) error {
	return couch.NewError(400, "invalid_operator", "Invalid operator: "+op)
}

func badArg(op string, arg any) error {
	raw, _ := json.Marshal(arg)
	return couch.NewError(400, "bad_arg",
		fmt.Sprintf("Bad argument for operator %s: %s", op, raw))
}

// parseSelector compiles a selector object from field conditions plus the
// top-level combination operators, all implicitly $and-ed.
func parseSelector(obj map[string]any) (node, error) {
	var clauses andNode
	for _, key := range sortedKeys(obj) {
		value := obj[key]
		if strings.HasPrefix(key, "$") {
			clause, err := parseCombination(key, value)
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, clause)
			continue
		}
		fields, err := parseField(strings.Split(key, "."), value)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, fields...)
	}
	return clauses, nil
}

func parseCombination(op string, arg any) (node, error) {
	switch op {
	case "$and", "$or", "$nor":
		items, ok := arg.([]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		subs := make([]node, 0, len(items))
		for _, item := range items {
			itemObj, ok := item.(map[string]any)
			if !ok {
				return nil, badArg(op, arg)
			}
			sub, err := parseSelector(itemObj)
			if err != nil {
				return nil, err
			}
			subs = append(subs, sub)
		}
		switch op {
		case "$and":
			return andNode(subs), nil
		case "$or":
			return orNode(subs), nil
		default:
			return norNode(subs), nil
		}
	case "$not":
		argObj, ok := arg.(map[string]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		sub, err := parseSelector(argObj)
		if err != nil {
			return nil, err
		}
		return notNode{sub: sub}, nil
	}
	return nil, invalidOperator(op)
}

// parseField compiles one field's value as an operator object, a nested
// selector (extending the path), or a literal $eq.
func parseField(path []string, value any) ([]node, error) {
	obj, isObj := value.(map[string]any)
	if !isObj {
		return []node{fieldNode{path: path, cond: opEq{arg: value}}}, nil
	}
	hasOp := false
	for key := range obj {
		if strings.HasPrefix(key, "$") {
			hasOp = true
			break
		}
	}
	if !hasOp {
		if len(obj) == 0 {
			// {} as a value: equality with the empty object.
			return []node{fieldNode{path: path, cond: opEq{arg: value}}}, nil
		}
		// Each member of a nested selector extends the path.
		var nodes []node
		for _, key := range sortedKeys(obj) {
			subPath := append(append([]string{}, path...), strings.Split(key, ".")...)
			subs, err := parseField(subPath, obj[key])
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, subs...)
		}
		return nodes, nil
	}
	c, err := parseCondObject(obj)
	if err != nil {
		return nil, err
	}
	return []node{fieldNode{path: path, cond: c}}, nil
}

// parseCondObject compiles an operator object like {"$gt":1,"$lt":5}
// (members are and-ed).
func parseCondObject(obj map[string]any) (cond, error) {
	var subs []cond
	for _, op := range sortedKeys(obj) {
		arg := obj[op]
		c, err := parseCond(op, arg)
		if err != nil {
			return nil, err
		}
		subs = append(subs, c)
	}
	if len(subs) == 1 {
		return subs[0], nil
	}
	return condAnd{subs: subs}, nil
}

func parseCond(op string, arg any) (cond, error) {
	switch op {
	case "$eq":
		return opEq{arg: arg}, nil
	case "$ne":
		return opNe{arg: arg}, nil
	case "$lt", "$lte", "$gt", "$gte":
		return opCmp{op: op, arg: arg}, nil
	case "$in", "$nin":
		items, ok := arg.([]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		return opIn{items: items, neg: op == "$nin"}, nil
	case "$exists":
		want, ok := arg.(bool)
		if !ok {
			return nil, badArg(op, arg)
		}
		return opExists{want: want}, nil
	case "$type":
		want, ok := arg.(string)
		if !ok {
			return nil, badArg(op, arg)
		}
		return opType{want: want}, nil
	case "$size":
		num, ok := arg.(json.Number)
		if !ok {
			return nil, badArg(op, arg)
		}
		n, err := num.Int64()
		if err != nil {
			return nil, badArg(op, arg)
		}
		return opSize{want: n}, nil
	case "$mod":
		items, ok := arg.([]any)
		if !ok || len(items) != 2 {
			return nil, badArg(op, arg)
		}
		divisor, err1 := intArg(items[0])
		remainder, err2 := intArg(items[1])
		if err1 != nil || err2 != nil || divisor == 0 {
			return nil, badArg(op, arg)
		}
		return opMod{divisor: divisor, remainder: remainder}, nil
	case "$regex":
		pattern, ok := arg.(string)
		if !ok {
			return nil, badArg(op, arg)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, badArg(op, arg)
		}
		return opRegex{re: re}, nil
	case "$all":
		items, ok := arg.([]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		return opAll{items: items}, nil
	case "$elemMatch", "$allMatch":
		sub, err := parseElemCond(arg, op)
		if err != nil {
			return nil, err
		}
		return opElemMatch{sub: sub, all: op == "$allMatch"}, nil
	case "$keyMapMatch":
		sub, err := parseElemCond(arg, op)
		if err != nil {
			return nil, err
		}
		return opKeyMapMatch{sub: sub}, nil
	case "$and", "$or", "$not", "$nor":
		return parseCondCombination(op, arg)
	}
	return nil, invalidOperator(op)
}

func parseCondCombination(op string, arg any) (cond, error) {
	if op == "$not" {
		argObj, ok := arg.(map[string]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		sub, err := parseCondObject(argObj)
		if err != nil {
			return nil, err
		}
		return condNot{sub: sub}, nil
	}
	items, ok := arg.([]any)
	if !ok {
		return nil, badArg(op, arg)
	}
	subs := make([]cond, 0, len(items))
	for _, item := range items {
		itemObj, ok := item.(map[string]any)
		if !ok {
			return nil, badArg(op, arg)
		}
		sub, err := parseCondObject(itemObj)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	switch op {
	case "$and":
		return condAnd{subs: subs}, nil
	case "$or":
		return condOr{subs: subs}, nil
	default: // $nor
		return condNot{sub: condOr{subs: subs}}, nil
	}
}

// parseElemCond compiles the argument of $elemMatch/$allMatch/$keyMapMatch.
// an operator object applies to the element itself, anything else is a full
// selector over object elements.
func parseElemCond(arg any, op string) (cond, error) {
	obj, ok := arg.(map[string]any)
	if !ok {
		return nil, badArg(op, arg)
	}
	hasOp := false
	for key := range obj {
		if strings.HasPrefix(key, "$") {
			hasOp = true
			break
		}
	}
	if hasOp {
		return parseCondObject(obj)
	}
	sel, err := parseSelector(obj)
	if err != nil {
		return nil, err
	}
	return selectorCond{sel: sel}, nil
}

func intArg(v any) (int64, error) {
	num, ok := v.(json.Number)
	if !ok {
		return 0, fmt.Errorf("not a number")
	}
	return num.Int64()
}
