package httpapi

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/couchgres/couchgres/internal/collate"
	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/mango"
	"github.com/couchgres/couchgres/internal/store"
)

// mangoMapper implements store.ViewMapper natively for query-language
// design docs: the "map function" emits the indexed field values.
type mangoMapper struct {
	defs      []store.MangoIndexDef // in ViewOrder
	selectors []*mango.Selector     // parsed partial filters (nil = none)
}

func newMangoMapper(vg *store.ViewGroup) (*mangoMapper, error) {
	m := &mangoMapper{defs: vg.MangoIndexes}
	for _, def := range vg.MangoIndexes {
		if def.PartialSelector == nil {
			m.selectors = append(m.selectors, nil)
			continue
		}
		sel, err := mango.Parse(def.PartialSelector)
		if err != nil {
			return nil, err
		}
		m.selectors = append(m.selectors, sel)
	}
	return m, nil
}

func (m *mangoMapper) MapDocs(ctx context.Context, sig string, fns []string, lib map[string]any, docs []json.RawMessage) ([][][]store.ViewEmit, error) {
	out := make([][][]store.ViewEmit, len(docs))
	for d, raw := range docs {
		doc, err := couch.DecodeJSON(raw)
		if err != nil {
			return nil, err
		}
		obj, _ := doc.(map[string]any)
		out[d] = make([][]store.ViewEmit, len(m.defs))
		for i, def := range m.defs {
			if m.selectors[i] != nil && !m.selectors[i].Matches(obj) {
				continue
			}
			key, ok := mangoIndexKey(obj, def)
			if !ok {
				continue // docs missing any indexed field stay out of the index
			}
			out[d][i] = []store.ViewEmit{{Key: key, Value: json.RawMessage(`null`)}}
		}
	}
	return out, nil
}

// mangoIndexKey builds the [v1, v2, ...] key array for one document.
func mangoIndexKey(doc map[string]any, def store.MangoIndexDef) (json.RawMessage, bool) {
	values := make([]any, 0, len(def.Fields))
	for _, field := range def.Fields {
		value, found := mango.Lookup(doc, field.Field)
		if !found {
			return nil, false
		}
		values = append(values, value)
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, false
	}
	return raw, true
}

// viewMapperFor picks the JS pool or the native mango mapper by language.
func (s *Server) viewMapperFor(vg *store.ViewGroup) (store.ViewMapper, error) {
	if vg.Language == "query" {
		return newMangoMapper(vg)
	}
	return s.mapper, nil
}

// --- planner ----------------------------------------------------------------

// mangoIndex is one candidate index (json indexes plus the _all_docs special).
type mangoIndex struct {
	ddocID      string // "" for _all_docs
	name        string
	sig         string
	def         store.MangoIndexDef
	special     bool
	partitioned bool
}

func (idx *mangoIndex) fieldNames() []string {
	names := make([]string, len(idx.def.Fields))
	for i, f := range idx.def.Fields {
		names[i] = f.Field
	}
	return names
}

// indexJSON is the shape _index and _explain report.
func (idx *mangoIndex) indexJSON() map[string]any {
	fields := make([]any, len(idx.def.Fields))
	for i, f := range idx.def.Fields {
		dir := "asc"
		if f.Desc {
			dir = "desc"
		}
		fields[i] = map[string]any{f.Field: dir}
	}
	def := map[string]any{"fields": fields}
	if idx.def.PartialSelector != nil {
		def["partial_filter_selector"] = idx.def.PartialSelector
	}
	if idx.special {
		return map[string]any{
			"ddoc": nil, "name": "_all_docs", "type": "special", "def": def,
		}
	}
	return map[string]any{
		"ddoc": idx.ddocID, "name": idx.name, "type": "json",
		"partitioned": idx.partitioned, "def": def,
	}
}

func allDocsIndex() *mangoIndex {
	return &mangoIndex{
		name:    "_all_docs",
		special: true,
		def: store.MangoIndexDef{
			Fields: []store.MangoIndexField{{Field: "_id"}},
		},
	}
}

// listMangoIndexes collects the database's candidate indexes.
func (s *Server) listMangoIndexes(r *http.Request, db *store.DB) ([]*mangoIndex, error) {
	ddocs, err := s.store.DesignDocs(r.Context(), db)
	if err != nil {
		return nil, err
	}
	indexes := []*mangoIndex{allDocsIndex()}
	for _, ddoc := range ddocs {
		vg, err := store.ViewGroupFromDDoc(ddoc.ID, ddoc.Body)
		if err != nil || vg == nil || vg.Language != "query" {
			continue
		}
		vg.ResolvePartitioned(db)
		for _, def := range vg.MangoIndexes {
			indexes = append(indexes, &mangoIndex{
				ddocID: ddoc.ID, name: def.Name, sig: vg.Sig, def: def,
				partitioned: vg.Partitioned,
			})
		}
	}
	return indexes, nil
}

// findRequest is a parsed _find / _explain body.
type findRequest struct {
	selector    *mango.Selector
	limit       int64
	skip        int64
	sortFields  []string
	sortDesc    bool
	fields      []string
	bookmark    string
	useIndex    []string // [ddoc] or [ddoc, name]
	conflicts   bool
	stats       bool
	update      bool
	rawSelector map[string]any
}

func parseFindRequest(body map[string]any) (*findRequest, error) {
	rawSelector, present := body["selector"]
	if !present {
		return nil, couch.NewError(400, "missing_required_key", "Missing required key: selector")
	}
	selector, err := mango.Parse(rawSelector)
	if err != nil {
		return nil, err
	}
	req := &findRequest{selector: selector, limit: 25, update: true}
	req.rawSelector, _ = rawSelector.(map[string]any)

	getInt := func(name string, into *int64) error {
		v, ok := body[name]
		if !ok {
			return nil
		}
		num, isNum := v.(json.Number)
		if isNum {
			if n, err := num.Int64(); err == nil && n >= 0 {
				*into = n
				return nil
			}
		}
		return couch.BadRequest("`" + name + "` must be a non-negative integer")
	}
	if err := getInt("limit", &req.limit); err != nil {
		return nil, err
	}
	if err := getInt("skip", &req.skip); err != nil {
		return nil, err
	}

	if rawSort, ok := body["sort"]; ok {
		items, isArr := rawSort.([]any)
		if !isArr {
			return nil, couch.BadRequest("`sort` must be an array")
		}
		var direction string
		for _, item := range items {
			switch t := item.(type) {
			case string:
				req.sortFields = append(req.sortFields, t)
				if direction == "" {
					direction = "asc"
				} else if direction != "asc" {
					return nil, couch.NewError(400, "unsupported_mixed_sort",
						"Sorts currently only support a single direction for all fields.")
				}
			case map[string]any:
				for field, rawDir := range t {
					dir, _ := rawDir.(string)
					if dir == "" {
						dir = "asc"
					}
					if direction == "" {
						direction = dir
					} else if direction != dir {
						return nil, couch.NewError(400, "unsupported_mixed_sort",
							"Sorts currently only support a single direction for all fields.")
					}
					req.sortFields = append(req.sortFields, field)
				}
			default:
				return nil, couch.BadRequest("`sort` entries must be strings or objects")
			}
		}
		req.sortDesc = direction == "desc"
	}

	if rawFields, ok := body["fields"]; ok {
		items, isArr := rawFields.([]any)
		if !isArr {
			return nil, couch.BadRequest("`fields` must be an array")
		}
		for _, item := range items {
			field, isStr := item.(string)
			if !isStr {
				return nil, couch.BadRequest("`fields` entries must be strings")
			}
			req.fields = append(req.fields, field)
		}
	}

	req.bookmark, _ = body["bookmark"].(string)
	switch t := body["use_index"].(type) {
	case string:
		if t != "" {
			req.useIndex = []string{t}
		}
	case []any:
		for _, item := range t {
			if s, ok := item.(string); ok {
				req.useIndex = append(req.useIndex, s)
			}
		}
	}
	req.conflicts, _ = body["conflicts"].(bool)
	req.stats, _ = body["execution_stats"].(bool)
	if v, ok := body["update"].(bool); ok {
		req.update = v
	}
	return req, nil
}

// planFind picks the index. Returns the chosen index (nil = _all_docs scan),
// the other candidates for _explain, and any warning. partition scopes the
// query: partitioned indexes serve partition queries, global ones the rest.
func (s *Server) planFind(indexes []*mangoIndex, req *findRequest, partition string, dbPartitioned bool) (chosen *mangoIndex, warning string, err error) {
	constrained := req.selector.ConstrainedFields()

	usable := func(idx *mangoIndex) bool {
		if idx.special {
			return len(req.sortFields) == 0 ||
				(len(req.sortFields) == 1 && req.sortFields[0] == "_id")
		}
		if idx.partitioned != (partition != "") {
			return false
		}
		fields := idx.fieldNames()
		// Sort fields must be a prefix of the index fields.
		if len(req.sortFields) > len(fields) {
			return false
		}
		for i, sortField := range req.sortFields {
			if fields[i] != sortField {
				return false
			}
		}
		if len(req.sortFields) > 0 {
			return true // the sort forces the index (missing-field docs drop out)
		}
		// Without a sort, every index field must be constrained so no
		// matching doc can be missing from the index.
		for _, field := range fields {
			if !constrained[field] {
				return false
			}
		}
		return true
	}

	// use_index forces the named index when it is usable.
	if len(req.useIndex) > 0 {
		wantDDoc := req.useIndex[0]
		if !strings.HasPrefix(wantDDoc, "_design/") {
			wantDDoc = "_design/" + wantDDoc
		}
		var named *mangoIndex
		for _, idx := range indexes {
			if idx.special || idx.ddocID != wantDDoc {
				continue
			}
			if len(req.useIndex) > 1 && idx.name != req.useIndex[1] {
				continue
			}
			named = idx
			break
		}
		if named != nil && usable(named) {
			return named, "", nil
		}
		label := wantDDoc
		if len(req.useIndex) > 1 {
			label += "/" + req.useIndex[1]
		}
		warning = label + " was not used because it does not contain a valid index for this query.\n"
	}

	for _, idx := range indexes {
		if idx.special {
			continue
		}
		if usable(idx) {
			return idx, warning, nil
		}
	}
	if len(req.sortFields) > 0 && !(len(req.sortFields) == 1 && req.sortFields[0] == "_id") {
		// On a partitioned db the wording names the index scope.
		switch {
		case partition != "":
			return nil, "", couch.NewError(400, "no_usable_index",
				"No partitioned index exists for this sort, try indexing by the sort fields.")
		case dbPartitioned:
			return nil, "", couch.NewError(400, "no_usable_index",
				"No global index exists for this sort, try indexing by the sort fields.")
		}
		return nil, "", couch.NewError(400, "no_usable_index",
			"No index exists for this sort, try indexing by the sort fields.")
	}
	return nil, warning + "No matching index found, create an index to optimize query time.", nil
}

// scanBounds derives the index scan range from the selector: a prefix of
// $eq fields, then an optional range on the next field.
func scanBounds(idx *mangoIndex, sel *mango.Selector) (start, end []byte, err error) {
	ranges := sel.FieldRanges()
	prefix := []byte{0x07} // collate's array tag
	for _, field := range idx.def.Fields {
		fr := ranges[field.Field]
		if fr == nil {
			break
		}
		if fr.HasEq {
			enc, err := collate.KeyOf(fr.Eq)
			if err != nil {
				return nil, nil, err
			}
			prefix = append(prefix, enc...)
			continue
		}
		start = append([]byte{}, prefix...)
		end = prefixSuccessor(prefix)
		if fr.Low != nil {
			enc, err := collate.KeyOf(fr.Low.Value)
			if err != nil {
				return nil, nil, err
			}
			bound := append(append([]byte{}, prefix...), enc...)
			if fr.Low.Inclusive {
				start = bound
			} else {
				start = prefixSuccessor(bound)
			}
		}
		if fr.High != nil {
			enc, err := collate.KeyOf(fr.High.Value)
			if err != nil {
				return nil, nil, err
			}
			bound := append(append([]byte{}, prefix...), enc...)
			if fr.High.Inclusive {
				end = prefixSuccessor(bound)
			} else {
				end = bound
			}
		}
		return start, end, nil
	}
	return prefix, prefixSuccessor(prefix), nil
}

// prefixSuccessor is the smallest byte string greater than every string
// with the given prefix.
func prefixSuccessor(prefix []byte) []byte {
	out := append([]byte{}, prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil // all 0xFF: no upper bound
}

// mangoBookmark round-trips the resume position.
type mangoBookmark struct {
	Key []byte `json:"k,omitempty"`
	ID  string `json:"id"`
}

func encodeBookmark(b *mangoBookmark) string {
	raw, _ := json.Marshal(b)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeBookmark(s string) (*mangoBookmark, error) {
	if s == "" || s == "nil" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		var b mangoBookmark
		if json.Unmarshal(raw, &b) == nil && b.ID != "" {
			return &b, nil
		}
	}
	return nil, couch.NewError(400, "invalid_bookmark", "Invalid bookmark value: \""+s+"\"")
}

// --- handlers ---------------------------------------------------------------

func (s *Server) mangoFind(w http.ResponseWriter, r *http.Request) error {
	started := time.Now()
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	partition := r.PathValue("partition")
	if partition != "" {
		if !db.Partitioned {
			return couch.BadRequest("Database is not partitioned")
		}
		if err := validatePartitionName(partition); err != nil {
			return err
		}
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	req, err := parseFindRequest(obj)
	if err != nil {
		return err
	}
	bookmark, err := decodeBookmark(req.bookmark)
	if err != nil {
		return err
	}
	indexes, err := s.listMangoIndexes(r, db)
	if err != nil {
		return err
	}
	chosen, warning, err := s.planFind(indexes, req, partition, db.Partitioned)
	if err != nil {
		return err
	}

	docs := []map[string]any{}
	var keysExamined, docsExamined int64
	var lastBookmark *mangoBookmark
	remaining := req.limit
	skip := req.skip

	handleDoc := func(doc *store.DocRow, key []byte) (bool, error) {
		docsExamined++
		full := doc.JSON()
		if !req.selector.Matches(full) {
			return true, nil
		}
		if skip > 0 {
			skip--
			return true, nil
		}
		if remaining <= 0 {
			return false, nil
		}
		if req.conflicts {
			if err := s.addConflictsMember(r.Context(), db, doc, full); err != nil {
				return false, err
			}
		}
		if len(req.fields) > 0 {
			full = mango.Project(full, req.fields)
		}
		docs = append(docs, full)
		lastBookmark = &mangoBookmark{Key: key, ID: doc.ID}
		remaining--
		return remaining > 0, nil
	}

	if chosen != nil {
		vg, err := s.loadViewGroup(r, db, strings.TrimPrefix(chosen.ddocID, "_design/"))
		if err != nil {
			return err
		}
		if req.update {
			mapper, err := s.viewMapperFor(vg)
			if err != nil {
				return err
			}
			if err := s.store.UpdateViewGroup(r.Context(), db, vg, mapper); err != nil {
				return err
			}
		}
		start, end, err := scanBounds(chosen, req.selector)
		if err != nil {
			return err
		}
		var afterKey []byte
		var afterID string
		if bookmark != nil {
			afterKey, afterID = bookmark.Key, bookmark.ID
		}
		err = s.store.ScanMangoIndex(r.Context(), db, chosen.sig, chosen.name,
			partition, start, end, afterKey, afterID, req.sortDesc,
			func(row *store.MangoIndexRow) (bool, error) {
				keysExamined++
				return handleDoc(row.Doc, row.KeyCollate)
			})
		if err != nil {
			return err
		}
	} else {
		afterID := ""
		if bookmark != nil {
			afterID = bookmark.ID
		}
		var lo, hi string
		if partition != "" {
			lo, hi = store.PartitionRange(partition)
		}
		err = s.store.ScanDocs(r.Context(), db, afterID, lo, hi, func(doc *store.DocRow) (bool, error) {
			keysExamined++
			return handleDoc(doc, nil)
		})
		if err != nil {
			return err
		}
	}

	response := map[string]any{"docs": docs}
	if lastBookmark != nil {
		response["bookmark"] = encodeBookmark(lastBookmark)
	} else if req.bookmark != "" {
		response["bookmark"] = req.bookmark
	} else {
		response["bookmark"] = "nil"
	}
	if warning != "" {
		response["warning"] = warning
	}
	if req.stats {
		response["execution_stats"] = map[string]any{
			"total_keys_examined":        keysExamined,
			"total_docs_examined":        docsExamined,
			"total_quorum_docs_examined": 0,
			"results_returned":           len(docs),
			"execution_time_ms":          float64(time.Since(started).Microseconds()) / 1000,
		}
	}
	writeJSON(w, 200, response)
	return nil
}

func (s *Server) addConflictsMember(ctx context.Context, db *store.DB, doc *store.DocRow, full map[string]any) error {
	leaves, err := s.store.Leaves(ctx, db, doc.ID)
	if err != nil {
		return err
	}
	var conflicts []string
	for _, leaf := range leaves {
		if leaf.Rev != doc.Rev && !leaf.Deleted {
			conflicts = append(conflicts, leaf.Rev.String())
		}
	}
	if len(conflicts) > 0 {
		full["_conflicts"] = conflicts
	}
	return nil
}

func (s *Server) mangoExplain(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	partition := r.PathValue("partition")
	if partition != "" && !db.Partitioned {
		return couch.BadRequest("Database is not partitioned")
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	req, err := parseFindRequest(obj)
	if err != nil {
		return err
	}
	indexes, err := s.listMangoIndexes(r, db)
	if err != nil {
		return err
	}
	chosen, _, err := s.planFind(indexes, req, partition, db.Partitioned)
	if err != nil {
		return err
	}
	chosenJSON := allDocsIndex().indexJSON()
	if chosen != nil {
		chosenJSON = chosen.indexJSON()
	}

	var candidates []any
	for _, idx := range indexes {
		if (chosen == nil && idx.special) || idx == chosen {
			continue
		}
		var analysis map[string]any
		if idx.special {
			analysis = map[string]any{
				"usable": true, "ranking": 1, "covering": nil,
				"reasons": []any{map[string]any{"name": "unfavored_type"}},
			}
		} else {
			analysis = map[string]any{
				"usable": false, "ranking": 1, "covering": false,
				"reasons": []any{map[string]any{"name": "field_mismatch"}},
			}
		}
		candidates = append(candidates, map[string]any{
			"index": idx.indexJSON(), "analysis": analysis,
		})
	}
	if candidates == nil {
		candidates = []any{}
	}

	fields := req.fields
	if fields == nil {
		fields = []string{}
	}
	direction := "fwd"
	if req.sortDesc {
		direction = "rev"
	}
	var indexableFields []string
	for field := range req.selector.ConstrainedFields() {
		indexableFields = append(indexableFields, field)
	}
	sort.Strings(indexableFields)
	if indexableFields == nil {
		indexableFields = []string{}
	}

	// The logical scan bounds as decoded key arrays, "<MAX>" marking the
	// open high end (CouchDB's rendering). Reverse scans swap them.
	startKey, endKey := explainBounds(chosen, req.selector)
	if req.sortDesc {
		startKey, endKey = endKey, startKey
	}

	sortOpts := map[string]any{}
	for _, field := range req.sortFields {
		dir := "asc"
		if req.sortDesc {
			dir = "desc"
		}
		sortOpts[field] = dir
	}
	useIndex := make([]any, 0, len(req.useIndex))
	for _, s := range req.useIndex {
		useIndex = append(useIndex, s)
	}

	writeJSON(w, 200, map[string]any{
		"dbname":           db.Name,
		"index":            chosenJSON,
		"index_candidates": candidates,
		"selector":         mango.Normalize(req.rawSelector),
		"partitioned":      db.Partitioned,
		"covering":         false,
		"limit":            req.limit,
		"skip":             req.skip,
		"fields":           fields,
		"mrargs": map[string]any{
			"conflicts": "undefined", "direction": direction,
			"start_key": startKey, "end_key": endKey, "include_docs": true,
			"partition": explainPartition(partition), "reduce": false, "stable": false,
			"update": true, "view_type": "map",
		},
		"opts": map[string]any{
			"allow_fallback": true, "bookmark": "nil", "conflicts": req.conflicts,
			"execution_stats": req.stats, "fields": fields, "limit": req.limit,
			"partition": partition, "r": 1, "skip": req.skip, "sort": sortOpts,
			"stable": false, "stale": false, "update": req.update, "use_index": useIndex,
		},
		"selector_hints": []any{map[string]any{
			"type": "json", "indexable_fields": indexableFields,
			"unindexable_fields": []any{},
		}},
	})
	return nil
}

// explainPartition renders mrargs.partition: the partition name or null.
func explainPartition(partition string) any {
	if partition == "" {
		return nil
	}
	return partition
}

// explainBounds renders the index scan range as decoded key arrays for
// _explain: the $eq prefix, then the range bound on the next field.
func explainBounds(idx *mangoIndex, sel *mango.Selector) (startKey, endKey []any) {
	startKey, endKey = []any{}, []any{"<MAX>"}
	if idx == nil {
		return startKey, endKey
	}
	ranges := sel.FieldRanges()
	var prefix []any
	for _, field := range idx.def.Fields {
		fr := ranges[field.Field]
		if fr == nil {
			break
		}
		if fr.HasEq {
			prefix = append(prefix, fr.Eq)
			continue
		}
		startKey = append([]any{}, prefix...)
		if fr.Low != nil {
			startKey = append(startKey, fr.Low.Value)
		}
		endKey = append(append([]any{}, prefix...), "<MAX>")
		if fr.High != nil {
			endKey = append(endKey[:len(endKey)-1], fr.High.Value)
		}
		return startKey, endKey
	}
	if len(prefix) > 0 {
		return prefix, append(append([]any{}, prefix...), "<MAX>")
	}
	return startKey, endKey
}

// --- _index CRUD ------------------------------------------------------------

func (s *Server) mangoIndexPost(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	if idxType, ok := obj["type"].(string); ok && idxType != "json" {
		return couch.NewError(503, "required index service unavailable", idxType)
	}
	indexDef, ok := obj["index"].(map[string]any)
	if !ok {
		return couch.BadRequest("Missing required key: index")
	}
	rawFields, ok := indexDef["fields"].([]any)
	if !ok || len(rawFields) == 0 {
		return couch.BadRequest("Index fields must be a non-empty array")
	}
	// Normalize fields to the [{field: dir}] form.
	fields := make([]any, 0, len(rawFields))
	fieldsObj := make(map[string]any, len(rawFields))
	for _, raw := range rawFields {
		switch t := raw.(type) {
		case string:
			fields = append(fields, map[string]any{t: "asc"})
			fieldsObj[t] = "asc"
		case map[string]any:
			for field, dir := range t {
				dirStr, _ := dir.(string)
				if dirStr != "desc" {
					dirStr = "asc"
				}
				fields = append(fields, map[string]any{field: dirStr})
				fieldsObj[field] = dirStr
			}
		default:
			return couch.BadRequest("Index fields must be strings or single-entry objects")
		}
	}
	partial, _ := indexDef["partial_filter_selector"].(map[string]any)
	if partial != nil {
		if _, err := mango.Parse(partial); err != nil {
			return err
		}
		// CouchDB stores the normalized form (visible in GET _index).
		partial = mango.Normalize(partial)
	}
	if partial == nil {
		partial = map[string]any{}
	}

	// Names default to a hash of the definition (shape-compatible with
	// CouchDB's generated names). The exact digest differs.
	canonical := couch.CanonicalBody(map[string]any{
		"fields": fields, "partial_filter_selector": partial,
	})
	digest := sha1.Sum(canonical)
	autoName := hex.EncodeToString(digest[:])
	name, _ := obj["name"].(string)
	if name == "" {
		name = autoName
	}
	ddocID, _ := obj["ddoc"].(string)
	if ddocID == "" {
		ddocID = autoName
	}
	ddocID = strings.TrimPrefix(ddocID, "_design/")
	fullDDocID := "_design/" + ddocID

	viewDef := map[string]any{
		"map": map[string]any{
			"fields":                  fieldsObj,
			"partial_filter_selector": partial,
		},
		"reduce":  "_count",
		"options": map[string]any{"def": map[string]any{"fields": rawFields}},
	}

	existing, err := s.store.GetDocAny(r.Context(), db, fullDDocID)
	result := "created"
	var ddocBody map[string]any
	var expectedRev *couch.Rev
	if err == nil && !existing.Deleted {
		ddocBody = existing.Body
		expectedRev = &existing.Rev
		views, _ := ddocBody["views"].(map[string]any)
		if views == nil {
			views = map[string]any{}
			ddocBody["views"] = views
		}
		if current, exists := views[name]; exists {
			if bytes.Equal(couch.CanonicalBody(map[string]any{"v": current}),
				couch.CanonicalBody(map[string]any{"v": viewDef})) {
				writeJSON(w, 200, map[string]any{
					"result": "exists", "id": fullDDocID, "name": name,
				})
				return nil
			}
		}
		views[name] = viewDef
	} else {
		ddocBody = map[string]any{
			"language": "query",
			"views":    map[string]any{name: viewDef},
		}
	}
	// `partitioned: false` on a partitioned db makes a global index. The
	// ddoc option is how the view group learns it.
	if p, isBool := obj["partitioned"].(bool); isBool && db.Partitioned {
		ddocBody["options"] = map[string]any{"partitioned": p}
	}
	if _, _, err := s.store.PutDoc(r.Context(), db, fullDDocID, ddocBody, nil, expectedRev, false, nil); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"result": result, "id": fullDDocID, "name": name})
	return nil
}

func (s *Server) mangoIndexList(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	indexes, err := s.listMangoIndexes(r, db)
	if err != nil {
		return err
	}
	sort.Slice(indexes, func(i, j int) bool {
		if indexes[i].special != indexes[j].special {
			return indexes[i].special
		}
		if indexes[i].ddocID != indexes[j].ddocID {
			return indexes[i].ddocID < indexes[j].ddocID
		}
		return indexes[i].name < indexes[j].name
	})
	list := make([]any, len(indexes))
	for i, idx := range indexes {
		list[i] = idx.indexJSON()
	}
	writeJSON(w, 200, map[string]any{
		"total_rows": len(list),
		"indexes":    list,
	})
	return nil
}

func (s *Server) mangoIndexDelete(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireDBAdmin(r, db); err != nil {
		return err
	}
	ddocID := "_design/" + r.PathValue("ddoc")
	name := r.PathValue("name")

	ddoc, err := s.store.GetDoc(r.Context(), db, ddocID, nil)
	if err != nil {
		// A tombstoned index ddoc reads as plain "missing" here.
		if ce, ok := err.(*couch.Error); ok && ce.Status == 404 {
			return couch.DocMissing()
		}
		return err
	}
	views, _ := ddoc.Body["views"].(map[string]any)
	if _, exists := views[name]; !exists {
		return couch.DocMissing()
	}
	delete(views, name)
	if len(views) == 0 {
		if _, _, err := s.store.PutDoc(r.Context(), db, ddocID, map[string]any{}, nil, &ddoc.Rev, true, nil); err != nil {
			return err
		}
	} else {
		if _, _, err := s.store.PutDoc(r.Context(), db, ddocID, ddoc.Body, nil, &ddoc.Rev, false, nil); err != nil {
			return err
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
	return nil
}
