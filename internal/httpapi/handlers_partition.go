package httpapi

// Purge and partitioned-database endpoints.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/store"
)

// purge is POST /{db}/_purge: remove leaf revisions without a trace.
func (s *Server) purge(w http.ResponseWriter, r *http.Request) error {
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
	requests := make(map[string][]couch.Rev, len(obj))
	for id, raw := range obj {
		if err := s.validateDocIDForDB(db, id); err != nil {
			return err
		}
		items, ok := raw.([]any)
		if !ok {
			return couch.BadRequest("Purge revisions must be an array")
		}
		var revs []couch.Rev
		for _, item := range items {
			revString, ok := item.(string)
			if !ok {
				return couch.BadRequest("Purge revisions must be strings")
			}
			rev, err := couch.ParseRev(revString)
			if err != nil {
				return err
			}
			revs = append(revs, rev)
		}
		requests[id] = revs
	}
	// Eagerly evict this process's entries. Other processes validate the
	// durable purge generation before reusing their cached view responses.
	s.viewCache.clearSchema(db.Schema)
	purged, err := s.store.Purge(r.Context(), db, requests)
	if err != nil {
		return err
	}
	result := make(map[string]any, len(purged))
	for id, revs := range purged {
		out := make([]string, len(revs))
		for i, rev := range revs {
			out[i] = rev.String()
		}
		result[id] = out
	}
	writeJSON(w, 201, map[string]any{"purge_seq": nil, "purged": result})
	return nil
}

func (s *Server) purgedInfosLimitGet(w http.ResponseWriter, r *http.Request) error {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return err
	}
	if err := s.requireMember(r, db); err != nil {
		return err
	}
	limit, err := s.store.PurgedInfosLimit(r.Context(), db)
	if err != nil {
		return err
	}
	writeJSON(w, 200, limit)
	return nil
}

func (s *Server) purgedInfosLimitPut(w http.ResponseWriter, r *http.Request) error {
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
	num, ok := body.(json.Number)
	if !ok {
		return couch.BadRequest("Rejecting non-integer purged_infos_limit")
	}
	limit, err := num.Int64()
	if err != nil || limit < 1 {
		return couch.BadRequest("Rejecting non-integer purged_infos_limit")
	}
	if err := s.store.SetPurgedInfosLimit(r.Context(), db, limit); err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"ok": true})
	return nil
}

// --- partitioned databases ---------------------------------------------------

// partitionDB resolves the db and checks it is partitioned.
func (s *Server) partitionDB(r *http.Request) (*store.DB, string, error) {
	db, err := s.store.GetDB(r.Context(), r.PathValue("db"))
	if err != nil {
		return nil, "", err
	}
	if err := s.requireMember(r, db); err != nil {
		return nil, "", err
	}
	if !db.Partitioned {
		return nil, "", couch.BadRequest("Database is not partitioned")
	}
	partition := r.PathValue("partition")
	if err := validatePartitionName(partition); err != nil {
		return nil, "", err
	}
	return db, partition, nil
}

func validatePartitionName(partition string) error {
	if partition == "" || strings.HasPrefix(partition, "_") {
		return couch.NewError(400, "illegal_partition", "Partition must not start with an underscore")
	}
	if strings.ContainsAny(partition, ":/") {
		return couch.NewError(400, "illegal_partition", "Malformed partition name")
	}
	return nil
}

// partitionQueryLimit enforces query_server_config/partition_query_limit
// on partition-scoped _all_docs and view queries (Mango is exempt).
func (s *Server) partitionQueryLimit(limit *int64) error {
	max := int64(s.config.getInt("query_server_config", "partition_query_limit", 268435456))
	if limit != nil && *limit > max {
		return couch.NewError(400, "query_parse_error",
			fmt.Sprintf("Limit is too large, must not exceed %d", max))
	}
	return nil
}

// validateDocIDForDB layers the partition rules on the usual docid checks.
func (s *Server) validateDocIDForDB(db *store.DB, docid string) error {
	if err := couch.ValidateDocID(docid); err != nil {
		return err
	}
	if !db.Partitioned ||
		strings.HasPrefix(docid, "_design/") || strings.HasPrefix(docid, "_local/") {
		return nil
	}
	partition, rest, found := strings.Cut(docid, ":")
	if !found || partition == "" || strings.HasPrefix(partition, "_") {
		return couch.NewError(400, "illegal_docid", "Doc id must be of form partition:id")
	}
	if rest == "" {
		return couch.NewError(400, "illegal_docid", "Document id must not be empty")
	}
	return nil
}

// checkPartitionLimit rejects interactive (new_edits) writes that would
// grow a partition past couchdb/max_partition_size. Deletes, shrinking
// updates, and replicated writes still land, so a full partition can be
// drained.
func (s *Server) checkPartitionLimit(r *http.Request, db *store.DB, docid string, deleting bool, body map[string]any) error {
	if !db.Partitioned || deleting ||
		strings.HasPrefix(docid, "_design/") || strings.HasPrefix(docid, "_local/") {
		return nil
	}
	max := int64(s.config.getInt("couchdb", "max_partition_size", 10737418240))
	lo, hi := store.PartitionRange(store.PartitionOf(docid))
	size, err := s.store.SizeDocsRange(r.Context(), db, lo, hi)
	if err != nil {
		return err
	}
	newRaw, _ := json.Marshal(body)
	oldSize, err := s.store.DocBodySize(r.Context(), db, docid)
	if err != nil {
		return err
	}
	// CouchDB's rule: once the partition sits at the limit, only writes
	// that shrink it may land (the write crossing the line is allowed).
	if int64(len(newRaw)) > oldSize && size >= max {
		return couch.NewError(403, "partition_overflow",
			"Partition limit exceeded due to update on '"+docid+"'")
	}
	return nil
}

// partitionInfo is GET /{db}/_partition/{partition}.
func (s *Server) partitionInfo(w http.ResponseWriter, r *http.Request) error {
	db, partition, err := s.partitionDB(r)
	if err != nil {
		return err
	}
	lo, hi := store.PartitionRange(partition)
	docCount, err := s.store.CountDocsRange(r.Context(), db, lo, hi, false)
	if err != nil {
		return err
	}
	delCount, err := s.store.CountDocsRange(r.Context(), db, lo, hi, true)
	if err != nil {
		return err
	}
	size, err := s.store.SizeDocsRange(r.Context(), db, lo, hi)
	if err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{
		"db_name":       db.Name,
		"partition":     partition,
		"doc_count":     docCount,
		"doc_del_count": delCount,
		"sizes":         map[string]int64{"active": size, "external": size},
	})
	return nil
}

// partitionAllDocs serves GET/POST /{db}/_partition/{partition}/_all_docs:
// the partition's id range with partition-scoped totals.
func (s *Server) partitionAllDocsGet(w http.ResponseWriter, r *http.Request) error {
	return s.partitionAllDocsImpl(w, r, nil)
}

func (s *Server) partitionAllDocsPost(w http.ResponseWriter, r *http.Request) error {
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	return s.partitionAllDocsImpl(w, r, obj)
}

func (s *Server) partitionAllDocsImpl(w http.ResponseWriter, r *http.Request, body map[string]any) error {
	db, partition, err := s.partitionDB(r)
	if err != nil {
		return err
	}
	req, err := buildAllDocsRequest(r.URL.Query(), body)
	if err != nil {
		return err
	}
	if err := s.partitionQueryLimit(req.params.Limit); err != nil {
		return err
	}
	lo, hi := store.PartitionRange(partition)
	clamp := func(v *string, def string, isLow bool) *string {
		if v == nil {
			return &def
		}
		if isLow {
			if *v < lo {
				return &def
			}
		} else if *v > hi {
			return &def
		}
		return v
	}
	if req.params.Descending {
		req.params.StartKey = clamp(req.params.StartKey, hi, false)
		req.params.EndKey = clamp(req.params.EndKey, lo, true)
	} else {
		req.params.StartKey = clamp(req.params.StartKey, lo, true)
		req.params.EndKey = clamp(req.params.EndKey, hi, false)
	}
	// The store's default offset is database-relative. Avoid paying for that
	// count because this endpoint replaces it with a partition-relative value.
	req.params.OmitOffset = !req.sendKeys

	page, err := s.runAllDocs(r, db, req)
	if err != nil {
		return err
	}
	total, err := s.store.CountDocsRange(r.Context(), db, lo, hi, false)
	if err != nil {
		return err
	}
	page.TotalRows = total
	if !req.sendKeys {
		var preceding int64
		if !req.params.Descending {
			if preceding, err = s.store.CountDocsRange(r.Context(), db, lo, *req.params.StartKey, false); err != nil {
				return err
			}
		} else {
			// Rows before a descending window: ids above the (inclusive)
			// start bound.
			upToIncl, err := s.store.CountDocsThrough(r.Context(), db, lo, *req.params.StartKey, false)
			if err != nil {
				return err
			}
			preceding = total - upToIncl
			if preceding < 0 {
				preceding = 0
			}
		}
		offset := preceding + req.params.Skip
		page.Offset = &offset
	}
	writeJSON(w, 200, pageJSON(page, req.params.IncludeDocs))
	return nil
}

// partitionView serves /{db}/_partition/{partition}/_design/{ddoc}/_view/{view}.
func (s *Server) partitionViewGet(w http.ResponseWriter, r *http.Request) error {
	return s.viewImplPartition(w, r, nil, r.PathValue("partition"))
}

func (s *Server) partitionViewPost(w http.ResponseWriter, r *http.Request) error {
	body, err := readJSONBody(r)
	if err != nil {
		return err
	}
	obj, ok := body.(map[string]any)
	if !ok {
		return couch.BadRequest("Request body must be a JSON object")
	}
	return s.viewImplPartition(w, r, obj, r.PathValue("partition"))
}
