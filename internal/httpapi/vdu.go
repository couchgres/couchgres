package httpapi

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/jsengine"
	"github.com/couchgres/couchgres/internal/store"
)

// vduFn is one validate_doc_update function. sig keys the JS context cache
// (md5 of the source, so identical functions share compiled contexts).
type vduFn struct {
	ddocID string
	sig    string
	src    string
	ddoc   json.RawMessage // the full ddoc: require() tree and `this`
}

// vduCache holds each database's validate functions, invalidated by watching
// the highest design-doc seq (one indexed query per single write or bulk
// request).
type vduCache struct {
	mu            sync.Mutex
	entries       map[string]vduCacheEntry
	designProbes  atomic.Uint64
	oldDocBatches atomic.Uint64
	securityReads atomic.Uint64
}

type vduCacheEntry struct {
	designSeq int64
	fns       []vduFn
}

func newVDUCache() *vduCache {
	return &vduCache{entries: make(map[string]vduCacheEntry)}
}

type vduCacheStats struct {
	DesignProbes  uint64
	OldDocBatches uint64
	SecurityReads uint64
}

func (c *vduCache) stats() vduCacheStats {
	return vduCacheStats{
		DesignProbes:  c.designProbes.Load(),
		OldDocBatches: c.oldDocBatches.Load(),
		SecurityReads: c.securityReads.Load(),
	}
}

// vduFns returns the database's validate_doc_update functions in design-doc
// id order.
func (s *Server) vduFns(ctx context.Context, db *store.DB) ([]vduFn, error) {
	s.vdu.designProbes.Add(1)
	designSeq, err := s.store.MaxDesignSeq(ctx, db)
	if err != nil {
		return nil, err
	}
	s.vdu.mu.Lock()
	entry, ok := s.vdu.entries[db.Name]
	s.vdu.mu.Unlock()
	if ok && entry.designSeq == designSeq {
		return entry.fns, nil
	}

	var fns []vduFn
	if designSeq > 0 {
		ddocs, err := s.store.DesignDocs(ctx, db)
		if err != nil {
			return nil, err
		}
		for _, ddoc := range ddocs {
			src, ok := ddoc.Body["validate_doc_update"].(string)
			if !ok || src == "" {
				continue
			}
			sum := md5.Sum([]byte(src))
			fns = append(fns, vduFn{
				ddocID: ddoc.ID,
				sig:    "vdu-" + ddoc.Rev.String() + "-" + hex.EncodeToString(sum[:]),
				src:    src,
				ddoc:   ddocJSON(ddoc),
			})
		}
	}
	s.vdu.mu.Lock()
	s.vdu.entries[db.Name] = vduCacheEntry{designSeq: designSeq, fns: fns}
	s.vdu.mu.Unlock()
	return fns, nil
}

// vduRequestSnapshot is the immutable validation state for one HTTP request.
// Bulk writes share it across every document, reducing VDU discovery, old
// winner loading, and security loading to at most one database call each.
type vduRequestSnapshot struct {
	fns     []vduFn
	oldDocs map[string]*store.DocRow
	oldRaw  map[string]json.RawMessage
	userRaw json.RawMessage
	secRaw  json.RawMessage
}

func (s *Server) newVDURequestSnapshot(
	r *http.Request,
	db *store.DB,
	docIDs []string,
	security map[string]any,
	fetchOldDocs bool,
) (*vduRequestSnapshot, error) {
	fns, err := s.vduFns(r.Context(), db)
	if err != nil {
		return nil, err
	}
	snapshot := &vduRequestSnapshot{fns: fns}
	if (len(fns) > 0 || fetchOldDocs) && len(docIDs) > 0 {
		s.vdu.oldDocBatches.Add(1)
		snapshot.oldDocs, err = s.store.GetDocsAny(r.Context(), db, docIDs)
		if err != nil {
			return nil, err
		}
	}
	if len(fns) == 0 {
		return snapshot, nil
	}

	snapshot.oldRaw = make(map[string]json.RawMessage, len(snapshot.oldDocs))
	for id, old := range snapshot.oldDocs {
		if old.Deleted {
			continue
		}
		raw, err := json.Marshal(old.JSON())
		if err != nil {
			return nil, err
		}
		snapshot.oldRaw[id] = raw
	}
	user := userOf(r)
	snapshot.userRaw, err = json.Marshal(map[string]any{
		"db": db.Name, "name": user.NameJSON(), "roles": user.RolesJSON(),
	})
	if err != nil {
		return nil, err
	}
	if security == nil {
		s.vdu.securityReads.Add(1)
		security, err = s.store.GetSecurity(r.Context(), db)
		if err != nil {
			return nil, err
		}
	}
	snapshot.secRaw, err = json.Marshal(security)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// validateDocUpdate runs every validate_doc_update function against the
// incoming write. newBody is the document body (underscore members split
// off), rev the rev the client supplied.
func (s *Server) validateDocUpdate(
	r *http.Request,
	db *store.DB,
	docid string,
	newBody map[string]any,
	rev *couch.Rev,
	deleted bool,
	attachments map[string]any,
) error {
	snapshot, err := s.newVDURequestSnapshot(
		r, db, []string{docid}, nil, false)
	if err != nil {
		return err
	}
	return s.validateDocUpdateWithSnapshot(
		r.Context(), snapshot, docid, newBody, rev, deleted, attachments)
}

func (s *Server) validateDocUpdateWithSnapshot(
	ctx context.Context,
	snapshot *vduRequestSnapshot,
	docid string,
	newBody map[string]any,
	rev *couch.Rev,
	deleted bool,
	attachments map[string]any,
) error {
	if snapshot == nil || len(snapshot.fns) == 0 {
		return nil
	}

	newDoc := make(map[string]any, len(newBody)+4)
	newDoc["_id"] = docid
	if rev != nil {
		newDoc["_rev"] = rev.String()
	}
	if deleted {
		newDoc["_deleted"] = true
	}
	if attachments != nil {
		newDoc["_attachments"] = attachments
	}
	for k, v := range newBody {
		newDoc[k] = v
	}
	newRaw, err := json.Marshal(newDoc)
	if err != nil {
		return err
	}

	for _, fn := range snapshot.fns {
		err := s.js.Validate(ctx, fn.sig, fn.src, newRaw, snapshot.oldRaw[docid],
			snapshot.userRaw, snapshot.secRaw, fn.ddoc)
		if err == nil {
			continue
		}
		var verr *jsengine.ValidationError
		if errors.As(err, &verr) {
			if verr.Kind == "unauthorized" {
				return couch.NewError(401, "unauthorized", verr.Message)
			}
			return couch.Forbidden(verr.Message)
		}
		if isJSProcessError(err) {
			return jsPoolError(err)
		}
		return couch.NewError(500, "unknown_error",
			"validation function "+fn.ddocID+": "+err.Error())
	}
	return nil
}
