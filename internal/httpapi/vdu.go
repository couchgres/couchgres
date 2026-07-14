package httpapi

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

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

// vduCache holds each database's validate functions, invalidated by
// watching the highest design-doc seq (one indexed count query per write).
type vduCache struct {
	mu      sync.Mutex
	entries map[string]vduCacheEntry
}

type vduCacheEntry struct {
	designSeq int64
	fns       []vduFn
}

func newVDUCache() *vduCache {
	return &vduCache{entries: make(map[string]vduCacheEntry)}
}

// vduFns returns the database's validate_doc_update functions in design-doc
// id order.
func (s *Server) vduFns(ctx context.Context, db *store.DB) ([]vduFn, error) {
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
	fns, err := s.vduFns(r.Context(), db)
	if err != nil {
		return err
	}
	if len(fns) == 0 {
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

	var oldRaw json.RawMessage
	if old, err := s.store.GetDocAny(r.Context(), db, docid); err == nil && !old.Deleted {
		if oldRaw, err = json.Marshal(old.JSON()); err != nil {
			return err
		}
	}

	user := userOf(r)
	var name any
	if !user.IsAnonymous() {
		name = user.Name
	}
	roles := user.Roles
	if roles == nil {
		roles = []string{}
	}
	userRaw, err := json.Marshal(map[string]any{
		"db": db.Name, "name": name, "roles": roles,
	})
	if err != nil {
		return err
	}
	secMap, err := s.store.GetSecurity(r.Context(), db)
	if err != nil {
		return err
	}
	secRaw, err := json.Marshal(secMap)
	if err != nil {
		return err
	}

	for _, fn := range fns {
		err := s.js.Validate(r.Context(), fn.sig, fn.src, newRaw, oldRaw, userRaw, secRaw, fn.ddoc)
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
		return couch.NewError(500, "unknown_error",
			"validation function "+fn.ddocID+": "+err.Error())
	}
	return nil
}
