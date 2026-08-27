package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// Purge removes the given leaf revisions and their exclusive ancestors from
// the database. It leaves no tombstones or changes-feed trace. Returns the revisions
// actually purged per doc (non-leaf and unknown revs are ignored, matching
// CouchDB).
func (s *Store) Purge(ctx context.Context, db *DB, requests map[string][]couch.Rev) (map[string][]couch.Rev, error) {
	// Eagerly evict this process's derived view state. Other processes reject
	// stale total-row entries through the durable purge generation.
	defer s.clearViewTotals(db)
	purged := make(map[string][]couch.Rev, len(requests))
	for id, revs := range requests {
		done, err := s.purgeDoc(ctx, db, id, revs)
		if err != nil {
			return nil, err
		}
		purged[id] = done
	}
	return purged, nil
}

func (s *Store) purgeDoc(ctx context.Context, db *DB, id string, revs []couch.Rev) ([]couch.Rev, error) {
	purged := []couch.Rev{}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, _, err := lockWinner(ctx, tx, db, id); err != nil {
		return nil, err
	}

	// Load the whole tree, including parent pointers and a child count per revision.
	type revNode struct {
		parentNum  int
		parentHash string
		hasParent  bool
		leaf       bool
		children   int
	}
	nodes := make(map[couch.Rev]*revNode)
	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT rev_num, rev_hash, parent_num, parent_hash, leaf FROM %s.revs WHERE id = $1",
		db.Schema), id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var rev couch.Rev
		var node revNode
		var pNum *int
		var pHash *string
		if err := rows.Scan(&rev.Num, &rev.Hash, &pNum, &pHash, &node.leaf); err != nil {
			rows.Close()
			return nil, err
		}
		if pNum != nil && pHash != nil {
			node.hasParent = true
			node.parentNum, node.parentHash = *pNum, *pHash
		}
		nodes[rev] = &node
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node.hasParent {
			parent := couch.Rev{Num: node.parentNum, Hash: node.parentHash}
			if p := nodes[parent]; p != nil {
				p.children++
			}
		}
	}

	// Only leaves are purged. Each takes its exclusive ancestor chain with it.
	var doomed []couch.Rev
	for _, rev := range revs {
		node := nodes[rev]
		if node == nil || !node.leaf {
			continue
		}
		purged = append(purged, rev)
		current, cnode := rev, node
		for {
			doomed = append(doomed, current)
			delete(nodes, current)
			if !cnode.hasParent {
				break
			}
			parent := couch.Rev{Num: cnode.parentNum, Hash: cnode.parentHash}
			pnode := nodes[parent]
			if pnode == nil {
				break
			}
			pnode.children--
			if pnode.children > 0 || pnode.leaf {
				break // Shared with another branch or a leaf itself.
			}
			current, cnode = parent, pnode
		}
	}
	if len(doomed) == 0 {
		return purged, tx.Rollback(ctx)
	}

	for _, rev := range doomed {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.revs WHERE id = $1 AND rev_num = $2 AND rev_hash = $3",
			db.Schema), id, rev.Num, rev.Hash); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.attachments WHERE doc_id = $1 AND rev_num = $2 AND rev_hash = $3",
			db.Schema), id, rev.Num, rev.Hash); err != nil {
			return nil, err
		}
	}

	// Any survivor whose children are all gone becomes a leaf again.
	survivorHasChild := make(map[couch.Rev]bool)
	for _, node := range nodes {
		if node.hasParent {
			survivorHasChild[couch.Rev{Num: node.parentNum, Hash: node.parentHash}] = true
		}
	}
	anyLeaf := false
	for rev, node := range nodes {
		isLeaf := !survivorHasChild[rev]
		if isLeaf {
			anyLeaf = true
		}
		if isLeaf != node.leaf {
			if _, err := tx.Exec(ctx, fmt.Sprintf(
				"UPDATE %s.revs SET leaf = $4 WHERE id = $1 AND rev_num = $2 AND rev_hash = $3",
				db.Schema), id, rev.Num, rev.Hash, isLeaf); err != nil {
				return nil, err
			}
		}
	}

	if !anyLeaf {
		// The document is gone entirely.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.docs WHERE id = $1", db.Schema), id); err != nil {
			return nil, err
		}
	} else if _, err := finishWrite(ctx, tx, db, id); err != nil {
		return nil, err
	}

	// Purged docs vanish from every view index.
	if err := s.purgeFromViews(ctx, tx, db, id, !anyLeaf); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx,
		"UPDATE couchgres.databases SET purge_seq = purge_seq + 1 WHERE name = $1",
		db.Name); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.notifyChanged(db.Name)
	return purged, nil
}

// purgeFromViews deletes the doc's rows from every view table when the doc
// disappeared (a surviving winner reindexes through finishWrite's seq bump).
func (s *Store) purgeFromViews(ctx context.Context, tx pgx.Tx, db *DB, id string, gone bool) error {
	if !gone {
		return nil
	}
	// View updaters lock their state row before changing indexed rows. Take the
	// same lock order so the purge-generation trigger cannot invert those locks
	// after this function deletes from the per-signature tables.
	rows, err := tx.Query(ctx, fmt.Sprintf(
		"SELECT sig FROM %s.view_state FOR UPDATE", db.Schema))
	if err != nil {
		return nil // No view_state table exists yet. Nothing to clean.
	}
	var sigs []string
	for rows.Next() {
		var sig string
		if err := rows.Scan(&sig); err != nil {
			rows.Close()
			return err
		}
		sigs = append(sigs, sig)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, sig := range sigs {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.%s WHERE doc_id = $1", db.Schema, viewTable(sig)), id); err != nil {
			return err
		}
	}
	return nil
}

// PurgeSeq reads the database's purge counter.
func (s *Store) PurgeSeq(ctx context.Context, db *DB) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx,
		"SELECT purge_seq FROM couchgres.databases WHERE name = $1", db.Name,
	).Scan(&seq)
	return seq, err
}

// PurgedInfosLimit reads and SetPurgedInfosLimit writes the per-database limit.
// It is bookkeeping only because no purge history is retained.
func (s *Store) PurgedInfosLimit(ctx context.Context, db *DB) (int64, error) {
	var limit int64
	err := s.pool.QueryRow(ctx,
		"SELECT purged_infos_limit FROM couchgres.databases WHERE name = $1", db.Name,
	).Scan(&limit)
	return limit, err
}

func (s *Store) SetPurgedInfosLimit(ctx context.Context, db *DB, limit int64) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE couchgres.databases SET purged_infos_limit = $1 WHERE name = $2",
		limit, db.Name)
	return err
}
