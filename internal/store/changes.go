package store

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// Change is one row of a database's changes feed.
type Change struct {
	Seq     int64
	ID      string
	Deleted bool
	// LeafRevs holds the winning rev first, then conflict leaves (only
	// populated beyond the winner for style=all_docs).
	LeafRevs []couch.Rev
	// ConflictRevs holds live non-winner leaves (populated for
	// conflicts=true with include_docs. It feeds the document's _conflicts member.
	ConflictRevs []couch.Rev
	// Body is the winning body (populated for include_docs).
	Body map[string]any
	// Attachments holds the winning revision's attachment metadata or bodies
	// when an include_docs response needs them.
	Attachments []Attachment
}

type ChangesParams struct {
	Since        int64
	Limit        *int64
	Descending   bool
	IncludeDocs  bool
	AllDocsStyle bool // style=all_docs reports every leaf revision.
	Conflicts    bool // conflicts=true collects live non-winner leaves.
	// IncludeAttachments adds attachment stubs to included docs. AttachmentData
	// inlines their bodies for attachments=true.
	IncludeAttachments bool
	AttachmentData     bool
	// DocIDs restricts the feed (filter=_doc_ids).
	DocIDs []string
	// DesignOnly restricts to design documents (filter=_design).
	DesignOnly bool
}

const pendingCountCacheCapacity = 1024

type pendingCountKey struct {
	schema   string
	after    int64
	maxSeq   int64
	purgeSeq int64
	snapshot string
}

// pendingCountCache is a bounded FIFO cache. A client controls after, so an
// unbounded map would turn arbitrary since values into retained memory.
type pendingCountCache struct {
	mu     sync.Mutex
	values map[pendingCountKey]int64
	order  []pendingCountKey
	next   int
}

func (c *pendingCountCache) get(key pendingCountKey) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

func (c *pendingCountCache) put(key pendingCountKey, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[pendingCountKey]int64, pendingCountCacheCapacity)
	}
	if _, ok := c.values[key]; ok {
		c.values[key] = value
		return
	}
	if len(c.order) < pendingCountCacheCapacity {
		c.order = append(c.order, key)
	} else {
		delete(c.values, c.order[c.next])
		c.order[c.next] = key
		c.next = (c.next + 1) % pendingCountCacheCapacity
	}
	c.values[key] = value
}

// Changes reads committed changes after Since, in seq order.
func (s *Store) Changes(ctx context.Context, db *DB, p *ChangesParams) ([]Change, error) {
	if !p.IncludeDocs && !p.AllDocsStyle && !p.Conflicts && !p.IncludeAttachments {
		return s.changes(ctx, s.pool, db, p)
	}
	tx, err := s.beginReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	changes, err := s.changes(ctx, tx, db, p)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return changes, nil
}

func (s *Store) changes(
	ctx context.Context,
	qx dbQueryer,
	db *DB,
	p *ChangesParams,
) ([]Change, error) {
	conditions := []string{"d.seq > $1"}
	args := []any{p.Since}
	if len(p.DocIDs) > 0 {
		args = append(args, p.DocIDs)
		conditions = append(conditions, fmt.Sprintf("d.id = ANY($%d)", len(args)))
	}
	if p.DesignOnly {
		conditions = append(conditions, "d.id LIKE '\\_design/%'")
	}
	order := "ASC"
	if p.Descending {
		order = "DESC"
	}
	limit := ""
	if p.Limit != nil {
		limit = fmt.Sprintf("LIMIT %d", *p.Limit)
	}
	// Bodies live in revs. Only include_docs pays for the join.
	bodySel, join := "null::jsonb", ""
	if p.IncludeDocs {
		bodySel, join = winnerBody, " "+winnerJoin(db.Schema)
	}
	query := fmt.Sprintf(
		"SELECT d.seq, d.id, d.rev_num, d.rev_hash, d.deleted, %s FROM %s.docs d%s WHERE %s ORDER BY d.seq %s %s",
		bodySel, db.Schema, join, joinAnd(conditions), order, limit)

	rows, err := qx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var changes []Change
	for rows.Next() {
		var c Change
		var rev couch.Rev
		var raw []byte
		if err := rows.Scan(&c.Seq, &c.ID, &rev.Num, &rev.Hash, &c.Deleted, &raw); err != nil {
			return nil, err
		}
		c.LeafRevs = []couch.Rev{rev}
		if p.IncludeDocs {
			if c.Body, err = decodeBody(raw); err != nil {
				return nil, err
			}
		}
		changes = append(changes, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	if p.AllDocsStyle || p.Conflicts {
		if err := s.attachConflictLeaves(
			ctx, qx, db, changes, p.AllDocsStyle, p.Conflicts); err != nil {
			return nil, err
		}
	}
	if p.IncludeAttachments {
		if err := s.attachChangeAttachments(
			ctx, qx, db, changes, p.AttachmentData); err != nil {
			return nil, err
		}
	}
	return changes, nil
}

// attachConflictLeaves adds non-winner leaf revisions to each change row.
// It adds every leaf to LeafRevs for style=all_docs and live leaves to
// ConflictRevs for conflicts=true.
func (s *Store) attachConflictLeaves(
	ctx context.Context,
	qx dbQueryer,
	db *DB,
	changes []Change,
	allLeaves, conflicts bool,
) error {
	if len(changes) == 0 {
		return nil
	}
	ids := make([]string, len(changes))
	for i, c := range changes {
		ids[i] = c.ID
	}
	leavesByID, err := batchLeafRevisions(ctx, qx, db, ids)
	if err != nil {
		return err
	}
	for i := range changes {
		leaves := leavesByID[changes[i].ID]
		sort.Slice(leaves, func(a, b int) bool {
			if leaves[a].Deleted != leaves[b].Deleted {
				return !leaves[a].Deleted
			}
			if leaves[a].Rev.Num != leaves[b].Rev.Num {
				return leaves[a].Rev.Num > leaves[b].Rev.Num
			}
			return leaves[a].Rev.Hash > leaves[b].Rev.Hash
		})
		for _, leaf := range leaves {
			if leaf.Rev == changes[i].LeafRevs[0] {
				continue
			}
			if allLeaves {
				changes[i].LeafRevs = append(changes[i].LeafRevs, leaf.Rev)
			}
			if conflicts && !leaf.Deleted {
				changes[i].ConflictRevs = append(changes[i].ConflictRevs, leaf.Rev)
			}
		}
	}
	return nil
}

func (s *Store) attachChangeAttachments(
	ctx context.Context,
	qx dbQueryer,
	db *DB,
	changes []Change,
	includeData bool,
) error {
	refs := make([]docRef, 0, len(changes))
	for i := range changes {
		if changes[i].Body != nil && len(changes[i].LeafRevs) > 0 {
			refs = append(refs, docRef{ID: changes[i].ID, Rev: changes[i].LeafRevs[0]})
		}
	}
	atts, err := batchAttachments(ctx, qx, db, refs, includeData)
	if err != nil {
		return err
	}
	for i := range changes {
		if changes[i].Body != nil && len(changes[i].LeafRevs) > 0 {
			ref := docRef{ID: changes[i].ID, Rev: changes[i].LeafRevs[0]}
			changes[i].Attachments = atts[ref]
		}
	}
	return nil
}

// AttachChangeAttachments batch-loads exact-revision attachments for changes
// that survived an application-level filter. Unfiltered feeds load them inside
// Changes' repeatable-read snapshot.
func (s *Store) AttachChangeAttachments(
	ctx context.Context,
	db *DB,
	changes []Change,
	includeData bool,
) error {
	return s.attachChangeAttachments(ctx, s.pool, db, changes, includeData)
}

// PendingAfter counts changes past the given seq (the "pending" member).
// Repeated polls first read the committed maximum docs sequence through the
// sequence index and reuse a bounded exact count for that generation. purge_seq
// is part of the key because a full purge can remove a docs row without moving
// its update sequence. The transaction snapshot protects the cache from an
// older sequence committing after a newer one (which would leave maxSeq
// unchanged).
func (s *Store) PendingAfter(ctx context.Context, db *DB, seq int64) (int64, error) {
	var maxSeq, purgeSeq int64
	var snapshot string
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce((SELECT seq FROM %s.docs ORDER BY seq DESC LIMIT 1), 0),
		        purge_seq, pg_current_snapshot()::text
		 FROM couchgres.databases WHERE name = $1`, db.Schema), db.Name,
	).Scan(&maxSeq, &purgeSeq, &snapshot); err != nil {
		return 0, err
	}
	if seq >= maxSeq {
		return 0, nil
	}
	key := pendingCountKey{
		schema: db.Schema, after: seq, maxSeq: maxSeq, purgeSeq: purgeSeq,
		snapshot: snapshot,
	}
	if n, ok := s.pendingCounts.get(key); ok {
		return n, nil
	}
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.docs WHERE seq > $1", db.Schema), seq).Scan(&n)
	if err == nil {
		s.pendingCounts.put(key, n)
	}
	return n, err
}

func joinAnd(conditions []string) string {
	out := conditions[0]
	for _, c := range conditions[1:] {
		out += " AND " + c
	}
	return out
}

// ---------------------------------------------------------------------------
// Change and cache invalidation notifications. One dedicated LISTEN
// connection fans Postgres NOTIFY events out to local subscribers.
// ---------------------------------------------------------------------------

type Broker struct {
	mu            sync.Mutex
	subs          map[string]map[chan struct{}]struct{} // Database name to subscribers.
	all           map[chan struct{}]struct{}            // Every-event subscribers.
	invalidations map[string]map[chan struct{}]struct{} // Cache name to subscribers.
}

// Subscribe returns a channel that receives a token whenever the named
// database changes, and a function to unsubscribe.
func (b *Broker) Subscribe(dbName string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	if b.subs[dbName] == nil {
		b.subs[dbName] = make(map[chan struct{}]struct{})
	}
	b.subs[dbName][ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs[dbName], ch)
		b.mu.Unlock()
	}
}

func (b *Broker) notify(dbName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs[dbName] {
		select {
		case ch <- struct{}{}:
		default: // subscriber already has a pending wake-up
		}
	}
	for ch := range b.all {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SubscribeInvalidation returns a channel that receives a token whenever the
// named process-local cache should check its durable version.
func (b *Broker) SubscribeInvalidation(name string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	if b.invalidations[name] == nil {
		b.invalidations[name] = make(map[chan struct{}]struct{})
	}
	b.invalidations[name][ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.invalidations[name], ch)
		b.mu.Unlock()
	}
}

func (b *Broker) notifyInvalidation(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.invalidations[name] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SubscribeAll wakes on activity in any database (the _db_updates feed).
func (b *Broker) SubscribeAll() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	if b.all == nil {
		b.all = make(map[chan struct{}]struct{})
	}
	b.all[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.all, ch)
		b.mu.Unlock()
	}
}

// StartBroker listens on the couchgres_changes and couchgres_invalidations
// channels until ctx ends, reconnecting with backoff on connection loss. It
// also starts the debounced pg_notify loop that fans this process's writes out
// to other processes on the same Postgres.
func (s *Store) StartBroker(ctx context.Context) *Broker {
	broker := &Broker{
		subs:          make(map[string]map[chan struct{}]struct{}),
		invalidations: make(map[string]map[chan struct{}]struct{}),
	}
	s.broker.Store(broker)
	go s.notifyLoop(ctx)
	go func() {
		for ctx.Err() == nil {
			if err := s.listen(ctx, broker); err != nil && ctx.Err() == nil {
				slog.Warn("changes listener reconnecting", "error", err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
				}
			}
		}
	}()
	return broker
}

// notifyChanged wakes subscribers of one database after a committed write.
// Local subscribers wake immediately through the broker. Listeners in other
// couchgres processes get a pg_notify from notifyLoop. It coalesces a burst
// of concurrent writers into one NOTIFY and avoids contention for
// Postgres's global notification queue lock once per transaction.
func (s *Store) notifyChanged(dbName string) {
	if b := s.broker.Load(); b != nil {
		b.notify(dbName)
	}
	s.notifyMu.Lock()
	s.notifyPending[dbName] = struct{}{}
	s.notifyMu.Unlock()
	select {
	case s.notifyKick <- struct{}{}:
	default: // a flush is already pending
	}
}

func (s *Store) notifyInvalidated(name string) {
	if b := s.broker.Load(); b != nil {
		b.notifyInvalidation(name)
	}
	s.notifyMu.Lock()
	s.invPending[name] = struct{}{}
	s.notifyMu.Unlock()
	select {
	case s.notifyKick <- struct{}{}:
	default:
	}
}

// notifyLoop turns pending database names into pg_notify calls, waiting a
// few milliseconds after each kick so concurrent writers share one round
// trip. Subscribers re-read from their last seq on wake, so coalescing
// never loses a change. It only defers the cross-process wake-up.
func (s *Store) notifyLoop(ctx context.Context) {
	const debounce = 3 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notifyKick:
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(debounce):
		}
		s.notifyMu.Lock()
		names := make([]string, 0, len(s.notifyPending))
		for name := range s.notifyPending {
			names = append(names, name)
		}
		invalidations := make([]string, 0, len(s.invPending))
		for name := range s.invPending {
			invalidations = append(invalidations, name)
		}
		clear(s.notifyPending)
		clear(s.invPending)
		s.notifyMu.Unlock()
		if len(names) == 0 && len(invalidations) == 0 {
			continue
		}
		if len(names) > 0 {
			if _, err := s.pool.Exec(ctx,
				"SELECT pg_notify('couchgres_changes', n) FROM unnest($1::text[]) AS t(n)",
				names,
			); err != nil && ctx.Err() == nil {
				slog.Warn("change notify failed; will retry on next write", "error", err)
				s.notifyMu.Lock()
				for _, name := range names {
					s.notifyPending[name] = struct{}{}
				}
				s.notifyMu.Unlock()
			}
		}
		if len(invalidations) > 0 {
			if _, err := s.pool.Exec(ctx,
				"SELECT pg_notify('couchgres_invalidations', n) FROM unnest($1::text[]) AS t(n)",
				invalidations,
			); err != nil && ctx.Err() == nil {
				slog.Warn("cache invalidation notify failed; will retry on next write", "error", err)
				s.notifyMu.Lock()
				for _, name := range invalidations {
					s.invPending[name] = struct{}{}
				}
				s.notifyMu.Unlock()
			}
		}
	}
}

func (s *Store) listen(ctx context.Context, broker *Broker) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN couchgres_changes"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "LISTEN couchgres_invalidations"); err != nil {
		return err
	}
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch notification.Channel {
		case "couchgres_changes":
			broker.notify(notification.Payload)
		case "couchgres_invalidations":
			broker.notifyInvalidation(notification.Payload)
		}
	}
}

// DBEvent is one row of the global created/updated/deleted feed.
type DBEvent struct {
	Seq  int64
	Name string
	Type string
}

// DBEventsSince reads persisted database lifecycle events (created/deleted).
// Per-write "updated" events are delivered live by the feeds, not stored.
func (s *Store) DBEventsSince(ctx context.Context, since int64) ([]DBEvent, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT event_seq, db_name, type FROM couchgres.db_events WHERE event_seq > $1 ORDER BY event_seq",
		since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[DBEvent])
}
