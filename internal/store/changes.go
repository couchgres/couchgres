package store

import (
	"context"
	"fmt"

	"log/slog"
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
}

type ChangesParams struct {
	Since        int64
	Limit        *int64
	Descending   bool
	IncludeDocs  bool
	AllDocsStyle bool // style=all_docs reports every leaf revision.
	Conflicts    bool // conflicts=true collects live non-winner leaves.
	// DocIDs restricts the feed (filter=_doc_ids).
	DocIDs []string
	// DesignOnly restricts to design documents (filter=_design).
	DesignOnly bool
}

// Changes reads committed changes after Since, in seq order.
func (s *Store) Changes(ctx context.Context, db *DB, p *ChangesParams) ([]Change, error) {
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

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
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

	if p.AllDocsStyle || p.Conflicts {
		if err := s.attachConflictLeaves(ctx, db, changes, p.AllDocsStyle, p.Conflicts); err != nil {
			return nil, err
		}
	}
	return changes, nil
}

// attachConflictLeaves adds non-winner leaf revisions to each change row.
// It adds every leaf to LeafRevs for style=all_docs and live leaves to
// ConflictRevs for conflicts=true.
func (s *Store) attachConflictLeaves(ctx context.Context, db *DB, changes []Change, allLeaves, conflicts bool) error {
	if len(changes) == 0 {
		return nil
	}
	ids := make([]string, len(changes))
	index := make(map[string]int, len(changes))
	for i, c := range changes {
		ids[i] = c.ID
		index[c.ID] = i
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT id, rev_num, rev_hash, deleted FROM %s.revs
		 WHERE id = ANY($1) AND leaf
		 ORDER BY id, deleted ASC, rev_num DESC, rev_hash DESC`, db.Schema), ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var rev couch.Rev
		var deleted bool
		if err := rows.Scan(&id, &rev.Num, &rev.Hash, &deleted); err != nil {
			return err
		}
		i := index[id]
		if rev == changes[i].LeafRevs[0] {
			continue
		}
		if allLeaves {
			changes[i].LeafRevs = append(changes[i].LeafRevs, rev)
		}
		if conflicts && !deleted {
			changes[i].ConflictRevs = append(changes[i].ConflictRevs, rev)
		}
	}
	return rows.Err()
}

// PendingAfter counts changes past the given seq (the "pending" member).
func (s *Store) PendingAfter(ctx context.Context, db *DB, seq int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.docs WHERE seq > $1", db.Schema), seq).Scan(&n)
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
