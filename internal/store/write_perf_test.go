package store

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type writeQueryCounter struct {
	queries atomic.Int64
}

func (c *writeQueryCounter) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	c.queries.Add(1)
	return ctx
}

func (*writeQueryCounter) TraceQueryEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceQueryEndData,
) {
}

func (c *writeQueryCounter) TraceBatchStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceBatchStartData,
) context.Context {
	c.queries.Add(1)
	return ctx
}

func (*writeQueryCounter) TraceBatchQuery(
	context.Context,
	*pgx.Conn,
	pgx.TraceBatchQueryData,
) {
}

func (*writeQueryCounter) TraceBatchEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceBatchEndData,
) {
}

func writePerfStore(t testing.TB, counter *writeQueryCounter) *Store {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testStoreURL())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 16
	cfg.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Skipf("Postgres unavailable at %s: %v", testStoreURL(), err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Skipf("Postgres unavailable at %s: %v", testStoreURL(), err)
	}
	s := &Store{
		pool:          pool,
		notifyPending: make(map[string]struct{}),
		invPending:    make(map[string]struct{}),
		notifyKick:    make(chan struct{}, 1),
	}
	if _, err := s.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}

func benchmarkSeedWrites(
	b *testing.B,
	s *Store,
	db *DB,
	writes []BulkWrite,
) []BulkResult {
	b.Helper()
	results, err := s.BulkPutDocs(b.Context(), db, writes)
	if err != nil {
		b.Fatal(err)
	}
	for i, result := range results {
		if result.Err != nil {
			b.Fatalf("seed %d: %v", i, result.Err)
		}
	}
	return results
}

func startWriteBenchmark(
	b *testing.B,
	s *Store,
	counter *writeQueryCounter,
) string {
	b.Helper()
	var lsn string
	if err := s.pool.QueryRow(b.Context(),
		"SELECT pg_current_wal_insert_lsn()::text").Scan(&lsn); err != nil {
		b.Fatal(err)
	}
	counter.queries.Store(0)
	b.ResetTimer()
	return lsn
}

func reportWriteBenchmark(
	b *testing.B,
	s *Store,
	counter *writeQueryCounter,
	startLSN string,
	latencies []time.Duration,
) {
	b.Helper()
	b.StopTimer()
	queries := counter.queries.Load()
	var walBytes float64
	if err := s.pool.QueryRow(b.Context(),
		"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)",
		startLSN).Scan(&walBytes); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(queries)/float64(b.N), "roundtrips/op")
	b.ReportMetric(walBytes/float64(b.N), "wal_bytes/op")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
	if len(latencies) > 0 {
		slices.Sort(latencies)
		p95 := latencies[(len(latencies)*95+99)/100-1]
		b.ReportMetric(float64(p95.Nanoseconds()), "p95_ns/op")
	}
}

func assertWriteRoundTrips(
	t *testing.T,
	counter *writeQueryCounter,
	want int64,
	write func() error,
) {
	t.Helper()
	counter.queries.Store(0)
	if err := write(); err != nil {
		t.Fatal(err)
	}
	if got := counter.queries.Load(); got != want {
		t.Fatalf("database round trips = %d, want %d", got, want)
	}
}

func TestPutDocRoundTrips(t *testing.T) {
	counter := &writeQueryCounter{}
	s := writePerfStore(t, counter)

	t.Run("create", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_create")
		assertWriteRoundTrips(t, counter, 4, func() error {
			_, _, err := s.PutDoc(t.Context(), db, "doc",
				map[string]any{"value": 1}, nil, nil, false, nil)
			return err
		})
	})

	t.Run("update", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_update")
		rev, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{"value": 1}, nil, nil, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		assertWriteRoundTrips(t, counter, 4, func() error {
			_, _, err := s.PutDoc(t.Context(), db, "doc",
				map[string]any{"value": 2}, nil, &rev, false, nil)
			return err
		})
	})

	t.Run("delete", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_delete")
		rev, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{"value": 1}, nil, nil, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		assertWriteRoundTrips(t, counter, 4, func() error {
			_, _, err := s.PutDoc(t.Context(), db, "doc",
				map[string]any{}, nil, &rev, true, nil)
			return err
		})
	})

	t.Run("conflict", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_conflict")
		if _, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{"value": 1}, nil, nil, false, nil); err != nil {
			t.Fatal(err)
		}
		counter.queries.Store(0)
		if _, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{"value": 2}, nil, nil, false, nil); err == nil {
			t.Fatal("expected conflict")
		}
		if got := counter.queries.Load(); got != 3 {
			t.Fatalf("database round trips = %d, want 3", got)
		}
	})

	t.Run("attachment_stub", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_stub")
		rev, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{"value": 1}, nil, nil, false, []AttachmentWrite{{
				Name: "payload.bin", ContentType: "application/octet-stream",
				Data: []byte("payload"),
			}})
		if err != nil {
			t.Fatal(err)
		}
		assertWriteRoundTrips(t, counter, 7, func() error {
			_, _, err := s.PutDoc(t.Context(), db, "doc",
				map[string]any{"value": 2}, nil, &rev, false,
				[]AttachmentWrite{{Name: "payload.bin"}})
			return err
		})
	})

	t.Run("tombstone_recreate", func(t *testing.T) {
		db := freshDB(t, s, "it_put_roundtrips_recreate")
		if _, _, err := s.PutDoc(t.Context(), db, "doc",
			map[string]any{}, nil, nil, true, nil); err != nil {
			t.Fatal(err)
		}
		assertWriteRoundTrips(t, counter, 4, func() error {
			_, _, err := s.PutDoc(t.Context(), db, "doc",
				map[string]any{"recreated": true}, nil, nil, false, nil)
			return err
		})
	})
}

func BenchmarkPutDocPaths(b *testing.B) {
	counter := &writeQueryCounter{}
	s := writePerfStore(b, counter)

	b.Run("create", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_create")
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, fmt.Sprintf("doc-%08d", i),
				map[string]any{"value": i}, nil, nil, false, nil); err != nil {
				b.Fatal(err)
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})

	b.Run("update", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_update")
		writes := make([]BulkWrite, b.N)
		for i := range writes {
			writes[i] = BulkWrite{ID: fmt.Sprintf("doc-%08d", i), Body: map[string]any{"value": 0}}
		}
		results := benchmarkSeedWrites(b, s, db, writes)
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, writes[i].ID,
				map[string]any{"value": 1}, nil, &results[i].Rev, false, nil); err != nil {
				b.Fatal(err)
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})

	b.Run("delete", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_delete")
		writes := make([]BulkWrite, b.N)
		for i := range writes {
			writes[i] = BulkWrite{ID: fmt.Sprintf("doc-%08d", i), Body: map[string]any{"value": i}}
		}
		results := benchmarkSeedWrites(b, s, db, writes)
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, writes[i].ID,
				map[string]any{}, nil, &results[i].Rev, true, nil); err != nil {
				b.Fatal(err)
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})

	b.Run("conflict", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_conflict")
		if _, _, err := s.PutDoc(b.Context(), db, "doc",
			map[string]any{"value": 0}, nil, nil, false, nil); err != nil {
			b.Fatal(err)
		}
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, "doc",
				map[string]any{"value": 1}, nil, nil, false, nil); err == nil {
				b.Fatal("expected conflict")
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})

	b.Run("attachment_stub", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_attachment_stub")
		writes := make([]BulkWrite, b.N)
		for i := range writes {
			writes[i] = BulkWrite{
				ID:   fmt.Sprintf("doc-%08d", i),
				Body: map[string]any{"value": 0},
				Atts: []AttachmentWrite{{
					Name: "payload.bin", ContentType: "application/octet-stream",
					Data: []byte("payload"),
				}},
			}
		}
		results := benchmarkSeedWrites(b, s, db, writes)
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, writes[i].ID,
				map[string]any{"value": 1}, nil, &results[i].Rev, false,
				[]AttachmentWrite{{Name: "payload.bin"}}); err != nil {
				b.Fatal(err)
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})

	b.Run("tombstone_recreate", func(b *testing.B) {
		db := freshDB(b, s, "bench_put_tombstone_recreate")
		writes := make([]BulkWrite, b.N)
		for i := range writes {
			writes[i] = BulkWrite{
				ID: fmt.Sprintf("doc-%08d", i), Body: map[string]any{}, Deleted: true,
			}
		}
		benchmarkSeedWrites(b, s, db, writes)
		latencies := make([]time.Duration, b.N)
		lsn := startWriteBenchmark(b, s, counter)
		for i := range b.N {
			started := time.Now()
			if _, _, err := s.PutDoc(b.Context(), db, writes[i].ID,
				map[string]any{"recreated": true}, nil, nil, false, nil); err != nil {
				b.Fatal(err)
			}
			latencies[i] = time.Since(started)
		}
		reportWriteBenchmark(b, s, counter, lsn, latencies)
	})
}

func BenchmarkPutDocConcurrentCreates(b *testing.B) {
	counter := &writeQueryCounter{}
	s := writePerfStore(b, counter)

	for _, clients := range []int{1, 16} {
		b.Run(fmt.Sprintf("%d_clients", clients), func(b *testing.B) {
			db := freshDB(b, s, fmt.Sprintf("bench_put_concurrent_%d", clients))
			latencies := make([]time.Duration, b.N)
			errs := make(chan error, clients)
			var wg sync.WaitGroup
			lsn := startWriteBenchmark(b, s, counter)
			for worker := range clients {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := worker; i < b.N; i += clients {
						started := time.Now()
						_, _, err := s.PutDoc(b.Context(), db,
							fmt.Sprintf("doc-%08d", i), map[string]any{"value": i},
							nil, nil, false, nil)
						latencies[i] = time.Since(started)
						if err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				b.Error(err)
			}
			reportWriteBenchmark(b, s, counter, lsn, latencies)
		})
	}
}

// BenchmarkDocumentCounterContention isolates the docs-table trigger from
// revision hashing and transactional sequence allocation. It makes contention
// on the exact database counters visible independently of the rest of PutDoc.
func BenchmarkDocumentCounterContention(b *testing.B) {
	counter := &writeQueryCounter{}
	s := writePerfStore(b, counter)

	for _, clients := range []int{1, 16} {
		b.Run(fmt.Sprintf("%d_clients", clients), func(b *testing.B) {
			db := freshDB(b, s, fmt.Sprintf("bench_doc_counters_%d", clients))
			query := fmt.Sprintf(
				`INSERT INTO %s.docs
				   (id, rev_num, rev_hash, deleted, seq, external_size)
				 VALUES ($1, 1, 'benchmark', false, $2, 1)`, db.Schema)
			latencies := make([]time.Duration, b.N)
			errs := make(chan error, clients)
			var wg sync.WaitGroup
			lsn := startWriteBenchmark(b, s, counter)
			for worker := range clients {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := worker; i < b.N; i += clients {
						started := time.Now()
						_, err := s.pool.Exec(b.Context(), query,
							fmt.Sprintf("doc-%08d", i), i+1)
						latencies[i] = time.Since(started)
						if err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				b.Error(err)
			}
			reportWriteBenchmark(b, s, counter, lsn, latencies)
		})
	}
}

// BenchmarkDatabaseStatsRead compares the fixed stripe aggregate used by
// database info with the former exact winner-table scan. The document count is
// intentionally large enough to make the different growth rates visible.
func BenchmarkDatabaseStatsRead(b *testing.B) {
	counter := &writeQueryCounter{}
	s := writePerfStore(b, counter)
	db := freshDB(b, s, "bench_database_stats_read")
	const documents = 100_000
	if _, err := s.pool.Exec(b.Context(), fmt.Sprintf(
		`INSERT INTO %s.docs
		   (id, rev_num, rev_hash, deleted, seq, external_size)
		 SELECT 'doc-' || lpad(n::text, 8, '0'), 1, 'benchmark', false, n, 128
		 FROM generate_series(1, $1) AS docs(n)`, db.Schema), documents); err != nil {
		b.Fatal(err)
	}

	b.Run("striped_metadata", func(b *testing.B) {
		var live, deleted, external int64
		b.ResetTimer()
		for range b.N {
			if err := s.pool.QueryRow(b.Context(), fmt.Sprintf(
				`SELECT coalesce(sum(doc_count), 0),
				        coalesce(sum(doc_del_count), 0),
				        coalesce(sum(external_size), 0)
				 FROM %s.database_stats`, db.Schema),
			).Scan(&live, &deleted, &external); err != nil {
				b.Fatal(err)
			}
		}
		if live != documents || deleted != 0 || external != documents*128 {
			b.Fatalf("striped stats = (%d, %d, %d)", live, deleted, external)
		}
	})

	b.Run("winner_table_scan", func(b *testing.B) {
		var live, deleted, external int64
		b.ResetTimer()
		for range b.N {
			if err := s.pool.QueryRow(b.Context(), fmt.Sprintf(
				`SELECT count(*) FILTER (WHERE NOT deleted),
				        count(*) FILTER (WHERE deleted),
				        coalesce(sum(external_size), 0)
				 FROM %s.docs`, db.Schema),
			).Scan(&live, &deleted, &external); err != nil {
				b.Fatal(err)
			}
		}
		if live != documents || deleted != 0 || external != documents*128 {
			b.Fatalf("winner-table stats = (%d, %d, %d)", live, deleted, external)
		}
	})
}
