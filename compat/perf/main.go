// compat/perf runs an identical workload on any CouchDB-compatible endpoint
//
//	go run ./compat/perf http://admin:secret@127.0.0.1:5984
//
// Benchmarks are run using a "perftest" database.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	db                = "perftest"
	sequentialWriteDB = db + "_sequential_writes"
	concurrentWriteDB = db + "_concurrent_writes"
	attachmentsDB     = db + "_attachments"
	benchmarkWindow   = 3 * time.Second
	bulkDocs          = 50_000
	bulkBatch         = 500
	concurrentClients = 16
	bulkGetBatch      = 100
	attachmentKB      = 100 // text/plain is gzip-compressed by both servers.
)

type client struct {
	base string
	auth string
	http *http.Client
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: perf http://admin:pass@host:port [-keep]")
		os.Exit(1)
	}
	c := newClient(os.Args[1])

	if !(len(os.Args) > 2 && os.Args[2] == "-keep") {
		defer deleteDatabase(c, db)
	}

	fmt.Printf("target: %s\nminimum sample time: %s\n\n", c.base, benchmarkWindow)
	results := [][2]string{}
	row := func(name, value string) {
		results = append(results, [2]string{name, value})
		fmt.Printf("%-46s %s\n", name, value)
	}

	// Repeat complete loads until the timed work spans the benchmark window.
	// Database resets are not timed, and the last load leaves the canonical
	// 50,000-document corpus in place for all read benchmarks.
	bulkCount := 0
	bulkElapsed := time.Duration(0)
	for bulkElapsed < benchmarkWindow {
		resetDatabase(c, db)
		start := time.Now()
		bulkLoad(c, db)
		bulkElapsed += time.Since(start)
		bulkCount += bulkDocs
	}
	row("bulk insert (_bulk_docs, 500-doc batches)",
		perSec(bulkCount, bulkElapsed, "docs/sec"))

	// Mutating benchmarks use disposable databases so longer measurements do
	// not change the corpus measured by the later scans and queries.
	resetDatabase(c, sequentialWriteDB)
	seqWrites, elapsed := runFor(benchmarkWindow, func(i int) {
		c.must("PUT", fmt.Sprintf("/%s/seq-%09d", sequentialWriteDB, i), doc(i), 201, 202)
	})
	row("single-doc PUT, 1 client",
		perSec(seqWrites, elapsed, "writes/sec"))
	deleteDatabase(c, sequentialWriteDB)

	resetDatabase(c, concurrentWriteDB)
	concWrites, elapsed := runConcurrentFor(benchmarkWindow, concurrentClients, func(_ int, i int64) {
		c.must("PUT", fmt.Sprintf("/%s/conc-%09d", concurrentWriteDB, i), doc(int(i)), 201, 202)
	})
	row(fmt.Sprintf("single-doc PUT, %d clients", concurrentClients),
		perSec(concWrites, elapsed, "writes/sec"))
	deleteDatabase(c, concurrentWriteDB)

	// Sequential reads of random loaded docs.
	rng := rand.New(rand.NewSource(1))
	seqReads, elapsed := runFor(benchmarkWindow, func(_ int) {
		c.must("GET", fmt.Sprintf("/%s/doc-%06d", db, rng.Intn(bulkDocs)), nil, 200)
	})
	row("single-doc GET, 1 client",
		perSec(seqReads, elapsed, "reads/sec"))

	// Concurrent reads.
	readRNGs := make([]*rand.Rand, concurrentClients)
	for i := range readRNGs {
		readRNGs[i] = rand.New(rand.NewSource(int64(100 + i)))
	}
	concReads, elapsed := runConcurrentFor(benchmarkWindow, concurrentClients, func(worker int, _ int64) {
		c.must("GET", fmt.Sprintf("/%s/doc-%06d", db, readRNGs[worker].Intn(bulkDocs)), nil, 200)
	})
	row(fmt.Sprintf("single-doc GET, %d clients", concurrentClients),
		perSec(concReads, elapsed, "reads/sec"))

	// _bulk_get, 100 docs per request (the PouchDB/replicator sync shape).
	bulkGetReqs, elapsed := runFor(benchmarkWindow, func(_ int) {
		docs := make([]any, 0, bulkGetBatch)
		for i := 0; i < bulkGetBatch; i++ {
			docs = append(docs, map[string]any{
				"id": fmt.Sprintf("doc-%06d", rng.Intn(bulkDocs)),
			})
		}
		c.must("POST", "/"+db+"/_bulk_get", map[string]any{"docs": docs}, 200)
	})
	row("_bulk_get, 100 docs per request",
		perSec(bulkGetReqs*bulkGetBatch, elapsed, "docs/sec"))

	// Attachments use a standalone PUT to create the document. text/plain is
	// compressible, so both servers gzip it at write time.
	resetDatabase(c, attachmentsDB)
	attSentence := "the quick brown fox jumps over the lazy dog... "
	attBody := strings.Repeat(attSentence, attachmentKB*1024/len(attSentence)+1)[:attachmentKB*1024]
	attWrites, elapsed := runFor(benchmarkWindow, func(i int) {
		c.mustRaw("PUT", fmt.Sprintf("/%s/att-%09d/file.txt", attachmentsDB, i),
			attBody, "text/plain", 201, 202)
	})
	row(fmt.Sprintf("attachment PUT, %dKB text, 1 client", attachmentKB),
		perSec(attWrites, elapsed, "atts/sec"))

	attReads, elapsed := runFor(benchmarkWindow, func(i int) {
		c.must("GET", fmt.Sprintf("/%s/att-%09d/file.txt", attachmentsDB, i%attWrites), nil, 200)
	})
	row(fmt.Sprintf("attachment GET, %dKB text, 1 client", attachmentKB),
		perSec(attReads, elapsed, "reads/sec"))
	deleteDatabase(c, attachmentsDB)

	// Repeat full scans; every request still traverses the same 50,000 rows.
	allDocsScans, elapsed := runFor(benchmarkWindow, func(_ int) {
		c.must("GET", "/"+db+"/_all_docs?include_docs=true", nil, 200)
	})
	row("_all_docs?include_docs=true (full scan)",
		perSec(allDocsScans*bulkDocs, elapsed, "rows/sec"))

	// Give each cold view build a unique map signature. Creating the design doc
	// is outside the timer; only the first query's full index build is measured.
	viewBuilds := 0
	viewBuildElapsed := time.Duration(0)
	for viewBuildElapsed < benchmarkWindow {
		design := fmt.Sprintf("perf_build_%03d", viewBuilds)
		mapSource := fmt.Sprintf(
			"function(doc){ /* build %d */ if (doc.group !== undefined) emit(doc.group, doc.score); }",
			viewBuilds,
		)
		putView(c, db, design, mapSource)
		start := time.Now()
		c.must("GET", fmt.Sprintf("/%s/_design/%s/_view/by_group?limit=1", db, design), nil, 200)
		viewBuildElapsed += time.Since(start)
		viewBuilds++
	}
	row("view build (JS map+_count, first query)",
		perSec(viewBuilds*bulkDocs, viewBuildElapsed, "docs/sec"))
	// Both servers run background maintenance after a big load (autovacuum
	// there, compaction/ken here). Wait for query latency to stabilize so
	// the warm phases measure steady state, not the maintenance window.
	warmViewPath := "/" + db + "/_design/perf_build_000/_view/by_group"
	settle(c, warmViewPath+"?key=1&reduce=false&limit=20")

	// Warm view queries run with a single client, then at concurrent capacity.
	viewQueries, elapsed := runFor(benchmarkWindow, func(_ int) {
		c.must("GET", fmt.Sprintf(
			"%s?key=%d&reduce=false&limit=20",
			warmViewPath, rng.Intn(100)), nil, 200)
	})
	row("view query, warm (key=N&limit=20)",
		perSec(viewQueries, elapsed, "queries/sec"))

	viewRNGs := make([]*rand.Rand, concurrentClients)
	for i := range viewRNGs {
		viewRNGs[i] = rand.New(rand.NewSource(int64(i)))
	}
	concurrentViewQueries, elapsed := runConcurrentFor(benchmarkWindow, concurrentClients, func(worker int, _ int64) {
		c.must("GET", fmt.Sprintf(
			"%s?key=%d&reduce=false&limit=20",
			warmViewPath, viewRNGs[worker].Intn(100)), nil, 200)
	})
	row(fmt.Sprintf("view query, warm, %d clients", concurrentClients),
		perSec(concurrentViewQueries, elapsed, "queries/sec"))

	// Mango index build and finds.
	c.must("POST", "/"+db+"/_index", map[string]any{
		"index": map[string]any{"fields": []string{"group", "score"}},
		"name":  "group-score",
	}, 200, 201)
	c.must("POST", "/"+db+"/_find", map[string]any{
		"selector": map[string]any{"group": 1}, "limit": 1,
	}, 200) // pay index build outside the timed loop
	settleWith(c, func() {
		c.must("POST", "/"+db+"/_find", map[string]any{
			"selector": map[string]any{"group": 1, "score": map[string]any{"$gt": 50}},
			"limit":    25,
		}, 200)
	})
	findQueries, elapsed := runFor(benchmarkWindow, func(_ int) {
		c.must("POST", "/"+db+"/_find", map[string]any{
			"selector": map[string]any{
				"group": rng.Intn(100),
				"score": map[string]any{"$gt": 50},
			},
			"limit": 25,
		}, 200)
	})
	row("_find, indexed (two-field selector, limit 25)",
		perSec(findQueries, elapsed, "queries/sec"))

	// Repeat full changes reads against the unchanged canonical data corpus.
	changesReads, elapsed := runFor(benchmarkWindow, func(_ int) {
		c.must("GET", "/"+db+"/_changes", nil, 200)
	})
	row("_changes (full read)",
		perSec(changesReads*bulkDocs, elapsed, "rows/sec"))

	fmt.Println("\nmarkdown row values:")
	for _, r := range results {
		fmt.Printf("| %s | %s |\n", r[0], r[1])
	}
}

func bulkLoad(c *client, database string) {
	for lo := 0; lo < bulkDocs; lo += bulkBatch {
		docs := make([]any, 0, bulkBatch)
		for i := lo; i < lo+bulkBatch; i++ {
			docs = append(docs, doc(i))
		}
		c.must("POST", "/"+database+"/_bulk_docs", map[string]any{"docs": docs}, 201)
	}
}

func putView(c *client, database, design, mapSource string) {
	c.must("PUT", fmt.Sprintf("/%s/_design/%s", database, design), map[string]any{
		"views": map[string]any{
			"by_group": map[string]any{
				"map":    mapSource,
				"reduce": "_count",
			},
		},
	}, 201, 202)
}

func resetDatabase(c *client, database string) {
	deleteDatabase(c, database)
	c.must("PUT", "/"+database, nil, 201, 202)
}

func deleteDatabase(c *client, database string) {
	c.must("DELETE", "/"+database, nil, 200, 202, 404)
}

// runFor executes complete operations until at least minimum has elapsed.
func runFor(minimum time.Duration, operation func(int)) (int, time.Duration) {
	start := time.Now()
	completed := 0
	for {
		operation(completed)
		completed++
		elapsed := time.Since(start)
		if elapsed >= minimum {
			return completed, elapsed
		}
	}
}

// runConcurrentFor starts all workers together and stops issuing work after
// minimum. The elapsed time includes in-flight operations completing.
func runConcurrentFor(
	minimum time.Duration,
	workers int,
	operation func(worker int, iteration int64),
) (int, time.Duration) {
	var ready sync.WaitGroup
	var running sync.WaitGroup
	var iteration atomic.Int64
	var completed atomic.Int64
	startGate := make(chan struct{})
	ready.Add(workers)
	running.Add(workers)

	var deadline time.Time
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer running.Done()
			ready.Done()
			<-startGate
			for time.Now().Before(deadline) {
				i := iteration.Add(1) - 1
				operation(worker, i)
				completed.Add(1)
			}
		}(worker)
	}

	ready.Wait()
	start := time.Now()
	deadline = start.Add(minimum)
	close(startGate)
	running.Wait()
	return int(completed.Load()), time.Since(start)
}

func doc(i int) map[string]any {
	return map[string]any{
		"_id":   fmt.Sprintf("doc-%06d", i),
		"type":  "event",
		"user":  fmt.Sprintf("user-%04d", i%1000),
		"group": i % 100,
		"score": float64(i%10000) / 100.0,
		"flag":  i%2 == 0,
		"note":  "the quick brown fox jumps over the lazy dog and keeps going",
	}
}

func perSec(n int, d time.Duration, unit string) string {
	return fmt.Sprintf("%9.0f %s  (%d in %.1fs)",
		float64(n)/d.Seconds(), unit, n, d.Seconds())
}

// settle waits until a canary query's latency stops improving (background
// maintenance has finished). It is capped at 90s.
func settle(c *client, path string) {
	settleWith(c, func() { c.must("GET", path, nil, 200) })
}

func settleWith(c *client, canary func()) {
	best := time.Duration(1<<62 - 1)
	calm := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		start := time.Now()
		for i := 0; i < 5; i++ {
			canary()
		}
		d := time.Since(start)
		if d < best {
			best = d
		}
		if d < best*3/2 {
			calm++
			if calm >= 3 {
				return
			}
		} else {
			calm = 0
		}
		time.Sleep(2 * time.Second)
	}
}

func newClient(raw string) *client {
	parsed, err := url.Parse(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad url:", err)
		os.Exit(1)
	}
	c := &client{http: &http.Client{Transport: &http.Transport{
		MaxIdleConns: concurrentClients * 2, MaxIdleConnsPerHost: concurrentClients * 2,
	}}}
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		cred := parsed.User.Username() + ":" + password
		c.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
		parsed.User = nil
	}
	c.base = strings.TrimRight(parsed.String(), "/")
	return c
}

func (c *client) request(method, path string, body any, okStatuses ...int) int {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		panic(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s: %v\n", method, path, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// mustRaw sends attachment bodies verbatim with the given content type.
// request and must JSON-encode their bodies.
func (c *client) mustRaw(method, path, body, contentType string, okStatuses ...int) {
	req, err := http.NewRequest(method, c.base+path, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", contentType)
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s: %v\n", method, path, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	for _, ok := range okStatuses {
		if resp.StatusCode == ok {
			return
		}
	}
	fmt.Fprintf(os.Stderr, "%s %s: unexpected status %d\n", method, path, resp.StatusCode)
	os.Exit(1)
}

func (c *client) must(method, path string, body any, okStatuses ...int) {
	status := c.request(method, path, body, okStatuses...)
	for _, ok := range okStatuses {
		if status == ok {
			return
		}
	}
	fmt.Fprintf(os.Stderr, "%s %s: unexpected status %d\n", method, path, status)
	os.Exit(1)
}
