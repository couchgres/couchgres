# couchgres

The simplicity of CouchDB with the durability of PostgreSQL.

## About

couchgres is a CouchDB compatible HTTP API server written in Go and backed by PostgreSQL.

## Features

- Mostly compatible with CouchDB 3.5. See [Differences from CouchDB](#differences-from-couchdb).
- Runs as a single binary in front of PostgreSQL.
- Authentication through Basic auth, cookie sessions, JWT, proxy auth, `_users`, and `_security`
- Replication
  - CouchDB can replicate to and from couchgres
  - couchgres can run replications through `_replicate` and `_replicator`
  - PouchDB sync is supported
- Views with JavaScript map/reduce on QuickJS and CouchDB compatible collation
- Mango `_find`, `_index`, and `_explain` with planning against indexes
- Partitioned databases, `_purge`, `_show`, `_list`, `_update`, and `_rewrite`
- Normal, longpoll, continuous, and eventsource changes feeds with JavaScript, view, and selector filters
- Embeded Fauxton

## Prerequisites

- Go 1.26+
- PostgreSQL 17+
- curl (to fetch Fauxton)
- Elixir 1.19+ (optional; only required for running compatibility checks)

## Quick Start

```sh
createdb couchgres
cp couchgres.example.yaml couchgres.yaml   # edit admins and PostgreSQL URL
make run
curl http://127.0.0.1:5984/
```

## Configuration

### couchgres.yaml

| Setting              | Description                                                                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `bind`, `port`       | Listener address. Defaults to `127.0.0.1:5984`.                                                                                            |
| `http.*`             | Listener limits for read-header, read, write, and idle timeouts plus maximum header bytes. See `couchgres.example.yaml` for safe defaults. |
| `postgres.url`       | PostgreSQL connection URL                                                                                                                  |
| `postgres.pool_size` | Connection pool size                                                                                                                       |
| `replicator.enabled` | Starts the durable `_replicator` background worker. Defaults to `true`; set `false` for API-only instances.                                |
| `admins`             | Server administrators. Plaintext passwords are PBKDF2-hashed into PostgreSQL on first start.                                               |
| `log`                | Log level                                                                                                                                  |

### Environment variables

| Variable                       | When                | Description                                                                                        |
| ------------------------------ | ------------------- | -------------------------------------------------------------------------------------------------- |
| `COUCHGRES_ADMIN`              | Server startup      | `name:password` pair that creates an administrator if none exists                                  |
| `COUCHGRES_REPLICATOR_ENABLED` | Server startup      | Boolean override for `replicator.enabled`                                                          |
| `COUCHGRES_HTTP_*`             | Server startup      | Overrides the corresponding `http` timeout or `max_header_bytes` listener setting                  |
| `COUCHGRES_LIVE_COUCH`         | Tests               | CouchDB URL (with credentials) for live collation and rev hash tests                               |
| `COUCHGRES_URL`                | Compatibility tools | couchgres base URL for `compat/` and `compat/clients/`                                             |
| `COUCH_URL`                    | Compatibility tools | CouchDB base URL for side by side checks. Leave credentials out so anonymous cases stay anonymous. |

### Runtime settings (Config API)

CouchDB-style settings are stored in PostgreSQL and exposed through `/_node/_local/_config/*`. The config API also supports CouchDB sections such as `couch_httpd_auth` and `chttpd`.

| Setting                                                                   | Description                                                                                                                                                                                                             |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `couchdb/default_security`                                                | `admin_only`, the CouchDB 3.x default, or `everyone` for public databases like pre 3.x CouchDB.                                                                                                                         |
| `couchgres/keep_superseded_bodies`                                        | Defaults to `false`, which removes a revision body when a newer revision replaces it. Set it to `true` to keep superseded bodies readable through `?rev=` until `POST /{db}/_compact`. Changes take effect immediately. |
| `couch_httpd_auth/{iterations,min_iterations,max_iterations}`             | PBKDF2 work-factor policy. The default is 600,000 iterations; runtime bounds may narrow but cannot exceed the process safety ceiling of 1,200,000.                                                                      |
| `chttpd_auth_lockout/{mode,threshold,max_objects,max_lifetime}`           | Repeated username/client-IP failure policy. Defaults to `enforce`, 5 failures, 10,000 tracked pairs, and a five-minute lifetime.                                                                                        |
| `chttpd/max_http_request_size`                                            | Global request-body ceiling. Defaults to 64 MiB and cannot exceed the 256 MiB process safety ceiling.                                                                                                                   |
| `couchgres_httpd/max_{small,query,document,bulk,attachment}_request_size` | Route-specific request-body ceilings. Defaults to 64 KiB, 1 MiB, 16 MiB, 64 MiB, and 64 MiB respectively; the global ceiling always wins.                                                                               |

## Performance and Benchmarks

### Overview

Results from `compat/perf` on an Apple M3 with 24 GB RAM, macOS, PostgreSQL 17, CouchDB 3.5.2, default package settings, and 50,000 documents of ~200 bytes each.

| Workload                                             | couchgres              | CouchDB 3.5.2       |
| ---------------------------------------------------- | ---------------------- | ------------------- |
| Bulk insert (`_bulk_docs`, 500-document batches)     | 23,600 docs/sec        | **45,000 docs/sec** |
| Single-document PUT, 1 client                        | **3,350 writes/sec**   | 1,580 writes/sec    |
| Single-document PUT, 16 clients                      | **10,700 writes/sec**  | 4,430 writes/sec    |
| Single-document GET, 1 client                        | **10,100 reads/sec**   | 3,400 reads/sec     |
| Single-document GET, 16 clients                      | **27,400 reads/sec**   | 14,800 reads/sec    |
| `_bulk_get`, 100 documents per request               | **116,000 docs/sec**   | 25,900 docs/sec     |
| Attachment PUT, 100KB text                           | **1,640 atts/sec**     | 750 atts/sec        |
| Attachment GET, 100KB text                           | **6,230 reads/sec**    | 2,800 reads/sec     |
| `_all_docs?include_docs=true`, full 55,000-row scan  | **211,000 rows/sec**   | 53,200 rows/sec     |
| View build, JavaScript map on first query after load | **61,000 docs/sec**    | 46,800 docs/sec     |
| Warm view query, `key=N&limit=20`                    | **6,990 queries/sec**  | 2,130 queries/sec   |
| Warm view query, 16 clients                          | **34,800 queries/sec** | 8,330 queries/sec   |
| `_find` with a two field index and limit 25          | 430 queries/sec        | **500 queries/sec** |
| `_changes`, full read                                | **540,000 rows/sec**   | 107,000 rows/sec    |

### Running Benchmarks

Run the benchmarks using:

```sh
go run ./cmd/couchgres couchgres.yaml &
go run ./compat/perf http://admin:password@127.0.0.1:5984
```

## Compatibility with CouchDB

### Overview

couchgres aims to be compatible with CouchDB 3.5. Clients, including replication and revision hashing, should behave the same aside from the differences below.

Against CouchDB's own Elixir integration suite (`test/elixir/` in apache/couchdb), run under the same conditions:

|               | passed | of executed |
| ------------- | ------ | ----------- |
| couchgres     | 459    | 514 (89%)   |
| CouchDB 3.5.2 | 466    | 524 (89%)   |

Most remaining failures also fail on CouchDB outside of a full developer setup, or is a documented difference below.

### Differences from CouchDB

- Document bodies are stored as JSONB.
  - Strings containing `\u0000` are rejected with HTTP 400, and number formatting is normalized.
  - For example, `1.0` remains semantically equivalent but may not return as byte identical JSON.
- Revision hashes use CouchDB's algorithm, allowing identical independent writes to converge without conflicts across mixed couchgres and CouchDB deployments. However, hashes may differ in two cases:
  - Attachment only writes through `PUT /{db}/{doc}/{att}` when the original document body was not key sorted. JSONB does not preserve object member order, so couchgres hashes the canonicalized body.
  - Writes containing compressible attachments. The hash includes the gzip digest, and our compressor's output (klauspost/compress) differs from zlib's.

  The hashes are still stable for a given write and replication copies revision IDs unchanged.

- Attachment gzip uses klauspost/compress, whose output is close to but not byte identical with CouchDB's zlib. As a result, `encoded_length` values and encoded form digests may differ slightly even though the decoded attachment content is identical. Byte parity would require linking zlib itself via cgo.
- Multipart responses order attachments by the revision that introduced them, then by name. When several attachments are added in one write, couchgres sorts them by name while CouchDB preserves their JSON order.
- HTTP 304 responses do not include `Content-Length: 0` because Go's HTTP server removes that header.
- By default, couchgres removes a superseded revision body when a newer revision is written. This is equivalent to immediate CouchDB compaction. Set `couchgres/keep_superseded_bodies` to `true` to keep superseded bodies and bodies of deleted revisions readable through `?rev=` until `_compact` runs.
- Attachment data for old revisions remains until `_compact`. Compaction removes attachment rows that are no longer referenced by a live revision and trims revision histories to `_revs_limit`.
- PostgreSQL manages physical file reclamation, so tools that expect `sizes.file` to shrink immediately after compaction will not see that behavior.
- When multiple Couchgres processes use the same PostgreSQL database, only one process runs the durable `_replicator` worker at a time. PostgreSQL advisory locks coordinate which process owns it. Processes with `replicator.enabled: false` still serve the HTTP API and accept `POST /_replicate` requests, but they do not run `_replicator` documents.
- Runtime configuration and database registry caches stay in sync across processes through PostgreSQL notifications and durable metadata versions.
- Temporary continuous `POST /_replicate` jobs, `_active_tasks`, and live `_scheduler/jobs` entries exist only in the process that created them. For replication that must survive process restarts or work across multiple processes, use `_replicator` documents.
- Clustering endpoints exist for compatibility but don't actually do anything.
- Only the JavaScript query server is supported. Erlang and CoffeeScript design document functions are not available.
- `authentication_db` is not configurable. `_users` validation and access rules are implemented directly against the `_users` database.
- `$regex` uses Go's RE2 engine, which does not support backreferences or lookaround. CouchDB uses a PCRE style `re` engine.
- `_explain` values for `index_candidates` and `selector_hints` are approximations.
- `_search` and `_nouveau` return HTTP 503, matching CouchDB without an attached search service.

### Verification

```sh
createdb couchgres_test
make test

# Side by side response comparison against CouchDB 3.5.
make fauxton && go run ./cmd/couchgres &
COUCH_URL=http://127.0.0.1:5984 COUCHGRES_URL=http://127.0.0.1:5984 go run ./compat

# Replicate between CouchDB and couchgres in four directions, then compare.
compat/replication-check.sh

# Compare view key ordering and rev hashes against a live CouchDB.
COUCHGRES_LIVE_COUCH=http://admin:secret@127.0.0.1:5984 \
  go test ./internal/collate ./internal/couch -run 'LiveCouch'

# Exercise nano, PouchDB sync, and couchdb-python.
COUCHGRES_URL=http://admin:secret@127.0.0.1:5984 compat/clients/run.sh

# CouchDB's Elixir suite (needs Elixir; point EX_COUCH_URL at couchgres):
git clone --depth 1 --branch 3.5.2 https://github.com/apache/couchdb couchdb-src
cd couchdb-src && export MIX_ENV=integration
mix local.hex --force && mix local.rebar --force && mix deps.get
EX_COUCH_URL=http://127.0.0.1:5984 EX_USERNAME=adm EX_PASSWORD=pass \
  mix test --exclude test/elixir/test/config/skip.elixir \
    test/elixir/test/basics_test.exs
```

## Development

### Architecture

```text
client -> couchgres -> PostgreSQL
            │
            ├─ HTTP layer (net/http ServeMux)
            │    routing, auth middleware, ETags, error catalog
            ├─ core
            │    rev trees, changes broker, security, Mango planner
            ├─ JS runtime (QuickJS)
            │    views, validate_doc_update, filters, shows/lists/updates
            └─ storage layer (pgx/v5)
                 schema per database, LISTEN/NOTIFY for _changes
```

Each database maps to a PostgreSQL schema. View indexes use per signature tables keyed by a memcomparable collation encoding, so PostgreSQL B-tree order matches CouchDB view order. Built-in reductions such as `_sum`, `_count`, `_stats`, and `_approx_count_distinct` run in PostgreSQL where possible.

### Project Layout

| Package              | Purpose                                                             |
| -------------------- | ------------------------------------------------------------------- |
| `cmd/couchgres`      | Configuration, bootstrap, and server startup                        |
| `internal/httpapi`   | Routing, middleware, and endpoint handlers                          |
| `internal/couch`     | Domain types, revision machinery, and CouchDB error catalog         |
| `internal/store`     | PostgreSQL storage, schema per database handling, and view indexing |
| `internal/collate`   | Memcomparable CouchDB collation encoding                            |
| `internal/jsengine`  | QuickJS VM pool and CouchDB view server JavaScript runtime          |
| `internal/mango`     | Selector evaluation, normalization, and planner primitives          |
| `internal/replicate` | Replication peers, jobs, and scheduler                              |
| `internal/config`    | `couchgres.yaml` loading                                            |
| `internal/fauxton`   | Embedded Fauxton UI                                                 |
| `compat/`            | Side by side response checker and YAML test cases                   |
| `compat/clients/`    | Client library checks (nano, PouchDB, couchdb-python)               |

### Tests

```sh
createdb couchgres_test
make test

# View indexing throughput and reduce query latency.
go test ./internal/httpapi -bench BenchmarkView -benchtime 10x -run xxx
```

See [Verification](#verification) for checks against a live CouchDB and client libraries.

### Ideas / Roadmap

- Support storing attachments in S3-compatible object storage.
