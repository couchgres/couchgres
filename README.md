<img src="images/logo.svg" alt="couchgres" width="300">

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

- Go 1.26.7+
- PostgreSQL 18+
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

| Setting                             | Description                                                                                                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `bind`, `port`                      | Listener address. Defaults to `127.0.0.1:5984`.                                                                                            |
| `http.*`                            | Listener limits for read-header, read, write, and idle timeouts plus maximum header bytes. See `couchgres.example.yaml` for safe defaults. |
| `postgres.url`                      | PostgreSQL connection URL                                                                                                                  |
| `postgres.pool_size`                | Connection pool size                                                                                                                       |
| `replicator.enabled`                | Starts the durable `_replicator` background worker. Defaults to `true`; set `false` for API-only instances.                                |
| `replicator.allow_public_networks`  | Defaults to `false`. Set `true` to allow replication to public addresses.                                                                  |
| `admins`                            | Server administrators. Plaintext passwords are PBKDF2-hashed into PostgreSQL on first start.                                               |
| `log`                               | Log level                                                                                                                                  |

### Environment variables

| Variable                                      | When                | Description                                                                                        |
| --------------------------------------------- | ------------------- | -------------------------------------------------------------------------------------------------- |
| `COUCHGRES_ADMIN`                             | Server startup      | `name:password` pair that creates an administrator if none exists                                  |
| `COUCHGRES_REPLICATOR_ENABLED`                | Server startup      | Boolean override for `replicator.enabled`                                                          |
| `COUCHGRES_REPLICATOR_ALLOW_PUBLIC_NETWORKS`  | Server startup      | Boolean override for `replicator.allow_public_networks`                                            |
| `COUCHGRES_HTTP_*`                            | Server startup      | Overrides the corresponding `http` timeout or `max_header_bytes` listener setting                  |
| `COUCHGRES_LIVE_COUCH`                        | Tests               | CouchDB URL (with credentials) for live collation and rev hash tests                               |
| `COUCHGRES_URL`                               | Compatibility tools | couchgres base URL for `compat/` and `compat/clients/`                                             |
| `COUCH_URL`                                   | Compatibility tools | CouchDB base URL for side by side checks. Leave credentials out so anonymous cases stay anonymous. |

### Runtime settings (Config API)

CouchDB-style settings are stored in PostgreSQL and exposed through `/_node/_local/_config/{section}/{setting}`.

| Setting                                         | Description                                                                                                                                                                                                             |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `chttpd/max_http_request_size`                  | Global request-body ceiling, in bytes. Defaults to 64 MiB and cannot exceed the 256 MiB process safety ceiling.                                                                                                         |
| `chttpd/secure_rewrites`                        | Defaults to `true`, restricting array and function rewrites to the source database and original HTTP method. With `false`, cross-scope or method-changing rewrites still require a server administrator.                |
| `chttpd_auth/proxy_authentication`              | Enables `X-Auth-CouchDB-*` proxy authentication. Signed headers are required by default.                                                                                                                                |
| `chttpd_auth_lockout/max_lifetime`              | Lifetime of a tracked pair, in milliseconds. Defaults to 300,000 (five minutes).                                                                                                                                        |
| `chttpd_auth_lockout/max_objects`               | Maximum number of tracked pairs. Defaults to 10,000.                                                                                                                                                                    |
| `chttpd_auth_lockout/mode`                      | Policy for repeated authentication failures per username/client-IP pair. Defaults to `enforce`.                                                                                                                         |
| `chttpd_auth_lockout/threshold`                 | Failure threshold. Defaults to 5 failures.                                                                                                                                                                              |
| `couch_httpd_auth/iterations`                   | PBKDF2 iteration count for password hashing. Defaults to 600,000 and must be between `min_iterations` and `max_iterations`.                                                                                             |
| `couch_httpd_auth/max_iterations`               | Maximum accepted iteration count. Defaults to 1,200,000. Runtime bounds may narrow the allowed range but cannot exceed this process safety ceiling.                                                                     |
| `couch_httpd_auth/min_iterations`               | Minimum accepted iteration count. Defaults to 1.                                                                                                                                                                        |
| `couch_httpd_auth/proxy_allow_insecure_headers` | Explicit acknowledgement required to allow unsigned proxy headers.                                                                                                                                                      |
| `couch_httpd_auth/proxy_trusted_cidrs`          | Comma-separated list of trusted direct-proxy CIDRs for unsigned mode.                                                                                                                                                   |
| `couch_httpd_auth/proxy_use_secret`             | Requires signed proxy headers. Defaults to `true`.                                                                                                                                                                      |
| `couch_httpd_auth/secret`                       | Shared secret for signing the username token using CouchDB-compatible HMAC-SHA1. Must be non-empty in signed mode.                                                                                                      |
| `couchdb/default_security`                      | `admin_only`, the CouchDB 3.x default, or `everyone` for public databases like pre 3.x CouchDB.                                                                                                                         |
| `couchgres/keep_superseded_bodies`              | Defaults to `false`, which removes a revision body when a newer revision replaces it. Set it to `true` to keep superseded bodies readable through `?rev=` until `POST /{db}/_compact`. Changes take effect immediately. |
| `couchgres_httpd/max_attachment_request_size`   | Attachment-request body ceiling. Defaults to 64 MiB.                                                                                                                                                                    |
| `couchgres_httpd/max_bulk_request_size`         | Bulk-request body ceiling. Defaults to 64 MiB.                                                                                                                                                                          |
| `couchgres_httpd/max_document_request_size`     | Document-request body ceiling. Defaults to 16 MiB.                                                                                                                                                                      |
| `couchgres_httpd/max_query_request_size`        | Query-request body ceiling. Defaults to 1 MiB.                                                                                                                                                                          |
| `couchgres_httpd/max_small_request_size`        | Small-request body ceiling. Defaults to 64 KiB.                                                                                                                                                                         |

Request-body limits are configured in bytes. The global `chttpd/max_http_request_size` ceiling always caps the route-specific limits.

Unsigned mode requires `proxy_use_secret=false`, `proxy_allow_insecure_headers=true`, and a trusted direct-proxy CIDR list.

When proxy authentication is enabled, the edge proxy must remove all incoming configured username, roles, and token headers before setting its own values. Unsigned mode is intended only for tightly controlled deployments: Couchgres matches `proxy_trusted_cidrs` against the direct socket peer (`RemoteAddr`) and never trusts `X-Forwarded-For` to establish proxy identity. Prefer signed mode even on private networks.

### Resource limits

Concurrent uploads share a 256 MiB capacity budget; excess requests may receive HTTP 503. Design-document JavaScript runs with memory and output limits to keep resource use bounded.

Administrators can monitor both through `GET /_node/_local/_stats`, under `couchgres.request_bodies` and `couchgres.javascript`.

## Performance and Benchmarks

### Overview

Median results from three `compat/perf` runs on an Apple M3 with 24 GB RAM, macOS, PostgreSQL 18.4, CouchDB 3.5.2, default server settings, and 50,000 documents of ~200 bytes each.

| Workload                                             | couchgres + PostgreSQL 18.4 | CouchDB 3.5.2       | couchgres vs CouchDB |
| ---------------------------------------------------- | --------------------------- | ------------------- | -------------------: |
| Bulk insert (`_bulk_docs`, 500-document batches)     | 30,251 docs/sec             | **43,035 docs/sec** |               -29.7% |
| Single-document PUT, 1 client                        | **4,681 writes/sec**        | 1,441 writes/sec    |              +224.8% |
| Single-document PUT, 16 clients                      | **7,689 writes/sec**        | 4,374 writes/sec    |               +75.8% |
| Single-document GET, 1 client                        | **12,117 reads/sec**        | 3,762 reads/sec     |              +222.1% |
| Single-document GET, 16 clients                      | **40,738 reads/sec**        | 14,509 reads/sec    |              +180.8% |
| `_bulk_get`, 100 documents per request               | **114,691 docs/sec**        | 24,983 docs/sec     |              +359.1% |
| Attachment PUT, 100KB text                           | **1,908 atts/sec**          | 764 atts/sec        |              +149.7% |
| Attachment GET, 100KB text                           | **6,783 reads/sec**         | 2,893 reads/sec     |              +134.5% |
| `_all_docs?include_docs=true`, full 55,200-row scan  | **159,671 rows/sec**        | 51,836 rows/sec     |              +208.0% |
| View build, JavaScript map on first query after load | **60,478 docs/sec**         | 47,904 docs/sec     |               +26.2% |
| Warm view query, `key=N&limit=20`                    | **2,129 queries/sec**       | 1,797 queries/sec   |               +18.5% |
| Warm view query, 16 clients                          | **42,327 queries/sec**      | 7,678 queries/sec   |              +451.3% |
| `_find` with a two field index and limit 25          | **1,912 queries/sec**       | 566 queries/sec     |              +237.8% |
| `_changes`, full read                                | **724,040 rows/sec**        | 106,489 rows/sec    |              +579.9% |

### Running Benchmarks

Run the benchmarks using:

```sh
make run &
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
- Replication URLs cannot use loopback or link-local addresses. For databases on this instance, use their names: `{"source":"orders","target":"orders_backup"}`.
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
createdb couchgres_httpapi_test
make test
make vulncheck

# Side by side response comparison against CouchDB 3.5.
make run &
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
createdb couchgres_httpapi_test
make test

# View indexing throughput and reduce query latency.
go test ./internal/httpapi -bench BenchmarkView -benchtime 10x -run xxx
```

The store and HTTP API packages use separate PostgreSQL databases so their
parallel tests cannot interfere with each other's schema migrations or open
transactions. Set `COUCHGRES_TEST_PG_URL` to override the store connection and
`COUCHGRES_HTTPAPI_TEST_PG_URL` to override the HTTP API connection.

See [Verification](#verification) for checks against a live CouchDB and client libraries.

### Ideas / Roadmap

- Support storing attachments in S3-compatible object storage.
