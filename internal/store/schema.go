package store

import "strings"

// SQL DDL. Per-database schema names are generated as db_<hex>.
// They are identifier-safe by construction, so interpolation is safe.

const globalDDL = `
CREATE SCHEMA IF NOT EXISTS couchgres;

CREATE TABLE IF NOT EXISTS couchgres.databases (
    name                text PRIMARY KEY,
    schema_name         text NOT NULL UNIQUE,
    partitioned         boolean NOT NULL DEFAULT false,
    revs_limit          int NOT NULL DEFAULT 1000,
    created_at          timestamptz NOT NULL DEFAULT now(),
    instance_start_time text NOT NULL
);
ALTER TABLE couchgres.databases
    ADD COLUMN IF NOT EXISTS purge_seq bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS purged_infos_limit bigint NOT NULL DEFAULT 1000;

CREATE TABLE IF NOT EXISTS couchgres.server_config (
    section text NOT NULL,
    key     text NOT NULL,
    value   text NOT NULL,
    PRIMARY KEY (section, key)
);

CREATE TABLE IF NOT EXISTS couchgres.db_events (
    event_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    db_name   text NOT NULL,
    type      text NOT NULL, -- created, updated, or deleted
    at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS couchgres.cache_versions (
    name       text PRIMARY KEY,
    version    bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);
`

// DDL for one CouchDB database. {s} is replaced with the schema name.
//
// docs caches the winning leaf per document ID. It stores metadata only. The
// body lives once in that leaf's revs row. Writing it to both tables doubled
// bulk-ingest WAL. revs is the full revision tree, where replication can create
// sibling conflict leaves. Attachments belong to the leaf revision
// that carries them.
const dbDDLTemplate = `
CREATE SCHEMA {s};

CREATE TABLE {s}.docs (
    id       text COLLATE "C" PRIMARY KEY,
    rev_num  int NOT NULL,
    rev_hash text NOT NULL,
    deleted  boolean NOT NULL DEFAULT false,
    seq      bigint NOT NULL
);
CREATE UNIQUE INDEX docs_seq_idx ON {s}.docs (seq);
-- The VDU cache asks for the newest design-doc sequence on every write. Without
-- this partial index, the planner sometimes answers max(seq) by walking
-- docs_seq_idx backward to find a design document. That is O(table) per write
-- when no design documents exist. ('0' is '/'+1, so the range is the
-- '_design/' prefix.)
CREATE INDEX docs_design_seq_idx ON {s}.docs (seq)
    WHERE id >= '_design/' AND id < '_design0';

CREATE TABLE {s}.revs (
    id          text COLLATE "C" NOT NULL,
    rev_num     int NOT NULL,
    rev_hash    text NOT NULL,
    parent_num  int,
    parent_hash text,
    deleted     boolean NOT NULL DEFAULT false,
    leaf        boolean NOT NULL,
    body        jsonb,
    PRIMARY KEY (id, rev_num, rev_hash)
);
CREATE INDEX revs_leaves_idx ON {s}.revs (id) WHERE leaf;

CREATE TABLE {s}.attachments (
    doc_id       text COLLATE "C" NOT NULL,
    rev_num      int NOT NULL,
    rev_hash     text NOT NULL,
    name         text NOT NULL,
    content_type text NOT NULL,
    digest       text NOT NULL,
    revpos       int NOT NULL,
    length       bigint NOT NULL,   -- decoded length
    encoding     text NOT NULL DEFAULT '',
    encoded_length bigint NOT NULL DEFAULT 0,
    data         bytea NOT NULL,    -- encoded form when encoding != ''
    PRIMARY KEY (doc_id, rev_num, rev_hash, name)
);

CREATE TABLE {s}.local_docs (
    id      text COLLATE "C" PRIMARY KEY,
    rev_num int NOT NULL,
    body    jsonb NOT NULL
);

CREATE TABLE {s}.security (
    singleton int PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    obj       jsonb NOT NULL
);

-- One row per design-doc signature. The per-signature data tables
-- (v_<sig>) are created lazily when a view is first built.
CREATE TABLE {s}.view_state (
    sig      text PRIMARY KEY,
    last_seq bigint NOT NULL DEFAULT 0
);

CREATE SEQUENCE {s}.update_seq;
`

func dbDDL(schema string) string {
	return strings.ReplaceAll(dbDDLTemplate, "{s}", schema)
}
