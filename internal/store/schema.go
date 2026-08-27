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
    ADD COLUMN IF NOT EXISTS purged_infos_limit bigint NOT NULL DEFAULT 1000,
    ADD COLUMN IF NOT EXISTS doc_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS doc_del_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS doc_counts_initialized boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS update_seq bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS update_seq_initialized boolean NOT NULL DEFAULT false;

-- Keep each view-state row on the database's durable purge generation.
CREATE OR REPLACE FUNCTION couchgres.sync_view_purge_seq()
RETURNS trigger
LANGUAGE plpgsql
AS $view_purge_seq$
BEGIN
    IF NEW.purge_seq IS DISTINCT FROM OLD.purge_seq THEN
        EXECUTE format(
            'UPDATE %I.view_state SET purge_seq = GREATEST(purge_seq, $1)',
            NEW.schema_name
        ) USING NEW.purge_seq;
    END IF;
    RETURN NEW;
END
$view_purge_seq$;
DROP TRIGGER IF EXISTS databases_sync_view_purge_seq ON couchgres.databases;
CREATE TRIGGER databases_sync_view_purge_seq
AFTER UPDATE OF purge_seq ON couchgres.databases
FOR EACH ROW EXECUTE FUNCTION couchgres.sync_view_purge_seq();

-- _all_dbs requires byte-order bounds and ordering regardless of the database's
-- default collation. This expression index supports both scan directions.
CREATE INDEX IF NOT EXISTS databases_name_c_idx
    ON couchgres.databases (name COLLATE "C");

-- Statement-level transition tables make one counter adjustment per write
-- statement, including a whole _bulk_docs winner refresh. The INSERT and UPDATE
-- triggers both run for INSERT ... ON CONFLICT DO UPDATE, but each transition
-- table contains only the rows for its event.
CREATE OR REPLACE FUNCTION couchgres.update_doc_counts()
RETURNS trigger
LANGUAGE plpgsql
AS $doc_counts$
DECLARE
    live_delta bigint := 0;
    deleted_delta bigint := 0;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT count(*) FILTER (WHERE NOT deleted),
               count(*) FILTER (WHERE deleted)
          INTO live_delta, deleted_delta
          FROM new_docs;
    ELSIF TG_OP = 'UPDATE' THEN
        SELECT
          (SELECT count(*) FILTER (WHERE NOT deleted) FROM new_docs)
            - (SELECT count(*) FILTER (WHERE NOT deleted) FROM old_docs),
          (SELECT count(*) FILTER (WHERE deleted) FROM new_docs)
            - (SELECT count(*) FILTER (WHERE deleted) FROM old_docs)
          INTO live_delta, deleted_delta;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT -(count(*) FILTER (WHERE NOT deleted)),
               -(count(*) FILTER (WHERE deleted))
          INTO live_delta, deleted_delta
          FROM old_docs;
    END IF;

    IF live_delta <> 0 OR deleted_delta <> 0 THEN
        UPDATE couchgres.databases
           SET doc_count = doc_count + live_delta,
               doc_del_count = doc_del_count + deleted_delta
         WHERE schema_name = TG_TABLE_SCHEMA;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'database metadata missing for schema %', TG_TABLE_SCHEMA;
        END IF;
    END IF;
    RETURN NULL;
END
$doc_counts$;

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
    sig       text PRIMARY KEY,
    last_seq  bigint NOT NULL DEFAULT 0,
    purge_seq bigint NOT NULL DEFAULT 0
);
`

func dbDDL(schema string) string {
	return strings.ReplaceAll(dbDDLTemplate, "{s}", schema) + docCountTriggers(schema)
}

const docCountTriggersTemplate = `
DROP TRIGGER IF EXISTS docs_count_insert ON {s}.docs;
CREATE TRIGGER docs_count_insert
AFTER INSERT ON {s}.docs
REFERENCING NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION couchgres.update_doc_counts();
DROP TRIGGER IF EXISTS docs_count_update ON {s}.docs;
CREATE TRIGGER docs_count_update
AFTER UPDATE ON {s}.docs
REFERENCING OLD TABLE AS old_docs NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION couchgres.update_doc_counts();
DROP TRIGGER IF EXISTS docs_count_delete ON {s}.docs;
CREATE TRIGGER docs_count_delete
AFTER DELETE ON {s}.docs
REFERENCING OLD TABLE AS old_docs
FOR EACH STATEMENT EXECUTE FUNCTION couchgres.update_doc_counts();
`

func docCountTriggers(schema string) string {
	return strings.ReplaceAll(docCountTriggersTemplate, "{s}", schema)
}
