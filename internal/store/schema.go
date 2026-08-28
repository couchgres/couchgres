package store

import (
	"strconv"
	"strings"
)

// SQL DDL. Per-database schema names are generated as db_<hex>.
// They are identifier-safe by construction, so interpolation is safe.

// databaseStatStripes must remain a power of two because the SQL uses its
// predecessor as a hash mask.
const databaseStatStripes = 32

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

-- Serialize partition growth decisions across Couchgres processes. Sorting
-- inside PostgreSQL gives multi-partition batches one stable lock order.
CREATE OR REPLACE FUNCTION couchgres.lock_partition_writes(
    database_schema text,
    partitions text[]
)
RETURNS void
LANGUAGE plpgsql
AS $partition_locks$
DECLARE
    partition_name text;
BEGIN
    FOR partition_name IN
        SELECT p
          FROM (SELECT DISTINCT p FROM unnest(partitions) AS t(p)) AS unique_partitions
         ORDER BY p COLLATE "C"
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended(database_schema || '/' || partition_name, 0)
        );
    END LOOP;
END
$partition_locks$;

-- _all_dbs requires byte-order bounds and ordering regardless of the database's
-- default collation. This expression index supports both scan directions.
CREATE INDEX IF NOT EXISTS databases_name_c_idx
    ON couchgres.databases (name COLLATE "C");

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
    id            text COLLATE "C" PRIMARY KEY,
    rev_num       int NOT NULL,
    rev_hash      text NOT NULL,
    deleted       boolean NOT NULL DEFAULT false,
    seq           bigint NOT NULL,
    external_size bigint NOT NULL DEFAULT 0 CHECK (external_size >= 0)
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
    external_size bigint NOT NULL DEFAULT 0 CHECK (external_size >= 0),
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

func dbDDL(schema string, partitioned bool) string {
	ddl := strings.ReplaceAll(dbDDLTemplate, "{s}", schema) + databaseStatsDDL(schema, partitioned)
	if partitioned {
		ddl += partitionStatsDDL(schema)
	}
	return ddl
}

// Exact database totals are split across 32 stable document-ID stripes. The
// table and its primary key stay in the database schema so each hot row is
// narrow. A single-document statement touches one stripe; bulk writers lock
// their union of INSERT and UPDATE stripes in ascending order before upserting.
// Partitioned database external bytes retain CouchDB's partition-document
// scope, while live/deleted counts continue to include design documents.
const databaseStatsDDLTemplate = `
CREATE TABLE {s}.database_stats (
    stripe        smallint PRIMARY KEY CHECK (stripe >= 0 AND stripe < {stripe_count}),
    doc_count     bigint NOT NULL DEFAULT 0 CHECK (doc_count >= 0),
    doc_del_count bigint NOT NULL DEFAULT 0 CHECK (doc_del_count >= 0),
    external_size bigint NOT NULL DEFAULT 0 CHECK (external_size >= 0)
);
INSERT INTO {s}.database_stats (stripe)
SELECT generate_series(0, {stripe_mask})::smallint;

CREATE FUNCTION {s}.lock_database_stats(document_ids text[])
RETURNS void
LANGUAGE plpgsql
AS $database_stat_locks$
BEGIN
    PERFORM stats.stripe
      FROM {s}.database_stats AS stats
     WHERE stats.stripe IN (
           SELECT DISTINCT (hashtextextended(id, 0) & {stripe_mask})::smallint
             FROM unnest(document_ids) AS ids(id)
       )
     ORDER BY stats.stripe
     FOR UPDATE;
END
$database_stat_locks$;

CREATE FUNCTION {s}.update_database_stats()
RETURNS trigger
LANGUAGE plpgsql
AS $database_stats$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE {s}.database_stats AS stats
           SET doc_count = stats.doc_count + delta.live_delta,
               doc_del_count = stats.doc_del_count + delta.deleted_delta,
               external_size = stats.external_size + delta.size_delta
          FROM (
            SELECT (hashtextextended(id, 0) & {stripe_mask})::smallint AS stripe,
                   count(*) FILTER (WHERE NOT deleted) AS live_delta,
                   count(*) FILTER (WHERE deleted) AS deleted_delta,
                   coalesce(sum(CASE WHEN {insert_external}
                                     THEN external_size ELSE 0 END), 0) AS size_delta
              FROM new_docs
             GROUP BY 1
          ) AS delta
         WHERE stats.stripe = delta.stripe;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE {s}.database_stats AS stats
           SET doc_count = stats.doc_count + delta.live_delta,
               doc_del_count = stats.doc_del_count + delta.deleted_delta,
               external_size = stats.external_size + delta.size_delta
          FROM (
            SELECT (hashtextextended(n.id, 0) & {stripe_mask})::smallint AS stripe,
                   sum(CASE WHEN NOT n.deleted THEN 1 ELSE 0 END
                     - CASE WHEN NOT o.deleted THEN 1 ELSE 0 END) AS live_delta,
                   sum(CASE WHEN n.deleted THEN 1 ELSE 0 END
                     - CASE WHEN o.deleted THEN 1 ELSE 0 END) AS deleted_delta,
                   sum(CASE WHEN {update_external}
                            THEN n.external_size - o.external_size ELSE 0 END) AS size_delta
              FROM new_docs AS n JOIN old_docs AS o USING (id)
             WHERE n.deleted IS DISTINCT FROM o.deleted
                OR ({update_external}
                    AND n.external_size IS DISTINCT FROM o.external_size)
             GROUP BY 1
          ) AS delta
         WHERE stats.stripe = delta.stripe;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE {s}.database_stats AS stats
           SET doc_count = stats.doc_count + delta.live_delta,
               doc_del_count = stats.doc_del_count + delta.deleted_delta,
               external_size = stats.external_size + delta.size_delta
          FROM (
            SELECT (hashtextextended(id, 0) & {stripe_mask})::smallint AS stripe,
                   -(count(*) FILTER (WHERE NOT deleted)) AS live_delta,
                   -(count(*) FILTER (WHERE deleted)) AS deleted_delta,
                   -coalesce(sum(CASE WHEN {delete_external}
                                      THEN external_size ELSE 0 END), 0) AS size_delta
              FROM old_docs
             GROUP BY 1
          ) AS delta
         WHERE stats.stripe = delta.stripe;
    END IF;
    RETURN NULL;
END
$database_stats$;

CREATE FUNCTION {s}.reject_doc_id_change()
RETURNS trigger
LANGUAGE plpgsql
AS $doc_id_immutable$
BEGIN
    RAISE EXCEPTION 'document ID cannot be changed';
END
$doc_id_immutable$;

CREATE TRIGGER docs_id_immutable
BEFORE UPDATE OF id ON {s}.docs
FOR EACH ROW EXECUTE FUNCTION {s}.reject_doc_id_change();

CREATE TRIGGER docs_count_insert
AFTER INSERT ON {s}.docs
REFERENCING NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_database_stats();
CREATE TRIGGER docs_count_update
AFTER UPDATE ON {s}.docs
REFERENCING OLD TABLE AS old_docs NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_database_stats();
CREATE TRIGGER docs_count_delete
AFTER DELETE ON {s}.docs
REFERENCING OLD TABLE AS old_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_database_stats();
`

func databaseStatsDDL(schema string, partitioned bool) string {
	insertExternal := "true"
	updateExternal := "true"
	deleteExternal := "true"
	if partitioned {
		insertExternal = "strpos(id, ':') > 1"
		updateExternal = "strpos(n.id, ':') > 1"
		deleteExternal = "strpos(id, ':') > 1"
	}
	return strings.NewReplacer(
		"{s}", schema,
		"{stripe_count}", strconv.Itoa(databaseStatStripes),
		"{stripe_mask}", strconv.Itoa(databaseStatStripes-1),
		"{insert_external}", insertExternal,
		"{update_external}", updateExternal,
		"{delete_external}", deleteExternal,
	).Replace(databaseStatsDDLTemplate)
}

// Partition statistics are striped by document hash so shrinking writes and
// metadata reads do not funnel through one hot tuple. Exact growth admission is
// coordinated separately by lock_partition_writes.
const partitionStatsDDLTemplate = `
CREATE TABLE {s}.partition_stats (
    partition     text COLLATE "C" NOT NULL,
    stripe        smallint NOT NULL CHECK (stripe >= 0 AND stripe < 16),
    doc_count     bigint NOT NULL DEFAULT 0 CHECK (doc_count >= 0),
    doc_del_count bigint NOT NULL DEFAULT 0 CHECK (doc_del_count >= 0),
    external_size bigint NOT NULL DEFAULT 0 CHECK (external_size >= 0),
    PRIMARY KEY (partition, stripe)
);

CREATE FUNCTION {s}.update_partition_stats()
RETURNS trigger
LANGUAGE plpgsql
AS $partition_stats$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO {s}.partition_stats AS stats
            (partition, stripe, doc_count, doc_del_count, external_size)
        SELECT delta.partition_key, delta.stripe_key, delta.doc_count,
               delta.doc_del_count, delta.external_size
          FROM (
            SELECT split_part(id, ':', 1) AS partition_key,
                   (hashtextextended(id, 0) & 15)::smallint AS stripe_key,
                   count(*) FILTER (WHERE NOT deleted) AS doc_count,
                   count(*) FILTER (WHERE deleted) AS doc_del_count,
                   coalesce(sum(external_size), 0) AS external_size
              FROM new_docs
             WHERE strpos(id, ':') > 1
             GROUP BY 1, 2
          ) AS delta
         ORDER BY delta.partition_key COLLATE "C", delta.stripe_key
        ON CONFLICT (partition, stripe) DO UPDATE
           SET doc_count = stats.doc_count + EXCLUDED.doc_count,
               doc_del_count = stats.doc_del_count + EXCLUDED.doc_del_count,
               external_size = stats.external_size + EXCLUDED.external_size;
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE {s}.partition_stats AS stats
           SET doc_count = stats.doc_count + delta.doc_count,
               doc_del_count = stats.doc_del_count + delta.doc_del_count,
               external_size = stats.external_size + delta.external_size
          FROM (
            SELECT split_part(id, ':', 1) AS partition,
                   (hashtextextended(id, 0) & 15)::smallint AS stripe,
                   sum(live_delta) AS doc_count,
                   sum(deleted_delta) AS doc_del_count,
                   sum(size_delta) AS external_size
              FROM (
                SELECT id,
                       CASE WHEN NOT deleted THEN 1 ELSE 0 END::bigint AS live_delta,
                       CASE WHEN deleted THEN 1 ELSE 0 END::bigint AS deleted_delta,
                       external_size::bigint AS size_delta
                  FROM new_docs
                UNION ALL
                SELECT id,
                       -CASE WHEN NOT deleted THEN 1 ELSE 0 END::bigint,
                       -CASE WHEN deleted THEN 1 ELSE 0 END::bigint,
                       -external_size::bigint
                  FROM old_docs
              ) AS changes
             WHERE strpos(id, ':') > 1
             GROUP BY 1, 2
            HAVING sum(live_delta) <> 0 OR sum(deleted_delta) <> 0 OR sum(size_delta) <> 0
          ) AS delta
         WHERE stats.partition = delta.partition AND stats.stripe = delta.stripe;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE {s}.partition_stats AS stats
           SET doc_count = stats.doc_count - delta.doc_count,
               doc_del_count = stats.doc_del_count - delta.doc_del_count,
               external_size = stats.external_size - delta.external_size
          FROM (
            SELECT split_part(id, ':', 1) AS partition,
                   (hashtextextended(id, 0) & 15)::smallint AS stripe,
                   count(*) FILTER (WHERE NOT deleted) AS doc_count,
                   count(*) FILTER (WHERE deleted) AS doc_del_count,
                   coalesce(sum(external_size), 0) AS external_size
              FROM old_docs
             WHERE strpos(id, ':') > 1
             GROUP BY 1, 2
          ) AS delta
         WHERE stats.partition = delta.partition AND stats.stripe = delta.stripe;
    END IF;
    RETURN NULL;
END
$partition_stats$;

CREATE TRIGGER docs_partition_stats_insert
AFTER INSERT ON {s}.docs
REFERENCING NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_partition_stats();
CREATE TRIGGER docs_partition_stats_update
AFTER UPDATE ON {s}.docs
REFERENCING OLD TABLE AS old_docs NEW TABLE AS new_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_partition_stats();
CREATE TRIGGER docs_partition_stats_delete
AFTER DELETE ON {s}.docs
REFERENCING OLD TABLE AS old_docs
FOR EACH STATEMENT EXECUTE FUNCTION {s}.update_partition_stats();
`

func partitionStatsDDL(schema string) string {
	return strings.ReplaceAll(partitionStatsDDLTemplate, "{s}", schema)
}
