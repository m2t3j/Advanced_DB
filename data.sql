CREATE SCHEMA IF NOT EXISTS qle;

-- One row per query execution
CREATE TABLE IF NOT EXISTS qle.query (
    query_id       SERIAL PRIMARY KEY,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sql_text       TEXT NOT NULL,
    runtime_ms     INTEGER,
    row_count      BIGINT,
    error_message  TEXT,          -- NULL if successful
    pinned_view_id INTEGER        -- FK to qle.pinned_view, nullable
);

-- Base tables referenced by each query (coarse provenance)
CREATE TABLE IF NOT EXISTS qle.query_table (
    query_id   INTEGER REFERENCES qle.query(query_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL
);

-- Lineage edges: which query came from which
CREATE TABLE IF NOT EXISTS qle.edge (
    parent_query_id INTEGER REFERENCES qle.query(query_id) ON DELETE CASCADE,
    child_query_id  INTEGER REFERENCES qle.query(query_id) ON DELETE CASCADE,
    edge_type       TEXT NOT NULL   -- 'derived', 'rerun', etc.
);

-- Materialized views you pinned
CREATE TABLE IF NOT EXISTS qle.pinned_view (
    view_id       SERIAL PRIMARY KEY,
    query_id      INTEGER REFERENCES qle.query(query_id),
    view_name     TEXT UNIQUE NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    storage_bytes BIGINT
);
