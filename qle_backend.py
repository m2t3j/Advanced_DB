# qle_backend.py
import re
import time
import psycopg2
import psycopg2.extras

# Your Postgres connection
DSN = "dbname=imdb user=postgres password=uromastyx host=localhost port=5432"


def get_conn():
    return psycopg2.connect(DSN)


# Naive table name extractor for FROM / JOIN clauses
TABLE_REGEX = re.compile(
    r"\bFROM\s+([a-zA-Z0-9_\.]+)|\bJOIN\s+([a-zA-Z0-9_\.]+)",
    re.IGNORECASE,
)


def extract_table_names(sql_text: str):
    tables = set()
    for m in TABLE_REGEX.finditer(sql_text):
        t1, t2 = m.groups()
        t = t1 or t2
        if t:
            tables.add(t.strip())
    return sorted(tables)


def run_query(sql_text: str, parent_query_ids=None):
    """
    Execute SQL, log it, and return (query_id, rows, cols, error_message).
    parent_query_ids: list[int] or None
    """
    parent_query_ids = parent_query_ids or []
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    start = time.time()
    error_message = None
    rows = []
    cols = []
    row_count = None
    runtime_ms = None

    try:
        cur.execute(sql_text)
        runtime_ms = int((time.time() - start) * 1000)

        if cur.description is not None:
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            row_count = len(rows)
        else:
            row_count = cur.rowcount

    except Exception as e:
        # Statement failed, but we still log the attempt
        conn.rollback()
        error_message = str(e)
        runtime_ms = int((time.time() - start) * 1000)

    # Log into qle.query
    cur.execute(
        """
        INSERT INTO qle.query (sql_text, runtime_ms, row_count, error_message)
        VALUES (%s, %s, %s, %s)
        RETURNING query_id
        """,
        (sql_text, runtime_ms, row_count, error_message),
    )
    query_id = cur.fetchone()["query_id"]

    # Log tables if execution succeeded
    if error_message is None:
        tables = extract_table_names(sql_text)
        for t in tables:
            cur.execute(
                """
                INSERT INTO qle.query_table (query_id, table_name)
                VALUES (%s, %s)
                """,
                (query_id, t),
            )

    # Log lineage edges
    for pid in parent_query_ids:
        cur.execute(
            """
            INSERT INTO qle.edge (parent_query_id, child_query_id, edge_type)
            VALUES (%s, %s, %s)
            """,
            (pid, query_id, "derived"),
        )

    conn.commit()
    cur.close()
    conn.close()
    return query_id, rows, cols, error_message


def get_query_history(limit=50):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT q.query_id,
               q.executed_at,
               q.runtime_ms,
               q.row_count,
               q.error_message,
               COALESCE(
                 array_agg(DISTINCT qt.table_name)
                 FILTER (WHERE qt.table_name IS NOT NULL),
                 '{}'
               ) AS tables
        FROM qle.query q
        LEFT JOIN qle.query_table qt ON q.query_id = qt.query_id
        GROUP BY q.query_id
        ORDER BY q.executed_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_lineage_graph():
    """Return (nodes, edges) for visualization."""
    nodes = get_query_history(limit=500)
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM qle.edge")
    edges = cur.fetchall()
    cur.close()
    conn.close()
    return nodes, edges


def get_query_details(query_id: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM qle.query WHERE query_id = %s", (query_id,))
    q = cur.fetchone()

    cur.execute(
        """
        SELECT table_name
        FROM qle.query_table
        WHERE query_id = %s
        """,
        (query_id,),
    )
    tables = [r["table_name"] for r in cur.fetchall()]

    # Get pinned view if any
    cur.execute(
        """
        SELECT pv.*
        FROM qle.pinned_view pv
        JOIN qle.query q2 ON q2.pinned_view_id = pv.view_id
        WHERE q2.query_id = %s
        """,
        (query_id,),
    )
    pv = cur.fetchone()

    cur.close()
    conn.close()
    return q, tables, pv


def pin_query_as_view(query_id: int):
    """Create a materialized view from the query's SQL and log it."""
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get SQL text
    cur.execute("SELECT sql_text FROM qle.query WHERE query_id = %s", (query_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise ValueError("Unknown query_id")
    sql_text = row["sql_text"]

    view_name = f"qle_view_{query_id}"

    # Create materialized view
    cur.execute(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS {sql_text};")

    # Measure storage
    cur.execute(
        "SELECT pg_relation_size(%s::regclass) AS bytes",
        (view_name,),
    )
    storage_bytes = cur.fetchone()["bytes"]

    # Insert into pinned_view
    cur.execute(
        """
        INSERT INTO qle.pinned_view (query_id, view_name, storage_bytes)
        VALUES (%s, %s, %s)
        ON CONFLICT (view_name) DO UPDATE
            SET storage_bytes = EXCLUDED.storage_bytes
        RETURNING view_id
        """,
        (query_id, view_name, storage_bytes),
    )
    view_id = cur.fetchone()["view_id"]

    # Update qle.query
    cur.execute(
        """
        UPDATE qle.query
        SET pinned_view_id = %s
        WHERE query_id = %s
        """,
        (view_id, query_id),
    )

    conn.commit()
    cur.close()
    conn.close()
    return view_id, view_name, storage_bytes


def list_pinned_views():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT pv.view_id,
               pv.view_name,
               pv.storage_bytes,
               pv.created_at,
               q.query_id,
               q.executed_at,
               q.sql_text
        FROM qle.pinned_view pv
        JOIN qle.query q ON q.query_id = pv.query_id
        ORDER BY pv.created_at DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def preview_view(view_name: str, limit: int = 50):
    """Return (rows, cols) from the materialized view."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SELECT * FROM {view_name} LIMIT %s;", (limit,))
    rows = cur.fetchall()
    cols = [d.name for d in cur.description]
    cur.close()
    conn.close()
    return rows, cols


def delete_query(query_id: int):
    """
    Delete a query and its lineage metadata.
    If the query has a pinned materialized view, drop that view and delete the pinned_view row.
    If, after deletion, qle.query is empty, reset the query_id and view_id sequences to start at 1.
    """
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Find any pinned view associated with this query
        cur.execute(
            """
            SELECT pv.view_id, pv.view_name
            FROM qle.pinned_view pv
            WHERE pv.query_id = %s
            """,
            (query_id,),
        )
        pv = cur.fetchone()

        if pv:
            view_name = pv["view_name"]

            # Drop the actual materialized view in the database
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

            # Clear pinned_view_id from any queries pointing to this view
            cur.execute(
                """
                UPDATE qle.query
                SET pinned_view_id = NULL
                WHERE pinned_view_id = %s
                """,
                (pv["view_id"],),
            )

            # Delete the pinned_view metadata row
            cur.execute(
                """
                DELETE FROM qle.pinned_view
                WHERE view_id = %s
                """,
                (pv["view_id"],),
            )

        # Delete the query row itself.
        # qle.query_table and qle.edge should have ON DELETE CASCADE on their FKs,
        # so associated rows will be removed automatically.
        cur.execute(
            """
            DELETE FROM qle.query
            WHERE query_id = %s
            """,
            (query_id,),
        )

        # Check if qle.query is now empty; if so, reset sequences
        cur.execute("SELECT COUNT(*) AS cnt FROM qle.query;")
        cnt = cur.fetchone()["cnt"]

        if cnt == 0:
            # Reset query_id and view_id sequences so next insert starts at 1
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('qle.query', 'query_id'),
                    1,
                    false
                );
                """
            )
            cur.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence('qle.pinned_view', 'view_id'),
                    1,
                    false
                );
                """
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise e
    cur.close()
    conn.close()


def clear_history():
    """
    Drop all pinned materialized views and truncate QLE metadata tables.
    Also resets SERIAL/identity counters so query_id starts back at 1.
    Does NOT touch underlying IMDB tables.
    """
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1) Drop all pinned materialized views first
        cur.execute("SELECT view_name FROM qle.pinned_view;")
        for (view_name,) in cur.fetchall():
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")

        # 2) Truncate metadata tables
        cur.execute(
            """
            TRUNCATE qle.edge,
                     qle.query_table,
                     qle.pinned_view,
                     qle.query
            CASCADE;
            """
        )

        # 3) Explicitly reset sequences for query_id and view_id
        cur.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('qle.query', 'query_id'),
                1,
                false
            );
            """
        )
        cur.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('qle.pinned_view', 'view_id'),
                1,
                false
            );
            """
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise e
    cur.close()
    conn.close()
