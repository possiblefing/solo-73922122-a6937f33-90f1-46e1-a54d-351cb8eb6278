"""PostgreSQL connection pool and query-template catalog.

The pool is sized to exactly two connections (min == max == 2) so the whole
application can never open more than two backend connections.  Every pooled
connection is configured read-only at session level; only the two SELECT
templates ever run through it.
"""

from __future__ import annotations

import os

import psycopg
from psycopg_pool import ConnectionPool

from engine import MAX_CONNS, STATEMENT_TIMEOUT_MS


def _make_configure(dsn: str):
    def _configure(conn: psycopg.Connection) -> None:
        # Remember the DSN on the connection so the engine can open a sibling
        # cancel connection without relying on driver-specific info fields.
        conn.replay_dsn = dsn
        # Read-only session: even an accidental write is rejected by the server.
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = on")
            cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
            cur.execute("SET application_name = 'load-replay'")
        conn.commit()
    return _configure


def create_pool() -> ConnectionPool:
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://replay:replay@db:5432/replay",
    )
    pool = ConnectionPool(
        conninfo=dsn,
        min_size=MAX_CONNS,
        max_size=MAX_CONNS,
        kwargs={"autocommit": False},
        configure=_make_configure(dsn),
        open=True,
        name="replay-pool",
    )
    # Fail fast at startup if the database is unreachable.
    pool.wait(timeout=30.0)
    return pool


def load_templates(pool: ConnectionPool) -> dict[str, str]:
    """Load the two read-only query templates from the catalog table."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, sql_text FROM query_template ORDER BY name"
            )
            templates = {name: sql for name, sql in cur.fetchall()}
    required = {"q1_daily_summary", "q2_settlement_audit"}
    missing = required - set(templates)
    if missing:
        raise RuntimeError(f"Missing query templates in catalog: {sorted(missing)}")
    return templates
