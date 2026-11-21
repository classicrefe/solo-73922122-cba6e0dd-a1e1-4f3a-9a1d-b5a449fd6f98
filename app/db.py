"""SQLite access helpers and schema.

WAL journaling plus ``BEGIN IMMEDIATE`` write transactions give us the
consistency guarantees the API relies on: version numbers are allocated
atomically, and "exactly one publication per source version" is enforced by a
unique constraint rather than by application-level checks alone.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_manifests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    day             TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,
    station_count   INTEGER NOT NULL,
    total_bytes     INTEGER NOT NULL,
    ranges_json     TEXT    NOT NULL,
    UNIQUE (day, version)
);

CREATE TABLE IF NOT EXISTS stations (
    manifest_id             INTEGER NOT NULL REFERENCES source_manifests(id) ON DELETE CASCADE,
    position                INTEGER NOT NULL,
    station_id              INTEGER NOT NULL,
    estimated_bytes         INTEGER NOT NULL,
    current_start_station   INTEGER,
    current_end_station     INTEGER,
    PRIMARY KEY (manifest_id, station_id),
    UNIQUE (manifest_id, position),
    CHECK (
        (current_start_station IS NULL AND current_end_station IS NULL)
        OR (current_start_station IS NOT NULL AND current_end_station IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS plans (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_manifest_id  INTEGER NOT NULL REFERENCES source_manifests(id),
    partition_count     INTEGER NOT NULL,
    max_partition_bytes INTEGER NOT NULL,
    rewrite_bytes       INTEGER NOT NULL,
    reused_bytes        INTEGER NOT NULL,
    plan_json           TEXT    NOT NULL,
    confirmed_at        TEXT,
    confirmed_by        TEXT,
    created_at          TEXT    NOT NULL,
    UNIQUE (source_manifest_id, partition_count, max_partition_bytes)
);

CREATE TABLE IF NOT EXISTS publications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_manifest_id  INTEGER NOT NULL UNIQUE REFERENCES source_manifests(id),
    plan_id             INTEGER NOT NULL REFERENCES plans(id),
    operator            TEXT    NOT NULL,
    created_at          TEXT    NOT NULL,
    payload_json        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_manifests_day_version
    ON source_manifests(day, version);
"""


class Database:
    """Owns the SQLite path; connections are short-lived and per-request."""

    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=15,
            isolation_level=None,  # explicit transactions
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 15000")
        return conn

    def initialize(self) -> None:
        parent = Path(self.path).parent
        parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialise writers and roll back on any exception."""

    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
