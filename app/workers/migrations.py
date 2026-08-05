"""SQLite migration utilities for clpr workers."""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
from pathlib import Path

LEDGER_TABLE = 'schema_migrations'

# Real schema evidence that a database has already been migrated by a build
# that predates the ledger. 001_init.sql creates vods; if vods exists, this is
# not a fresh database.
POPULATED_EVIDENCE_TABLE = 'vods'

ALTER_ADD_COLUMN_RE = re.compile(
    r'\bALTER\s+TABLE\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s+ADD\s+COLUMN\s+(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b',
    re.IGNORECASE,
)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
    return any((r[1] or '').lower() == column.lower() for r in rows)


def _run_statement(conn: sqlite3.Connection, statement: str) -> None:
    stmt = statement.strip()
    if not stmt:
        return

    alter = ALTER_ADD_COLUMN_RE.search(stmt)
    if alter:
        table = alter.group('table')
        column = alter.group('column')
        if _column_exists(conn, table, column):
            return

    try:
        conn.execute(stmt)
    except sqlite3.OperationalError as exc:
        msg = str(exc).lower()
        if alter and 'duplicate column name' in msg:
            return
        raise


def _utc_now_iso() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace('+00:00', 'Z')
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _ensure_ledger(conn: sqlite3.Connection, paths: list[Path]) -> None:
    """Create the applied-migrations ledger, backfilling pre-ledger databases.

    A database created before the ledger existed already has every migration
    applied but no ledger rows. Honouring an empty ledger there would re-run
    every migration once more, and 005_maybe_state.sql is no longer re-runnable
    (its DROP TABLE candidates violates the clips -> candidates foreign key that
    006 added). So when the ledger is created for the first time on a database
    that real schema evidence shows is already populated, every migration file
    present is recorded as already applied instead of being re-run.

    A genuinely fresh/empty database has no evidence table, gets an empty
    ledger, and runs every migration normally.
    """
    if _table_exists(conn, LEDGER_TABLE):
        return

    already_populated = _table_exists(conn, POPULATED_EVIDENCE_TABLE)

    conn.execute(
        f'CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ('
        ' filename TEXT PRIMARY KEY,'
        ' applied_at TEXT NOT NULL'
        ')'
    )

    if already_populated:
        applied_at = _utc_now_iso()
        conn.executemany(
            f'INSERT OR IGNORE INTO {LEDGER_TABLE}(filename, applied_at) VALUES (?, ?)',
            [(p.name, applied_at) for p in paths],
        )

    conn.commit()


def apply_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    """Apply each .sql migration in sorted filename order, exactly once.

    Applied migrations are recorded in the schema_migrations ledger and are
    never run again. Statements that do still run keep the existing tolerant
    behaviour: ALTER TABLE .. ADD COLUMN is guarded against duplicate-column
    reruns.
    """
    paths = sorted(migrations_dir.glob('*.sql'))
    _ensure_ledger(conn, paths)

    applied = {
        row[0] for row in conn.execute(f'SELECT filename FROM {LEDGER_TABLE}').fetchall()
    }

    for path in paths:
        if path.name in applied:
            continue

        sql = path.read_text(encoding='utf-8')
        for stmt in sql.split(';'):
            _run_statement(conn, stmt)

        conn.execute(
            f'INSERT OR REPLACE INTO {LEDGER_TABLE}(filename, applied_at) VALUES (?, ?)',
            (path.name, _utc_now_iso()),
        )
        conn.commit()
