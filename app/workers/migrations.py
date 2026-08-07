"""PostgreSQL migration utilities for clpr workers (D-052 P3).

Applies .sql files from app/migrations_pg/ exactly once, recorded in the
schema_migrations ledger (same apply-once discipline as the SQLite-era D-027
ledger). Connections come from the shared adapter app/workers/db.py; tables
per app/docs/naming-map.md (vods -> recordings).
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import psycopg2.errors

LEDGER_TABLE = 'schema_migrations'

# Real schema evidence that a database has already been migrated by a build
# that predates the ledger. 001_consolidated_schema.sql creates recordings;
# if recordings exists, this is not a fresh database.
POPULATED_EVIDENCE_TABLE = 'recordings'

ALTER_ADD_COLUMN_RE = re.compile(
    r'\bALTER\s+TABLE\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s+ADD\s+COLUMN\s+(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b',
    re.IGNORECASE,
)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
        (table.lower(), column.lower()),
    )
    return cur.fetchone() is not None


def _run_statement(cur, statement: str) -> None:
    stmt = statement.strip()
    if not stmt:
        return

    alter = ALTER_ADD_COLUMN_RE.search(stmt)
    if alter:
        table = alter.group('table')
        column = alter.group('column')
        if _column_exists(cur, table, column):
            return
        # A failed statement aborts the PG transaction, so the duplicate-column
        # fallback needs a savepoint to remain able to continue afterwards.
        cur.execute('SAVEPOINT clpr_alter_guard')
        try:
            cur.execute(stmt)
        except psycopg2.errors.DuplicateColumn:
            cur.execute('ROLLBACK TO SAVEPOINT clpr_alter_guard')
            return
        cur.execute('RELEASE SAVEPOINT clpr_alter_guard')
        return

    cur.execute(stmt)


def _utc_now_iso() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace('+00:00', 'Z')
    )


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'public' AND table_name = %s",
        (table.lower(),),
    )
    return cur.fetchone() is not None


def _ensure_ledger(conn, cur, paths: list[Path]) -> None:
    """Create the applied-migrations ledger, backfilling pre-ledger databases.

    A database created before the ledger existed already has every migration
    applied but no ledger rows. Honouring an empty ledger there would re-run
    every migration once more. So when the ledger is created for the first
    time on a database that real schema evidence shows is already populated,
    every migration file present is recorded as already applied instead of
    being re-run.

    A genuinely fresh/empty database has no evidence table, gets an empty
    ledger, and runs every migration normally. (In the consolidated PG
    database, 001_consolidated_schema.sql creates the ledger and records
    itself, so this branch is a safety net, not the normal path.)
    """
    if _table_exists(cur, LEDGER_TABLE):
        return

    already_populated = _table_exists(cur, POPULATED_EVIDENCE_TABLE)

    cur.execute(
        f'CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ('
        ' filename text PRIMARY KEY,'
        ' applied_at text NOT NULL'
        ')'
    )

    if already_populated:
        applied_at = _utc_now_iso()
        cur.executemany(
            f'INSERT INTO {LEDGER_TABLE}(filename, applied_at) VALUES (%s, %s)'
            ' ON CONFLICT (filename) DO NOTHING',
            [(p.name, applied_at) for p in paths],
        )

    conn.commit()


def apply_migrations(conn, migrations_dir: Path) -> None:
    """Apply each .sql migration in sorted filename order, exactly once.

    Applied migrations are recorded in the schema_migrations ledger and are
    never run again. Statements that do still run keep the existing tolerant
    behaviour: ALTER TABLE .. ADD COLUMN is guarded against duplicate-column
    reruns.
    """
    cur = conn.cursor()
    paths = sorted(migrations_dir.glob('*.sql'))
    _ensure_ledger(conn, cur, paths)

    cur.execute(f'SELECT filename FROM {LEDGER_TABLE}')
    applied = {row[0] for row in cur.fetchall()}

    for path in paths:
        if path.name in applied:
            continue

        sql = path.read_text(encoding='utf-8')
        for stmt in sql.split(';'):
            _run_statement(cur, stmt)

        cur.execute(
            f'INSERT INTO {LEDGER_TABLE}(filename, applied_at) VALUES (%s, %s)'
            ' ON CONFLICT (filename) DO UPDATE SET applied_at = EXCLUDED.applied_at',
            (path.name, _utc_now_iso()),
        )
        conn.commit()
