"""SQLite migration utilities for clpr workers."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

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


def apply_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    """Apply all .sql migrations in sorted filename order."""
    for path in sorted(migrations_dir.glob('*.sql')):
        sql = path.read_text(encoding='utf-8')
        for stmt in sql.split(';'):
            _run_statement(conn, stmt)
