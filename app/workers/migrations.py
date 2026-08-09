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

# Matches the "ALTER TABLE <table>" prefix of a statement. Used with .match()
# against the already-.strip()'d statement text, so it only fires when the
# statement genuinely STARTS with ALTER TABLE (golden-review F13 ID-06: the
# old regex used .search() over the whole statement, which is also what let
# it silently see only the FIRST of several ADD COLUMN clauses).
ALTER_TABLE_RE = re.compile(
    r'ALTER\s+TABLE\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\b',
    re.IGNORECASE,
)

# Matches a single "ADD COLUMN <name>" clause. Applied to one clause already
# isolated by _split_top_level, so this never has to reason about a comma
# inside a SIBLING clause's CHECK(...) or DEFAULT.
ADD_COLUMN_CLAUSE_RE = re.compile(
    r'ADD\s+COLUMN\s+(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b',
    re.IGNORECASE,
)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns"
        " WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
        (table.lower(), column.lower()),
    )
    return cur.fetchone() is not None


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = 'public' AND table_name = %s",
        (table.lower(),),
    )
    return cur.fetchone() is not None


def _split_statements(sql: str) -> list[str]:
    """Split a .sql file into individual statements on top-level semicolons.

    SQL-aware, unlike the old `sql.split(';')` (golden-review F13 ID-01): a
    `--` line comment is stripped before any semicolon inside it can be
    mistaken for a statement boundary, and a semicolon or `--` inside a
    single- or double-quoted literal is part of that literal, never a
    boundary or a comment start. A doubled quote ('' or "") is the standard
    SQL escape for a literal quote character and does not end the literal.

    Out of scope (none of the current migrations need it): dollar-quoted
    string bodies ($$...$$), as used for PL/pgSQL function bodies.
    """
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_single:
            buf.append(ch)
            if ch == "'":
                if sql[i + 1:i + 2] == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(ch)
            if ch == '"':
                if sql[i + 1:i + 2] == '"':
                    buf.append('"')
                    i += 2
                    continue
                in_double = False
            i += 1
            continue
        if ch == '-' and sql[i + 1:i + 2] == '-':
            # Line comment: everything to the newline is stripped; the
            # newline itself is kept so the reconstructed statement text
            # still reads naturally.
            nl = sql.find('\n', i)
            if nl == -1:
                break  # comment runs to EOF; nothing left to add
            i = nl
            continue
        if ch == "'":
            in_single = True
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            buf.append(ch)
            i += 1
            continue
        if ch == ';':
            statements.append(''.join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    statements.append(''.join(buf))
    return statements


def _split_top_level(text: str, sep: str = ',') -> list[str]:
    """Split text on `sep` at paren-depth 0, outside quoted literals.

    Used to break an ALTER TABLE statement's comma-joined action-clause list
    (e.g. "ADD COLUMN a int, ADD COLUMN b int") into individual clauses
    without being fooled by a comma inside one clause's own CHECK(...) or
    DEFAULT. Comments are assumed already stripped: this only ever runs on
    statement text produced by _split_statements.
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if in_single:
            buf.append(ch)
            if ch == "'":
                if text[i + 1:i + 2] == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(ch)
            if ch == '"':
                if text[i + 1:i + 2] == '"':
                    buf.append('"')
                    i += 2
                    continue
                in_double = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            buf.append(ch)
            i += 1
            continue
        if ch == '(':
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ')':
            depth -= 1
            buf.append(ch)
            i += 1
            continue
        if ch == sep and depth == 0:
            parts.append(''.join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    parts.append(''.join(buf))
    return parts


def _run_add_column_clauses(cur, table: str, clauses: list[tuple[str, str]]) -> None:
    """Apply only the ADD COLUMN clauses whose column is not already there.

    `clauses` is a list of (column_name, clause_sql) pairs parsed from ONE
    ALTER TABLE statement's action list. Each missing column is applied as
    its OWN single-clause ALTER TABLE statement, inside a savepoint
    (golden-review F13 ID-06 fix): re-running the ORIGINAL multi-clause
    statement verbatim would abort atomically the moment any one clause hit
    a duplicate column, silently skipping whichever columns still needed to
    be added. Per-clause application makes a partially-applied multi-column
    ALTER converge instead.
    """
    for column, clause_sql in clauses:
        if _column_exists(cur, table, column):
            continue
        rebuilt = f'ALTER TABLE {table} {clause_sql}'
        # A failed statement aborts the PG transaction, so the duplicate-
        # column fallback needs a savepoint to remain able to continue.
        cur.execute('SAVEPOINT clpr_alter_guard')
        try:
            cur.execute(rebuilt)
        except psycopg2.errors.DuplicateColumn:
            cur.execute('ROLLBACK TO SAVEPOINT clpr_alter_guard')
            continue
        cur.execute('RELEASE SAVEPOINT clpr_alter_guard')


def _run_statement(cur, statement: str) -> None:
    stmt = statement.strip()
    if not stmt:
        return

    table_match = ALTER_TABLE_RE.match(stmt)
    if table_match:
        table = table_match.group('table')
        action_text = stmt[table_match.end():]
        clauses = [c.strip() for c in _split_top_level(action_text)]
        clauses = [c for c in clauses if c]

        parsed: list[tuple[str, str]] = []
        all_add_column = bool(clauses)
        for clause in clauses:
            col_match = ADD_COLUMN_CLAUSE_RE.match(clause)
            if col_match:
                parsed.append((col_match.group('column'), clause))
            else:
                all_add_column = False

        if parsed and all_add_column:
            _run_add_column_clauses(cur, table, parsed)
            return
        # Not a pure ADD-COLUMN ALTER (ADD CONSTRAINT, DROP, RENAME, or a mix
        # of clause types): run verbatim, same as any other statement. This
        # matches the pre-existing (unguarded) behaviour for statements like
        # ADD CONSTRAINT, which the old regex never matched either.

    cur.execute(stmt)


def _utc_now_iso() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace('+00:00', 'Z')
    )


def _ensure_ledger(conn, cur, paths: list[Path]) -> None:
    """Create the applied-migrations ledger, backfilling pre-ledger databases.

    A database created before the ledger existed already has every migration
    applied but no ledger rows. Honouring an empty ledger there would re-run
    every migration once more. So when the ledger is created for the first
    time on a database that real schema evidence shows is already populated,
    every migration file present is recorded as already applied instead of
    being re-run.

    A genuinely fresh/empty database has no evidence table, gets an empty
    ledger, and runs every migration normally -- and on a fresh database this
    function's plain CREATE TABLE IF NOT EXISTS (no backfill, since
    already_populated is False) IS the normal path, not a safety net: it runs
    on every first-time bootstrap, before 001_consolidated_schema.sql ever
    executes, because apply_migrations() calls this function first. 001 also
    creates schema_migrations (CREATE TABLE IF NOT EXISTS) and records itself
    (INSERT .. ON CONFLICT DO NOTHING), and post-B12 (golden-review F13) both
    creations are idempotent, so the two do not collide regardless of which
    one a fresh DB happens to hit first. The true safety net is narrower: only
    the already_populated BACKFILL branch above, which exists solely for a
    database that had `recordings` rows before this ledger was introduced and
    needs every migration retroactively marked applied so it is not re-run.
    That legacy-transition case is the one path this function exists for that
    is not exercised on an ordinary fresh install.
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
    reruns, per clause.
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
        for stmt in _split_statements(sql):
            _run_statement(cur, stmt)

        cur.execute(
            f'INSERT INTO {LEDGER_TABLE}(filename, applied_at) VALUES (%s, %s)'
            ' ON CONFLICT (filename) DO UPDATE SET applied_at = EXCLUDED.applied_at',
            (path.name, _utc_now_iso()),
        )
        conn.commit()
