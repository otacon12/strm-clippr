# PORTING CHECKLIST — sqlite3 worker → PostgreSQL (D-052 P3)

The exact mechanical recipe followed for the exemplar port
(`app/workers/zebra_detect.py`, proven on the real consolidated data via
`app/scripts/pg_test_harness.sh`). Apply every numbered pattern to every
worker. Read `app/migrations_pg/001_consolidated_schema.sql` and
`app/docs/naming-map.md` FIRST — never write a table/column name from memory.

---

## DISCOVERED PATTERNS THE CONTRACT MISSED (read these first)

These bit during the exemplar port and are NOT in the port-rules contract:

**D1. `conn.execute(...)` does not exist in psycopg2 — every call needs a cursor.**
sqlite3 has a connection-level `execute()` convenience; psycopg2 does not.
Open one cursor (`cur = conn.cursor()`) and route every statement through it.
Helper functions that took `conn` now take `cur` (or both, if they commit).

**D2. `.execute(...).fetchone()` chaining breaks.** sqlite3's `execute()`
returns the cursor, so `conn.execute(q, p).fetchone()` chains. psycopg2's
`cursor.execute()` returns `None`. Split into two statements:
```python
# before (sqlite)
row = conn.execute('SELECT state FROM vods WHERE id = ?', (vod_id,)).fetchone()
# after (pg)
cur.execute('SELECT state FROM recordings WHERE id = %s', (recording_id,))
row = cur.fetchone()
```

**D3. `with sqlite3.connect(...) as conn:` does NOT translate to `with conn:`.**
Both commit/rollback on exit and NEITHER closes, but the sqlite idiom hides
the transaction management the PG port must own explicitly. Replace the
`with`-block with explicit control flow so failure paths are visible:
```python
conn = db.connect()
try:
    cur = conn.cursor()
    ...work...
    conn.commit()
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()
```

**D4. Delete explicit `BEGIN;` statements.** With `autocommit = False`
(the adapter's setting), psycopg2 opens a transaction implicitly at the first
statement. A sqlite-era `conn.execute('BEGIN;')` is at best redundant and at
worst an "already in transaction" error. The reads before the writes are
already inside the same transaction — do not try to recreate sqlite's
autocommit-then-BEGIN shape.

**D5. There is no default database any more — and that is deliberate.**
The sqlite workers silently fell back to `./clpr.db` when `CLPR_DB_PATH` was
unset. The adapter's `get_db_url()` instead prints `ERROR: CLPR_DB_URL is not
set ...` to stderr and exits 1. Do not add a default URL to any worker: a
silent fallback to the wrong database is the charter's proxy trap. Note
`sys.exit(1)` raises `SystemExit`, which is NOT caught by the workers'
`except Exception` main wrapper — it propagates cleanly as exit 1 (verified).

**D6. Harness fact: role before schema.** `001_consolidated_schema.sql`
GRANTs to `app_rw`, so the throwaway cluster must `CREATE ROLE app_rw LOGIN`
BEFORE applying 001 or the apply fails. The harness then connects workers AS
`app_rw` (not the superuser), so every proof also exercises the live grants.

**D7. Harness fact: keep the socket dir path short.** Unix socket paths cap
at ~103 chars; macOS `mktemp -d` defaults under `/var/folders/...`. The
harness uses `mktemp -d /tmp/clpr_pgh.XXXXXXXX`.

---

## The mechanical recipe (contract patterns, with before/after)

### 1. Connection: sqlite3 → the shared adapter
```python
# before
import sqlite3
def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')
with sqlite3.connect(get_db_path()) as conn:
    ...
# after
import db          # app/workers/db.py — workers run as scripts from app/workers, so plain import works
conn = db.connect()   # reads CLPR_DB_URL, autocommit OFF; see D3 for the try/finally shape
```
Drop `import sqlite3` and `import os` if now unused. Nothing sqlite survives.

### 2. Placeholders: `?` → `%s`
```python
# before
'SELECT state FROM vods WHERE id = ?'
# after
'SELECT state FROM recordings WHERE id = %s'
```
Parameters stay tuples. Never string-format values in.

### 3. Renames: every table/column per naming-map.md
`vods`→`recordings`, `vod_id`→`recording_id`, `candidates`→`clip_candidates`,
`beats`→`trigger_beats`, `audio_energy`→`audio_energy_buckets`,
`transcript_signal_candidates`→`llm_signal_candidates`. Unchanged:
`transcript_segments`, `chat_messages`, `chat_raw`, `clips`,
`poison_regions`, `schema_migrations`. Rename Python identifiers too
(`vod_id` variables → `recording_id`) so the code cannot half-say both.
```python
# before
INSERT INTO beats(vod_id, ts_utc, note, offset_s, source) VALUES (?, ?, ?, ?, ?)
# after
INSERT INTO trigger_beats(recording_id, ts_utc, note, offset_s, source) VALUES (%s, %s, %s, %s, %s)
```

### 4. `INSERT OR IGNORE` → `ON CONFLICT (...) DO NOTHING`
Name the REAL conflict-target columns from 001's UNIQUE constraints — never
a bare `ON CONFLICT DO NOTHING` (a bare form swallows conflicts you did not
mean to tolerate):
```sql
-- before
INSERT OR IGNORE INTO chat_messages(vod_id, offset_s, author, text) VALUES (?, ?, ?, ?)
-- after (conflict target = the table's UNIQUE constraint in 001)
INSERT INTO chat_messages(recording_id, offset_s, author, text) VALUES (%s, %s, %s, %s)
ON CONFLICT (recording_id, offset_s, author, text) DO NOTHING
```
Targets from 001: `audio_energy_buckets (recording_id, start_s, end_s)`;
`chat_messages (recording_id, offset_s, author, text)`;
`clip_candidates (recording_id, start_s, end_s)`; `clips (candidate_id)`;
`recordings (path)`; `llm_signal_candidates` — its uniqueness is the
EXPRESSION index `idx_llm_signal_candidates_unique`, so the conflict target
must be written as the index expression list:
`ON CONFLICT (recording_id, start_s, end_s, category, source, (COALESCE(trigger_offset_s, -1))) DO NOTHING`.
HARNESS-VERIFIED on PG 16.14 as `app_rw` against the real data, both arms:
duplicate of a NULL-`trigger_offset_s` row → `INSERT 0 0` (count unchanged);
fresh non-NULL row → `INSERT 0 1` then the identical repeat → `INSERT 0 0`.
The expression MUST be written exactly as in the index (parenthesized
`(COALESCE(trigger_offset_s, -1))`) or PG cannot infer the index.

### 5. `PRAGMA` lines: delete
```python
# before
conn.execute('PRAGMA foreign_keys = ON;')
# after — (nothing; FKs are always on in PostgreSQL)
```

### 6. `cursor.lastrowid` → `RETURNING id`
```python
# before
cur = conn.execute('INSERT INTO clips(...) VALUES (...)', p)
clip_id = cur.lastrowid
# after
cur.execute('INSERT INTO clips(...) VALUES (...) RETURNING id', p)
clip_id = cur.fetchone()[0]
```

### 7. `sqlite_master` introspection → `information_schema`
```python
# before
conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (t,))
# after
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s", (t,))
```

### 8. `conn.executescript(sql)` → psycopg2 execute of the script content
`cur.execute(script_text)` runs multiple `;`-separated statements in one call.
Files applied by psql stay files (the harness and migrations_pg apply .sql
via `psql -v ON_ERROR_STOP=1 -f`).

### 9. CLI contract: `--vod-id` STAYS
External flag names are a contract (n8n workflows invoke them). Keep
`--vod-id`; bind it to `recording_id` internally:
```python
parser.add_argument('--vod-id', type=int, required=True)   # unchanged
return detect(args.vod_id)                                  # detect(recording_id: int)
```

### 10. RESULT line: byte-compatible except id fields may read `recording=N`
```
# before
RESULT zebra_detect vod=12 triggers_found=4
# after (sanctioned reading of the contract: id field renamed, everything else byte-identical)
RESULT zebra_detect recording=12 triggers_found=4
```

### 11. Failure verdicts to stderr (D-047) — unchanged, verify it survived
The `ERROR: {exc}` → stderr + exit 1 wrapper in every worker's `__main__`
stays. Verified on the exemplar: unknown id → `ERROR: recording_id not
found: 999` on stderr, exit 1; unset URL → adapter ERROR on stderr, exit 1.

### 12. Dialect notes that needed NO change (checked, not assumed)
`ABS()`, `LIMIT`, `ORDER BY start_s, id`, `COALESCE` all exist in PG.
`double precision` comes back as Python `float` like sqlite REAL (no
`Decimal` — that only appears for `numeric`, which 001 never uses).
ISO-8601 timestamps remain `text` (port fidelity). Default psycopg2 cursors
return tuples, same indexing as sqlite3 rows. `poison_reviewed` stays an
`integer` 0/1, not boolean.

---

## Verification (every ported worker, before reporting)

Run under `app/scripts/pg_test_harness.sh` (throwaway socket-only cluster,
real schema, real consolidated data — parallel-safe):

1. `python3 -m py_compile` the worker (and any touched module).
2. A run that MUST produce non-empty output (positive control — a clean zero
   is a claim, charter 1.5 gate 3). Exemplar: recording 12 →
   `triggers_found=4`, then a rerun → `triggers_found=0` (idempotency), then
   `SELECT` the written rows back as `app_rw`.
3. The briefed/real-data runs, RESULT lines quoted verbatim.
4. The failure path: bad id / unset `CLPR_DB_URL` → ERROR on stderr, exit 1.
