#!/usr/bin/env python3
"""consolidate_to_pg.py — D-052 P2: consolidate the SQLite sources into PG-ready SQL.

Generates (never applies) two files in --out-dir:

  consolidated_data.sql   BEGIN; explicit-id INSERTs into the RENAMED tables
                          (app/docs/naming-map.md is the contract) + per-table
                          setval() sequence fixes; COMMIT.
  verify_consolidation.sql  psql assertions (DO $$ RAISE EXCEPTION $$) whose
                          expected counts are RECOMPUTED independently from the
                          sources via SQL joins — never echoed from the writer.

Sources
  --imac-db      the iMac seed DB (richest). Pass a COPY; opened read-only.
  --server-dump  a `sqlite3 iterdump()` file from the Hetzner container DB;
                 loaded into an in-memory sqlite via executescript (never
                 regex-parsed).
  MacBook DB     DELIBERATELY SKIPPED: per D-021 it held only a duplicate
                 vod-1 transcript, nothing unique. The manifest states this.

ID strategy
  iMac rows keep their original ids. Server rows get ids offset above the
  iMac MAX(id) per table (offset recorded in the manifest). All FK references
  (vod_id -> recording_id, clips.candidate_id) are remapped consistently.

Conflict rule (encoded even though zero collisions are expected)
  When both sources collide on a table's natural unique key, the row with the
  LATER of its timestamp-ish column wins (ISO-8601 string comparison, per the
  naming map's port-fidelity note). Tables without a timestamp-ish column,
  and exact ties, resolve to the iMac (seed) row. A merged entity keeps the
  iMac id; the winner's VALUES are used; children of both sources remap their
  FKs to the kept id. Every resolution is listed in the manifest.

Refusals (a failed run writes NOTHING to --out-dir)
  - candidates_v2 must have 0 rows in every source that has the table
    (naming-map exclusion); otherwise exit 2 with no output files.
  - NUL characters in text, non-finite floats are rendered defensively;
    orphan FK rows or unexpected value types abort loudly.

schema_migrations is NOT consolidated from either source: the PG table is the
PG apply-once ledger (owned by app/migrations_pg/, already seeded by
001_consolidated_schema.sql). Copying SQLite ledger rows in would assert
migrations PG never ran.

display_name: '<session_label> stream (<duration_s rounded half-up to whole
minutes> min)'; NULL duration (none in the real data) yields NULL.

Deterministic: same inputs -> byte-identical outputs (no clocks, fixed order).
Stdlib only.
"""

import argparse
import hashlib
import math
import os
import sqlite3
import sys

# --------------------------------------------------------------------------
# Table contract (app/docs/naming-map.md). Column kinds: i=integer, d=double
# precision, t=text. fk names the PG parent table whose id map translates it.
# natural_key are PG-side column names (post-FK-translation); ts_col is the
# timestamp-ish column for the conflict rule (None -> seed wins).
# --------------------------------------------------------------------------
TABLES = [
    {
        "sqlite": "vods", "pg": "recordings",
        "cols": [("id", "id", "i", None),
                 ("path", "path", "t", None),
                 ("session_label", "session_label", "t", None),
                 ("duration_s", "duration_s", "d", None),
                 ("ingested_at", "ingested_at", "t", None),
                 ("state", "state", "t", None),
                 ("poison_reviewed", "poison_reviewed", "i", None)],
        "natural_key": ("path",), "ts_col": "ingested_at",
    },
    {
        "sqlite": "poison_regions", "pg": "poison_regions",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("start_s", "start_s", "d", None),
                 ("end_s", "end_s", "d", None),
                 ("source", "source", "t", None),
                 ("reason", "reason", "t", None)],
        "natural_key": None, "ts_col": None,
    },
    {
        "sqlite": "beats", "pg": "trigger_beats",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("ts_utc", "ts_utc", "t", None),
                 ("note", "note", "t", None),
                 ("offset_s", "offset_s", "d", None),
                 ("source", "source", "t", None)],
        "natural_key": None, "ts_col": None,
    },
    {
        "sqlite": "transcript_segments", "pg": "transcript_segments",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("start_s", "start_s", "d", None),
                 ("end_s", "end_s", "d", None),
                 ("text", "text", "t", None)],
        "natural_key": None, "ts_col": None,
    },
    {
        "sqlite": "audio_energy", "pg": "audio_energy_buckets",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("start_s", "start_s", "d", None),
                 ("end_s", "end_s", "d", None),
                 ("loudness_lufs", "loudness_lufs", "d", None)],
        "natural_key": ("recording_id", "start_s", "end_s"), "ts_col": None,
    },
    {
        "sqlite": "transcript_signal_candidates", "pg": "llm_signal_candidates",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("start_s", "start_s", "d", None),
                 ("end_s", "end_s", "d", None),
                 ("category", "category", "t", None),
                 ("reason", "reason", "t", None),
                 ("confidence", "confidence", "d", None),
                 ("source", "source", "t", None),
                 ("trigger_offset_s", "trigger_offset_s", "d", None),
                 ("run_id", "run_id", "t", None),
                 ("created_at", "created_at", "t", None)],
        # mirrors idx_llm_signal_candidates_unique (COALESCE(trigger_offset_s,-1))
        "natural_key": ("recording_id", "start_s", "end_s", "category", "source",
                        "__coalesce_trigger_offset_s"),
        "ts_col": "created_at",
    },
    {
        "sqlite": "chat_raw", "pg": "chat_raw",
        "cols": [("id", "id", "i", None),
                 ("session_label", "session_label", "t", None),
                 ("ts_utc", "ts_utc", "t", None),
                 ("author", "author", "t", None),
                 ("text", "text", "t", None),
                 ("captured_by_run", "captured_by_run", "t", None)],
        "natural_key": None, "ts_col": None,
    },
    {
        "sqlite": "chat_messages", "pg": "chat_messages",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("offset_s", "offset_s", "d", None),
                 ("author", "author", "t", None),
                 ("text", "text", "t", None)],
        "natural_key": ("recording_id", "offset_s", "author", "text"), "ts_col": None,
    },
    {
        "sqlite": "candidates", "pg": "clip_candidates",
        "cols": [("id", "id", "i", None),
                 ("vod_id", "recording_id", "i", "recordings"),
                 ("start_s", "start_s", "d", None),
                 ("end_s", "end_s", "d", None),
                 ("score", "score", "d", None),
                 ("signal_audio", "signal_audio", "d", None),
                 ("signal_transcript", "signal_transcript", "d", None),
                 ("signal_chat", "signal_chat", "d", None),
                 ("signal_beat_boost", "signal_beat_boost", "d", None),
                 ("state", "state", "t", None),
                 ("created_by_run", "created_by_run", "t", None),
                 ("created_at", "created_at", "t", None)],
        "natural_key": ("recording_id", "start_s", "end_s"), "ts_col": "created_at",
    },
    {
        "sqlite": "clips", "pg": "clips",
        "cols": [("id", "id", "i", None),
                 ("candidate_id", "candidate_id", "i", "clip_candidates"),
                 ("file_path", "file_path", "t", None),
                 ("duration_s", "duration_s", "d", None),
                 ("state", "state", "t", None),
                 ("created_by_run", "created_by_run", "t", None),
                 ("created_at", "created_at", "t", None),
                 ("drive_synced_at", "drive_synced_at", "t", None),
                 ("drive_sync_path", "drive_sync_path", "t", None)],
        "natural_key": ("candidate_id",), "ts_col": "created_at",
    },
]

INSERT_ORDER = [t["pg"] for t in TABLES]


def fail(msg, code=2):
    sys.stderr.write("REFUSED: %s\n" % msg)
    sys.exit(code)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# SQL value rendering (PG dialect)
# --------------------------------------------------------------------------

def render_text(v, ctx):
    if v is None:
        return "NULL"
    if not isinstance(v, str):
        fail("unexpected non-text value %r in %s" % (v, ctx))
    if "\x00" in v:
        fail("NUL character in text value in %s (PG text cannot hold NUL)" % ctx)
    return "'" + v.replace("'", "''") + "'"


def render_double(v, ctx):
    if v is None:
        return "NULL"
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        fail("unexpected non-numeric value %r in %s" % (v, ctx))
    f = float(v)
    if math.isnan(f):
        return "'NaN'"
    if math.isinf(f):
        return "'Infinity'" if f > 0 else "'-Infinity'"
    return repr(f)


def render_int(v, ctx):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        fail("unexpected bool value in %s" % ctx)
    if isinstance(v, float):
        if v != int(v):
            fail("non-integral value %r in integer column (%s)" % (v, ctx))
        v = int(v)
    if not isinstance(v, int):
        fail("unexpected non-integer value %r in %s" % (v, ctx))
    return str(v)


RENDERERS = {"i": render_int, "d": render_double, "t": render_text}


# --------------------------------------------------------------------------
# Source loading
# --------------------------------------------------------------------------

def open_imac(path):
    if not os.path.isfile(path):
        fail("iMac DB not found: %s" % path)
    con = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    con.row_factory = sqlite3.Row
    return con

def load_server_dump(path):
    if not os.path.isfile(path):
        fail("server dump not found: %s" % path)
    with open(path, "r", encoding="utf-8") as f:
        script = f.read()
    con = sqlite3.connect(":memory:")
    con.executescript(script)
    con.row_factory = sqlite3.Row
    return con


def table_exists(con, name):
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def guard_candidates_v2(con, label):
    """naming-map exclusion: candidates_v2 must be empty wherever it exists."""
    if table_exists(con, "candidates_v2"):
        n = con.execute("SELECT COUNT(*) FROM candidates_v2").fetchone()[0]
        if n != 0:
            fail("candidates_v2 has %d row(s) in the %s source; the naming map "
                 "requires 0 (empty vestige of the failed 005 re-run, D-027). "
                 "Refusing to generate." % (n, label))
        return n
    return None


def read_rows(con, spec, label):
    """Rows of spec's sqlite table as {sqlite_col: value}, ordered by id."""
    if not table_exists(con, spec["sqlite"]):
        return []
    cols = ", ".join(c[0] for c in spec["cols"])
    out = []
    for r in con.execute(
            "SELECT %s FROM %s ORDER BY id" % (cols, spec["sqlite"])):
        out.append({spec["cols"][i][0]: r[i] for i in range(len(spec["cols"]))})
    return out


# --------------------------------------------------------------------------
# Merge machinery
# --------------------------------------------------------------------------

def natural_key(spec, pg_row):
    if spec["natural_key"] is None:
        return None
    key = []
    for k in spec["natural_key"]:
        if k == "__coalesce_trigger_offset_s":
            v = pg_row["trigger_offset_s"]
            key.append(-1.0 if v is None else float(v))
        else:
            key.append(pg_row[k])
    return tuple(key)


def to_pg_row(spec, src_row, fk_maps, label):
    """Translate a source row's FK columns through the id maps."""
    row = {}
    for s_col, pg_col, _kind, fk in spec["cols"]:
        v = src_row[s_col]
        if fk is not None and v is not None:
            m = fk_maps[fk]
            if v not in m:
                fail("orphan FK: %s.%s=%r (%s source) has no parent in %s"
                     % (spec["sqlite"], s_col, v, label, fk))
            v = m[v]
        row[pg_col] = v
    return row


def merge(imac_con, server_con):
    """Merge both sources; returns (final_rows, offsets, conflicts, id_maps).

    final_rows: {pg_table: [pg_row dicts, sorted by id]}
    offsets:    {pg_table: int} (server id offset, recorded even if unused)
    conflicts:  list of dicts describing each natural-key collision resolution
    id_maps:    {pg_table: {"imac": {old: new}, "server": {old: new}}}
    """
    final_rows, offsets, conflicts, id_maps = {}, {}, [], {}
    for spec in TABLES:
        pg = spec["pg"]
        imac_rows = read_rows(imac_con, spec, "imac")
        server_rows = read_rows(server_con, spec, "server")
        imac_max = max((r["id"] for r in imac_rows), default=0)
        offsets[pg] = imac_max
        id_maps[pg] = {"imac": {}, "server": {}}

        # parent-table id maps for this table's FK translation
        fk_maps = {parent: {**id_maps[parent]["imac"], **id_maps[parent]["server"]}
                   for parent in {c[3] for c in spec["cols"] if c[3]}}
        # (imac FKs are identity maps, but go through the same translation)
        imac_fk_maps = {parent: id_maps[parent]["imac"]
                        for parent in {c[3] for c in spec["cols"] if c[3]}}

        rows = {}          # final id -> pg_row
        key_index = {}     # natural key -> final id
        for r in imac_rows:
            pg_row = to_pg_row(spec, r, imac_fk_maps, "imac")
            fid = pg_row["id"]
            id_maps[pg]["imac"][r["id"]] = fid
            rows[fid] = pg_row
            k = natural_key(spec, pg_row)
            if k is not None:
                if k in key_index:
                    fail("duplicate natural key inside the imac source: %s %r"
                         % (pg, k))
                key_index[k] = fid

        server_fk_maps = {parent: {**id_maps[parent]["imac"],
                                   **id_maps[parent]["server"]}
                          for parent in fk_maps}
        for r in server_rows:
            pg_row = to_pg_row(spec, r, server_fk_maps, "server")
            k = natural_key(spec, pg_row)
            if k is not None and k in key_index:
                kept_id = key_index[k]
                incumbent = rows[kept_id]
                ts = spec["ts_col"]
                if ts is not None and (pg_row[ts] or "") > (incumbent[ts] or ""):
                    winner = "server"
                    new_row = dict(pg_row)
                    new_row["id"] = kept_id     # merged entity keeps the seed id
                    rows[kept_id] = new_row
                else:
                    winner = "imac"             # no ts column, or tie: seed wins
                conflicts.append({
                    "table": pg, "key": k, "winner": winner,
                    "kept_id": kept_id, "server_id": r["id"],
                    "imac_ts": incumbent.get(ts) if ts else None,
                    "server_ts": pg_row.get(ts) if ts else None,
                })
                id_maps[pg]["server"][r["id"]] = kept_id
            else:
                fid = r["id"] + offsets[pg]
                if fid in rows:
                    fail("id collision after offset in %s: %d" % (pg, fid))
                pg_row = dict(pg_row)
                pg_row["id"] = fid
                id_maps[pg]["server"][r["id"]] = fid
                rows[fid] = pg_row
                if k is not None:
                    key_index[k] = fid

        final_rows[pg] = [rows[i] for i in sorted(rows)]
    return final_rows, offsets, conflicts, id_maps


def add_display_names(final_rows):
    """recordings.display_name = '<session_label> stream (<minutes> min)'.

    Minutes = duration_s / 60 rounded half-up. NULL duration -> NULL name
    (none in the real data; counted in the manifest).
    """
    null_names = 0
    for row in final_rows["recordings"]:
        d = row["duration_s"]
        if d is None:
            row["display_name"] = None
            null_names += 1
        else:
            minutes = int(float(d) / 60.0 + 0.5)
            row["display_name"] = "%s stream (%d min)" % (row["session_label"], minutes)
    return null_names


# --------------------------------------------------------------------------
# Independent expected-count recount (for verify_consolidation.sql).
# Pure SQL over the two source connections — never reads the writer's output.
# Server vods are matched to iMac vods by path; child conflicts are joins on
# the natural keys through that path mapping.
# --------------------------------------------------------------------------

def independent_counts(imac_con, server_con):
    def one(con, table):
        if not table_exists(con, table):
            return 0
        return con.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]

    counts = {}
    for spec in TABLES:
        counts[spec["pg"]] = {
            "imac": one(imac_con, spec["sqlite"]),
            "server": one(server_con, spec["sqlite"]),
        }

    # conflict recount via ATTACH + joins on the server connection.
    # Plain path via bound parameter (URI strings are not interpreted on this
    # connection); the file exists and is only ever SELECTed from.
    imac_path = imac_con.execute("PRAGMA database_list").fetchall()[0][2]
    server_con.execute("ATTACH DATABASE ? AS imac", (imac_path,))
    try:
        q = {}
        q["recordings"] = """
            SELECT COUNT(*) FROM main.vods s JOIN imac.vods i ON i.path = s.path"""
        q["audio_energy_buckets"] = """
            SELECT COUNT(*) FROM main.audio_energy s
            JOIN main.vods sv ON sv.id = s.vod_id
            JOIN imac.vods iv ON iv.path = sv.path
            JOIN imac.audio_energy i ON i.vod_id = iv.id
                 AND i.start_s = s.start_s AND i.end_s = s.end_s"""
        q["chat_messages"] = """
            SELECT COUNT(*) FROM main.chat_messages s
            JOIN main.vods sv ON sv.id = s.vod_id
            JOIN imac.vods iv ON iv.path = sv.path
            JOIN imac.chat_messages i ON i.vod_id = iv.id
                 AND i.offset_s = s.offset_s AND i.author = s.author
                 AND i.text = s.text"""
        q["llm_signal_candidates"] = """
            SELECT COUNT(*) FROM main.transcript_signal_candidates s
            JOIN main.vods sv ON sv.id = s.vod_id
            JOIN imac.vods iv ON iv.path = sv.path
            JOIN imac.transcript_signal_candidates i ON i.vod_id = iv.id
                 AND i.start_s = s.start_s AND i.end_s = s.end_s
                 AND i.category = s.category AND i.source = s.source
                 AND IFNULL(i.trigger_offset_s, -1) = IFNULL(s.trigger_offset_s, -1)"""
        q["clip_candidates"] = """
            SELECT COUNT(*) FROM main.candidates s
            JOIN main.vods sv ON sv.id = s.vod_id
            JOIN imac.vods iv ON iv.path = sv.path
            JOIN imac.candidates i ON i.vod_id = iv.id
                 AND i.start_s = s.start_s AND i.end_s = s.end_s"""
        # a server clip conflicts iff its candidate merged into an iMac
        # candidate that already has an iMac clip (natural key: candidate_id)
        q["clips"] = """
            SELECT COUNT(*) FROM main.clips sc
            JOIN main.candidates s ON s.id = sc.candidate_id
            JOIN main.vods sv ON sv.id = s.vod_id
            JOIN imac.vods iv ON iv.path = sv.path
            JOIN imac.candidates i ON i.vod_id = iv.id
                 AND i.start_s = s.start_s AND i.end_s = s.end_s
            JOIN imac.clips ic ON ic.candidate_id = i.id"""
        for spec in TABLES:
            pg = spec["pg"]
            if pg in q and counts[pg]["server"] > 0:
                c = server_con.execute(q[pg]).fetchone()[0]
            else:
                c = 0
            counts[pg]["conflicts"] = c
            counts[pg]["total"] = counts[pg]["imac"] + counts[pg]["server"] - c

        # spot-check facts, recounted from the sources. Approved ids exclude
        # any candidate overridden by a later-winning server row (conflict
        # rule); with zero conflicts this is exactly the iMac approved set.
        if table_exists(server_con, "candidates") and table_exists(server_con, "vods"):
            approved_ids = [r[0] for r in server_con.execute("""
                SELECT i.id FROM imac.candidates i
                WHERE i.state = 'approved'
                  AND NOT EXISTS (
                    SELECT 1 FROM main.candidates s
                    JOIN main.vods sv ON sv.id = s.vod_id
                    JOIN imac.vods iv ON iv.path = sv.path AND iv.id = i.vod_id
                    WHERE s.start_s = i.start_s AND s.end_s = i.end_s
                      AND s.created_at > i.created_at)
                ORDER BY i.id""")]
        else:
            approved_ids = [r[0] for r in imac_con.execute(
                "SELECT id FROM candidates WHERE state='approved' ORDER BY id")]
        if table_exists(server_con, "vods"):
            server_vods = [(r[0], r[1]) for r in server_con.execute(
                "SELECT id, path FROM main.vods ORDER BY id")]
        else:
            server_vods = []
    finally:
        server_con.execute("DETACH DATABASE imac")
    return counts, approved_ids, server_vods


# --------------------------------------------------------------------------
# Emitters
# --------------------------------------------------------------------------

def emit_data_sql(final_rows, counts):
    lines = []
    a = lines.append
    a("-- consolidated_data.sql — generated by app/scripts/consolidate_to_pg.py (D-052 P2)")
    a("-- Explicit-id INSERTs into the renamed PG tables (app/docs/naming-map.md).")
    a("-- Sources: iMac seed DB + server container dump. MacBook DB skipped per D-021")
    a("-- (held only a duplicate vod-1 transcript, nothing unique).")
    a("-- schema_migrations is NOT loaded here: the PG ledger belongs to app/migrations_pg/.")
    a("-- Apply with: psql -v ON_ERROR_STOP=1 -f consolidated_data.sql  (file carries its own txn)")
    a("BEGIN;")
    for spec in TABLES:
        pg = spec["pg"]
        rows = final_rows[pg]
        a("")
        a("-- %s: %d rows (imac %d + server %d - conflicts %d)"
          % (pg, len(rows), counts[pg]["imac"], counts[pg]["server"],
             counts[pg]["conflicts"]))
        pg_cols = [c[1] for c in spec["cols"]]
        kinds = {c[1]: c[2] for c in spec["cols"]}
        if pg == "recordings":
            pg_cols = pg_cols + ["display_name"]
            kinds["display_name"] = "t"
        col_list = ", ".join(pg_cols)
        for row in rows:
            vals = ", ".join(
                RENDERERS[kinds[c]](row[c], "%s.%s id=%s" % (pg, c, row["id"]))
                for c in pg_cols)
            a("INSERT INTO %s (%s) VALUES (%s);" % (pg, col_list, vals))
    a("")
    a("-- sequence fixes (naming-map identity note)")
    for spec in TABLES:
        a("SELECT setval(pg_get_serial_sequence('%s', 'id'), "
          "(SELECT COALESCE(MAX(id), 1) FROM %s));" % (spec["pg"], spec["pg"]))
    a("")
    a("COMMIT;")
    a("")
    return "\n".join(lines)


def emit_verify_sql(counts, approved_ids, server_recordings, null_display_names):
    lines = []
    a = lines.append
    a("-- verify_consolidation.sql — generated by app/scripts/consolidate_to_pg.py (D-052 P2)")
    a("-- Expected counts recomputed INDEPENDENTLY from the SQLite sources (SQL joins),")
    a("-- not echoed from the generator's write path. Run AFTER consolidated_data.sql:")
    a("--   psql -v ON_ERROR_STOP=1 -f verify_consolidation.sql")
    a("-- Any mismatch raises an exception and aborts.")

    def assert_count(label, query, expected):
        a("")
        a("DO $$")
        a("DECLARE n bigint;")
        a("BEGIN")
        a("  SELECT COUNT(*) INTO n FROM %s;" % query)
        a("  IF n <> %d THEN" % expected)
        a("    RAISE EXCEPTION '%s: expected %d rows, found %%', n;" % (label, expected))
        a("  END IF;")
        a("END $$;")

    for spec in TABLES:
        pg = spec["pg"]
        assert_count(pg, pg, counts[pg]["total"])

    # spot check: the iMac's approved candidates, by id, still approved
    if approved_ids:
        ids_csv = ", ".join(str(i) for i in approved_ids)
        assert_count(
            "approved clip_candidates (iMac ids: %s)" % ids_csv,
            "clip_candidates WHERE id IN (%s) AND state = 'approved'" % ids_csv,
            len(approved_ids))

    # spot check: each server recording present under its remapped id + path
    for new_id, path in server_recordings:
        assert_count(
            "server recording remapped to id %d" % new_id,
            "recordings WHERE id = %d AND path = %s"
            % (new_id, render_text(path, "verify server recording path")),
            1)

    # spot check: display_name populated (NULL only where duration_s was NULL)
    assert_count("recordings.display_name IS NULL",
                 "recordings WHERE display_name IS NULL", null_display_names)

    a("")
    a("SELECT 'verify_consolidation: ALL CHECKS PASSED' AS result;")
    a("")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------

def print_manifest(args, imac_sha, dump_sha, counts, offsets, conflicts,
                   null_display_names, out_files):
    p = print
    p("=== D-052 P2 CONSOLIDATION MANIFEST ===")
    p("SOURCES")
    p("  imac_db:     %s" % args.imac_db)
    p("               sha256=%s" % imac_sha)
    p("  server_dump: %s" % args.server_dump)
    p("               sha256=%s" % dump_sha)
    p("  macbook_db:  SKIPPED (deliberate) — per D-021 the MacBook DB held only a")
    p("               duplicate vod-1 transcript, nothing unique; it is not consolidated.")
    p("GUARDS")
    p("  candidates_v2: 0 rows verified in every source that has the table (naming-map")
    p("                 exclusion; non-zero would have refused generation).")
    p("  schema_migrations: NOT consolidated from either source — the PG table is the")
    p("                 apply-once ledger owned by app/migrations_pg/ (already seeded).")
    p("CONFLICT RULE")
    p("  natural-key collision -> later timestamp-ish column wins (ISO string compare);")
    p("  no timestamp column, or tie -> iMac (seed) row wins; merged entity keeps the")
    p("  iMac id and children remap to it. Every resolution is listed below.")
    p("PER-TABLE (pg_table: imac + server - conflicts = total | server id offset)")
    for spec in TABLES:
        pg = spec["pg"]
        c = counts[pg]
        p("  %-24s %5d + %5d - %d = %5d | +%d"
          % (pg, c["imac"], c["server"], c["conflicts"], c["total"], offsets[pg]))
    p("CONFLICTS")
    if not conflicts:
        p("  (none — as expected: iMac paths are GOLDMINE .mov, the server's is a")
        p("   container .wav; zero natural-key overlap)")
    else:
        for c in conflicts:
            p("  %s key=%r -> %s wins (kept id %d; server id %d remapped; "
              "imac_ts=%r server_ts=%r)"
              % (c["table"], c["key"], c["winner"], c["kept_id"],
                 c["server_id"], c["imac_ts"], c["server_ts"]))
    p("DISPLAY NAMES")
    p("  recordings.display_name = '<session_label> stream (<minutes> min)'; "
      "%d NULL (NULL duration)" % null_display_names)
    p("OUTPUTS")
    for name, sha in out_files:
        p("  %-26s sha256=%s" % (name, sha))
    p("=== END MANIFEST ===")


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--imac-db", required=True,
                    help="path to a COPY of the iMac seed DB (opened read-only)")
    ap.add_argument("--server-dump", required=True,
                    help="path to the server sqlite iterdump() .sql file")
    ap.add_argument("--out-dir", required=True,
                    help="directory for consolidated_data.sql + verify_consolidation.sql")
    args = ap.parse_args()

    imac_sha = sha256_file(args.imac_db)
    dump_sha = sha256_file(args.server_dump)

    imac_con = open_imac(args.imac_db)
    server_con = load_server_dump(args.server_dump)

    # Refusal gates run BEFORE any output file is opened (a failed run writes nothing).
    guard_candidates_v2(imac_con, "imac")
    guard_candidates_v2(server_con, "server")

    final_rows, offsets, conflicts, _id_maps = merge(imac_con, server_con)
    null_display_names = add_display_names(final_rows)

    counts, approved_ids, server_vods = independent_counts(imac_con, server_con)

    # cross-check: the writer's row totals must equal the independent recount
    for spec in TABLES:
        pg = spec["pg"]
        if len(final_rows[pg]) != counts[pg]["total"]:
            fail("internal cross-check failed for %s: writer holds %d rows, "
                 "independent recount expects %d"
                 % (pg, len(final_rows[pg]), counts[pg]["total"]))
    if len(conflicts) != sum(counts[t["pg"]]["conflicts"] for t in TABLES):
        fail("internal cross-check failed: writer saw %d conflicts, "
             "independent recount %d"
             % (len(conflicts),
                sum(counts[t["pg"]]["conflicts"] for t in TABLES)))

    # server recordings under their final ids (through the merge map)
    server_rec_map = _id_maps["recordings"]["server"]
    server_recordings = [(server_rec_map[i], path) for i, path in server_vods]

    data_sql = emit_data_sql(final_rows, counts)
    verify_sql = emit_verify_sql(counts, approved_ids, server_recordings,
                                 null_display_names)

    os.makedirs(args.out_dir, exist_ok=True)
    out_files = []
    for name, content in [("consolidated_data.sql", data_sql),
                          ("verify_consolidation.sql", verify_sql)]:
        b = content.encode("utf-8")
        with open(os.path.join(args.out_dir, name), "wb") as f:
            f.write(b)
        out_files.append((name, sha256_bytes(b)))

    print_manifest(args, imac_sha, dump_sha, counts, offsets, conflicts,
                   null_display_names, out_files)


if __name__ == "__main__":
    main()
