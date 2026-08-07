#!/usr/bin/env python3
"""ingest_vods: scan VOD directory and register eligible files in recordings.
Reads CLPR_DB_URL; exits non-zero on any failure.
Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods -> recordings). The
worker name (and its RESULT/VODS_* output labels) are an external contract
and stay as-is.
"""

import datetime as dt
import os
import re
import subprocess
import sys
from pathlib import Path

import db

try:
    from migrations import apply_migrations
except ModuleNotFoundError:
    from .migrations import apply_migrations

VIDEO_ROOT = Path('/Volumes/GOLDMINE/vibecoder-recordings/')
VIDEO_EXTS = {'.mov', '.mp4', '.mkv'}
MIN_AGE_SECONDS = 30 * 60


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def redacted_db_url() -> str:
    """CLPR_DB_URL with any password redacted (userinfo or ?password= form) —
    this string is printed into worker logs and must never leak a secret."""
    url = os.environ.get('CLPR_DB_URL', '')
    url = re.sub(r'://([^:/@]*):[^@]*@', r'://\1:***@', url)
    url = re.sub(r'([?&]password=)[^&]*', r'\1***', url)
    return url


def ensure_schema(conn) -> None:
    migrations_dir = Path(__file__).resolve().parent.parent / 'migrations_pg'
    apply_migrations(conn, migrations_dir)


def parse_session_label(filename: str) -> str:
    # Expected like: 2026-07-20 10-44-08.mov -> 2026-07-20
    return filename.split(' ')[0]


def probe_duration_seconds(path: Path) -> str:
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(path),
    ]
    print(f'FFPROBE_CMD {cmd}')
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, check=True)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or '').rstrip('\n')
        raise RuntimeError(
            f'ffprobe failed for {path} exit={exc.returncode} stderr={stderr}'
        ) from exc

    raw = proc.stdout.strip()
    if raw == '':
        raise RuntimeError(f'ffprobe returned empty duration output for {path}')
    print(f'FFPROBE_RAW path="{path}" output="{raw}"')
    return raw


def main() -> int:
    if not VIDEO_ROOT.exists():
        raise FileNotFoundError(f'VOD root not found: {VIDEO_ROOT}')

    now_epoch = dt.datetime.now(dt.timezone.utc).timestamp()
    inserted = 0
    skipped_recent = 0
    existing = 0
    failed = 0

    conn = db.connect()
    try:
        cur = conn.cursor()
        ensure_schema(conn)

        # autocommit is OFF: the first statement below opens the transaction
        # implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        for p in sorted(VIDEO_ROOT.iterdir()):
            if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                continue

            try:
                # Per-file savepoint: a failed SQL statement aborts a PG
                # transaction, so without this the FILE_FAILED-and-continue
                # contract could not survive a SQL error on one file.
                cur.execute('SAVEPOINT ingest_file')
                mtime_epoch = p.stat().st_mtime
                age_seconds = now_epoch - mtime_epoch
                if age_seconds < MIN_AGE_SECONDS:
                    skipped_recent += 1
                    print(f'SKIP_RECENT file="{p.name}" age_seconds={age_seconds:.1f} reason="modified <30m"')
                    continue

                session_label = parse_session_label(p.name)
                duration_raw = probe_duration_seconds(p)
                duration_s = float(duration_raw)
                ingested_at = utc_now_iso()

                cur.execute(
                    '''
                    INSERT INTO recordings(path, session_label, duration_s, ingested_at, state)
                    VALUES (%s, %s, %s, %s, 'ingested')
                    ON CONFLICT (path) DO NOTHING
                    ''',
                    (str(p), session_label, duration_s, ingested_at),
                )
                inserted_now = cur.rowcount == 1

                cur.execute('SELECT id FROM recordings WHERE path = %s', (str(p),))
                recording_id_row = cur.fetchone()
                recording_id = recording_id_row[0] if recording_id_row else None

                if inserted_now:
                    inserted += 1
                    print(
                        'INGESTED '
                        f'id={recording_id} '
                        f'path="{p}" '
                        f'session_label="{session_label}" '
                        f'duration_s={duration_s}'
                    )
                else:
                    existing += 1
                    print(f'ALREADY_EXISTS id={recording_id} path="{p}"')
            except Exception as exc:
                failed += 1
                print(f'FILE_FAILED path="{p}" reason="{exc}"')
                cur.execute('ROLLBACK TO SAVEPOINT ingest_file')
                continue

        conn.commit()

        cur.execute('SELECT id, path, session_label, duration_s, ingested_at, state FROM recordings ORDER BY id')
        rows = cur.fetchall()
        print(f'VODS_TOTAL {len(rows)}')
        for r in rows:
            print(f'VODS_ROW {r}')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f'RESULT ingest_vods ok=1 inserted={inserted} existing={existing} skipped_recent={skipped_recent} failed={failed} db_path="{redacted_db_url()}"')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
