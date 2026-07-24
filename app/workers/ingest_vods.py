#!/usr/bin/env python3
"""ingest_vods: scan VOD directory and register eligible files in vods.
Reads CLPR_DB_PATH; exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

import datetime as dt
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

try:
    from migrations import apply_migrations
except ModuleNotFoundError:
    from .migrations import apply_migrations

VIDEO_ROOT = Path('/Volumes/GOLDMINE/vibecoder-recordings/')
VIDEO_EXTS = {'.mov', '.mp4', '.mkv'}
MIN_AGE_SECONDS = 30 * 60


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def ensure_schema(conn: sqlite3.Connection) -> None:
    migrations_dir = Path(__file__).resolve().parent.parent / 'migrations'
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

    db_path = get_db_path()
    now_epoch = dt.datetime.now(dt.timezone.utc).timestamp()
    inserted = 0
    skipped_recent = 0
    existing = 0
    failed = 0

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        ensure_schema(conn)

        conn.execute('BEGIN;')
        try:
            for p in sorted(VIDEO_ROOT.iterdir()):
                if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                    continue

                try:
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

                    cur = conn.execute(
                        '''
                        INSERT OR IGNORE INTO vods(path, session_label, duration_s, ingested_at, state)
                        VALUES (?, ?, ?, ?, 'ingested')
                        ''',
                        (str(p), session_label, duration_s, ingested_at),
                    )

                    vod_id_row = conn.execute('SELECT id FROM vods WHERE path = ?', (str(p),)).fetchone()
                    vod_id = vod_id_row[0] if vod_id_row else None

                    if cur.rowcount == 1:
                        inserted += 1
                        print(
                            'INGESTED '
                            f'id={vod_id} '
                            f'path="{p}" '
                            f'session_label="{session_label}" '
                            f'duration_s={duration_s}'
                        )
                    else:
                        existing += 1
                        print(f'ALREADY_EXISTS id={vod_id} path="{p}"')
                except Exception as exc:
                    failed += 1
                    print(f'FILE_FAILED path="{p}" reason="{exc}"')
                    continue

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        rows = conn.execute('SELECT id, path, session_label, duration_s, ingested_at, state FROM vods ORDER BY id').fetchall()
        print(f'VODS_TOTAL {len(rows)}')
        for r in rows:
            print(f'VODS_ROW {r}')

    print(f'RESULT ingest_vods ok=1 inserted={inserted} existing={existing} skipped_recent={skipped_recent} failed={failed} db_path="{db_path}"')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
