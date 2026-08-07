#!/usr/bin/env python3
"""chat_ingest: map captured live chat rows into recording-relative offsets for one recording.
Reads CLPR_DB_URL. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods -> recordings,
vod_id -> recording_id). The --vod-id CLI flag is an external contract and
stays; it binds to recording_id internally.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import db


def parse_obs_filename_start_utc(path_value: str) -> dt.datetime:
    name = Path(path_value).name
    stem = Path(name).stem
    try:
        base_local_naive = dt.datetime.strptime(stem, '%Y-%m-%d %H-%M-%S')
    except ValueError as exc:
        raise RuntimeError(
            f'recording filename does not match expected OBS pattern YYYY-MM-DD HH-MM-SS.ext: {name}'
        ) from exc

    try:
        pacific = ZoneInfo('America/Los_Angeles')
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError('zoneinfo lookup failed for America/Los_Angeles') from exc

    local_dt = base_local_naive.replace(tzinfo=pacific)
    return local_dt.astimezone(dt.timezone.utc)


def parse_iso_utc(s: str) -> dt.datetime:
    norm = s.strip()
    if norm.endswith('Z'):
        norm = norm[:-1] + '+00:00'
    parsed = dt.datetime.fromisoformat(norm)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fetch_recording(cur, recording_id: int) -> tuple[str, str, float]:
    cur.execute(
        'SELECT path, session_label, duration_s FROM recordings WHERE id = %s',
        (recording_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'recording_id not found: {recording_id}')
    path, session_label, duration_s = row
    if duration_s is None:
        raise RuntimeError(f'recording_id has null duration_s: {recording_id}')
    return str(path), str(session_label), float(duration_s)


def ingest(recording_id: int) -> int:
    rows_inserted = 0
    rows_skipped_duplicate = 0

    conn = db.connect()
    try:
        cur = conn.cursor()
        recording_path, session_label, duration_s = fetch_recording(cur, recording_id)
        recording_start = parse_obs_filename_start_utc(recording_path)
        recording_end = recording_start + dt.timedelta(seconds=duration_s)

        cur.execute(
            '''
            SELECT id, ts_utc, author, text
            FROM chat_raw
            WHERE session_label = %s
            ORDER BY id
            ''',
            (session_label,),
        )
        raw_rows = cur.fetchall()

        # autocommit is OFF: the statements above already opened the
        # transaction implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        for raw_id, ts_utc, author, text in raw_rows:
            chat_ts = parse_iso_utc(str(ts_utc))
            if chat_ts < recording_start or chat_ts > recording_end:
                continue

            offset_s = (chat_ts - recording_start).total_seconds()
            cur.execute(
                '''
                INSERT INTO chat_messages(recording_id, offset_s, author, text)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (recording_id, offset_s, author, text) DO NOTHING
                ''',
                (recording_id, offset_s, author, text),
            )
            if cur.rowcount == 1:
                rows_inserted += 1
            else:
                rows_skipped_duplicate += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f'RESULT chat_ingest recording={recording_id} rows_inserted={rows_inserted} rows_skipped_duplicate={rows_skipped_duplicate}')
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest chat_raw rows into chat_messages for a single recording')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return ingest(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
