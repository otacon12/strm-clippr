#!/usr/bin/env python3
"""chat_ingest: map captured live chat rows into vod-relative offsets for one VOD.
Reads CLPR_DB_PATH. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def parse_obs_filename_start_utc(path_value: str) -> dt.datetime:
    name = Path(path_value).name
    stem = Path(name).stem
    try:
        base = dt.datetime.strptime(stem, '%Y-%m-%d %H-%M-%S')
    except ValueError as exc:
        raise RuntimeError(
            f'vod filename does not match expected OBS pattern YYYY-MM-DD HH-MM-SS.ext: {name}'
        ) from exc
    return base.replace(tzinfo=dt.timezone.utc)


def parse_iso_utc(s: str) -> dt.datetime:
    norm = s.strip()
    if norm.endswith('Z'):
        norm = norm[:-1] + '+00:00'
    parsed = dt.datetime.fromisoformat(norm)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fetch_vod(conn: sqlite3.Connection, vod_id: int) -> tuple[str, str, float]:
    row = conn.execute(
        'SELECT path, session_label, duration_s FROM vods WHERE id = ?',
        (vod_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    path, session_label, duration_s = row
    if duration_s is None:
        raise RuntimeError(f'vod_id has null duration_s: {vod_id}')
    return str(path), str(session_label), float(duration_s)


def ingest(vod_id: int) -> int:
    db_path = get_db_path()
    rows_inserted = 0
    rows_skipped_duplicate = 0

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        vod_path, session_label, duration_s = fetch_vod(conn, vod_id)
        vod_start = parse_obs_filename_start_utc(vod_path)
        vod_end = vod_start + dt.timedelta(seconds=duration_s)

        raw_rows = conn.execute(
            '''
            SELECT id, ts_utc, author, text
            FROM chat_raw
            WHERE session_label = ?
            ORDER BY id
            ''',
            (session_label,),
        ).fetchall()

        conn.execute('BEGIN;')
        try:
            for raw_id, ts_utc, author, text in raw_rows:
                chat_ts = parse_iso_utc(str(ts_utc))
                if chat_ts < vod_start or chat_ts > vod_end:
                    continue

                offset_s = (chat_ts - vod_start).total_seconds()
                cur = conn.execute(
                    '''
                    INSERT OR IGNORE INTO chat_messages(vod_id, offset_s, author, text)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (vod_id, offset_s, author, text),
                )
                if cur.rowcount == 1:
                    rows_inserted += 1
                else:
                    rows_skipped_duplicate += 1

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(f'RESULT chat_ingest vod={vod_id} rows_inserted={rows_inserted} rows_skipped_duplicate={rows_skipped_duplicate}')
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest chat_raw rows into chat_messages for a single VOD')
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
