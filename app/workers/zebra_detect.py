#!/usr/bin/env python3
"""zebra_detect: scan transcript segments for zebra trigger word and write beats.
Poison gate is enforced first. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sqlite3
import sys

try:
    from poison_gate import require_poison_reviewed_or_raise
except ModuleNotFoundError:
    from .poison_gate import require_poison_reviewed_or_raise

ZEBRA_RE = re.compile(r"\bzebra\b", re.IGNORECASE)


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def fetch_vod_state(conn: sqlite3.Connection, vod_id: int) -> str:
    row = conn.execute('SELECT state FROM vods WHERE id = ?', (vod_id,)).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    return str(row[0])


def state_is_transcribed_or_later(state: str) -> bool:
    return state in {'transcribed', 'detected', 'done'}


def beat_exists(conn: sqlite3.Connection, vod_id: int, offset_s: float) -> bool:
    row = conn.execute(
        '''
        SELECT 1
        FROM beats
        WHERE vod_id = ?
          AND offset_s IS NOT NULL
          AND ABS(offset_s - ?) < 0.0005
        LIMIT 1
        ''',
        (vod_id, offset_s),
    ).fetchone()
    return row is not None


def detect(vod_id: int) -> int:
    db_path = get_db_path()
    triggers_found = 0

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')

        # Poison gate first, non-negotiable.
        require_poison_reviewed_or_raise(conn, vod_id)

        state = fetch_vod_state(conn, vod_id)
        if not state_is_transcribed_or_later(state):
            raise RuntimeError(f'vod_id={vod_id} must be transcribed or later; got state={state}')

        rows = conn.execute(
            '''
            SELECT start_s, text
            FROM transcript_segments
            WHERE vod_id = ?
            ORDER BY start_s, id
            ''',
            (vod_id,),
        ).fetchall()

        conn.execute('BEGIN;')
        try:
            for start_s, text in rows:
                segment_text = str(text or '')
                if not ZEBRA_RE.search(segment_text):
                    continue
                offset_s = float(start_s)
                if beat_exists(conn, vod_id, offset_s):
                    continue
                conn.execute(
                    '''
                    INSERT INTO beats(vod_id, ts_utc, note, offset_s, source)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (vod_id, utc_now_iso(), 'auto-detected trigger word', offset_s, 'zebra_trigger'),
                )
                triggers_found += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(f'RESULT zebra_detect vod={vod_id} triggers_found={triggers_found}')
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Detect zebra trigger word in transcript and add beats')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return detect(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
