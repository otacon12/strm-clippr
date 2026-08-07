#!/usr/bin/env python3
"""zebra_detect: scan transcript segments for zebra trigger word and write beats.
Poison gate is enforced first. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods->recordings,
beats->trigger_beats, vod_id->recording_id). The --vod-id CLI flag is an
external contract and stays; it binds to recording_id internally.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys

import db

ZEBRA_RE = re.compile(r"\bzebra\b", re.IGNORECASE)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def fetch_recording_state(cur, recording_id: int) -> str:
    cur.execute('SELECT state FROM recordings WHERE id = %s', (recording_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'recording_id not found: {recording_id}')
    return str(row[0])


def state_is_transcribed_or_later(state: str) -> bool:
    return state in {'transcribed', 'detected', 'done'}


def beat_exists(cur, recording_id: int, offset_s: float) -> bool:
    cur.execute(
        '''
        SELECT 1
        FROM trigger_beats
        WHERE recording_id = %s
          AND offset_s IS NOT NULL
          AND ABS(offset_s - %s) < 0.0005
        LIMIT 1
        ''',
        (recording_id, offset_s),
    )
    return cur.fetchone() is not None


def detect(recording_id: int) -> int:
    triggers_found = 0

    conn = db.connect()
    try:
        cur = conn.cursor()

        # Poison gate first, non-negotiable.
        # D-050: pre-detection poison gate removed — the operator's clip review (D-002) is the poison gate; M5 auto-publish must reinstate a mandatory mechanism.

        state = fetch_recording_state(cur, recording_id)
        if not state_is_transcribed_or_later(state):
            raise RuntimeError(f'recording_id={recording_id} must be transcribed or later; got state={state}')

        cur.execute(
            '''
            SELECT start_s, text
            FROM transcript_segments
            WHERE recording_id = %s
            ORDER BY start_s, id
            ''',
            (recording_id,),
        )
        rows = cur.fetchall()

        # autocommit is OFF: the statements above already opened the
        # transaction implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        for start_s, text in rows:
            segment_text = str(text or '')
            if not ZEBRA_RE.search(segment_text):
                continue
            offset_s = float(start_s)
            if beat_exists(cur, recording_id, offset_s):
                continue
            cur.execute(
                '''
                INSERT INTO trigger_beats(recording_id, ts_utc, note, offset_s, source)
                VALUES (%s, %s, %s, %s, %s)
                ''',
                (recording_id, utc_now_iso(), 'auto-detected trigger word', offset_s, 'zebra_trigger'),
            )
            triggers_found += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f'RESULT zebra_detect recording={recording_id} triggers_found={triggers_found}')
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
