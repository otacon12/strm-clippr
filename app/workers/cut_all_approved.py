#!/usr/bin/env python3
"""cut_all_approved: render approved candidates in batch via cut_clip.render_clip.
Reads CLPR_DB_PATH and exits non-zero only for wrapper-level failures.
Per-candidate render failures are isolated and reported; batch continues.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys

try:
    from cut_clip import render_clip
except ModuleNotFoundError:
    from .cut_clip import render_clip


def get_db_path() -> str:
    import os

    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def fetch_approved_with_clip_presence(conn: sqlite3.Connection) -> list[tuple[int, int]]:
    rows = conn.execute(
        '''
        SELECT c.id, CASE WHEN cl.id IS NULL THEN 0 ELSE 1 END AS has_clip
        FROM candidates c
        LEFT JOIN clips cl ON cl.candidate_id = c.id
        WHERE c.state = 'approved'
        ORDER BY c.id
        '''
    ).fetchall()
    return [(int(r[0]), int(r[1])) for r in rows]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Batch render approved candidates via cut_clip.render_clip')
    parser.add_argument('--force', action='store_true', help='re-render all approved candidates, including already-rendered')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = get_db_path()

    processed = 0
    succeeded = 0
    failed = 0
    skipped_already_rendered = 0

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        rows = fetch_approved_with_clip_presence(conn)

    for candidate_id, has_clip in rows:
        processed += 1

        if has_clip and not args.force:
            skipped_already_rendered += 1
            print(f'SKIP_ALREADY_RENDERED candidate={candidate_id}')
            continue

        try:
            render_clip(candidate_id)
            succeeded += 1
        except Exception as exc:
            failed += 1
            print(f'CANDIDATE_FAILED candidate={candidate_id} error="{exc}"')
            continue

    print(
        'RESULT cut_all_approved '
        f'processed={processed} '
        f'succeeded={succeeded} '
        f'failed={failed} '
        f'skipped_already_rendered={skipped_already_rendered}'
    )
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
