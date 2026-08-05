#!/usr/bin/env python3
"""sync_clip_to_drive: copy one rendered clip's local file into a locally-synced
Google Drive Desktop folder (which uploads it automatically), and record that
the copy happened.
Reads CLPR_DB_PATH and exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sqlite3
import sys
from pathlib import Path


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def require_env(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Required env var missing: {name}')
    return value


def sync_clip(candidate_id: int) -> int:
    drive_sync_dir = require_env('CLPR_DRIVE_SYNC_DIR')

    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')

        row = conn.execute(
            'SELECT candidate_id, file_path, state, drive_synced_at FROM clips WHERE candidate_id = ?',
            (candidate_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f'no clips row for candidate_id={candidate_id}; render it first with cut_clip.py')

        _, file_path, state, _drive_synced_at = row

        if state != 'rendered':
            raise RuntimeError(
                f'clip state must be rendered to sync: candidate_id={candidate_id} state={state}'
            )

        src_path = Path(file_path)
        if not src_path.exists():
            raise RuntimeError(f'source file missing on disk: {file_path}')

        dest_path = Path(drive_sync_dir) / src_path.name

        print(f'SYNC_CMD src="{file_path}" dst="{dest_path}"')
        shutil.copy2(src_path, dest_path)

        src_size = src_path.stat().st_size
        dst_size = dest_path.stat().st_size
        if src_size != dst_size:
            raise RuntimeError(f'post-copy size mismatch: src={src_size} dst={dst_size}')

        try:
            conn.execute(
                'UPDATE clips SET drive_synced_at = ?, drive_sync_path = ? WHERE candidate_id = ?',
                (utc_now_iso(), str(dest_path), candidate_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(f'RESULT sync_clip_to_drive candidate={candidate_id} ok=1 dest="{dest_path}" size={dst_size}')
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync one rendered clip's file to the local Drive-synced folder")
    parser.add_argument('--candidate-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return sync_clip(args.candidate_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
