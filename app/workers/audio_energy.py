#!/usr/bin/env python3
"""audio_energy: extract 5s loudness buckets for one VOD using ffmpeg ebur128.
Reads CLPR_DB_PATH by env var name. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sqlite3
import subprocess
import sys
import time

try:
    from poison_gate import require_poison_reviewed_or_raise
except ModuleNotFoundError:
    from .poison_gate import require_poison_reviewed_or_raise

try:
    from obs_guard import require_obs_idle_or_raise
except ModuleNotFoundError:
    from .obs_guard import require_obs_idle_or_raise

LOUDNESS_RE = re.compile(r't:\s*([\d.]+).*?\bM:\s*(-?[\d.]+|-inf)')
BUCKET_SECONDS = 5.0


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def fetch_vod_path(conn: sqlite3.Connection, vod_id: int) -> str:
    row = conn.execute('SELECT path FROM vods WHERE id = ?', (vod_id,)).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    return row[0]


def parse_momentary_loudness(stderr_text: str) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for line in stderr_text.splitlines():
        m = LOUDNESS_RE.search(line)
        if not m:
            continue
        t_s = float(m.group(1))
        m_raw = m.group(2)
        loudness = -70.0 if m_raw == '-inf' else float(m_raw)
        out.append((t_s, loudness))
    return out


def bucketize_max(readings: list[tuple[float, float]]) -> list[tuple[float, float, float]]:
    bucket_max: dict[float, float] = {}
    for t_s, loudness in readings:
        start_s = math.floor(t_s / BUCKET_SECONDS) * BUCKET_SECONDS
        prev = bucket_max.get(start_s)
        if prev is None or loudness > prev:
            bucket_max[start_s] = loudness

    rows = []
    for start_s in sorted(bucket_max.keys()):
        end_s = start_s + BUCKET_SECONDS
        rows.append((start_s, end_s, bucket_max[start_s]))
    return rows


def audio_energy(vod_id: int) -> int:
    require_obs_idle_or_raise('audio_energy')

    db_path = get_db_path()
    started = time.monotonic()

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        require_poison_reviewed_or_raise(conn, vod_id)
        vod_path = fetch_vod_path(conn, vod_id)

        cmd = ['ffmpeg', '-i', vod_path, '-af', 'ebur128', '-f', 'null', '-']
        print(f'CMD {cmd}')
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as exc:
            stdout = (exc.stdout or '').rstrip('\n')
            stderr = (exc.stderr or '').rstrip('\n')
            raise RuntimeError(
                f'ffmpeg failed exit={exc.returncode} cmd={cmd} stdout="{stdout}" stderr="{stderr}"'
            ) from exc

        readings = parse_momentary_loudness(proc.stderr or '')
        if not readings:
            raise RuntimeError('no ebur128 readings parsed from ffmpeg output')

        buckets = bucketize_max(readings)
        if not buckets:
            raise RuntimeError('no 5-second buckets produced from parsed loudness readings')

        conn.execute('BEGIN;')
        try:
            conn.execute('DELETE FROM audio_energy WHERE vod_id = ?', (vod_id,))
            conn.executemany(
                '''
                INSERT INTO audio_energy(vod_id, start_s, end_s, loudness_lufs)
                VALUES (?, ?, ?, ?)
                ''',
                [(vod_id, start_s, end_s, loudness) for start_s, end_s, loudness in buckets],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    elapsed_s = time.monotonic() - started
    print(
        f'RESULT audio_energy vod_id={vod_id} ok=1 buckets={len(buckets)} '
        f'elapsed_s={elapsed_s:.3f} vod_path="{vod_path}"'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extract 5s audio-energy buckets for one VOD using ffmpeg ebur128')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return audio_energy(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
