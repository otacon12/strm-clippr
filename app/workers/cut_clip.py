#!/usr/bin/env python3
"""cut_clip: render one approved candidate to vertical 1080x1920 MP4.
Reads CLPR_DB_PATH and exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

PAD_SECONDS = 1.5  # small default padding so cuts do not feel abrupt; tunable later.


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def run_capture(cmd: list[str]) -> subprocess.CompletedProcess:
    print(f'CMD {cmd}')
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or '').rstrip('\n')
        stderr = (exc.stderr or '').rstrip('\n')
        raise RuntimeError(
            f'command failed exit={exc.returncode} cmd={cmd} stdout="{stdout}" stderr="{stderr}"'
        ) from exc


def measure_duration_s(path: Path) -> float:
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(path),
    ]
    proc = run_capture(cmd)
    raw = proc.stdout.strip()
    if raw == '':
        raise RuntimeError(f'ffprobe returned empty duration output for {path}')
    print(f'FFPROBE_RAW path="{path}" output="{raw}"')
    return float(raw)


def fetch_candidate(conn: sqlite3.Connection, candidate_id: int) -> tuple[int, float, float, str, str, float]:
    row = conn.execute(
        '''
        SELECT c.vod_id, c.start_s, c.end_s, c.state, v.path, v.duration_s
        FROM candidates c
        JOIN vods v ON v.id = c.vod_id
        WHERE c.id = ?
        ''',
        (candidate_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f'candidate_id not found: {candidate_id}')

    vod_id, start_s, end_s, state, vod_path, vod_duration_s = row
    if vod_duration_s is None:
        raise RuntimeError(f'vod duration_s is NULL for candidate_id={candidate_id}')

    return int(vod_id), float(start_s), float(end_s), str(state), str(vod_path), float(vod_duration_s)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def render_clip(candidate_id: int) -> int:
    db_path = get_db_path()
    run_id = f'cut_clip_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')

        clips_table = conn.execute("select count(*) from sqlite_master where type='table' and name='clips'").fetchone()[0]
        if clips_table != 1:
            raise RuntimeError('clips table missing; run apply_migrations() before cut_clip')

        vod_id, start_s, end_s, state, vod_path, vod_duration_s = fetch_candidate(conn, candidate_id)

        if state != 'approved':
            raise RuntimeError(
                f'candidate must be approved before cut: candidate_id={candidate_id} state={state}'
            )

        cut_start_s = clamp(start_s - PAD_SECONDS, 0.0, vod_duration_s)
        cut_end_s = clamp(end_s + PAD_SECONDS, 0.0, vod_duration_s)
        if cut_end_s <= cut_start_s:
            raise RuntimeError(
                f'invalid cut window after padding/clamp: candidate_id={candidate_id} '
                f'cut_start_s={cut_start_s} cut_end_s={cut_end_s}'
            )
        cut_duration_s = cut_end_s - cut_start_s

        out_dir = Path(__file__).resolve().parent.parent / 'clips_out'
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f'{vod_id}_{candidate_id}.mp4'

        if out_path.exists():
            out_path.unlink()

        ffmpeg_cmd = [
            'ffmpeg',
            '-y',
            '-ss', f'{cut_start_s:.3f}',
            '-i', vod_path,
            '-t', f'{cut_duration_s:.3f}',
            '-vf', 'crop=608:1080:(in_w-608)/2:0,scale=1080:1920:flags=lanczos,fps=30',
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-r', '30',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-movflags', '+faststart',
            str(out_path),
        ]

        try:
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            duration_s = measure_duration_s(out_path)

            try:
                conn.execute(
                    '''
                    INSERT INTO clips(candidate_id, file_path, duration_s, state, created_by_run, created_at)
                    VALUES (?, ?, ?, 'rendered', ?, ?)
                    ON CONFLICT(candidate_id) DO UPDATE SET
                        file_path=excluded.file_path,
                        duration_s=excluded.duration_s,
                        state='rendered',
                        created_by_run=excluded.created_by_run,
                        created_at=excluded.created_at
                    ''',
                    (candidate_id, str(out_path), duration_s, run_id, utc_now_iso()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            print(f'RESULT cut_clip candidate={candidate_id} ok=1 file="{out_path}" duration_s={duration_s:.3f}')
            return 0
        except Exception:
            if out_path.exists():
                out_path.unlink()
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Render one approved candidate to 9:16 vertical MP4 clip')
    parser.add_argument('--candidate-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return render_clip(args.candidate_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
