#!/usr/bin/env python3
"""render_from_slice: render one approved candidate's PRE-STAGED slice to the
vertical 1080x1920 full-frame-fit-with-blur MP4 (D-053 workflow B, verdict router).

The input is the surgical slice Workflow A staged at $CLPR_SLICES_DIR/c<id>.mp4
(stream-copied padded window, so NO -ss/-t here: the slice IS the cut window).
The filter_complex and every encode setting are copied byte-for-byte from
cut_clip.py — those settings are operator-proven on a live Instagram post
(D-023); change nothing.

Output: $CLPR_RENDER_OUT (default /home/node/.n8n-files, the n8n file-node
allow-list dir, D-044) / <session_label>_c<candidate_id>_<category>.mp4 —
the descriptive delivery name (deliver_approved.py naming ruling, 2026-08-06).

Deliberately NO obs_guard: this worker runs server-side in the n8n container
(no OBS, no encoder to protect) and its input is a seconds-long slice, not a
multi-hour VOD. D-009's gate protects the streaming Macs, not this box.

Fail-loud contract (D-047: a failing child's stdout is discarded by n8n, so
ERROR goes to stderr): missing CLPR_SLICES_DIR, missing slice file (distinct
error naming deliver_approved.py as the Mac-side fallback), unknown/unapproved
candidate. A failed run writes nothing (charter gate 9): the partial output
file is unlinked and the clips upsert never commits.

Connects via the shared adapter app/workers/db.py (CLPR_DB_URL). Prints
machine-parseable RESULT line last, with the real ffprobe'd duration.

PostgreSQL-native (D-052 P3): tables and columns per app/docs/naming-map.md.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import db


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def require_env(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Required env var missing: {name}')
    return value


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


def fetch_candidate(cur, candidate_id: int) -> dict:
    """Candidate + recording session_label + llm_signal category ('unknown'
    when absent) — the category lookup idiom from deliver_approved.py."""
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = 'llm_signal_candidates'"
    )
    has_llm_signal = cur.fetchone() is not None

    category_select = (
        '(SELECT t.category FROM llm_signal_candidates t '
        ' WHERE t.recording_id = c.recording_id AND t.start_s = c.start_s '
        ' ORDER BY t.id LIMIT 1)'
        if has_llm_signal else 'NULL'
    )

    cur.execute(
        f'''
        SELECT c.recording_id, c.start_s, c.state, r.session_label,
               {category_select} AS category
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        WHERE c.id = %s
        ''',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'candidate_id not found: {candidate_id}')

    return {
        'recording_id': int(row[0]),
        'start_s': float(row[1]),
        'state': str(row[2]),
        'session_label': str(row[3]),
        'category': str(row[4]) if row[4] is not None else 'unknown',
    }


def render_from_slice(candidate_id: int) -> int:
    run_id = f'render_from_slice_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    slices_dir = require_env('CLPR_SLICES_DIR')
    out_dir = Path(os.environ.get('CLPR_RENDER_OUT', '/home/node/.n8n-files').strip()
                   or '/home/node/.n8n-files')

    conn = db.connect()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'clips'"
        )
        if cur.fetchone() is None:
            raise RuntimeError('clips table missing; apply migrations_pg/001 before render_from_slice')

        cand = fetch_candidate(cur, candidate_id)

        if cand['state'] != 'approved':
            raise RuntimeError(
                f'candidate must be approved before render: candidate_id={candidate_id} '
                f'state={cand["state"]}'
            )

        slice_path = Path(slices_dir) / f'c{candidate_id}.mp4'
        if not slice_path.exists():
            raise RuntimeError(
                f'SLICE_MISSING: no staged slice at {slice_path} for candidate_id={candidate_id}. '
                'Workflow A stages slices only for VODs it analyzed with the video present in the '
                'Drive archive; this candidate has none on this machine. Fallback: run the Mac-side '
                'batch deliverer (python3 workers/deliver_approved.py), which renders from the full '
                'recording on a machine that has it.'
            )

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f'{cand["session_label"]}_c{candidate_id}_{cand["category"]}.mp4'

        if out_path.exists():
            out_path.unlink()

        # Copied EXACTLY from cut_clip.py (operator-proven live on Instagram, D-023).
        filter_complex = (
            '[0:v]split=2[bg][fg];'
            '[bg]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,gblur=sigma=20[bg2];'
            '[fg]scale=1080:-2:flags=lanczos[fg2];'
            '[bg2][fg2]overlay=(W-w)/2:(H-h)/2,fps=30[v]'
        )

        # No -ss/-t: the slice already IS the padded cut window (D-053).
        ffmpeg_cmd = [
            'ffmpeg',
            '-y',
            '-i', str(slice_path),
            '-filter_complex', filter_complex,
            '-map', '[v]',
            '-map', '0:a?',
            '-c:v', 'libx264',
            '-preset', 'slow',
            '-crf', '18',
            '-profile:v', 'high',
            '-pix_fmt', 'yuv420p',
            '-r', '30',
            '-c:a', 'aac',
            '-b:a', '192k',
            '-movflags', '+faststart',
            str(out_path),
        ]

        try:
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            duration_s = measure_duration_s(out_path)

            cur.execute(
                '''
                INSERT INTO clips(candidate_id, file_path, duration_s, state, created_by_run, created_at)
                VALUES (%s, %s, %s, 'rendered', %s, %s)
                ON CONFLICT (candidate_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    duration_s = EXCLUDED.duration_s,
                    state = 'rendered',
                    created_by_run = EXCLUDED.created_by_run,
                    created_at = EXCLUDED.created_at
                ''',
                (candidate_id, str(out_path), duration_s, run_id, utc_now_iso()),
            )
            conn.commit()

            print(
                f'RESULT render_from_slice candidate={candidate_id} ok=1 '
                f'file="{out_path}" duration_s={duration_s:.3f}'
            )
            return 0
        except Exception:
            if out_path.exists():
                out_path.unlink()
            raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Render one approved candidate to 9:16 vertical MP4 from its pre-staged slice (D-053)'
    )
    parser.add_argument('--candidate-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return render_from_slice(args.candidate_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
