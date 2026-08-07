#!/usr/bin/env python3
"""render_from_slice: render one approved candidate's PRE-STAGED slice to the
vertical 1080x1920 full-frame-fit-with-blur MP4 (D-053 workflow B, verdict router).

The input is the surgical slice Workflow A staged at $CLPR_SLICES_DIR/c<id>.mp4
(stream-copied SLICE_PAD_S-padded window around the IMMUTABLE ORIGINAL
start_s/end_s — slice_geometry, D-055). The render therefore ALWAYS TRIMS:
a 10s-padded slice rendered whole would ship a ~20s-too-long clip. The cut
window is the EFFECTIVE window (COALESCE(adjusted_*, original)) +/-
PUBLISH_PAD_S.

WITNESSED GEOMETRY (D-055 fixer): the slice's absolute coordinates come from
the REQUIRED sidecar c<id>.json that slice_candidates writes at staging time
— never re-derived from the formula, because the actual file can disobey it
(a -c copy head snaps to the keyframe BEFORE the requested start so real
media starts EARLIER than nominal; a staging-time video-end clamp shortens
it; a stale pre-D-055 slice under the same name has different geometry
entirely). A missing/unparseable/mismatched sidecar fails loudly with
SLICE_SIDECAR_MISSING (distinct from SLICE_MISSING): re-run slice_candidates
to restage, or use workers/deliver_approved.py on the Mac. A sidecar whose
recorded length disagrees with the actual file (ffprobe, > 0.35 s) fails as
STALE_SLICE. Containment: the target cut must sit inside
[abs_start_s, abs_end_s] (+/- 0.25 s tolerance) or the render fails loudly
with SLICE_WINDOW_EXCEEDED — EXCEPT clamping that only mirrors the video's
own edges (the ORIGINAL window's padded cut itself starts before abs_start_s
/ ends beyond abs_end_s), which is exactly what cut_clip.py does on the full
recording, so an unedited candidate near the video's edges always renders
while an operator EDIT beyond the slice's media is always the exceed error.
The trim is implemented as ffmpeg INPUT options (-ss/-t before -i); the
filter_complex and every encode setting are copied byte-for-byte from
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
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import db
import slice_geometry

# Containment tolerance (seconds): absorbs container-duration rounding; this
# check is a safety net against edits the slice cannot serve, not frame-exact.
CONTAIN_TOL_S = 0.25

# Belt-check tolerance (seconds): the sidecar's recorded slice length vs the
# actual file's ffprobe'd length. Any larger disagreement means the file under
# c<id>.mp4 is not the file the sidecar witnessed => STALE_SLICE.
STALE_TOL_S = 0.35

SIDECAR_SCHEMA = 1  # must match slice_candidates.SIDECAR_SCHEMA


def load_sidecar(sidecar_path: Path, candidate_id: int) -> dict:
    """Load + validate the REQUIRED slice geometry sidecar (D-055 fixer).

    No formula fallback exists on purpose: no legacy slices exist anywhere
    live (verified 2026-08-06: the server slices dir does not exist yet), so
    any slice without a valid sidecar is unwitnessed geometry and must be
    restaged, never guessed at.
    """
    fail_hint = (
        're-run slice_candidates to restage, or use workers/deliver_approved.py '
        'on the Mac.'
    )
    if not sidecar_path.exists():
        raise RuntimeError(
            f'SLICE_SIDECAR_MISSING: no geometry sidecar at {sidecar_path} for '
            f'candidate_id={candidate_id}; the slice geometry is unwitnessed. {fail_hint}'
        )
    try:
        data = json.loads(sidecar_path.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError(
            f'SLICE_SIDECAR_MISSING: unparseable geometry sidecar at {sidecar_path} '
            f'for candidate_id={candidate_id} ({exc!r}). {fail_hint}'
        ) from exc
    if not isinstance(data, dict) or data.get('schema') != SIDECAR_SCHEMA:
        raise RuntimeError(
            f'SLICE_SIDECAR_MISSING: sidecar at {sidecar_path} has wrong shape/schema '
            f'(expected schema={SIDECAR_SCHEMA}) for candidate_id={candidate_id}. {fail_hint}'
        )
    if data.get('candidate_id') != candidate_id:
        raise RuntimeError(
            f'SLICE_SIDECAR_MISSING: sidecar candidate_id mismatch at {sidecar_path} '
            f'(sidecar says {data.get("candidate_id")!r}, expected {candidate_id}). {fail_hint}'
        )
    for key in ('abs_start_s', 'abs_end_s', 'actual_duration_s'):
        v = data.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v):
            raise RuntimeError(
                f'SLICE_SIDECAR_MISSING: sidecar at {sidecar_path} has non-finite/missing '
                f'{key} for candidate_id={candidate_id}. {fail_hint}'
            )
    return data


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
        SELECT c.recording_id, c.start_s, c.end_s,
               c.adjusted_start_s, c.adjusted_end_s,
               c.state, r.session_label,
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
        'end_s': float(row[2]),
        'adjusted_start_s': float(row[3]) if row[3] is not None else None,
        'adjusted_end_s': float(row[4]) if row[4] is not None else None,
        'state': str(row[5]),
        'session_label': str(row[6]),
        'category': str(row[7]) if row[7] is not None else 'unknown',
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

        # ---- D-055 geometry (WITNESSED): the target cut inside the slice -----
        # The slice's absolute coordinates come from the sidecar the stager
        # wrote from the ACTUAL produced file — never from the formula, which
        # the real file may not obey (keyframe snap, staging clamp, stale
        # pre-D-055 slice under the same name).
        sidecar = load_sidecar(slice_path.with_suffix('.json'), candidate_id)
        abs_start_s = float(sidecar['abs_start_s'])
        abs_end_s = float(sidecar['abs_end_s'])

        # Belt check: the file under c<id>.mp4 must BE the file the sidecar
        # witnessed. abs_start_s was anchored as abs_end_s - actual_duration at
        # staging time, so any larger disagreement means the bytes changed.
        actual_slice_len_s = measure_duration_s(slice_path)
        if abs(actual_slice_len_s - (abs_end_s - abs_start_s)) > STALE_TOL_S:
            raise RuntimeError(
                f'STALE_SLICE: candidate_id={candidate_id} slice {slice_path} '
                f'actual length {actual_slice_len_s:.3f}s disagrees with its sidecar '
                f'({abs_end_s - abs_start_s:.3f}s = abs_end_s - abs_start_s) by more '
                f'than {STALE_TOL_S}s — the file is not the one the sidecar witnessed. '
                're-run slice_candidates to restage, or use workers/deliver_approved.py '
                'on the Mac.'
            )

        eff_start_s, eff_end_s = slice_geometry.effective_window(
            cand['start_s'], cand['end_s'],
            cand['adjusted_start_s'], cand['adjusted_end_s'],
        )
        pad = slice_geometry.PUBLISH_PAD_S

        # Target cut (shipped-clip invariant: effective window +/- PUBLISH_PAD_S)
        # in ABSOLUTE video coordinates, BEFORE clamping.
        target_start_abs_s = eff_start_s - pad
        target_end_abs_s = eff_end_s + pad

        # A clamp at the slice's edge is legal ONLY when the ORIGINAL window's
        # padded cut itself crosses that edge — i.e. the shortfall mirrors the
        # VIDEO's own edge (t=0 floor at staging / staging-time video-end
        # clamp), which is exactly what cut_clip.py does on the full recording.
        # Geometry note: with SLICE_PAD_S (10) >> PUBLISH_PAD_S (1.5) the
        # original padded cut can only cross abs_start_s when the staging
        # formula floored at t=0, and only cross abs_end_s when staging clamped
        # to the video's end — so these conditions ARE the video-edge tests.
        # An operator EDIT that reaches past media the video itself had (e.g.
        # eff_start - pad < 0 with the original also near t=0) clamps the same
        # way the Mac fallback would, so erroring there would buy nothing.
        start_clamp_legal = (cand['start_s'] - pad) < (abs_start_s + CONTAIN_TOL_S)
        end_clamp_legal = (cand['end_s'] + pad) > (abs_end_s - CONTAIN_TOL_S)

        # CONTAINMENT: exceed = the cut needs media the FULL VIDEO has but the
        # slice does not. An EDIT beyond the slice's media is always the exceed
        # error; an unedited candidate near the video's edges always renders.
        exceeds_start = (
            target_start_abs_s < abs_start_s - CONTAIN_TOL_S and not start_clamp_legal
        )
        exceeds_end = (
            target_end_abs_s > abs_end_s + CONTAIN_TOL_S and not end_clamp_legal
        )
        if exceeds_start or exceeds_end:
            raise RuntimeError(
                f'SLICE_WINDOW_EXCEEDED: candidate_id={candidate_id} target cut '
                f'[{target_start_abs_s:.3f}..{target_end_abs_s:.3f}]s (absolute) is not '
                f'contained in the staged slice {slice_path} '
                f'(slice covers [{abs_start_s:.3f}..{abs_end_s:.3f}]s, '
                f'actual_len={actual_slice_len_s:.3f}s). The adjusted window needs '
                'media the slice does not contain. Fallback: run the Mac-side batch '
                'deliverer (python3 workers/deliver_approved.py), which renders from '
                'the full recording on a machine that has it.'
            )

        # In-slice offsets against the WITNESSED absolute start; clamp to what
        # the slice actually holds (the same edge behavior cut_clip.py ships
        # today for windows near t=0 / the video's end).
        offset_s = max(0.0, target_start_abs_s - abs_start_s)
        end_in_slice_s = min(target_end_abs_s - abs_start_s, actual_slice_len_s)
        cut_duration_s = end_in_slice_s - offset_s
        if cut_duration_s <= 0:
            raise RuntimeError(
                f'invalid cut window after clamp: candidate_id={candidate_id} '
                f'offset_s={offset_s:.3f} end_in_slice_s={end_in_slice_s:.3f}'
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

        # D-055: ALWAYS trim — the slice carries SLICE_PAD_S headroom, so
        # rendering it whole would ship a ~20s-too-long clip. The trim is
        # INPUT options (-ss/-t before -i); filter/encode flags stay
        # byte-identical to cut_clip.py.
        ffmpeg_cmd = [
            'ffmpeg',
            '-y',
            '-ss', f'{offset_s:.3f}',
            '-t', f'{cut_duration_s:.3f}',
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
