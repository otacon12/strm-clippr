#!/usr/bin/env python3
"""slice_candidates: stream-copy per-candidate slices from the archive video (D-053 workflow A tail).

For every clip_candidates row of one recording in state candidate/maybe/approved
(never rejected/poisoned) that does not already have a slice file, cut a padded
window from the archive video with `ffmpeg -c copy` (no re-encode) into
$CLPR_SLICES_DIR/c<candidate_id>.mp4.

D-055 geometry: staging bounds come from slice_geometry (the one-truth module)
with SLICE_PAD_S (10 s) headroom around the IMMUTABLE ORIGINAL window
(start_s/end_s). Slices deliberately IGNORE the adjusted_start_s/adjusted_end_s
columns: the original window is the fixed basis, so an operator edit before OR
after staging never invalidates a staged slice.

WITNESSED GEOMETRY (D-055 fixer): every staged slice gets a SIDECAR
c<id>.json next to it recording the slice's REAL absolute coordinates —
the renderer reads the sidecar, never re-derives from the formula, because
the actual file can disobey the formula (a keyframe-snapped -c copy head,
a staging-time video-end clamp, or a stale pre-D-055 slice under the same
name). abs_end from a -to copy cut is near-exact while the head snaps to
the keyframe BEFORE the requested start, so the sidecar anchors
abs_start_s = abs_end_s - actual_duration_s, absorbing the snap.

SOURCE WITNESS (schema 2, SRD-06 / golden-review F9 fixer): the sidecar also
records source_path, source_size_bytes and source_duration_s — the SOURCE
VIDEO's identity at cut time. Geometry alone cannot detect a slice cut from
the WRONG video: a geometry-valid, intact slice reused after recordings.path
was repointed to a different file (this happened live: recording 19
repointed to a local Drive-streamed mp4, commit 36c6e91), or after a
candidate id was recycled by --reset-ids across wipe generations, is
invisible to render_from_slice's STALE_SLICE ffprobe check, because that
check compares length against a window the stager itself anchored FROM THE
SAME (wrong) file. The schema is now the ONE TRUTH in slice_geometry.py
(SIDECAR_SCHEMA), imported by both this stager and render_from_slice —
previously each carried its own hardcoded copy, synced only by comment.

Idempotent: slice + valid sidecar whose SOURCE WITNESS matches the current
video (source_path + source_size_bytes) => skip; anything else is RESTAGED
(both files regenerated) — a slice without a valid sidecar heals stale
pre-D-055 slices staged under the same c<id>.mp4 name with different
geometry, and a slice whose sidecar witnesses a DIFFERENT source file heals
a stale slice cut from the wrong video. Both files are written via a .part
temp + atomic os.replace, sidecar LAST (it is the commit marker: a crash
between mp4 and sidecar leaves slice-without-sidecar, which the restage rule
self-heals).

Connects via the shared adapter app/workers/db.py (CLPR_DB_URL). CLPR_SLICES_DIR
is REQUIRED (fail loudly when unset); the n8n node exports it, local tests set a
temp dir. Per-candidate failure isolation: one bad candidate never aborts the
rest. Prints machine-parseable RESULT line last; on nonzero exit the RESULT and
ERROR lines also go to stderr (D-047: n8n discards a failing child's stdout).
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

SLICE_STATES = ('candidate', 'maybe', 'approved')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def measure_duration_s(path: Path) -> float:
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f'ffprobe failed exit={proc.returncode} path={path} stderr="{(proc.stderr or "").strip()}"'
        )
    raw = proc.stdout.strip()
    if raw == '':
        raise RuntimeError(f'ffprobe returned empty duration output for {path}')
    return float(raw)


def slices_dir_from_env() -> Path:
    raw = os.environ.get('CLPR_SLICES_DIR', '').strip()
    if not raw:
        raise RuntimeError(
            'CLPR_SLICES_DIR is not set (required: the slices output directory; '
            'the n8n node exports it, local tests set a temp dir)'
        )
    return Path(raw)


def sidecar_path_for(out_path: Path) -> Path:
    """c<id>.mp4 -> c<id>.json (the slice's geometry witness, D-055 fixer)."""
    return out_path.with_suffix('.json')


def load_valid_sidecar(sidecar_path: Path, candidate_id: int) -> dict | None:
    """Parse + validate an existing sidecar. None on ANY defect (missing,
    unparseable, wrong schema, wrong candidate_id, non-finite coordinates,
    missing/invalid source witness) — the caller treats None as 'restage
    both files'. NOTE: a VALID sidecar's source fields may still not match
    the CURRENT video; that is a separate check the caller makes (source
    identity, not sidecar validity — see the skip rule in run())."""
    try:
        raw = sidecar_path.read_text(encoding='utf-8')
        data = json.loads(raw)
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get('schema') != slice_geometry.SIDECAR_SCHEMA:
        return None
    if data.get('candidate_id') != candidate_id:
        return None
    for key in ('abs_start_s', 'abs_end_s', 'actual_duration_s', 'source_duration_s'):
        v = data.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v):
            return None
    source_path = data.get('source_path')
    if not isinstance(source_path, str) or not source_path:
        return None
    source_size_bytes = data.get('source_size_bytes')
    if not isinstance(source_size_bytes, int) or isinstance(source_size_bytes, bool) or source_size_bytes < 0:
        return None
    return data


def write_sidecar_atomic(sidecar_path: Path, payload: dict) -> None:
    part = sidecar_path.with_name(sidecar_path.name + '.part')
    if part.exists():
        part.unlink()
    part.write_text(json.dumps(payload, ensure_ascii=False) + '\n', encoding='utf-8')
    os.replace(part, sidecar_path)


def slice_one(video: Path, video_duration_s: float, video_size_bytes: int, candidate_id: int,
              start_s: float, end_s: float, out_path: Path) -> None:
    # D-055: staging bounds from the ORIGINAL window only, SLICE_PAD_S headroom,
    # end clamped to the video duration here at staging time (slice_geometry).
    cut_start_s = slice_geometry.slice_start(start_s)
    cut_end_s = slice_geometry.slice_end(end_s, video_duration_s)
    if cut_end_s <= cut_start_s:
        raise RuntimeError(
            f'invalid cut window after padding/clamp: candidate_id={candidate_id} '
            f'cut_start_s={cut_start_s} cut_end_s={cut_end_s}'
        )

    part_path = out_path.with_name(out_path.name + '.part')
    if part_path.exists():
        part_path.unlink()

    cmd = [
        'ffmpeg',
        '-v', 'error',
        '-y',
        '-ss', f'{cut_start_s:.3f}',
        '-to', f'{cut_end_s:.3f}',
        '-i', str(video),
        '-c', 'copy',
        '-f', 'mp4',
        str(part_path),
    ]
    print(f'CMD {cmd}')
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        if part_path.exists():
            part_path.unlink()
        raise RuntimeError(
            f'ffmpeg failed exit={proc.returncode} candidate_id={candidate_id} '
            f'stderr="{(proc.stderr or "").strip()}"'
        )
    if not part_path.exists() or part_path.stat().st_size == 0:
        if part_path.exists():
            part_path.unlink()
        raise RuntimeError(
            f'ffmpeg reported success but produced no bytes: candidate_id={candidate_id} path={part_path}'
        )
    os.replace(part_path, out_path)

    # ---- Sidecar witness (D-055 fixer): the slice's REAL absolute coords. ----
    # A -c copy input cut's END (-to) is near-exact, but its HEAD snaps to the
    # keyframe AT/BEFORE the requested start, so the actual media starts
    # EARLIER than nominal. Anchoring abs_start_s = abs_end_s - actual_duration
    # absorbs the snap; the renderer trusts these two numbers, never the formula.
    try:
        actual_duration_s = measure_duration_s(out_path)
    except Exception:
        # Gate 9 (a failed run writes nothing): a slice we could not measure
        # must not survive as an unwitnessed file a later run would skip.
        if out_path.exists():
            out_path.unlink()
        raise

    # requested_* are the PRE-CLAMP formula values (diagnostic only — a
    # staging-time video-end clamp is visible as requested_end_s > abs_end_s;
    # the renderer must never use them for math).
    #
    # source_* (schema 2, SRD-06 fixer): the SOURCE VIDEO's identity at cut
    # time, so a later run can tell a genuinely-unchanged source apart from a
    # repointed/recycled one — geometry alone cannot (see module docstring).
    sidecar = {
        'schema': slice_geometry.SIDECAR_SCHEMA,
        'candidate_id': candidate_id,
        'source_path': str(video),
        'source_size_bytes': video_size_bytes,
        'source_duration_s': video_duration_s,
        'abs_start_s': cut_end_s - actual_duration_s,
        'abs_end_s': cut_end_s,
        'requested_start_s': cut_start_s,
        'requested_end_s': slice_geometry.slice_end(end_s),  # unclamped
        'actual_duration_s': actual_duration_s,
        'staged_at': utc_now_iso(),
    }
    write_sidecar_atomic(sidecar_path_for(out_path), sidecar)


def run(vod_id: int, video: Path) -> int:
    slices_dir = slices_dir_from_env()

    if not video.is_file():
        raise RuntimeError(f'video file not found: {video}')
    video_duration_s = measure_duration_s(video)
    # Measured once here and passed to every slice_one() call below, same
    # pattern as video_duration_s — the source video is a fixed input for the
    # whole run. Written into every sidecar as the source-identity witness
    # (schema 2, SRD-06 fixer) so a later run can tell this exact file apart
    # from a repointed/recycled one.
    video_size_bytes = os.stat(video).st_size
    print(f'VIDEO {video} duration_s={video_duration_s:.3f} size_bytes={video_size_bytes}')

    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT id FROM recordings WHERE id = %s', (vod_id,))
        if cur.fetchone() is None:
            raise RuntimeError(f'recording not found: vod_id={vod_id}')
        cur.execute(
            'SELECT id, start_s, end_s, state FROM clip_candidates '
            'WHERE recording_id = %s AND state IN %s ORDER BY id',
            (vod_id, SLICE_STATES),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    slices_dir.mkdir(parents=True, exist_ok=True)

    sliced = 0
    skipped_existing = 0
    failed = 0
    for candidate_id, start_s, end_s, state in rows:
        out_path = slices_dir / f'c{int(candidate_id)}.mp4'
        sidecar_path = sidecar_path_for(out_path)
        # Idempotency (D-055 fixer + SRD-06 source witness): only a slice +
        # a VALID sidecar whose SOURCE WITNESS matches the CURRENT video
        # counts as done. A slice without a valid sidecar is restaged — the
        # heal path for stale pre-D-055 slices staged under the same name
        # with other geometry. A slice with a valid sidecar witnessing a
        # DIFFERENT source (recordings.path repointed, or an id recycled by
        # --reset-ids) is also restaged — geometry alone cannot see this;
        # only the source identity can.
        if out_path.exists() and out_path.stat().st_size > 0:
            sidecar_data = load_valid_sidecar(sidecar_path, int(candidate_id))
            if sidecar_data is not None:
                if (sidecar_data.get('source_path') == str(video)
                        and sidecar_data.get('source_size_bytes') == video_size_bytes):
                    skipped_existing += 1
                    print(f'SKIP_EXISTING candidate={int(candidate_id)} path={out_path}')
                    continue
                print(
                    f'RESTAGE candidate={int(candidate_id)} path={out_path} '
                    f'reason=source_mismatch '
                    f'sidecar_source_path={sidecar_data.get("source_path")!r} '
                    f'sidecar_source_size_bytes={sidecar_data.get("source_size_bytes")!r} '
                    f'current_source_path={str(video)!r} '
                    f'current_source_size_bytes={video_size_bytes}'
                )
            else:
                print(
                    f'RESTAGE candidate={int(candidate_id)} path={out_path} '
                    'reason=missing_or_invalid_sidecar'
                )
        try:
            slice_one(video, video_duration_s, video_size_bytes, int(candidate_id),
                      float(start_s), float(end_s), out_path)
            sliced += 1
            print(
                f'SLICED candidate={int(candidate_id)} state={state} '
                f'path={out_path} bytes={out_path.stat().st_size}'
            )
        except Exception as exc:  # per-candidate isolation: log, count, continue
            failed += 1
            print(f'ERROR: slice failed candidate={int(candidate_id)}: {exc}', file=sys.stderr)

    result_line = (
        f'RESULT slice_candidates recording={vod_id} '
        f'sliced={sliced} skipped_existing={skipped_existing} failed={failed}'
    )
    print(result_line)
    if failed > 0:
        # D-047: n8n discards a failing child's stdout — verdict to stderr too.
        print(result_line, file=sys.stderr)
        print(f'ERROR: {failed} candidate slice(s) failed for recording={vod_id}', file=sys.stderr)
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Stream-copy padded per-candidate slices from the archive video'
    )
    parser.add_argument('--vod-id', type=int, required=True)
    parser.add_argument('--video', type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(args.vod_id, args.video)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
