#!/usr/bin/env python3
"""deliver_approved: batch-deliver every approved candidate's rendered clip to the
locally-synced Google Drive folder (CLPR_DRIVE_SYNC_DIR), rendering first when needed.

Order of operations is sync-first: clips that are already rendered are delivered
BEFORE any render is attempted, so an OBS gate refusal (expected while OBS is
live, same as run_pipeline's GATE C) never blocks delivery of existing files.

The delivered copy gets a DESCRIPTIVE name (operator-approved naming ruling,
2026-08-06): <session_label>_c<candidate_id>_<category>.mp4, where category is
looked up from llm_signal_candidates matching (recording_id, start_s), or
'unknown' when absent. The local file in clips_out keeps its existing name.

Idempotent: a candidate whose delivered copy already exists at the destination
with matching byte size is skipped.

D-055: every cut honors the EFFECTIVE window COALESCE(adjusted_start_s,
start_s) .. COALESCE(adjusted_end_s, end_s), originals never overwritten. The
real render path here is cut_clip.render_clip (which reads only the original
window and is deliberately untouched — it is the operator-proven D-023 path),
so rendering dispatches: unedited candidates keep cut_clip.render_clip
byte-identical behavior; adjusted candidates render via render_adjusted_clip
below, which mirrors cut_clip exactly but cuts the effective window with
slice_geometry.PUBLISH_PAD_S breathing room (one truth for the pad).

Per-candidate failure isolation: one failure never aborts the batch.
Connects via the shared adapter app/workers/db.py (CLPR_DB_URL). Prints
machine-parseable RESULT line last; failure details also go to stderr (D-047
host rule). Exit 0 iff nothing FAILED (obs_blocked is not failure).

PostgreSQL port (D-052 P3): tables and columns per app/docs/naming-map.md.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sys
from pathlib import Path

import db
import slice_geometry

try:
    import cut_clip
    from obs_guard import require_obs_idle_or_raise
except ModuleNotFoundError:
    from . import cut_clip
    from .obs_guard import require_obs_idle_or_raise


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def require_env(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Required env var missing: {name}')
    return value


def is_obs_gate_refusal(exc: Exception) -> bool:
    return 'OBS is actively' in str(exc)


def delivered_name(session_label: str, candidate_id: int, category: str) -> str:
    return f'{session_label}_c{candidate_id}_{category}.mp4'


def fetch_approved(cur) -> list[dict]:
    """All approved candidates with recording session_label, clip row (if any)
    and llm_signal category ('unknown' when absent)."""
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
        SELECT c.id, c.recording_id, c.start_s, r.session_label,
               cl.file_path, cl.state,
               {category_select} AS category
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        LEFT JOIN clips cl ON cl.candidate_id = c.id
        WHERE c.state = 'approved'
        ORDER BY c.id
        '''
    )
    rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            'candidate_id': int(r[0]),
            'recording_id': int(r[1]),
            'start_s': float(r[2]),
            'session_label': str(r[3]),
            'clip_file_path': str(r[4]) if r[4] is not None else None,
            'clip_state': str(r[5]) if r[5] is not None else None,
            'category': str(r[6]) if r[6] is not None else 'unknown',
        })
    return out


def is_sync_eligible(cand: dict) -> bool:
    return (
        cand['clip_file_path'] is not None
        and cand['clip_state'] == 'rendered'
        and Path(cand['clip_file_path']).exists()
    )


def dest_path_for(drive_sync_dir: str, cand: dict) -> Path:
    return Path(drive_sync_dir) / delivered_name(
        cand['session_label'], cand['candidate_id'], cand['category']
    )


def already_delivered(cand: dict, dest: Path) -> bool:
    return (
        cand['clip_file_path'] is not None
        and Path(cand['clip_file_path']).exists()
        and dest.exists()
        and dest.stat().st_size == Path(cand['clip_file_path']).stat().st_size
    )


def sync_candidate(conn, cur, cand: dict, dest: Path) -> None:
    """sync_clip_to_drive's mechanism (copy2, byte-size verification,
    drive_synced_at/drive_sync_path columns) with the descriptive dest name."""
    src_path = Path(cand['clip_file_path'])

    print(f'SYNC_CMD src="{src_path}" dst="{dest}"')
    shutil.copy2(src_path, dest)

    src_size = src_path.stat().st_size
    dst_size = dest.stat().st_size
    if src_size != dst_size:
        raise RuntimeError(f'post-copy size mismatch: src={src_size} dst={dst_size}')

    try:
        cur.execute(
            'UPDATE clips SET drive_synced_at = %s, drive_sync_path = %s WHERE candidate_id = %s',
            (utc_now_iso(), str(dest), cand['candidate_id']),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    print(
        f'SYNCED candidate={cand["candidate_id"]} dest="{dest}" size={dst_size}'
    )


def refresh_clip_fields(cur, cand: dict) -> None:
    cur.execute(
        'SELECT file_path, state FROM clips WHERE candidate_id = %s',
        (cand['candidate_id'],),
    )
    row = cur.fetchone()
    if row:
        cand['clip_file_path'] = str(row[0]) if row[0] is not None else None
        cand['clip_state'] = str(row[1]) if row[1] is not None else None


def fetch_adjusted_window(cur, candidate_id: int) -> tuple[float | None, float | None]:
    cur.execute(
        'SELECT adjusted_start_s, adjusted_end_s FROM clip_candidates WHERE id = %s',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'candidate_id not found: {candidate_id}')
    return (
        float(row[0]) if row[0] is not None else None,
        float(row[1]) if row[1] is not None else None,
    )


def render_adjusted_clip(candidate_id: int) -> int:
    """cut_clip.render_clip's exact mechanics, cutting the D-055 EFFECTIVE
    window instead of the original one.

    Mirrors app/workers/cut_clip.py render_clip (lines 92-192) step for step:
    OBS gate, clips-table guard, approved-state guard, pad+clamp, same out_dir
    and <recording_id>_<candidate_id>.mp4 name (so refresh_clip_fields/sync see
    it identically), the D-023 operator-proven filter_complex and encode flags
    byte-for-byte (cut_clip.py lines 133-159), the same clips upsert, and
    failure unlinks the partial output (charter gate 9). Only the cut window
    differs: COALESCE(adjusted, original) +/- slice_geometry.PUBLISH_PAD_S
    (== cut_clip.PAD_SECONDS, one truth in slice_geometry).
    """
    require_obs_idle_or_raise('deliver_approved')

    run_id = f'deliver_adjusted_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    conn = db.connect()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'clips'"
        )
        if cur.fetchone() is None:
            raise RuntimeError('clips table missing; apply migrations_pg/001 before deliver_approved')

        cur.execute(
            '''
            SELECT c.recording_id, c.start_s, c.end_s,
                   c.adjusted_start_s, c.adjusted_end_s,
                   c.state, r.path, r.duration_s
            FROM clip_candidates c
            JOIN recordings r ON r.id = c.recording_id
            WHERE c.id = %s
            ''',
            (candidate_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f'candidate_id not found: {candidate_id}')
        (recording_id, start_s, end_s, adjusted_start_s, adjusted_end_s,
         state, recording_path, recording_duration_s) = row
        if recording_duration_s is None:
            raise RuntimeError(f'recording duration_s is NULL for candidate_id={candidate_id}')
        recording_id = int(recording_id)
        recording_duration_s = float(recording_duration_s)

        if str(state) != 'approved':
            raise RuntimeError(
                f'candidate must be approved before cut: candidate_id={candidate_id} state={state}'
            )

        eff_start_s, eff_end_s = slice_geometry.effective_window(
            float(start_s), float(end_s),
            float(adjusted_start_s) if adjusted_start_s is not None else None,
            float(adjusted_end_s) if adjusted_end_s is not None else None,
        )
        pad = slice_geometry.PUBLISH_PAD_S

        cut_start_s = cut_clip.clamp(eff_start_s - pad, 0.0, recording_duration_s)
        cut_end_s = cut_clip.clamp(eff_end_s + pad, 0.0, recording_duration_s)
        if cut_end_s <= cut_start_s:
            raise RuntimeError(
                f'invalid cut window after padding/clamp: candidate_id={candidate_id} '
                f'cut_start_s={cut_start_s} cut_end_s={cut_end_s}'
            )
        cut_duration_s = cut_end_s - cut_start_s

        out_dir = Path(__file__).resolve().parent.parent / 'clips_out'
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f'{recording_id}_{candidate_id}.mp4'

        if out_path.exists():
            out_path.unlink()

        # Copied EXACTLY from cut_clip.py lines 133-159 (operator-proven live
        # on Instagram, D-023); change nothing.
        filter_complex = (
            '[0:v]split=2[bg][fg];'
            '[bg]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,gblur=sigma=20[bg2];'
            '[fg]scale=1080:-2:flags=lanczos[fg2];'
            '[bg2][fg2]overlay=(W-w)/2:(H-h)/2,fps=30[v]'
        )

        ffmpeg_cmd = [
            'ffmpeg',
            '-y',
            '-ss', f'{cut_start_s:.3f}',
            '-i', str(recording_path),
            '-t', f'{cut_duration_s:.3f}',
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
            ffmpeg_proc = cut_clip.run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            duration_s = cut_clip.measure_duration_s(out_path)

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
                f'RESULT deliver_adjusted candidate={candidate_id} ok=1 '
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


def render_candidate(cur, candidate_id: int) -> int:
    """D-055 render dispatch: unedited candidates keep the operator-proven
    cut_clip.render_clip path byte-identical; candidates with any adjusted
    column set render the effective window via render_adjusted_clip."""
    adjusted_start_s, adjusted_end_s = fetch_adjusted_window(cur, candidate_id)
    if adjusted_start_s is None and adjusted_end_s is None:
        return cut_clip.render_clip(candidate_id)
    return render_adjusted_clip(candidate_id)


def report_failure(candidate_id: int, stage: str, exc: Exception) -> None:
    msg = f'CANDIDATE_FAILED candidate={candidate_id} stage={stage} error="{exc}"'
    print(msg)
    print(msg, file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Batch-deliver approved candidates: render if needed, sync to CLPR_DRIVE_SYNC_DIR under descriptive names'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='report the approved set and per-candidate planned actions; execute nothing')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    drive_sync_dir = require_env('CLPR_DRIVE_SYNC_DIR')

    rendered_now = 0
    synced_now = 0
    already = 0
    obs_blocked = 0
    failed = 0

    conn = db.connect()
    try:
        cur = conn.cursor()
        candidates = fetch_approved(cur)
        approved = len(candidates)

        if args.dry_run:
            planned_sync = 0
            planned_render = 0
            for cand in candidates:
                dest = dest_path_for(drive_sync_dir, cand)
                if already_delivered(cand, dest):
                    action = 'already_delivered'
                    already += 1
                elif is_sync_eligible(cand):
                    action = 'sync'
                    planned_sync += 1
                else:
                    action = 'render_then_sync'
                    planned_render += 1
                print(
                    f'PLAN candidate={cand["candidate_id"]} action={action} '
                    f'category={cand["category"]} dest="{dest}"'
                )
            print(
                'DRY_RUN executes nothing: '
                f'planned_sync={planned_sync} planned_render_then_sync={planned_render} '
                f'already_delivered={already}'
            )
            print(
                'RESULT deliver_approved '
                f'ok=1 approved={approved} rendered_now=0 synced_now=0 '
                f'already_delivered={already} obs_blocked=0 failed=0'
            )
            return 0

        # ---- Phase 1: sync-eligible first, so an OBS refusal never blocks
        # delivery of files that already exist.
        pending_render: list[dict] = []
        for cand in candidates:
            cid = cand['candidate_id']
            dest = dest_path_for(drive_sync_dir, cand)
            try:
                if already_delivered(cand, dest):
                    already += 1
                    print(f'SKIP_ALREADY_DELIVERED candidate={cid} dest="{dest}"')
                elif is_sync_eligible(cand):
                    sync_candidate(conn, cur, cand, dest)
                    synced_now += 1
                elif cand['clip_file_path'] is not None and cand['clip_state'] == 'rendered':
                    # clips row says rendered but the file is gone: needs re-render.
                    print(
                        f'SOURCE_MISSING candidate={cid} '
                        f'file="{cand["clip_file_path"]}" will_attempt_render=1'
                    )
                    pending_render.append(cand)
                else:
                    pending_render.append(cand)
            except Exception as exc:
                conn.rollback()  # clear any aborted transaction before continuing the batch
                failed += 1
                report_failure(cid, 'sync', exc)

        # ---- Phase 2: render the rest (OBS-gated), then sync each fresh render.
        if pending_render:
            gate_open = True
            try:
                require_obs_idle_or_raise('deliver_approved')
            except Exception as exc:
                gate_open = False
                obs_blocked = len(pending_render)
                print(
                    f'GATE deliver_approved OBS_BUSY renders_blocked={obs_blocked} '
                    f'reason="{exc}" '
                    '(expected while OBS is live; already-rendered clips were synced above)'
                )

            if gate_open:
                for cand in pending_render:
                    cid = cand['candidate_id']
                    try:
                        render_candidate(cur, cid)
                    except Exception as exc:
                        if is_obs_gate_refusal(exc):
                            obs_blocked += 1
                            print(
                                f'GATE deliver_approved OBS_BUSY candidate={cid} '
                                f'reason="{exc}"'
                            )
                        else:
                            failed += 1
                            report_failure(cid, 'render', exc)
                        continue

                    rendered_now += 1
                    try:
                        refresh_clip_fields(cur, cand)
                        if not is_sync_eligible(cand):
                            raise RuntimeError(
                                f'render reported ok but no syncable clip row/file for candidate={cid}'
                            )
                        sync_candidate(conn, cur, cand, dest_path_for(drive_sync_dir, cand))
                        synced_now += 1
                    except Exception as exc:
                        conn.rollback()  # clear any aborted transaction before continuing the batch
                        failed += 1
                        report_failure(cid, 'sync_after_render', exc)
    finally:
        conn.close()

    ok = 1 if failed == 0 else 0
    print(
        'RESULT deliver_approved '
        f'ok={ok} approved={approved} rendered_now={rendered_now} synced_now={synced_now} '
        f'already_delivered={already} obs_blocked={obs_blocked} failed={failed}'
    )
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
