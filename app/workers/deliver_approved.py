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

D-063: THIS MACHINE CANNOT BURN CAPTIONS, SO IT REFUSES THE CANDIDATES THAT
WANT THEM — AT BOTH STAGES. The render stage refuses through
cut_clip.require_no_caption_request (one truth for the message and the
capability probe). The SYNC stage refuses too, and that is the half that is
easy to miss: a clip rendered BEFORE the operator ticked captions is already on
disk and byte-matches nothing about the request, so without a sync-stage guard
it would be delivered uncaptioned against an explicit ask and then recorded as
delivered. The mirror case is guarded as well — a clip whose file WAS burned
while the toggle has since been switched off is not delivered either, because
shipping captions he switched off is the same failure pointing the other way.
Both refusals are per-candidate: the rest of the batch is unaffected, and both
are counted into RESULT's refused_captions as well as into failed.

WHAT IS EXPLICITLY NOT A REFUSAL: a clip the server rendered with captions
asked for whose window holds NO SPEECH (captions_requested=1, captions_burned=0,
captions_cue_count=0). That file is exactly what was requested — there was
nothing to burn — so it delivers normally. Reading only captions_burned cannot
tell it apart from "this machine cannot burn", which is why all three columns
are fetched.

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
    """All approved candidates with recording session_label, clip row (if any,
    including drive_synced_at — the D-056 delivery witness) and llm_signal
    category ('unknown' when absent)."""
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
               cl.file_path, cl.state, cl.drive_synced_at,
               {category_select} AS category,
               c.burn_captions, cl.captions_burned,
               cl.captions_requested, cl.captions_cue_count
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
            'drive_synced_at': str(r[6]) if r[6] is not None else None,
            'category': str(r[7]) if r[7] is not None else 'unknown',
            # D-063: the INTENT (candidate) and the FACT (clip row) are read as
            # two separate things on purpose — they are allowed to disagree,
            # and the disagreement is precisely what must stop a delivery.
            'burn_captions': int(r[8]),
            'captions_burned': int(r[9]) if r[9] is not None else None,
            # D-063 fixer: requested + cue_count are read too, because
            # captions_burned ALONE cannot tell the two zero-cases apart —
            # "this machine could not burn it" and "the server burned nothing
            # because nobody speaks in this window" are both burned=0, and only
            # one of them is a reason to refuse a delivery.
            'captions_requested': int(r[10]) if r[10] is not None else None,
            'captions_cue_count': int(r[11]) if r[11] is not None else None,
        })
    return out


def captions_honest_no_speech(cand: dict) -> bool:
    """Was this file rendered WITH captions asked for, and nothing said in it?

    render_from_slice writes exactly this triple when the shipped window holds
    no speech: requested=1, burned=0, cue_count=0. That is a SUCCESSFUL render
    of precisely what was asked for, not a shortfall — there was nothing to
    burn. Only render_from_slice can ever write requested=1 (both Mac paths
    hardcode 0/0/NULL), so this triple is unforgeable by the machine that
    cannot burn.
    """
    return (
        cand['captions_requested'] == 1
        and cand['captions_burned'] == 0
        and cand['captions_cue_count'] == 0
    )


def require_captions_consistent(cand: dict) -> None:
    """D-063: refuse any candidate whose captions INTENT and clip FACT disagree.

    Two directions, both of which would otherwise ship a file that contradicts
    what the operator asked for:

      wants captions, file has none  -> cut_clip's shared refusal (this machine
          cannot burn), raised even when a perfectly good uncaptioned file is
          already sitting there ready to copy.
      wants none, file was burned    -> the operator switched captions off after
          a server-side burn. The bytes on disk still carry them, so delivering
          would publish captions he removed.

    TWO CASES THAT MUST NOT BE REFUSED, and the guard got the second of them
    wrong until the adversarial sweep caught it:

      wants captions, file HAS them  -> the n8n server lane burned it. Nothing
          is being asked of this machine's ffmpeg — it is a file copy of
          exactly what was requested — so gating on the request alone would
          strand every server-burned clip in the queue forever.
      wants captions, file was rendered WITH captions asked for and the window
          holds NO SPEECH -> requested=1, burned=0, cue_count=0. The server
          already did exactly what was asked and there was nothing to burn.
          Refusing here refused a correct file forever, told the operator via
          the review card to "re-render on the server lane" (which reproduces
          the identical row), and — because every refusal counts into `failed`
          and main() returns 1 when failed>0 — turned the WHOLE batch to ok=0
          on every future run. One honest render, one permanently red batch.

    So the guard keys on a real DISAGREEMENT between the intent and the file,
    never on the intent by itself and never on burned=0 by itself.
    """
    if (cand['burn_captions'] == 1
            and cand['captions_burned'] != 1
            and not captions_honest_no_speech(cand)):
        cut_clip.require_no_caption_request(
            cand['candidate_id'], cand['burn_captions'], 'deliver_approved.sync'
        )
    if cand['burn_captions'] == 0 and cand['captions_burned'] == 1:
        raise cut_clip.CaptionRefused(
            f'CAPTIONS_STALE_BURN: candidate_id={cand["candidate_id"]} has captions '
            'switched OFF (clip_candidates.burn_captions=0) but its rendered clip was '
            'burned WITH captions (clips.captions_burned=1), so delivering it would '
            'publish captions that were switched off. Refusing this candidate: the '
            'rest of the batch is unaffected. Fixes: re-tick captions for this clip in '
            'the review UI, or re-render it without captions on the n8n server lane.'
        )


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


def delivery_file_check(cand: dict, dest: Path) -> tuple[bool, str]:
    """Re-verify the FILE-PROXY claim of delivery, and say why it failed.

    ONE truth for "the delivered copy is really sitting at dest" (charter gate
    1): already_delivered() below and the D-056 witness backfill both ask this
    single question. Matched is True only when the destination exists AND its
    byte size equals the source clip's — an absent, unreadable or size-
    mismatched destination is NEVER evidence of a delivery, because a witness
    that can be forged from a bad file is not a witness (charter gate 22).
    """
    src = cand['clip_file_path']
    if src is None:
        return False, 'no_clip_file_path'
    src_path = Path(src)
    if not src_path.exists():
        return False, 'source_missing'
    if not dest.exists():
        return False, 'dest_missing'
    src_size = src_path.stat().st_size
    dst_size = dest.stat().st_size
    if src_size != dst_size:
        return False, f'size_mismatch_src={src_size}_dst={dst_size}'
    return True, 'match'


def already_delivered(cand: dict, dest: Path) -> bool:
    """Unchanged semantics (source exists, dest exists, byte sizes equal),
    now derived from delivery_file_check so there is only one copy of the
    predicate to drift."""
    matched, _reason = delivery_file_check(cand, dest)
    return matched


def write_witness(conn, cur, candidate_id: int, dest: Path) -> str:
    """Write the D-056 delivery witness (clips.drive_synced_at + drive_sync_path)
    and return the timestamp written. ONE truth for the witness write: a fresh
    sync and the backfill below both go through here.

    D-056 fixer (charter gate 9 — a failed write must never look like a
    success): the UPDATE must match EXACTLY one clips row. Without this
    assertion a rowcount of 0 (the clips row deleted or re-keyed under the run)
    committed happily and printed SYNCED while leaving NO witness — which is
    exactly the state that pins an approved card in the operator's Pending
    queue forever. Mirrors the n8n "Mark Delivered" node's assertion in
    clpr/n8n/clpr-verdicts.json (rowcount != 1 -> rollback, error, exit 1).
    Raising keeps per-candidate failure isolation: main() counts it FAILED and
    the batch continues.
    """
    ts = utc_now_iso()
    try:
        cur.execute(
            'UPDATE clips SET drive_synced_at = %s, drive_sync_path = %s WHERE candidate_id = %s',
            (ts, str(dest), candidate_id),
        )
        rowcount = cur.rowcount
        if rowcount != 1:
            conn.rollback()
            raise RuntimeError(
                f'delivery-witness UPDATE matched {rowcount} clips rows for '
                f'candidate_id={candidate_id} (expected exactly 1); nothing committed'
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return ts


def backfill_witness(conn, cur, cand: dict, dest: Path) -> None:
    """D-056 fixer (charter gate 1: ONE truth per thing).

    already_delivered() skipped on a byte-matched destination file WITHOUT
    touching the witness, so idempotency ran on the file proxy while the review
    surface ran on clips.drive_synced_at. If a previous run copied the file but
    died before the UPDATE committed, every later run printed
    SKIP_ALREADY_DELIVERED while the witness stayed NULL — pinning that
    approved card in the operator's Pending queue as "awaiting delivery"
    FOREVER while this worker reported it delivered. Two proxies for one
    property, disagreeing in silence.

    This is witness REPAIR, not a policy change: CLPR_DRIVE_SYNC_DIR *is* the
    locally-mounted Drive folder, so a byte-matched file sitting in it means
    the delivery really did happen. The caller has already re-verified the
    bytes via delivery_file_check — this is never reached from an absent or
    size-mismatched destination.
    """
    if cand['drive_synced_at'] is not None:
        return
    ts = write_witness(conn, cur, cand['candidate_id'], dest)
    cand['drive_synced_at'] = ts
    print(f'WITNESS_BACKFILLED candidate={cand["candidate_id"]} path="{dest}"')


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

    write_witness(conn, cur, cand['candidate_id'], dest)

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
                   c.state, r.path, r.duration_s, c.burn_captions
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
         state, recording_path, recording_duration_s, burn_captions) = row
        if recording_duration_s is None:
            raise RuntimeError(f'recording duration_s is NULL for candidate_id={candidate_id}')
        recording_id = int(recording_id)
        recording_duration_s = float(recording_duration_s)

        if str(state) != 'approved':
            raise RuntimeError(
                f'candidate must be approved before cut: candidate_id={candidate_id} state={state}'
            )

        # D-063: same refusal as cut_clip.render_clip, same message, one truth.
        cut_clip.require_no_caption_request(
            candidate_id, burn_captions, 'deliver_approved.render_adjusted_clip'
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

            # D-063: identical to cut_clip's — this path cannot burn either, so
            # it writes the honest 0/0/NULL and resets those columns on
            # re-render (a server-burned clip re-rendered here must stop
            # claiming captions the new file does not have).
            cur.execute(
                '''
                INSERT INTO clips(candidate_id, file_path, duration_s, state, created_by_run, created_at,
                                  captions_requested, captions_burned, captions_cue_count)
                VALUES (%s, %s, %s, 'rendered', %s, %s, 0, 0, NULL)
                ON CONFLICT (candidate_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    duration_s = EXCLUDED.duration_s,
                    state = 'rendered',
                    created_by_run = EXCLUDED.created_by_run,
                    created_at = EXCLUDED.created_at,
                    captions_requested = 0,
                    captions_burned = 0,
                    captions_cue_count = NULL
                ''',
                (candidate_id, str(out_path), duration_s, run_id, utc_now_iso()),
            )
            conn.commit()

            print(
                f'RESULT deliver_adjusted candidate={candidate_id} ok=1 '
                f'file="{out_path}" duration_s={duration_s:.3f} '
                f'captions_requested=0 captions_burned=0'
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
    # D-063 fixer: caption refusals are counted in their OWN field as well as
    # in `failed`, never instead of it. Folded only into `failed` they were
    # indistinguishable from a real delivery error in the one line the operator
    # reads, while the dry run had reported them explicitly — the two RESULT
    # lines described the same batch in different vocabularies.
    refused_captions = 0

    conn = db.connect()
    try:
        cur = conn.cursor()
        candidates = fetch_approved(cur)
        approved = len(candidates)

        if args.dry_run:
            planned_sync = 0
            planned_render = 0
            planned_refused = 0
            for cand in candidates:
                dest = dest_path_for(drive_sync_dir, cand)
                # D-063: the dry run must SHOW the refusal, not discover it at
                # execution time. It reports, it never raises: a dry run
                # executes nothing, including nothing that fails.
                caption_block = None
                if cand['drive_synced_at'] is None:
                    try:
                        require_captions_consistent(cand)
                    except Exception as exc:
                        caption_block = str(exc)
                if caption_block is not None:
                    planned_refused += 1
                    print(
                        f'PLAN candidate={cand["candidate_id"]} action=refuse_captions '
                        f'category={cand["category"]} reason="{caption_block}"'
                    )
                    continue
                if cand['drive_synced_at'] is not None:
                    # D-056: the delivery witness is set — this candidate is
                    # delivered, full stop. Never re-rendered, never re-synced.
                    action = 'skip_delivered'
                    already += 1
                elif already_delivered(cand, dest):
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
                f'already_delivered={already} refused_captions={planned_refused}'
            )
            # failed=0 is the truth: a dry run executes nothing, so nothing
            # failed. The refusals are a PLAN and are reported as their own
            # field rather than dressed up as failures that did not happen.
            print(
                'RESULT deliver_approved '
                f'ok=1 approved={approved} rendered_now=0 synced_now=0 '
                f'already_delivered={already} obs_blocked=0 failed=0 '
                f'refused_captions={planned_refused}'
            )
            return 0

        # ---- Phase 1: sync-eligible first, so an OBS refusal never blocks
        # delivery of files that already exist.
        pending_render: list[dict] = []
        for cand in candidates:
            cid = cand['candidate_id']
            dest = dest_path_for(drive_sync_dir, cand)
            try:
                if cand['drive_synced_at'] is not None:
                    # D-056 cross-lane rule: a clips row with drive_synced_at
                    # set IS the delivery witness (written by the n8n Mark
                    # Delivered node or by sync_candidate below). Skip FIRST —
                    # before any file check — so a delivered candidate whose
                    # local clip file is gone can never fall through to a
                    # re-render/re-sync (double delivery).
                    already += 1
                    print(
                        f'SKIP_DELIVERED candidate={cid} '
                        f'drive_synced_at={cand["drive_synced_at"]}'
                    )
                    continue

                # D-063: the captions guard sits AFTER the delivered skip and
                # BEFORE every file check, so it can never re-open an already
                # delivered clip and can never be reached around by a
                # byte-matching file that predates the operator's request.
                require_captions_consistent(cand)

                matched, reason = delivery_file_check(cand, dest)
                if matched:
                    already += 1
                    print(f'SKIP_ALREADY_DELIVERED candidate={cid} dest="{dest}"')
                    # D-056 fixer: the file proxy says delivered and the bytes
                    # agree, so make the WITNESS agree too — otherwise this
                    # candidate is skipped here on every future run while the
                    # review surface keeps it in Pending forever.
                    backfill_witness(conn, cur, cand, dest)
                    continue

                if dest.exists():
                    # A destination file exists but does NOT byte-match the
                    # source. It is not evidence of a delivery, so: do not
                    # skip, do not backfill (the witness must stay unforgeable
                    # — charter gate 22). Say so loudly and fall through to the
                    # normal deliver path below, i.e. treat it as undelivered.
                    print(
                        f'WITNESS_MISMATCH candidate={cid} dest="{dest}" reason={reason} '
                        'treated_as=undelivered no_witness_written=1'
                    )

                if is_sync_eligible(cand):
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
                if isinstance(exc, cut_clip.CaptionRefused):
                    refused_captions += 1
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
                            # Unreachable while the sync-stage guard above runs
                            # first (it refuses the same disagreement before a
                            # candidate can reach this queue), but counted here
                            # too so the field stays true if that ordering ever
                            # changes rather than silently under-reporting.
                            if isinstance(exc, cut_clip.CaptionRefused):
                                refused_captions += 1
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
        f'already_delivered={already} obs_blocked={obs_blocked} failed={failed} '
        f'refused_captions={refused_captions}'
    )
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
