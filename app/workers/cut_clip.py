#!/usr/bin/env python3
"""cut_clip: render one approved candidate to vertical 1080x1920 MP4.
Connects via the shared adapter app/workers/db.py (CLPR_DB_URL) and exits
non-zero on any failure. Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): tables and columns per app/docs/naming-map.md.
The --candidate-id CLI flag is an external contract and stays.

D-063: THIS RENDERER CANNOT BURN CAPTIONS, AND SAYS SO INSTEAD OF PRETENDING.
It is the Mac-side path, and the operator's ffmpeg is built without libass, so
it has no `subtitles` filter at all — the burn is not merely unimplemented
here, it is impossible on this machine. A candidate with
clip_candidates.burn_captions = 1 is therefore REFUSED loudly
(CAPTIONS_UNSUPPORTED_HERE) rather than rendered without the captions it asked
for. Delivering an uncaptioned file against an explicit request, and then
recording it as delivered, is exactly the class of lie this project keeps
paying for. Everything about the render itself is unchanged: with the flag off
(the default, and every clip that exists today) not one byte of the D-023
command differs, and the clips row records captions_requested = 0,
captions_burned = 0 — which is TRUE of every file this renderer can produce.
"""

from __future__ import annotations

import argparse
import datetime as dt
import socket
import subprocess
import sys
from pathlib import Path

import db

try:
    from obs_guard import require_obs_idle_or_raise
except ModuleNotFoundError:
    from .obs_guard import require_obs_idle_or_raise

try:
    import render_from_slice
except ModuleNotFoundError:  # pragma: no cover - package-relative import
    from . import render_from_slice

PAD_SECONDS = 1.5  # small default padding so cuts do not feel abrupt; tunable later.


class CaptionRefused(RuntimeError):
    """A candidate refused because its captions INTENT and the file disagree.

    A TYPE, not a message prefix, because deliver_approved counts these into
    its own RESULT field and matching on the text of an error message is the
    proxy version of asking what actually happened: reword the message and the
    counter silently goes to zero while still reporting ok=1. Subclasses
    RuntimeError so every existing per-candidate handler keeps treating it as
    the failure it is — the refusal is COUNTED separately, never RECLASSIFIED
    out of `failed`, or a batch would report success while clips the operator
    asked for sat undelivered.
    """


def require_no_caption_request(candidate_id: int, burn_captions: int, stage: str) -> None:
    """ONE TRUTH for the Mac-side caption refusal (D-063).

    Every Mac-side entry point routes through here: cut_clip.render_clip (also
    reached from cut_all_approved.py), deliver_approved.render_adjusted_clip,
    and deliver_approved's SYNC stage — because a clip rendered BEFORE the flag
    was ticked is already on disk and would otherwise be delivered uncaptioned
    against an explicit request, which is the same lie with fewer steps.

    The capability is PROBED and quoted in the message rather than asserted, so
    the error stays true on a machine that later gains libass: this renderer
    still does not implement burning, and the message says which of the two
    reasons applies here.
    """
    if int(burn_captions) != 1:
        return
    has_filter = render_from_slice.ffmpeg_has_subtitles_filter()
    capability = (
        'this ffmpeg HAS the subtitles filter, but this Mac-side renderer does not '
        'implement burning at all'
        if has_filter else
        'this ffmpeg has no `subtitles` filter, so it was built without libass and '
        'cannot burn anything'
    )
    raise CaptionRefused(
        f'CAPTIONS_UNSUPPORTED_HERE: candidate_id={candidate_id} asks for burned-in '
        f'captions (clip_candidates.burn_captions=1) at stage={stage}, but on host '
        f'"{socket.gethostname()}" {capability}. Refusing this candidate: nothing is '
        'rendered, nothing is delivered, and no clip row will claim captions it does '
        'not have. The rest of the batch is unaffected. Fixes: approve it so the n8n '
        'server lane renders it (that ffmpeg has libass), or untick captions for this '
        'clip in the review UI, or install an ffmpeg build that includes libass here.'
    )


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


def fetch_candidate(cur, candidate_id: int) -> tuple[int, float, float, str, str, float, int]:
    cur.execute(
        '''
        SELECT c.recording_id, c.start_s, c.end_s, c.state, r.path, r.duration_s,
               c.burn_captions
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        WHERE c.id = %s
        ''',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'candidate_id not found: {candidate_id}')

    (recording_id, start_s, end_s, state, recording_path, recording_duration_s,
     burn_captions) = row
    if recording_duration_s is None:
        raise RuntimeError(f'recording duration_s is NULL for candidate_id={candidate_id}')

    return (
        int(recording_id),
        float(start_s),
        float(end_s),
        str(state),
        str(recording_path),
        float(recording_duration_s),
        int(burn_captions),
    )


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def render_clip(candidate_id: int) -> int:
    require_obs_idle_or_raise('cut_clip')

    run_id = f'cut_clip_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    conn = db.connect()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'clips'"
        )
        if cur.fetchone() is None:
            raise RuntimeError('clips table missing; apply migrations_pg/001 before cut_clip')

        (recording_id, start_s, end_s, state, recording_path, recording_duration_s,
         burn_captions) = fetch_candidate(cur, candidate_id)

        if state != 'approved':
            raise RuntimeError(
                f'candidate must be approved before cut: candidate_id={candidate_id} state={state}'
            )

        # D-063: refuse BEFORE any work, so a candidate that wants captions
        # never produces a file this machine would have to lie about.
        require_no_caption_request(candidate_id, burn_captions, 'cut_clip.render_clip')

        cut_start_s = clamp(start_s - PAD_SECONDS, 0.0, recording_duration_s)
        cut_end_s = clamp(end_s + PAD_SECONDS, 0.0, recording_duration_s)
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
            '-i', recording_path,
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
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            duration_s = measure_duration_s(out_path)

            # D-063: this renderer can only ever produce an uncaptioned file
            # (it refuses the request above), so it writes the honest 0/0/NULL
            # and RESETS those columns on re-render. Without the reset, a clip
            # previously burned by the server lane and re-rendered here would
            # keep claiming captions the new file does not have.
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
                f'RESULT cut_clip candidate={candidate_id} ok=1 file="{out_path}" '
                f'duration_s={duration_s:.3f} captions_requested=0 captions_burned=0'
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
