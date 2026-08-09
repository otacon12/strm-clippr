#!/usr/bin/env python3
"""build_srt: PRODUCT 2 of the post kit — the real subtitle file for one clip.

D-062 ruling 5: the post kit's "captions" are the actual speech
transcription, shipped as a real SRT. Platform auto-captions mangle this
project's vocabulary (n8n, psycopg2, Coolify) every time and the stored
whisper transcript already got them right, so the captions are built from the
transcript the pipeline already holds rather than re-derived or re-heard.

WHY THIS IS ITS OWN MODULE (justification, since the brief allowed one file):
this is the ONE part of the post kit that is pure deterministic arithmetic
with no network, no model and no spend. Separating it means the geometry can
be proven on real data without an HTTP seam, and captions can be regenerated
for an already-delivered clip without paying for two LLM calls. It is
imported by generate_post_kit.py and also runs standalone.

THE ONLY HARD PROBLEM HERE IS REBASING, AND IT IS A D-055 PROBLEM.
Transcript segments are stored in ABSOLUTE recording seconds. The shipped clip
starts somewhere else entirely, because the render covers the EFFECTIVE window
(COALESCE(adjusted, original)) plus PUBLISH_PAD_S on each side. So every
segment time shifts by the clip's own t=0 in absolute coordinates.

TWO RENDERERS SHIP CLIPS AND THEIR t=0 DIFFERS, so the basis is chosen from
the WITNESS OF WHICH ONE ACTUALLY RAN — clips.created_by_run — never from
whether some file happens to exist on this machine:

  'render_from_slice_*'  -> basis 'sidecar'.   The server rendered from a
      pre-staged slice, so t0 = abs_start_s + max(0, (eff_start - pad) - abs_start_s)
      where abs_start_s is the WITNESSED absolute start in the slice's
      c<id>.json sidecar (read via render_from_slice.load_sidecar, one truth).
      This is exactly render_from_slice.py's own offset arithmetic.

  'cut_clip_*' / 'deliver_adjusted_*'  -> basis 'formula'.  The Mac rendered
      from the full recording, so t0 = clamp(eff_start - pad, 0, recording duration),
      which is cut_clip.py line 117 and deliver_approved.py line 332.

  anything else -> LOUD FAILURE. An unknown renderer means unknown geometry,
      and misaligned captions on a public post are exactly the kind of
      confidently-wrong artifact this project refuses to ship.

AND THE ARITHMETIC IS CROSS-CHECKED AGAINST A REAL MEASUREMENT, NOT TRUSTED.
clips.duration_s is an ffprobe'd fact about the shipped file. The derived
window must reproduce it within CLIP_DURATION_TOL_S or the build fails loudly.
Measured on the eight real delivered clips in the consolidated database, the
formula reproduces the measured duration with a maximum error of 0.0134 s
(sub-frame at 30 fps), so the 0.5 s tolerance is roughly 37x the observed
worst case and still far too tight to hide a wrong basis.

A window with NO transcript segments produces NO SRT (None, count 0) rather
than an empty file. Under-claim: no captions beats invented captions.

Connects via the shared adapter app/workers/db.py (CLPR_DB_URL). Tables per
app/docs/naming-map.md. RESULT line last. Exits non-zero on any failure and
prints the failure verdict to stderr (D-047).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import db
import render_from_slice
import slice_geometry
import transcript_signal as ts

# The derived clip window must reproduce the ffprobe'd clips.duration_s within
# this many seconds. See the module docstring for the measured basis.
CLIP_DURATION_TOL_S = 0.5

# clips.created_by_run prefix -> geometry basis. The prefixes are the run_id
# literals in the renderers themselves: cut_clip.py line 95, deliver_approved.py
# line 286, render_from_slice.py's run_id assignment.
RUN_PREFIX_BASIS: tuple[tuple[str, str], ...] = (
    ('render_from_slice_', 'sidecar'),
    ('cut_clip_', 'formula'),
    ('deliver_adjusted_', 'formula'),
)

# The shortest cue this writes. A zero-length cue is invalid SRT and some
# players drop every cue after one. NOTE: this no longer does the
# boundary-filter job (see MIN_SEGMENT_OVERLAP_S below, which is 500x
# larger and is checked first); it is kept only as a defensive floor
# against a degenerate zero-length cue, which the overlap check already
# makes unreachable in practice.
MIN_CUE_S = 0.001

# Operator ruling, 2026-08-08 (golden-review F8/MK-01, proven by execution):
# a boundary segment counts as IN THE CLIP only when at least this many
# seconds of its OWN span fall inside the clip window. Below this floor the
# segment is dropped entirely -- from the cues, transcript_plain (the
# anti-invention quote gate's haystack) and transcript_lines -- rather than
# kept and clamped. Before this existed, rebase_segments clamped a boundary
# segment's TIMES to the clip but kept its FULL TEXT at any nonzero overlap
# (the old MIN_CUE_S = 0.001 floor): measured, a clip at 100.0-110.0s with a
# segment at 91.0-100.05s produced a cue (0.0, 0.05) carrying the whole
# 9-second sentence, which then (a) let the quote gate accept a quotation of
# speech the viewer never hears, (b) told Gemini's vision prompt a 9s
# sentence happened in 50ms, and (c) shipped in the .srt as a sentence
# flashed for 0.05s. 0.5s is the operator's chosen floor; do not adjust it
# without a fresh ruling.
MIN_SEGMENT_OVERLAP_S = 0.5


def basis_for_run(created_by_run: str) -> str:
    """Which geometry witness applies, from the renderer that actually ran."""
    run = str(created_by_run or '')
    for prefix, basis in RUN_PREFIX_BASIS:
        if run.startswith(prefix):
            return basis
    raise RuntimeError(
        f'UNKNOWN_RENDERER: clips.created_by_run={run!r} matches no known renderer '
        f'({", ".join(p for p, _ in RUN_PREFIX_BASIS)}), so the clip geometry is '
        'unknown and the caption rebasing cannot be derived. Refusing to guess: '
        'misaligned captions would ship silently.'
    )


def srt_timestamp(t_s: float) -> str:
    """Seconds -> SRT 'HH:MM:SS,mmm'. Negative input is a caller bug."""
    if t_s < 0:
        raise RuntimeError(f'negative SRT timestamp: {t_s}')
    total_ms = int(round(float(t_s) * 1000.0))
    ms = total_ms % 1000
    total_s = total_ms // 1000
    s = total_s % 60
    m = (total_s // 60) % 60
    h = total_s // 3600
    return f'{h:02d}:{m:02d}:{s:02d},{ms:03d}'


def rebase_segments(segments: list, clip_t0_abs_s: float, clip_duration_s: float) -> list[tuple[float, float, str]]:
    """Absolute transcript segments -> clip-relative cues, clamped to the clip.

    A segment is IN THE CLIP only when at least MIN_SEGMENT_OVERLAP_S of its
    own span falls inside the clip window (operator ruling 2026-08-08,
    golden-review F8/MK-01). A segment that clears that bar is kept and its
    TIMES are clamped to the clip (the operator's pad routinely cuts into
    the first and last sentence, so a clamped-time cue for a segment that is
    substantially inside the clip is expected and correct); a segment that
    only grazes the boundary, has no overlap at all, or has no text, is
    dropped entirely -- and since transcript_plain and transcript_lines are
    both derived from this function's return value, a dropped segment never
    reaches the quote gate's haystack either.
    """
    cues: list[tuple[float, float, str]] = []
    for seg in segments:
        text = str(seg.text or '').strip()
        if not text:
            continue
        start = float(seg.start_s) - clip_t0_abs_s
        end = float(seg.end_s) - clip_t0_abs_s
        start = max(0.0, min(start, clip_duration_s))
        end = max(0.0, min(end, clip_duration_s))
        # Clamping to [0, clip_duration_s] IS the overlap computation: the
        # clamped span left after intersecting the segment's (unclamped)
        # relative times with the clip window is exactly how much of the
        # segment's own duration falls inside the clip. Checked BEFORE the
        # degenerate-cue floor below, because that floor is 500x smaller and
        # would let a boundary sliver through.
        overlap_s = end - start
        if overlap_s < MIN_SEGMENT_OVERLAP_S:
            continue
        if end - start < MIN_CUE_S:
            continue
        cues.append((start, end, text))
    cues.sort(key=lambda c: (c[0], c[1]))
    return cues


def render_srt(cues: list[tuple[float, float, str]]) -> str | None:
    """Cues -> SRT text. None when there are no cues (never an empty file)."""
    if not cues:
        return None
    blocks = []
    for index, (start, end, text) in enumerate(cues, start=1):
        blocks.append(
            f'{index}\n{srt_timestamp(start)} --> {srt_timestamp(end)}\n{text}\n'
        )
    # LF line endings, trailing blank line after the final cue (SRT requires
    # the cue separator, and every modern parser accepts LF).
    #
    # The final '\n' is what makes that comment TRUE. Each block already ends
    # with one newline, so the join alone left the last cue with no separator
    # after it while the comment above promised one: a doc-versus-code
    # disagreement in a file format some strict parsers do reject on. The
    # cheap fix is the code, not the comment.
    return '\n'.join(blocks) + '\n'


def fetch_clip_geometry_inputs(cur, candidate_id: int) -> dict:
    """Everything the rebasing needs, from the DB, in one read.

    The clips row is REQUIRED: without it nothing was rendered, so there is no
    clip to caption.
    """
    cur.execute(
        '''
        SELECT c.recording_id, c.start_s, c.end_s,
               c.adjusted_start_s, c.adjusted_end_s,
               c.state, c.post_kit_enabled,
               r.session_label, r.duration_s,
               k.file_path, k.duration_s, k.created_by_run,
               k.drive_synced_at, k.drive_sync_path
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        LEFT JOIN clips k ON k.candidate_id = c.id
        WHERE c.id = %s
        ''',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'candidate_id not found: {candidate_id}')
    if row[9] is None:
        raise RuntimeError(
            f'CLIP_NOT_RENDERED: candidate_id={candidate_id} has no clips row, so no '
            'clip exists to caption or describe. Render it first (the verdict webhook '
            'runs workers/render_from_slice.py, or run workers/deliver_approved.py on '
            'the Mac).'
        )
    if row[10] is None:
        raise RuntimeError(
            f'CLIP_DURATION_MISSING: clips.duration_s is NULL for candidate_id={candidate_id}, '
            'so the shipped clip length is unknown and the caption rebasing cannot be '
            'cross-checked.'
        )
    return {
        'recording_id': int(row[0]),
        'start_s': float(row[1]),
        'end_s': float(row[2]),
        'adjusted_start_s': float(row[3]) if row[3] is not None else None,
        'adjusted_end_s': float(row[4]) if row[4] is not None else None,
        'state': str(row[5]),
        'post_kit_enabled': int(row[6]),
        'session_label': str(row[7]),
        'recording_duration_s': float(row[8]) if row[8] is not None else None,
        'clip_file_path': str(row[9]),
        'clip_duration_s': float(row[10]),
        'created_by_run': str(row[11] or ''),
        'drive_synced_at': str(row[12]) if row[12] is not None else None,
        'drive_sync_path': str(row[13]) if row[13] is not None else None,
    }


def resolve_clip_zero(info: dict, candidate_id: int, slices_dir: str | None) -> dict:
    """Absolute recording time of the shipped clip's t=0, plus its basis.

    The cross-check against the ffprobe'd clips.duration_s is what makes this a
    verified number rather than a plausible one.
    """
    basis = basis_for_run(info['created_by_run'])
    eff_start_s, eff_end_s = slice_geometry.effective_window(
        info['start_s'], info['end_s'],
        info['adjusted_start_s'], info['adjusted_end_s'],
    )
    pad = slice_geometry.PUBLISH_PAD_S
    rec_dur = info['recording_duration_s']

    if basis == 'sidecar':
        if not slices_dir:
            raise RuntimeError(
                f'SRT_GEOMETRY_UNAVAILABLE: candidate_id={candidate_id} was rendered by '
                f'{info["created_by_run"]} (slice renderer), so its geometry witness is the '
                'staged slice sidecar, but CLPR_SLICES_DIR is not set on this machine. '
                'Run this where the slices live, or pass --slices-dir.'
            )
        slice_path = Path(slices_dir) / f'c{candidate_id}.mp4'
        # One truth for sidecar validation: render_from_slice's own loader.
        sidecar = render_from_slice.load_sidecar(slice_path.with_suffix('.json'), candidate_id)
        abs_start_s = float(sidecar['abs_start_s'])
        abs_end_s = float(sidecar['abs_end_s'])
        # render_from_slice.py: offset_s = max(0, target_start_abs - abs_start_s),
        # and clip t=0 is abs_start_s + offset_s.
        clip_t0_abs_s = abs_start_s + max(0.0, (eff_start_s - pad) - abs_start_s)
        # Its end clamps to what the slice actually holds.
        clip_end_abs_s = min(eff_end_s + pad, abs_end_s)
    else:
        # cut_clip.py line 117 / deliver_approved.py line 332: clamp to the
        # recording's own edges.
        if rec_dur is None:
            raise RuntimeError(
                f'SRT_GEOMETRY_UNAVAILABLE: recordings.duration_s is NULL for '
                f'recording_id={info["recording_id"]}, so the formula basis cannot clamp.'
            )
        clip_t0_abs_s = ts.clamp(eff_start_s - pad, 0.0, rec_dur)
        clip_end_abs_s = ts.clamp(eff_end_s + pad, 0.0, rec_dur)

    derived_duration_s = clip_end_abs_s - clip_t0_abs_s
    measured_duration_s = info['clip_duration_s']
    delta = abs(derived_duration_s - measured_duration_s)
    if delta > CLIP_DURATION_TOL_S:
        raise RuntimeError(
            f'SRT_GEOMETRY_MISMATCH: candidate_id={candidate_id} basis={basis} derives a clip '
            f'window of {derived_duration_s:.3f}s but the shipped file measures '
            f'{measured_duration_s:.3f}s (ffprobe, clips.duration_s), a disagreement of '
            f'{delta:.3f}s which exceeds {CLIP_DURATION_TOL_S}s. The caption rebasing would be '
            'wrong by roughly that much, so nothing is written. Check whether the candidate '
            'window was edited after the clip was rendered.'
        )

    return {
        'basis': basis,
        'clip_t0_abs_s': clip_t0_abs_s,
        'clip_end_abs_s': clip_end_abs_s,
        'derived_duration_s': derived_duration_s,
        'measured_duration_s': measured_duration_s,
        'duration_delta_s': delta,
        'eff_start_s': eff_start_s,
        'eff_end_s': eff_end_s,
    }


def build_for_candidate(cur, candidate_id: int, slices_dir: str | None) -> dict:
    """The full deterministic product: geometry, cues, SRT text, transcript.

    The transcript returned here is built only from segments with at least
    MIN_SEGMENT_OVERLAP_S of their own span inside the clip's actual
    coverage (rebase_segments enforces the cutoff; a boundary sliver is
    dropped, not kept whole), so the same text feeds the captions, the
    vision prompt and the writer prompt. Anything a model quotes therefore
    has substantial overlap with the shipped clip's audio, not a boundary
    sliver and not merely somewhere in the recording.
    """
    info = fetch_clip_geometry_inputs(cur, candidate_id)
    geom = resolve_clip_zero(info, candidate_id, slices_dir)

    t0 = geom['clip_t0_abs_s']
    # The clip's REAL length is the measured one, so cues clamp to the file the
    # operator actually has.
    duration = geom['measured_duration_s']

    segments = ts.fetch_segments(cur, info['recording_id'])
    window_segments = ts.transcript_slice_for_window(segments, t0, t0 + duration)
    cues = rebase_segments(window_segments, t0, duration)
    srt_text = render_srt(cues)

    # Clip-relative, for prompts: a model reasoning about "the first two
    # seconds" needs clip time, not recording time.
    transcript_lines = ''.join(f'[{s:.2f}-{e:.2f}] {t}\n' for s, e, t in cues)
    # Plain text only, for the verbatim-quote gate.
    transcript_plain = ' '.join(t for _s, _e, t in cues)

    return {
        'candidate_id': candidate_id,
        'info': info,
        'geometry': geom,
        'cue_count': len(cues),
        'srt_text': srt_text,
        'transcript_lines': transcript_lines,
        'transcript_plain': transcript_plain,
    }


def run(candidate_id: int, out_path: str | None, slices_dir: str | None) -> int:
    conn = db.connect()
    try:
        cur = conn.cursor()
        result = build_for_candidate(cur, candidate_id, slices_dir)
    finally:
        conn.close()

    geom = result['geometry']
    srt_text = result['srt_text']

    if out_path:
        if srt_text is None:
            raise RuntimeError(
                f'NO_TRANSCRIPT_IN_WINDOW: candidate_id={candidate_id} has no transcript '
                'segments inside the shipped clip, so there is nothing to caption and no '
                'file is written. An empty SRT would be a claim that nothing was said.'
            )
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(srt_text, encoding='utf-8')

    print(
        f'GEOMETRY candidate={candidate_id} basis={geom["basis"]} '
        f'clip_t0_abs_s={geom["clip_t0_abs_s"]:.3f} '
        f'derived_duration_s={geom["derived_duration_s"]:.3f} '
        f'measured_duration_s={geom["measured_duration_s"]:.3f} '
        f'delta_s={geom["duration_delta_s"]:.3f}'
    )
    print(
        f'RESULT build_srt candidate={candidate_id} ok=1 cues={result["cue_count"]} '
        f'basis={geom["basis"]} written={"1" if out_path else "0"} '
        f'file="{out_path or ""}"'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Build the rebased SRT (post kit product 2) for one candidate'
    )
    parser.add_argument('--candidate-id', type=int, required=True)
    parser.add_argument('--out', type=str, default=None, help='write the SRT here')
    parser.add_argument(
        '--slices-dir', type=str, default=None,
        help='override CLPR_SLICES_DIR (only needed for slice-rendered clips)',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    slices_dir = args.slices_dir or os.environ.get('CLPR_SLICES_DIR', '').strip() or None
    return run(args.candidate_id, args.out, slices_dir)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
