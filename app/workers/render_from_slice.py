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
allow-list dir, D-044) / <session_label>_<offset>_<category>_c<candidate_id>.mp4
(e.g. 2026-08-04_1910_00h14m32s_funny_c109.mp4) — the ONE delivered-clip
naming convention, built by deliver_approved.delivered_name() and imported
here rather than reimplemented (D-068, 2026-08-07).

D-063 BURNED-IN SPEECH CAPTIONS (2026-08-07 ruling: "C" — on demand, a UI
option while approving). When clip_candidates.burn_captions = 1 this render
builds the clip's SRT from the stored whisper segments and burns it in during
the SAME pass, by appending ONE subtitles filter to the end of the D-023 chain
— after the scale/overlay/fps, so the text is drawn at final 1080x1920 and is
never blurred or resampled. When the flag is 0 the ffmpeg argv is byte-identical
to the pre-D-063 command: the D-023 chain is stored as its body plus its '[v]'
label, and the caption filter is spliced between them, so not one byte of the
operator-proven chain is rewritten on the default path.

Three ways this refuses to lie about a burn (a clip recorded as captioned that
has no captions is the exact failure class this project keeps paying for):
  - CAPABILITY IS PROBED, NEVER ASSUMED. `ffmpeg -filters` must really list
    `subtitles` or the render fails with CAPTIONS_UNSUPPORTED naming the host.
    The operator's Mac genuinely lacks it (no libass), so this is not a
    theoretical branch.
  - NO SPEECH IN THE WINDOW IS NOT A FAILURE. No cues means no SRT, no filter,
    and clips.captions_burned = 0 with captions_cue_count = 0 — recorded
    honestly, render still succeeds.
  - THE CLIP ROW CARRIES BOTH FACTS, frozen at render time
    (captions_requested / captions_burned / captions_cue_count, 006), so no
    later reader has to infer anything from a candidate flag the operator may
    have flipped since.

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
import re
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import db
import slice_geometry

try:
    import deliver_approved
except ModuleNotFoundError:
    from . import deliver_approved
# NOTE: imported as the WHOLE module (deliver_approved.delivered_name), not
# `from deliver_approved import delivered_name` -- the latter is a real
# ImportError here. deliver_approved imports cut_clip, which imports THIS
# module (render_from_slice), which was importing deliver_approved: a
# 3-module cycle. `from X import Y` needs Y to already exist on X's
# (partially-initialized) module object at that point in the chain, and it
# does not yet -- deliver_approved's `import cut_clip` line runs before its
# `delivered_name` def. `import X` (binding the module, not the name) defers
# attribute access to call time, by which point every module has finished
# loading, so it is safe -- confirmed empirically (see verification output).
# This matches cut_clip.py's own existing pattern for importing THIS module.

# Containment tolerance (seconds): absorbs container-duration rounding; this
# check is a safety net against edits the slice cannot serve, not frame-exact.
CONTAIN_TOL_S = 0.25

# Belt-check tolerance (seconds): the sidecar's recorded slice length vs the
# actual file's ffprobe'd length. Any larger disagreement means the file under
# c<id>.mp4 is not the file the sidecar witnessed => STALE_SLICE.
STALE_TOL_S = 0.35

SIDECAR_SCHEMA = 1  # must match slice_candidates.SIDECAR_SCHEMA

# ---------------------------------------------------------------------------
# D-063 BURNED-IN CAPTIONS
# ---------------------------------------------------------------------------

# THE D-023 CHAIN, SPLIT AT ITS OUTPUT LABEL AND NOWHERE ELSE.
#
# FILTER_COMPLEX_D023 below is byte-for-byte the string cut_clip.py has shipped
# since D-023 (operator-proven on a live Instagram post). It is stored as
# body + '[v]' purely so a caption filter can be spliced in FRONT of the label
# without rewriting a single byte of the proven chain. With captions off the
# concatenation reproduces the original literal exactly, which the inert proof
# checks by diffing the whole ffmpeg argv against the pre-change command.
FILTER_COMPLEX_D023_BODY = (
    '[0:v]split=2[bg][fg];'
    '[bg]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,gblur=sigma=20[bg2];'
    '[fg]scale=1080:-2:flags=lanczos[fg2];'
    '[bg2][fg2]overlay=(W-w)/2:(H-h)/2,fps=30'
)
FILTER_COMPLEX_D023 = FILTER_COMPLEX_D023_BODY + '[v]'

# THE CAPTION STYLE, IN libass UNITS, MEASURED ON THE REAL SERVER RENDERER.
#
# Every number here was calibrated against the n8n container's own ffmpeg
# (5.1.9 + libass) on 2026-08-07 by rendering 1080x1920 frames and measuring
# where the ink actually landed — never derived from theory.
#
# THE UNIT TRAP THAT MAKES THESE NUMBERS LOOK WRONG: ffmpeg converts an SRT to
# ASS with the ASS default canvas PlayRes 384x288, and libass scales that
# canvas onto the frame on each axis independently. Measured scale factors are
# therefore 1920/288 = 6.667 vertical and 1080/384 = 2.8125 horizontal. So
# these are NOT pixels. Multiply before judging them:
#
#   FontSize=15   -> 15 * 6.667  = 100 px em on a 1920-tall frame (5.2% of the
#                    frame height). Measured: a 59-character segment, the 88th
#                    percentile of this project's 8,466 real transcript
#                    segments, wraps to 4 short lines spanning 18%..83% of the
#                    width.
#   MarginV=68    -> 68 * 6.667  = 453 px of clearance under the text (23.6% of
#                    the height). The review UI's own platform guide marks the
#                    bottom 22% as the platform's UI, so this clears it with
#                    room, measured at 452-472 px across real segments.
#   MarginL/R=62  -> 62 * 2.8125 = 174 px each side (16.1%). The same UI marks
#                    a right rail 16% wide for the platform's buttons, so the
#                    text band stops exactly short of it and stays clear of the
#                    left edge by the same amount. This is also what keeps the
#                    lines SHORT: usable width is 68% of the frame.
#   Outline=2.5 + Shadow=1 on a black outline with white fill — a stroke, not a
#                    box, so the frame never reads as "majority text" (which
#                    Instagram demotes) while staying legible on any
#                    background. Verified visually on a white frame and on a
#                    saturated colour-bar frame pulled back from the server.
#   FontName      -> Liberation Sans, chosen from the container's ACTUAL font
#                    list (fc-list: 16 faces, the four Liberation families).
#                    Never a font nobody has seen on the box.
#   Alignment=2   -> bottom centre, stated explicitly rather than inherited.
CAPTION_FORCE_STYLE = (
    'FontName=Liberation Sans,Bold=1,FontSize=15,'
    'PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BackColour=&H80000000,'
    'BorderStyle=1,Outline=2.5,Shadow=1,'
    'Alignment=2,MarginL=62,MarginR=62,MarginV=68'
)

# FILTERGRAPH ESCAPING, DETERMINED EMPIRICALLY, NOT FROM THE DOCS.
#
# An option value inside a filtergraph is unescaped TWICE, and the two passes
# have DIFFERENT special characters. Measured 2026-08-07 by feeding hostile
# paths through a real ffmpeg and reading back the path it reported opening:
#
#   pass 2 (innermost, the filter's own option tokenizer): \ ' : = ,
#   pass 1 (outermost, the filtergraph description tokenizer): \ ' , ; [ ]
#
# So a value is escaped for pass 2 first, then the RESULT is escaped for pass
# 1. Everything simpler fails: quoting alone silently DELETES an apostrophe
# (the second pass eats it as a quote character), and single-escaping a colon
# lets the second pass split the option there.
#
# Verified end to end against the real `subtitles` filter on the n8n container:
#   /tmp/no such dir/it's a:test,x;y[z]/cap.srt
# emitted as
#   /tmp/no such dir/it\\\'s a\\:test\\\,x\;y\[z\]/cap.srt
# and ffmpeg reported opening exactly the original path. A path with no special
# characters passes through completely unchanged.
FILTER_ESCAPE_PASS2 = "\\':=,"
FILTER_ESCAPE_PASS1 = "\\',;[]"


# CUE LENGTH, CAPPED — BECAUSE libass WRAPS FOREVER AND THE FRAME DOES NOT.
#
# The style above deliberately leaves only 68% of the frame width usable
# (MarginL/R clear the platform's right-hand button rail), and at a 100 px em
# that is roughly 13-15 characters per line. libass will happily wrap a long
# cue into as many lines as it needs and draw them straight up the frame, so
# the cue's CHARACTER COUNT is what decides how much of the picture the text
# covers. Nothing capped it.
#
# MEASURED on this project's real corpus (7,252 non-empty transcript segments
# in app/clpr.db.archive-20260806, read in full — not sampled):
#   p50 = 35 chars, p88 = 63, p95 = 85, p99 = 110, max = 220.
# At ~13 chars/line and a ~120 px line height, 110 chars is about 8 lines
# (~47% of the frame height) and 220 chars is about 16 lines, which OVERFLOWS
# the 1,467 px available above MarginV entirely. A frame that is half text is
# the exact "majority text" shape D-061 says these clips must not become, so
# this is not a cosmetic nicety.
#
# THE CAP IS 42 CHARACTERS ~= 3 lines ~= 19% of the frame height. 2,643 of the
# 7,252 segments (36%) are longer and get split into consecutive sub-cues at
# word boundaries. No text is ever dropped, reordered or hyphenated.
MAX_CAPTION_CHARS = 42

# ...BUT A SPLIT THAT FLASHES IS WORSE THAN A LONG LINE, so the number of
# sub-cues is clamped by TIME as well as by length: no sub-cue is ever given
# less than this. A 43-character segment that lasts 1.0 s (the tightest real
# case in the corpus) is therefore left whole rather than cut into two 0.5 s
# flashes — 43 chars in 1.0 s was already unreadable and splitting it would
# only make it blink. Every genuinely long segment has the time to spare: the
# corpus's longest are 220 chars/17.0 s, 192/22.0 s, 188/18.0 s.
MIN_SUBCUE_S = 0.8


# WHISPER'S NON-SPEECH ANNOTATIONS ARE NOT SPEECH, AND MUST NOT BE BURNED.
#
# whisper emits bracketed stage directions for everything it hears that is not
# words: [BLANK_AUDIO], [INAUDIBLE], (COUGHING), (crowd murmuring), [ Silence ].
# They are useful in a subtitle FILE, where a viewer reads them as description.
# Burned into the picture at a 100 px em they are just wrong text on the
# operator's clip, and D-063's own noun is "speech captions".
#
# MEASURED on the full corpus (7,252 segments in app/clpr.db.archive-20260806,
# read in full): 970 bracketed spans, of which 946 survive rebasing and 940 are
# the ENTIRE cue. [BLANK_AUDIO] alone appears in 578 segments — 8% of every
# segment this project has ever transcribed — so at roughly 8 segments per clip
# the FIRST captioned clip would almost certainly have carried one.
#
# THE RULE IS DELIBERATELY THE NARROW ONE: drop a cue whose WHOLE text is
# annotation, never edit text inside a cue. An earlier draft stripped bracketed
# spans wherever they appeared, and a wider draft still keyed on the span being
# upper-case — which the corpus killed outright, because all 35 distinct
# lower-case bracketed spans ((indistinct), (laughs), (door closes)) are
# annotations too and NOT ONE bracketed span in the whole corpus is speech. So
# case discriminates nothing and is not used. What the narrow rule gives up is
# ~6 cues where an annotation trails real speech ("Evan McPherson. (clears
# throat)"); those keep their text byte-intact, which is the safe direction —
# deleting a word the operator actually said is the failure that matters.
#
# A window that holds nothing but annotations therefore ends with NO cues, and
# falls into the existing no-speech path: captions_requested=1,
# captions_burned=0, captions_cue_count=0. That is not a workaround, it is
# literally true — nobody spoke.
ANNOTATION_ONLY_RE = re.compile(r'^(?:[\[(][^\[\]()]*[\])]\s*)+$')


def is_annotation_only(text: str) -> bool:
    """Is this cue nothing but whisper non-speech annotation(s)?"""
    clean = normalize_cue_text(text)
    return bool(clean) and bool(ANNOTATION_ONLY_RE.match(clean))


def drop_annotation_cues(cues: list) -> list:
    """Remove cues that carry no speech at all. Never edits a cue's text."""
    return [c for c in cues if not is_annotation_only(c[2])]


def normalize_cue_text(text: str) -> str:
    """Collapse ALL whitespace in a cue to single spaces.

    Two reasons, and the second one is a real format bug rather than tidiness:
    an SRT cue block is TERMINATED by a blank line, so a cue whose text
    contains one silently ends the block early and the remainder is parsed as a
    new (malformed) cue. Zero of this project's 7,252 stored segments contain a
    newline today, so this is latent rather than live — but the text is now
    burned into pixels with captions_burned=1 asserted about it, and a latent
    parse bug in something the operator cannot inspect afterwards is not a
    thing to leave standing.
    """
    return ' '.join(str(text).split())


def split_cue_text(text: str, max_chars: int = MAX_CAPTION_CHARS) -> list[str]:
    """Split one cue's text into <= max_chars chunks at word boundaries.

    Balanced by word count across the minimum number of chunks that can hold
    the text, so 63 characters becomes two ~32-character chunks rather than a
    42 and a 21. Guarantees, all of them checked by the corpus proof:
      - ' '.join(result) == normalize_cue_text(text). Nothing is lost, nothing
        is reordered, nothing is invented.
      - result is never empty for non-empty text.
      - a single word longer than max_chars (a URL, a pasted identifier) is
        kept WHOLE in its own oversized chunk. Never hyphenated, and never a
        loop that cannot make progress.
    """
    clean = normalize_cue_text(text)
    if not clean:
        return []
    if len(clean) <= max_chars:
        return [clean]
    # Fewest parts the cap allows (greedy word-packing at the cap is optimal
    # for "minimum number of lines"), then rebalanced to that same count.
    return pack_into_at_most(clean, len(pack_words(clean.split(), max_chars)))


def pack_words(words: list[str], budget: int) -> list[str]:
    """Greedy word-packing into lines of at most `budget` characters.

    A single word longer than the budget gets a line to itself rather than
    being hyphenated or dropped, so this always terminates and is always
    lossless.
    """
    chunks: list[list[str]] = [[]]
    length = 0
    for word in words:
        add = len(word) if not chunks[-1] else len(word) + 1
        if chunks[-1] and length + add > budget:
            chunks.append([word])
            length = len(word)
        else:
            chunks[-1].append(word)
            length += add
    return [' '.join(c) for c in chunks if c]


def pack_into_at_most(text: str, n_max: int) -> list[str]:
    """Pack `text` into at most n_max parts, as EVENLY as those parts allow.

    THE BALANCE IS THE POINT, and the corpus proof rejected two earlier
    versions of this function for lacking it. Packing greedily straight to the
    cap produces a full first part and a stub tail — 46 characters became "42
    chars" + "some" — and a 4-character stub is the part that most needs its
    reading time and gets the least of it. Packing to total/n_max instead
    tends to OVERSHOOT the part count on word boundaries (47 characters wanting
    2 parts came out as 3), which then tripped the time clamp and gave up on
    splitting altogether.

    So: scan the budget upward from the even share and take the FIRST budget
    that fits in n_max parts. That is the most even packing that still respects
    the part count. It always terminates, because a budget of len(text) packs
    into one part.
    """
    words = text.split()
    if not words:
        return []
    n_max = max(1, n_max)
    for budget in range(max(1, math.ceil(len(text) / n_max)), len(text) + 1):
        parts = pack_words(words, budget)
        if len(parts) <= n_max:
            return parts
    return [text]


def split_long_cues(cues: list, max_chars: int = MAX_CAPTION_CHARS) -> list:
    """Cap every cue's on-screen text, dividing its time span across the parts.

    The number of parts is clamped so that no part is on screen for less than
    MIN_SUBCUE_S, and a cue with no time to spare is returned unchanged: an
    over-long line is a worse frame, a 0.3 s flash is an unreadable one, and
    only one of those two is recoverable by looking at the clip.

    TIME IS DIVIDED EQUALLY, NOT IN PROPORTION TO CHARACTER COUNT. Proportional
    division was the first attempt and the corpus proof rejected it: a short
    part is exactly the one that most needs its full reading time, and
    proportional division gives it the least. Equal division makes the floor
    PROVABLE rather than approximate — every part gets span/len(parts), and
    len(parts) is already clamped so that quotient is at least MIN_SUBCUE_S.
    It costs almost nothing in accuracy because split_cue_text now balances the
    parts by length, so equal time is close to proportional time anyway, and
    the drift inside one whisper segment is well under a second.

    Every cue is normalized whether or not it is split.
    """
    out: list = []
    for start, end, text in cues:
        clean = normalize_cue_text(text)
        if not clean:
            continue
        start = float(start)
        end = float(end)
        span = end - start
        parts = split_cue_text(clean, max_chars)
        if len(parts) > 1:
            allowed = max(1, int(span // MIN_SUBCUE_S)) if span > 0 else 1
            if allowed < len(parts):
                # Not enough time for the full split. Re-pack into the number
                # of parts the clock can actually carry — the clock wins, so
                # these parts are allowed to exceed MAX_CAPTION_CHARS. An
                # over-long line is a worse frame; a sub-second flash is an
                # unreadable one. allowed == 1 gives the original cue back.
                parts = pack_into_at_most(clean, allowed)
        if len(parts) <= 1:
            out.append((start, end, clean))
            continue
        # Equal slices, with the LAST end pinned to the original end so
        # floating-point drift can never extend a cue past the one it came from.
        step = span / len(parts)
        cursor = start
        for i, part in enumerate(parts):
            part_end = end if i == len(parts) - 1 else start + step * (i + 1)
            out.append((cursor, part_end, part))
            cursor = part_end
    return out


def escape_filter_value(value: str) -> str:
    """Escape one filtergraph option VALUE for both of ffmpeg's unescape passes."""
    inner = ''.join(('\\' + ch) if ch in FILTER_ESCAPE_PASS2 else ch for ch in value)
    return ''.join(('\\' + ch) if ch in FILTER_ESCAPE_PASS1 else ch for ch in inner)


def subtitles_filter(srt_path: str) -> str:
    """The single filter appended to the D-023 chain when captions are on."""
    return (
        f'subtitles=filename={escape_filter_value(str(srt_path))}'
        f':force_style={escape_filter_value(CAPTION_FORCE_STYLE)}'
    )


def ffmpeg_has_subtitles_filter() -> bool:
    """Does THIS ffmpeg actually carry the subtitles filter (libass)?

    Probed, never assumed: the operator's Mac ffmpeg is built without libass
    and physically cannot burn anything, while the n8n container's can. A probe
    that cannot itself fail loudly is not a probe (charter 1.5 gate 2), so a
    failing `ffmpeg -filters` raises rather than quietly reporting False.
    """
    proc = run_capture(['ffmpeg', '-hide_banner', '-filters'])
    for line in (proc.stdout or '').splitlines():
        parts = line.split()
        # Every filter line is "<flags> <name> <io> <description>".
        if len(parts) >= 2 and parts[1] == 'subtitles':
            print(f'CAPTION_PROBE subtitles_filter_present=1 line="{line.strip()}"')
            return True
    print('CAPTION_PROBE subtitles_filter_present=0')
    return False


def require_subtitles_capability(candidate_id: int) -> None:
    """Fail loudly, naming this machine, when asked to burn on a build that cannot."""
    if ffmpeg_has_subtitles_filter():
        return
    raise RuntimeError(
        f'CAPTIONS_UNSUPPORTED: candidate_id={candidate_id} asks for burned-in captions '
        f'(clip_candidates.burn_captions=1) but the ffmpeg on host '
        f'"{socket.gethostname()}" has no `subtitles` filter, so it was built without '
        'libass and cannot burn anything. Refusing to render: an uncaptioned file '
        'delivered against an explicit request is exactly the lie this check exists to '
        'prevent. Fixes: render this candidate on the n8n server lane (its ffmpeg has '
        'libass), or install an ffmpeg build that includes libass on this machine, or '
        'untick captions for this clip in the review UI.'
    )


def build_caption_srt_text(cur, recording_id: int, clip_t0_abs_s: float,
                           clip_duration_s: float) -> tuple[str | None, int]:
    """The clip's SRT text and cue count, from the stored whisper segments.

    ONE TRUTH FOR THE REBASING: build_srt.py owns it, and its own functions are
    used here rather than reimplemented, so a burned caption and the post kit's
    downloadable .srt can never drift apart.

    What is deliberately NOT reused is build_srt.build_for_candidate: it starts
    from the clips row and cross-checks its derived window against the ffprobe'd
    clips.duration_s. Neither exists yet at this point — the file is about to be
    encoded. Here the clip's t=0 and length are not derived at all, they ARE the
    -ss and -t this very command is about to pass to ffmpeg, so the cross-check
    has nothing to check and no basis to guess.

    Returns (srt_text, cue_count). srt_text is None when the window holds no
    speech, which is an honest and expected outcome, never an error.
    """
    # Deferred imports: build_srt imports THIS module (for load_sidecar), so a
    # module-level import here would be a cycle. By call time this module is
    # fully initialised, so the deferred import is safe and the default
    # (captions-off) path never pays for it at all.
    import build_srt
    import transcript_signal as ts

    segments = ts.fetch_segments(cur, recording_id)
    window_segments = ts.transcript_slice_for_window(
        segments, clip_t0_abs_s, clip_t0_abs_s + clip_duration_s
    )
    cues = build_srt.rebase_segments(window_segments, clip_t0_abs_s, clip_duration_s)
    # D-063 fixer: whisper's non-speech annotations are not speech. Dropped
    # before the split so an all-annotation window ends with zero cues and
    # takes the honest no-speech path rather than burning "[BLANK_AUDIO]".
    cues = drop_annotation_cues(cues)
    # D-063 fixer: cap the on-screen text. This is deliberately applied HERE,
    # in the burn path only, and NOT inside build_srt: the post kit's
    # downloadable .srt is read by a platform that does its own layout at its
    # own width, so it keeps whisper's segmentation, while the burned version
    # has to fit a frame whose usable width this module's own style chose. One
    # truth for the REBASING (build_srt) with a display constraint on top is
    # honest; pushing the constraint down into build_srt would change a
    # delivered artifact this ruling never touched.
    cues = split_long_cues(cues)
    return build_srt.render_srt(cues), len(cues)


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
               {category_select} AS category,
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

    return {
        'recording_id': int(row[0]),
        'start_s': float(row[1]),
        'end_s': float(row[2]),
        'adjusted_start_s': float(row[3]) if row[3] is not None else None,
        'adjusted_end_s': float(row[4]) if row[4] is not None else None,
        'state': str(row[5]),
        'session_label': str(row[6]),
        'category': str(row[7]) if row[7] is not None else 'unknown',
        'burn_captions': int(row[8]),
    }


def render_from_slice(candidate_id: int) -> int:
    run_id = f'render_from_slice_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    slices_dir = require_env('CLPR_SLICES_DIR')
    out_dir = Path(os.environ.get('CLPR_RENDER_OUT', '/home/node/.n8n-files').strip()
                   or '/home/node/.n8n-files')

    # D-063: declared out here so the OUTERMOST finally can remove the scratch
    # SRT on literally every exit path, not merely the ones around the encode.
    srt_path: Path | None = None

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
        out_path = out_dir / deliver_approved.delivered_name(
            cand['session_label'], cand['start_s'], cand['category'], candidate_id
        )

        if out_path.exists():
            out_path.unlink()

        # Copied EXACTLY from cut_clip.py (operator-proven live on Instagram, D-023).
        filter_complex = FILTER_COMPLEX_D023

        # ---- D-063 captions: build the SRT and splice ONE filter in ---------
        # Everything in this block is skipped entirely when the flag is 0, and
        # `filter_complex` above is then the pre-D-063 literal byte for byte.
        captions_requested = 1 if cand['burn_captions'] == 1 else 0
        captions_burned = 0
        captions_cue_count = None

        if captions_requested == 1:
            # Capability first: refuse before doing any work on a box that
            # cannot deliver what was asked (this is the Mac, in practice).
            require_subtitles_capability(candidate_id)

            # The clip's t=0 and length are the -ss/-t below, not a derivation.
            srt_text, captions_cue_count = build_caption_srt_text(
                cur, cand['recording_id'], abs_start_s + offset_s, cut_duration_s
            )
            if srt_text is None:
                # No speech in the shipped window. Nothing to burn, nothing
                # written, and the clip row will say so: requested 1, burned 0,
                # cues 0. Under-claim beats an empty caption track that claims
                # nothing was said.
                print(
                    f'CAPTIONS_NO_SPEECH candidate={candidate_id} cues=0 '
                    'burn_skipped=1 render_continues=1'
                )
            else:
                fd, tmp_name = tempfile.mkstemp(
                    prefix=f'clpr_c{candidate_id}_', suffix='.srt'
                )
                with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                    fh.write(srt_text)
                srt_path = Path(tmp_name)
                filter_complex = (
                    FILTER_COMPLEX_D023_BODY + ',' + subtitles_filter(srt_path) + '[v]'
                )
                captions_burned = 1
                print(
                    f'CAPTIONS_ON candidate={candidate_id} cues={captions_cue_count} '
                    f'srt="{srt_path}" bytes={len(srt_text.encode("utf-8"))}'
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

            # D-063: the three caption columns are written on EVERY render and
            # RESET by the DO UPDATE, never left from a previous one. A clip
            # re-rendered without captions must stop claiming it has them
            # (charter gate 4: a contract change is a breaking change until
            # every consumer is swept, and the stalest consumer is the row
            # itself).
            cur.execute(
                '''
                INSERT INTO clips(candidate_id, file_path, duration_s, state, created_by_run, created_at,
                                  captions_requested, captions_burned, captions_cue_count)
                VALUES (%s, %s, %s, 'rendered', %s, %s, %s, %s, %s)
                ON CONFLICT (candidate_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    duration_s = EXCLUDED.duration_s,
                    state = 'rendered',
                    created_by_run = EXCLUDED.created_by_run,
                    created_at = EXCLUDED.created_at,
                    captions_requested = EXCLUDED.captions_requested,
                    captions_burned = EXCLUDED.captions_burned,
                    captions_cue_count = EXCLUDED.captions_cue_count
                ''',
                (candidate_id, str(out_path), duration_s, run_id, utc_now_iso(),
                 captions_requested, captions_burned, captions_cue_count),
            )
            conn.commit()

            print(
                f'RESULT render_from_slice candidate={candidate_id} ok=1 '
                f'file="{out_path}" duration_s={duration_s:.3f} '
                f'captions_requested={captions_requested} captions_burned={captions_burned} '
                f'captions_cue_count={"" if captions_cue_count is None else captions_cue_count}'
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
        # The SRT is scratch, never a deliverable: it lives in the system temp
        # dir (never the render output dir, where it would look like something
        # shipped) and goes away on EVERY path — success, encode failure, or a
        # failure between writing it and reaching the encode.
        if srt_path is not None and srt_path.exists():
            srt_path.unlink()


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
