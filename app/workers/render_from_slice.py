#!/usr/bin/env python3
"""render_from_slice: render one approved candidate's PRE-STAGED slice to the
vertical 1080x1920 full-frame-fit-with-blur MP4 (D-053 workflow B, verdict router).

The input is the surgical slice Workflow A staged at $CLPR_SLICES_DIR/c<id>.mp4
(stream-copied SLICE_PAD_S-padded window around the IMMUTABLE ORIGINAL
start_s/end_s — slice_geometry, D-055). The render therefore ALWAYS TRIMS:
a 10s-padded slice rendered whole would ship a ~20s-too-long clip. The cut
window is the EFFECTIVE window (COALESCE(adjusted_*, original)) +/-
slice_geometry.render_pad_s(adjusted_start_s, adjusted_end_s) -- PUBLISH_PAD_S
for an unedited candidate, but 0.0 the moment the operator has adjusted
either edge, so a trim is never silently un-trimmed back open (live fix,
2026-08-08).

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
here rather than reimplemented (D-068, 2026-08-07). A RE-render of the same
candidate (clips.render_seq > 1, migration 011) appends _r<render_seq>
before the extension, e.g. ..._c109_r2.mp4 — see delivered_name()'s
docstring and the render_seq lookup below.

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

D-074 BURNED HOOK + OPERATOR COLOR CHOICES (2026-08-08 amendment to D-063 and
D-061). Three more opt-in columns on clip_candidates (migration 010), all
additive/nullable-or-defaulted: burn_hook (same intent shape as
burn_captions, DEFAULT 0), caption_color and hook_color (nullable hex, NULL =
renderer default). D-061 still holds by default -- the hook stays a CSS
overlay the operator types into the platform's own tool -- but ticking
burn_hook now burns it into the SAME render pass as the clip, via a SECOND
`subtitles` filter chained after the captions one (own .ass file, own style,
so D-063's calibrated caption geometry is never touched by the hook path).
Both the caption ink and the hook ink are operator-chosen colors, each with
its own AUTO-CONTRAST backing (WCAG relative luminance decides whether the
outline/box stays this renderer's original dark treatment or flips to a
light one) rather than a fixed style, per the operator's own rule:
"background and foreground should change based on readability." With
burn_hook=0 and both colors NULL -- every existing candidate, unedited --
the ffmpeg argv is byte-identical to the pre-D-074 build, same discipline as
D-063's own default path. The active post kit's hook_withheld variant is
what gets burned (post_kits has no selected-variant column; withheld is the
deliberate default, see fetch_active_hook_variant). No kit yet is NOT an
error: the render proceeds without the hook, logged loudly.

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

SIDECAR_SCHEMA = slice_geometry.SIDECAR_SCHEMA  # single source of truth now (was a hand-synced duplicate, SRD-06)

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
#   RETUNED 2026-08-08 to TV-SUBTITLE proportions. Operator, on seeing the
#   first burn: "you thought caption taking up most of screen was good idea?
#   make these like TV subtitles...bottom (where the bottom blur is)
#   appropriately sized so it only take 1/2 lines max". The old values were
#   internally consistent and still wrong: a 100 px em over a deliberately
#   NARROWED text band forces more wrapping, and six lines of 100 px type grows
#   UPWARD out of the blur band and over the speaker's face. Size and width
#   were fighting each other.
#
#   FontSize=9    -> 9 * 6.667   = 60 px em on a 1920-tall frame (3.1% of the
#                    height). Broadcast subtitles sit near 3%; the old 5.2% was
#                    display-copy scale, not caption scale.
#   MarginL/R=30  -> 30 * 2.8125 = 84 px each side (7.8%), so the usable band is
#                    912 px = 84% of the width, UP from 68%. Widening the band
#                    is what actually buys the line count: at ~31 px average
#                    advance for 60 px bold sans, a line holds ~29 characters,
#                    so the 40-char cap lands at TWO lines, never more.
#                    The right rail is no longer avoided: captions sit BELOW the
#                    video content in the blurred letterbox, where the
#                    platform's action buttons do not overlap them.
#   MarginV=68    -> 68 * 6.667  = 453 px of clearance under the text (23.6%).
#                    UNCHANGED, and now doing its real job. For 16:9 content
#                    fitted into 9:16 the picture ends 657 px above the bottom,
#                    and the platform's own UI covers the bottom 422 px, so the
#                    window for captions is 422..657 px. Two 72 px lines from a
#                    453 px baseline occupy 453..597 px — inside that window,
#                    in the blur, clear of both.
#   Outline=1.6 + Shadow=0.8 on a black outline with white fill — a stroke,
#                    not a box, so the frame never reads as "majority text"
#                    (which Instagram demotes) while staying legible on any
#                    background. CHANGED from Outline=2.5/Shadow=1 in the same
#                    2026-08-08 retune as the sizing above; that pair was the
#                    one visually verified against a white frame and a
#                    saturated colour-bar frame pulled back from the server.
#                    These current values have NOT yet had that same visual
#                    pass — re-verification against the retuned FontSize/
#                    margins is still pending.
#   FontName      -> Liberation Sans, chosen from the container's ACTUAL font
#                    list (fc-list: 16 faces, the four Liberation families).
#                    Never a font nobody has seen on the box.
#   Alignment=2   -> bottom centre, stated explicitly rather than inherited.
CAPTION_FORCE_STYLE = (
    'FontName=Liberation Sans,Bold=1,FontSize=9,'
    'PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BackColour=&H80000000,'
    'BorderStyle=1,Outline=1.6,Shadow=0.8,'
    'Alignment=2,MarginL=30,MarginR=30,MarginV=68'
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
# RE-DERIVED 2026-08-08 for the retuned geometry above (this section
# previously cited pre-retune numbers: a 68% usable band, a 100 px em and
# ~13-15 chars/line, none of which match CAPTION_FORCE_STYLE any more). The
# style above now leaves 84.4% of the frame's 1080 px width usable (912 px,
# after MarginL/R=30 scales to ~84 px each side), and at the 60 px em
# (FontSize=9 scaled onto the 1920-tall frame) that is ~29 characters per
# line at ~31 px average advance for 60 px bold sans (see the geometry block
# above). libass will still happily wrap a long cue into as many lines as it
# needs and draw them straight up the frame, so the cue's CHARACTER COUNT is
# still what decides how much of the picture the text covers. Nothing caps
# that except this constant.
#
# MEASURED on this project's real corpus (7,252 non-empty transcript segments
# in app/clpr.db.archive-20260806, read in full — not sampled):
#   p50 = 35 chars, p88 = 63, p95 = 85, p99 = 110, max = 220.
# At ~29 chars/line and a ~72 px line height (the 60 px em with normal
# leading, matching the "72 px lines" cited in the geometry block above), an
# UNCAPPED p99 segment (110 chars) would run to about 4 lines (~15% of the
# frame height) and the uncapped max (220 chars) to about 8 lines (~30%). A
# frame that is a third text is still well past the "majority text" shape
# D-061 says these clips must not become, so a cap is still not a cosmetic
# nicety, even though the retuned geometry no longer overflows the frame the
# way the pre-retune numbers did.
#
# THE CAP IS 40 CHARACTERS ~= 2 lines ~= 7.5% of the frame height (2 lines at
# ~72 px each on the 1920 px-tall frame). 2,643 of the 7,252 segments (36%)
# are longer and get split into consecutive sub-cues at word boundaries. No
# text is ever dropped, reordered or hyphenated.
MAX_CAPTION_CHARS = 40

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


def subtitles_filter(srt_path: str, caption_color: str | None = None) -> str:
    """The single filter appended to the D-023 chain when captions are on.

    caption_color (clip_candidates.caption_color, D-074) is None on every
    pre-D-074 candidate and on any candidate the operator never touched, and
    None reproduces CAPTION_FORCE_STYLE's original literal byte for byte via
    build_caption_force_style -- see that function.
    """
    return (
        f'subtitles=filename={escape_filter_value(str(srt_path))}'
        f':force_style={escape_filter_value(build_caption_force_style(caption_color))}'
    )


# ---------------------------------------------------------------------------
# D-074 HOOK BURN + OPERATOR COLOR CHOICES (2026-08-08)
#
# An amendment to D-063, not a replacement: burning the hook is an OPT-IN
# (clip_candidates.burn_hook, migration 010, DEFAULT 0), and picking a color
# is optional too (caption_color / hook_color, both nullable). With every one
# of those three columns at its default/NULL, this renderer's output is
# byte-identical to the pre-D-074 build -- verified by the inert proof.
#
# THE OPERATOR'S OWN REQUIREMENT: "background and foreground should change
# based on readability." So the backing (outline + semi-opaque box) is never
# a fixed choice, it is DERIVED from the chosen ink color's WCAG relative
# luminance -- light ink keeps this renderer's original dark backing, dark
# ink flips to a light one. The review UI duplicates this exact formula in
# JavaScript for its live preview (review_ui.html); THIS function is the
# truth, the preview is presentation-only.
# ---------------------------------------------------------------------------

HEX_COLOR_RE = re.compile(r'^#?([0-9A-Fa-f]{6})$')

# The ink every reader falls back to when the operator has not chosen one, or
# chose something migration 010's CHECK would have rejected. White, because
# it is EXACTLY the color CAPTION_FORCE_STYLE has always burned, so
# parse_hex_color(None) reproduces today's caption style byte for byte.
DEFAULT_INK_HEX = 'FFFFFF'


def parse_hex_color(value: str | None) -> str:
    """A bare 6-hex-digit RRGGBB string (no '#', uppercase) for `value`, or
    DEFAULT_INK_HEX when `value` is None or fails the same regex migration
    010 enforces at the database. The CHECK constraint already stops a bad
    value from being stored; this is the second line of defense for any
    caller (a stale row from before the CHECK existed, a hand-built test
    fixture) and the ONE place that decides what "no real color" renders as.
    """
    if isinstance(value, str):
        m = HEX_COLOR_RE.match(value.strip())
        if m:
            return m.group(1).upper()
    return DEFAULT_INK_HEX


def relative_luminance(hex_rgb: str) -> float:
    """WCAG relative luminance of a 6-hex-digit RRGGBB string, in [0, 1].
    https://www.w3.org/TR/WCAG21/#dfn-relative-luminance -- the standard
    formula, not a perceptual approximation: each channel is normalized to
    [0, 1], gamma-expanded, then combined with the ITU-R BT.709 weights.
    """
    def channel(cc: str) -> float:
        v = int(cc, 16) / 255.0
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = channel(hex_rgb[0:2]), channel(hex_rgb[2:4]), channel(hex_rgb[4:6])
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def ass_bgr(hex_rgb: str, alpha_hex: str) -> str:
    """A 6-hex-digit RRGGBB string to an ASS &HAABBGGRR color literal. ASS
    stores color channels in BLUE-GREEN-RED order (the reverse of CSS/HTML
    hex), behind a leading alpha byte where 00 is fully opaque and FF is
    fully transparent -- the opposite sense of a CSS alpha channel.
    """
    rr, gg, bb = hex_rgb[0:2], hex_rgb[2:4], hex_rgb[4:6]
    return f'&H{alpha_hex}{bb}{gg}{rr}'.upper()


def auto_contrast_backing(hex_rgb: str) -> tuple[str, str]:
    """(OutlineColour, BackColour) ASS literals that stay legible against
    `hex_rgb` ink, the operator's readability rule made concrete. Light ink
    (relative luminance > 0.5) gets this renderer's ORIGINAL dark outline and
    dark semi-opaque box (the exact literals CAPTION_FORCE_STYLE has always
    used); dark ink flips to a white outline and a light semi-opaque box.
    0.5 is the WCAG mid-point, not a tuned threshold -- there was no prior
    art to tune against, and the mid-point is the honest "which one is it
    closer to" boundary.
    """
    if relative_luminance(hex_rgb) > 0.5:
        return ass_bgr('000000', '00'), ass_bgr('000000', '80')
    return ass_bgr('FFFFFF', '00'), ass_bgr('FFFFFF', '80')


def build_caption_force_style(caption_color: str | None) -> str:
    """The caption ASS force_style string, dynamic on the operator's chosen
    ink (clip_candidates.caption_color). Every field but PrimaryColour/
    OutlineColour/BackColour is exactly the geometry measured and retuned
    2026-08-08 (see the block comment above CAPTION_FORCE_STYLE) -- unchanged
    by this function. With caption_color None or invalid this reproduces the
    pre-D-074 literal 'PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,
    BackColour=&H80000000' byte for byte (the inert proof asserts this
    equality against the original CAPTION_FORCE_STYLE constant).
    """
    ink_hex = parse_hex_color(caption_color)
    primary = ass_bgr(ink_hex, '00')
    outline, back = auto_contrast_backing(ink_hex)
    return (
        'FontName=Liberation Sans,Bold=1,FontSize=9,'
        f'PrimaryColour={primary},OutlineColour={outline},BackColour={back},'
        'BorderStyle=1,Outline=1.6,Shadow=0.8,'
        'Alignment=2,MarginL=30,MarginR=30,MarginV=68'
    )


# ---------------------------------------------------------------------------
# THE BURNED HOOK. A second, independent `subtitles` filter pass rather than
# a second style folded into the captions' own .ass, for two reasons:
#   1. The captions path is SRT + force_style, proven and calibrated (the
#      block comment above CAPTION_FORCE_STYLE). Reusing it completely
#      unchanged means adding the hook can never regress D-063's captions
#      geometry, whatever the hook path does.
#   2. A dedicated .ass file for the hook ALONE can declare its own
#      PlayResX/PlayResY matching the final frame (1080x1920) exactly, so
#      every geometry number below is a REAL PIXEL VALUE -- no need for the
#      288x384-canvas scaling math the caption style carries (that scaling
#      exists only because ffmpeg's SRT-to-ASS auto-conversion assumes a
#      384x288 default canvas when the SRT itself declares none; a hand-
#      authored .ass has no such default to work around).
# Two `subtitles` filters chain in one linear filter_complex exactly like any
# other pair of sequential filters: the second reads the first's output.
#
# GEOMETRY, matching the review UI mockup's .hookband CSS (review_ui.html):
#   top: 16%       -> MarginV = 0.16 * 1920 = 307.2 px, rounded to 307.
#   left/right: 6% -> MarginL = MarginR = 0.06 * 1080 = 64.8 px, rounded to 65.
# FontSize = 90: "~1.5x caption size" per the brief -- the caption em is
#   FontSize=9 on the 288-tall canvas = 9 * (1920/288) = 60 real px; 60 * 1.5
#   = 90, and this file's own PlayResY=1920 makes 90 a real pixel value with
#   no further scaling.
# Outline=16 / Shadow=8: the SAME stroke-to-em ratio measured for captions
#   (Outline 1.6 * 6.667 = 10.67 real px on a 60px em = 17.8% of the em),
#   scaled by the identical 1.5x as FontSize: 10.67 * 1.5 = 16.0,
#   5.33 * 1.5 = 8.0 -- both land on clean integers.
# BorderStyle=1 (stroke + shadow, never a box) for the same reason D-063's
#   caption style uses it: a box reads as "majority text" faster than a
#   stroke does, and majority-text framing is exactly what D-061 exists to
#   avoid.
# Alignment=8 (top-center): libass's numpad-style alignment, top row.
# ---------------------------------------------------------------------------

HOOK_PLAY_RES_W = 1080
HOOK_PLAY_RES_H = 1920
HOOK_FONT_SIZE = 90
HOOK_MARGIN_V = 307
HOOK_MARGIN_LR = 65
HOOK_OUTLINE = 16
HOOK_SHADOW = 8


def ass_escape_hook_text(text: str) -> str:
    """Escape a human-typed hook for safe inclusion in an ASS Dialogue Text
    field. `{` and `}` open/close an ASS override block; unescaped, either
    one in operator-typed text would be parsed as (almost certainly invalid)
    styling and could swallow everything after it. `\\{` / `\\}` are ASS's
    own literal-brace escapes. A real newline becomes `\\N`, ASS's hard line
    break (plain `\\n` is a soft break libass does not always honor). Commas
    need no escaping: Text is the LAST field of a Dialogue line, so the ASS
    spec itself takes everything after the 9th comma verbatim to end of line.
    """
    escaped = text.replace('{', '\\{').replace('}', '\\}')
    return re.sub(r'\r\n|\r|\n', r'\\N', escaped)


def ass_timestamp(seconds: float) -> str:
    """H:MM:SS.CC -- the ASS Dialogue timestamp format (centiseconds)."""
    total_cs = round(max(0.0, seconds) * 100)
    cs = total_cs % 100
    total_s = total_cs // 100
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    return f'{h}:{m:02d}:{s:02d}.{cs:02d}'


def build_hook_ass_text(hook_text: str, hook_color: str | None, duration_s: float) -> str:
    """A complete, self-contained .ass burning ONE static hook line for the
    whole clip. Its own PlayResX/PlayResY = the real 1080x1920 output frame,
    so every field below is a real pixel value (see the geometry block
    above) with none of the caption style's canvas-scaling arithmetic.
    """
    ink_hex = parse_hex_color(hook_color)
    primary = ass_bgr(ink_hex, '00')
    outline, back = auto_contrast_backing(ink_hex)
    style = (
        f'Style: Hook,Liberation Sans,{HOOK_FONT_SIZE},{primary},&H000000FF,'
        f'{outline},{back},-1,0,0,0,100,100,0,0,1,{HOOK_OUTLINE},{HOOK_SHADOW},'
        f'8,{HOOK_MARGIN_LR},{HOOK_MARGIN_LR},{HOOK_MARGIN_V},1'
    )
    dialogue = (
        f'Dialogue: 0,{ass_timestamp(0)},{ass_timestamp(duration_s)},Hook,,0,0,0,,'
        f'{ass_escape_hook_text(hook_text)}'
    )
    return (
        '[Script Info]\n'
        'ScriptType: v4.00+\n'
        f'PlayResX: {HOOK_PLAY_RES_W}\n'
        f'PlayResY: {HOOK_PLAY_RES_H}\n'
        'WrapStyle: 0\n'
        'ScaledBorderAndShadow: yes\n'
        '\n'
        '[V4+ Styles]\n'
        'Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, '
        'BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, '
        'BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n'
        f'{style}\n'
        '\n'
        '[Events]\n'
        'Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n'
        f'{dialogue}\n'
    )


def hook_subtitles_filter(ass_path) -> str:
    """The single filter that burns the hook. No force_style: the style
    lives IN the .ass file's own [V4+ Styles] section (build_hook_ass_text),
    so there is nothing to override at the filter level.
    """
    return f'subtitles=filename={escape_filter_value(str(ass_path))}'


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


def require_subtitles_capability(candidate_id: int, feature: str = 'captions') -> None:
    """Fail loudly, naming this machine, when asked to burn on a build that
    cannot. Shared by BOTH burn paths (D-063 captions, D-074 hook): the probe
    is identical either way, since both ride the same `subtitles` filter.
    `feature` only changes which flag/label the message names.
    """
    if ffmpeg_has_subtitles_filter():
        return
    if feature == 'hook':
        raise RuntimeError(
            f'HOOK_UNSUPPORTED: candidate_id={candidate_id} asks for a burned-in hook '
            f'(clip_candidates.burn_hook=1) but the ffmpeg on host '
            f'"{socket.gethostname()}" has no `subtitles` filter, so it was built without '
            'libass and cannot burn anything. Refusing to render: a hook silently dropped '
            'from a file delivered against an explicit request is exactly the lie this '
            'check exists to prevent. Fixes: render this candidate on the n8n server lane '
            '(its ffmpeg has libass), or install an ffmpeg build that includes libass on '
            'this machine, or untick "Burn hook text into video" for this clip in the '
            'review UI.'
        )
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


def fetch_active_hook_variant(cur, candidate_id: int) -> str | None:
    """The active post kit's on-video hook text, or None when no active kit
    exists (or its hook_withheld is somehow blank -- the schema forbids that
    for a 'generated' kit, but an 'operator_edit' kit is not held to the same
    length CHECK, so this stays defensive rather than trusting the row).

    VARIANT A (hook_withheld) ONLY. post_kits (003) stores three deliberately
    different hook variants (withheld/domain/payoff, D-061) and no column
    records which one the operator is treating as "the" hook: the review
    UI's own selection (hookPick) is LOCAL browser state, never persisted
    (its own comment: "The choice is persisted only by Save, as part of the
    version he was reading"). So there is no selected-variant fact to read
    here. hook_withheld is the deliberate default: it is the first variant
    offered in the UI, the one every kit view opens on
    (selectedHookKey() falls back to 'withheld'), and withholding is the
    least presumptive of the three concreteness levels (003's own reasoning
    for spreading the variants along that axis, rather than guessing which
    level reads best). A real selected-variant column is a follow-up
    decision, not this one's.
    """
    cur.execute(
        'SELECT hook_withheld FROM post_kits WHERE candidate_id = %s AND is_active = 1',
        (candidate_id,),
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    text = str(row[0])
    return text if text.strip() else None


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
               c.burn_captions,
               c.burn_hook, c.caption_color, c.hook_color
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
        'burn_hook': int(row[9]),
        'caption_color': row[10],
        'hook_color': row[11],
    }


def render_from_slice(candidate_id: int) -> int:
    run_id = f'render_from_slice_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    slices_dir = require_env('CLPR_SLICES_DIR')
    out_dir = Path(os.environ.get('CLPR_RENDER_OUT', '/home/node/.n8n-files').strip()
                   or '/home/node/.n8n-files')

    # D-063: declared out here so the OUTERMOST finally can remove the scratch
    # SRT on literally every exit path, not merely the ones around the encode.
    srt_path: Path | None = None
    # D-074: same reasoning, for the scratch hook .ass.
    hook_ass_path: Path | None = None

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
        # NO PAD ON AN OPERATOR EDIT (live fix, 2026-08-08): 0.0 the moment
        # either adjusted column is set, else PUBLISH_PAD_S — see
        # slice_geometry.render_pad_s for the full rationale (the pad exists
        # for the DETECTOR's guess, not to un-trim an explicit operator edit).
        pad = slice_geometry.render_pad_s(cand['adjusted_start_s'], cand['adjusted_end_s'])

        # Target cut (shipped-clip invariant: effective window +/- pad, where
        # pad is 0 on an operator edit) in ABSOLUTE video coordinates, BEFORE
        # clamping.
        target_start_abs_s = eff_start_s - pad
        target_end_abs_s = eff_end_s + pad

        # A clamp at the slice's edge is legal ONLY when the ORIGINAL window's
        # padded cut itself crosses that edge — i.e. the shortfall mirrors the
        # VIDEO's own edge (t=0 floor at staging / staging-time video-end
        # clamp), which is exactly what cut_clip.py does on the full recording.
        # Geometry note: with SLICE_PAD_S (10) >> pad (<= PUBLISH_PAD_S = 1.5)
        # the original padded cut can only cross abs_start_s when the staging
        # formula floored at t=0, and only cross abs_end_s when staging clamped
        # to the video's end — so these conditions ARE the video-edge tests.
        # An operator EDIT that reaches past media the video itself had (e.g.
        # eff_start - pad < 0 with the original also near t=0) clamps the same
        # way the Mac fallback would, so erroring there would buy nothing. This
        # reuses the SAME `pad` (0 on an edit, matching what cut_clip.py's
        # fallback would now also apply), so the legality test stays exactly
        # in step with the actual target cut computed above.
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

        # migration 011 (versioned re-renders): the NEXT render_seq must be
        # known BEFORE the encode starts, unlike cut_clip.py and
        # deliver_approved.render_adjusted_clip -- their LOCAL out_path
        # (clips_out/<recording_id>_<candidate_id>.mp4) never depended on
        # render_seq at all (D-068: "the local file in clips_out keeps its
        # existing name"), so they can bump the column via a plain
        # `clips.render_seq + 1` in their end-of-render upsert and let
        # deliver_approved.py's LATER, separate sync step read the fresh
        # value back (refresh_clip_fields, a genuine follow-up SELECT in a
        # different transaction). Here, out_path itself (this worker's
        # out_path IS the delivered name, D-068) drives the temp file's
        # prefix and the ffmpeg output before any DB write happens, so
        # there is no later point at which a RETURNING/follow-up SELECT
        # could still change the filename. A plain pre-encode SELECT (not a
        # bump -- nothing is written yet) is therefore the correct order
        # here: read the CURRENT value, if any, and add 1; a candidate with
        # no clips row yet (never rendered) has no row to read, so seq is 1,
        # matching the column's own DEFAULT that a fresh INSERT would take
        # below. The literal value computed here is passed explicitly into
        # the end-of-render upsert's render_seq column (not re-derived via
        # `clips.render_seq + 1` a second time), so the filename actually
        # written to disk and the DB's record of it can never disagree.
        cur.execute('SELECT render_seq FROM clips WHERE candidate_id = %s', (candidate_id,))
        render_seq_row = cur.fetchone()
        next_render_seq = int(render_seq_row[0]) + 1 if render_seq_row is not None else 1

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / deliver_approved.delivered_name(
            cand['session_label'], cand['start_s'], cand['category'], candidate_id,
            next_render_seq,
        )

        # `filter_stages` starts as ONLY the D-023 body (no label). Copied
        # EXACTLY from cut_clip.py (operator-proven live on Instagram, D-023).
        # With NEITHER captions NOR the hook requested, `','.join([body]) +
        # '[v]'` below reduces to `body + '[v]'`, which is FILTER_COMPLEX_D023
        # itself, byte for byte -- the D-074 refactor that lets captions AND
        # the hook chain onto the SAME linear filtergraph changes nothing on
        # the default path (asserted by the inert proof).
        filter_stages = [FILTER_COMPLEX_D023_BODY]

        # ---- D-063 captions: build the SRT and splice ONE filter in ---------
        # Everything in this block is skipped entirely when the flag is 0.
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
                filter_stages.append(subtitles_filter(srt_path, cand['caption_color']))
                captions_burned = 1
                print(
                    f'CAPTIONS_ON candidate={candidate_id} cues={captions_cue_count} '
                    f'srt="{srt_path}" bytes={len(srt_text.encode("utf-8"))}'
                )

        # ---- D-074 hook: fetch the active kit's variant A and splice ONE ----
        # more filter in, chained AFTER captions (order does not matter here:
        # the two bands, top and bottom, never overlap). Everything in this
        # block is skipped entirely when the flag is 0.
        hook_requested = 1 if cand['burn_hook'] == 1 else 0
        hook_burned = 0

        if hook_requested == 1:
            hook_text = fetch_active_hook_variant(cur, candidate_id)
            if hook_text is None:
                # No active kit yet (the common case: kits generate AFTER
                # delivery, D-061). Nothing to burn, nothing written, and the
                # render proceeds exactly as if burn_hook were 0 -- this is
                # NOT an error, matching the captions no-speech case above.
                print(
                    f'HOOK_NO_KIT candidate={candidate_id} burn_skipped=1 '
                    'render_continues=1'
                )
            else:
                # Capability first, same discipline as captions -- but only
                # once there is genuinely something to burn, so a machine
                # without libass never fails a render that would have skipped
                # the hook anyway (the no-kit case above).
                require_subtitles_capability(candidate_id, feature='hook')

                ass_text = build_hook_ass_text(hook_text, cand['hook_color'], cut_duration_s)
                fd, tmp_name = tempfile.mkstemp(
                    prefix=f'clpr_c{candidate_id}_hook_', suffix='.ass'
                )
                with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                    fh.write(ass_text)
                hook_ass_path = Path(tmp_name)
                filter_stages.append(hook_subtitles_filter(hook_ass_path))
                hook_burned = 1
                print(
                    f'HOOK_ON candidate={candidate_id} text="{hook_text}" '
                    f'ass="{hook_ass_path}" bytes={len(ass_text.encode("utf-8"))}'
                )

        filter_complex = ','.join(filter_stages) + '[v]'

        # D-055: ALWAYS trim — the slice carries SLICE_PAD_S headroom, so
        # rendering it whole would ship a ~20s-too-long clip. The trim is
        # INPUT options (-ss/-t before -i); filter/encode flags stay
        # byte-identical to cut_clip.py.
        #
        # B6 fixer: ffmpeg writes to a unique temp file in the SAME directory
        # as out_path (mkstemp, dir=out_dir), never to out_path directly. Two
        # concurrent renders of the same candidate each get their own temp
        # file and no longer race on one shared path; a render that fails
        # (capability check, caption build, or the ffmpeg process itself) can
        # never destroy a previous good render sitting at out_path, because
        # out_path is never opened for writing until the NEW encode has
        # already succeeded (see the os.replace below).
        #
        # LIVE FIXER (2026-08-08): the suffix MUST end in `.mp4`. B6 shipped
        # `suffix='.part'`, and real ffmpeg infers its output MUXER from the
        # output filename's extension when no `-f` is given -- a temp path
        # ending in `.part` has no extension ffmpeg recognizes, so it refused
        # with "Unable to find a suitable output format" on the very first
        # live render. B6's own control stubbed ffmpeg (a fake executable that
        # never inspects its argv), so this never had a chance to fire in
        # review: the host's container-by-extension inference is part of the
        # code (charter gate 6) and only the real binary can exercise it.
        # `.part.mp4` keeps both properties at once: unique + same-dir (so
        # os.replace onto out_path stays an atomic same-filesystem rename) AND
        # ends in `.mp4` (so ffmpeg's extension sniff still resolves to the
        # mp4 muxer). `-f mp4` was deliberately NOT added instead: the
        # extension fix also keeps any other extension-sniffing tool (ffprobe,
        # a future glob) working against the temp name, and is a smaller,
        # more literal diff against the B6 baseline.
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=f'{out_path.name}.', suffix='.part.mp4', dir=str(out_dir)
        )
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)

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
            str(tmp_path),
        ]

        try:
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            # The encode succeeded (run_capture raises on a nonzero ffmpeg
            # exit, caught below) -- promote the temp file to out_path. Same
            # filesystem (both under out_dir), so this is an atomic rename:
            # out_path is either the complete old file or the complete new
            # one, never a truncated write-in-progress, at every instant.
            os.replace(tmp_path, out_path)

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
                                  captions_requested, captions_burned, captions_cue_count, render_seq)
                VALUES (%s, %s, %s, 'rendered', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (candidate_id) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    duration_s = EXCLUDED.duration_s,
                    state = 'rendered',
                    created_by_run = EXCLUDED.created_by_run,
                    created_at = EXCLUDED.created_at,
                    captions_requested = EXCLUDED.captions_requested,
                    captions_burned = EXCLUDED.captions_burned,
                    captions_cue_count = EXCLUDED.captions_cue_count,
                    -- migration 011: EXCLUDED.render_seq is the SAME
                    -- next_render_seq value already baked into out_path
                    -- above (a literal, not a second `clips.render_seq + 1`
                    -- computation), so the filename on disk and the DB's
                    -- record of it can never disagree.
                    render_seq = EXCLUDED.render_seq,
                    -- A RE-RENDER INVALIDATES THE DELIVERY WITNESS (2026-08-08).
                    --
                    -- drive_synced_at is D-056's proof that the bytes in Drive
                    -- ARE this clip. A re-render replaces those bytes, so the
                    -- proof stops being true the moment this row is updated --
                    -- and leaving it set produced exactly that: after the
                    -- operator un-ticked captions and this worker re-rendered,
                    -- the row read captions_burned=0 / delivered, while the
                    -- file in Drive still had captions burned in. The database
                    -- and Drive disagreed and nothing could tell.
                    --
                    -- Clearing it re-opens the clip for delivery (the pending
                    -- queue is state='approved' AND drive_synced_at IS NULL),
                    -- which is what makes an un-tick actually reach Drive
                    -- instead of being recorded and ignored.
                    drive_synced_at = NULL,
                    drive_sync_path = NULL
                ''',
                (candidate_id, str(out_path), duration_s, run_id, utc_now_iso(),
                 captions_requested, captions_burned, captions_cue_count, next_render_seq),
            )
            conn.commit()

            print(
                f'RESULT render_from_slice candidate={candidate_id} ok=1 '
                f'file="{out_path}" duration_s={duration_s:.3f} '
                f'captions_requested={captions_requested} captions_burned={captions_burned} '
                f'captions_cue_count={"" if captions_cue_count is None else captions_cue_count} '
                f'hook_requested={hook_requested} hook_burned={hook_burned}'
            )
            return 0
        except Exception:
            # B6 fixer: clean up only the TEMP file, never out_path. Before
            # the os.replace above, out_path is untouched (the previous good
            # render, if any, survives); after it, out_path IS the new,
            # complete, correct render and tmp_path no longer exists (the
            # rename consumed it), so this is a no-op for a post-replace
            # failure (duration probe / DB write) -- exactly "on failure,
            # remove only the temp".
            if tmp_path.exists():
                tmp_path.unlink()
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
        # D-074: the hook .ass is scratch too, same discipline.
        if hook_ass_path is not None and hook_ass_path.exists():
            hook_ass_path.unlink()


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
