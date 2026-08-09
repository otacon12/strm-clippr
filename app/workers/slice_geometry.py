#!/usr/bin/env python3
"""slice_geometry: the ONE TRUTH for D-055 slice/render geometry constants.

D-055 rules (operator-approved):

- SLICE BOUNDS DERIVE FROM THE IMMUTABLE ORIGINAL WINDOW ONLY (clip_candidates
  start_s/end_s), never from the operator-adjusted columns. Why: the original
  window is the immutable basis, so an operator edit before OR after staging
  never invalidates a staged slice.
- SLICE_PAD_S (10.0 s) is staging headroom:
      slice_start = max(0.0, original_start - SLICE_PAD_S)
      slice_end   = original_end + SLICE_PAD_S
  slice_end is clamped to the video duration at STAGING time. These formulas
  are STAGING-TIME truth only: the actual produced file can disobey them
  (a -c copy head snaps to the keyframe before the requested start; the
  video-duration clamp shortens the tail), so the stager records the REAL
  absolute coordinates in a c<id>.json sidecar and renderers read THAT
  witness — never re-derive geometry from these formulas (D-055 fixer).
- PUBLISH_PAD_S (1.5 s) is the shipped-clip breathing room and MUST equal the
  pad cut_clip.py already uses (see the citation at the constant below).
- EFFECTIVE WINDOW for every cut =
      COALESCE(adjusted_start_s, start_s) .. COALESCE(adjusted_end_s, end_s)
  Originals are NEVER overwritten.
- SHIPPED CLIP INVARIANT: rendered output covers the effective window
  +/- PUBLISH_PAD_S — identical to today's shipped behavior for unedited
  candidates.
- NO PAD ON AN OPERATOR EDIT (live fix, 2026-08-08): PUBLISH_PAD_S exists to
  give the DETECTOR's guess breathing room. Applied on top of an operator
  edit it silently un-trims up to 1.5s per side -- live case: original window
  0->10.02, operator trimmed via the slider to 0.8->9.5, and the padded
  render came back at 11.08s, bringing the trimmed-away opening back
  ("the bounced clip didn't use my edited section"). So when EITHER
  adjusted_start_s or adjusted_end_s is present, the render pad is 0.0 on
  BOTH sides -- the effective window IS the render window, exactly as the
  operator set it. See render_pad_s() below, the single source every
  consumer (all three renderers + build_srt's t=0 derivation) now routes
  through -- no consumer computes this pad locally anymore.
"""

from __future__ import annotations

# Staging headroom around the ORIGINAL candidate window, so in-slice edits have
# room to move without restaging (D-055).
SLICE_PAD_S = 10.0

# Shipped-clip breathing room. MUST equal cut_clip.py's pad — verified by
# reading app/workers/cut_clip.py (2026-08-06): line 25 is
#     PAD_SECONDS = 1.5  # small default padding so cuts do not feel abrupt; tunable later.
# cut_clip.py is the operator-proven Mac-side path (D-023) and keeps its own
# constant; if either value ever changes, both must change together.
PUBLISH_PAD_S = 1.5

# ---------------------------------------------------------------------------
# Sidecar schema version (SRD-06 / golden-review F9 fixer, 2026-08-08): the
# ONE TRUTH for the c<id>.json geometry sidecar's schema.
#
# Why it lives HERE: the stager (slice_candidates.py, which WRITES sidecars)
# and the renderer (render_from_slice.py, which READS and validates them)
# each carried their OWN hardcoded `SIDECAR_SCHEMA = 1`, kept in sync only by
# a comment ("must match slice_candidates.SIDECAR_SCHEMA") -- an honor-system
# duplicate with no import edge enforcing it, i.e. exactly the "two copies of
# one thing always drift" trap. Both modules already import slice_geometry
# for the staging-geometry formulas above, so the constant now has exactly
# one home and the drift is structurally impossible instead of merely
# commented against.
#
# Why 2: schema 1 sidecars recorded geometry only (abs_start_s, abs_end_s,
# actual_duration_s) -- never the SOURCE VIDEO's identity, even though the
# stager always has it in hand. A geometry-valid slice was therefore silently
# REUSED after its source video changed underneath it: recordings.path
# repointed to a different file, or a candidate id recycled by --reset-ids
# across wipe generations. render_from_slice's STALE_SLICE check cannot catch
# this, because it compares ffprobe'd length against a window the stager
# itself anchored FROM THAT SAME (wrong) file, so an intact stale slice
# passes with a difference of ~0. Schema 2 adds the source-identity witness
# (source_path, source_size_bytes, source_duration_s) so a source swap is
# detectable. The bump is INTENTIONALLY BREAKING: every existing schema-1
# sidecar fails validation and its slice is restaged on the next run -- the
# designed heal path, not a cost.
SIDECAR_SCHEMA = 2


def slice_start(original_start_s: float) -> float:
    """NOMINAL absolute start of the staged slice, from the ORIGINAL window
    only. Staging-time truth: the actual file's head may start EARLIER (the
    -c copy keyframe snap) — renderers read the sidecar witness, not this."""
    return max(0.0, float(original_start_s) - SLICE_PAD_S)


def slice_end(original_end_s: float, duration_s: float | None = None) -> float:
    """NOMINAL absolute end of the staged slice, from the ORIGINAL window only.

    duration_s (the video duration) is applied as a clamp at STAGING time when
    known. Renderers do not know it and must read the sidecar witness the
    stager wrote from the actual slice file instead of assuming the clamp.
    """
    end = float(original_end_s) + SLICE_PAD_S
    if duration_s is not None:
        end = min(end, float(duration_s))
    return end


def effective_window(start_s: float, end_s: float,
                     adjusted_start_s: float | None,
                     adjusted_end_s: float | None) -> tuple[float, float]:
    """COALESCE(adjusted, original) for both edges — the window every cut uses.

    Originals are never overwritten; an unedited candidate (both adjusted
    columns NULL) yields exactly the original window.
    """
    eff_start = float(adjusted_start_s) if adjusted_start_s is not None else float(start_s)
    eff_end = float(adjusted_end_s) if adjusted_end_s is not None else float(end_s)
    return (eff_start, eff_end)


def render_pad_s(adjusted_start_s: float | None, adjusted_end_s: float | None) -> float:
    """The publish pad to apply to the effective window for THIS render.

    0.0 the moment EITHER adjusted column is present -- an operator edit is
    an explicit, deliberate window, and PUBLISH_PAD_S exists only to give the
    DETECTOR's unedited guess breathing room. Applying it on top of an edit
    silently un-trims up to PUBLISH_PAD_S per side (live, 2026-08-08: a
    0.8->9.5 trim of a 0->10.02 original came back at 11.08s because the pad
    clamped back to the trimmed-away opening). An unadjusted candidate (both
    columns NULL) is untouched: PUBLISH_PAD_S, identical to today.

    Single source of truth: every consumer (render_from_slice.py,
    cut_clip.py, deliver_approved.render_adjusted_clip, and build_srt.py's
    t=0 derivation on both the sidecar and formula bases) calls this instead
    of reading PUBLISH_PAD_S directly, so the render window and the caption
    window can never disagree about which pad applied.
    """
    if adjusted_start_s is not None or adjusted_end_s is not None:
        return 0.0
    return PUBLISH_PAD_S
