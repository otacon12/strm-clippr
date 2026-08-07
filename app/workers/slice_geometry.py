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
