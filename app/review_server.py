#!/usr/bin/env python3
"""review_server: local operator-only review surface for clip candidates.
Binds to loopback only. Connects to the consolidated PostgreSQL via the shared
adapter app/workers/db.py (CLPR_DB_URL). No external dependencies beyond psycopg2.

PostgreSQL port (D-052 P3): tables and columns per app/docs/naming-map.md.
The JSON keys `vod_id`/`vod_path` and the /api/candidates + /media/<vod_id>
URL paths are an external contract consumed by review_ui.html and stay as-is
(SQL aliases map them onto the renamed schema). PG-only addition:
`display_name` (recordings.display_name) rides along in candidate payloads.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent / 'workers'))
import db  # noqa: E402  (app/workers/db.py — the shared adapter)
# GET /api/candidates/<id>/cues (reactive-editor live preview): these are the
# REAL functions build_srt.py already uses to turn transcript segments into
# cues, imported rather than reimplemented, so the pending-screen preview and
# the eventual burn can never disagree about the boundary rule (charter 1.5
# gate 1 — one truth). See _serve_cues's own docstring for how this differs
# from _serve_captions's "does not build the SRT" rule just below.
import build_srt  # noqa: E402  (app/workers/build_srt.py)
import slice_geometry  # noqa: E402  (app/workers/slice_geometry.py)
import transcript_signal as ts  # noqa: E402  (app/workers/transcript_signal.py)

# CLPR_BIND overrides the bind address; unset/default behavior (loopback-only)
# is unchanged. The UI has no auth, so binding anything other than 127.0.0.1
# is for a private network only (e.g. a tailnet IP, CLPR_BIND=<tailnet-ip>,
# or CLPR_BIND=0.0.0.0 to bind all interfaces) — never expose this on a
# public/untrusted network. Single address only; the server binds one socket.
HOST = os.environ.get('CLPR_BIND', '127.0.0.1')
PORT = int(os.environ.get('CLPR_REVIEW_PORT', '8737'))

# THE TWO-VOCABULARIES RULE (2026-08-08).
#
# `recordings.path` means different things depending on which lane registered
# the recording, and the review UI is on a THIRD machine from either:
#   local lane   -> /Volumes/GOLDMINE/vibecoder-recordings/<stem>.mov  (a video)
#   portable lane-> /home/node/.n8n/clpr/media/<stem>.wav              (SERVER audio)
# Serving `recordings.path` verbatim therefore 404s for every n8n-analyzed VOD,
# and the player just silently does nothing. That was hand-patched once, for
# recording 19, by rewriting the row. Rewriting a row fixes one VOD; this
# resolves it for all of them.
#
# THE STEM IS THE ONLY STABLE KEY across lanes (same reason restore-inputs.py
# joins on it, and the same stem D-068 derives session_label from). So: if the
# stored path is not readable here, look for <stem>.<video-ext> in the local
# video directories. Audio extensions are deliberately NOT candidates - a .wav
# resolves to nothing rather than to an unplayable file.
LOCAL_VOD_DIRS_DEFAULT = (
    '/Users/fifgen/Library/CloudStorage/GoogleDrive-seun@gmgt.co/My Drive/projects/stream/to_clip',
    '/Volumes/GOLDMINE/vibecoder-recordings',
    str(Path(__file__).resolve().parent / 'clips_out'),
)
VIDEO_EXTS = ('.mov', '.mp4', '.m4v', '.mkv', '.webm', '.avi')


def local_vod_dirs() -> list[Path]:
    """Directories searched for a playable source video, in order.

    Override with CLPR_LOCAL_VOD_DIRS (colon-separated) on a machine whose
    Drive mount or GOLDMINE path differs.
    """
    raw = os.environ.get('CLPR_LOCAL_VOD_DIRS', '').strip()
    dirs = raw.split(':') if raw else list(LOCAL_VOD_DIRS_DEFAULT)
    return [Path(d) for d in dirs if d]


LOCAL_CLIP_DIRS_DEFAULT = (
    '/Users/fifgen/Library/CloudStorage/GoogleDrive-seun@gmgt.co/My Drive/projects/stream/appr_clips',
    '/Users/fifgen/Library/CloudStorage/GoogleDrive-seun@gmgt.co/My Drive/projects/stream/used_clips',
    str(Path(__file__).resolve().parent / 'clips_out'),
)


def local_clip_dirs() -> list[Path]:
    """Directories searched for a RENDERED clip. Override with
    CLPR_LOCAL_CLIP_DIRS (colon-separated)."""
    raw = os.environ.get('CLPR_LOCAL_CLIP_DIRS', '').strip()
    dirs = raw.split(':') if raw else list(LOCAL_CLIP_DIRS_DEFAULT)
    return [Path(d) for d in dirs if d]


def find_by_name(name: str, dirs: list[Path]) -> Path | None:
    """Exact-filename lookup across dirs. Used for rendered clips, whose names
    are already unique and fully-formed (D-068) -- unlike source VODs, where the
    extension differs per lane and only the stem is stable."""
    for d in dirs:
        cand = d / name
        if cand.is_file():
            return cand
    return None


def resolve_local_clip(file_path: str | None, drive_sync_path: str | None) -> Path | None:
    """A locally-readable RENDERED clip, or None.

    THE SAME TWO-VOCABULARIES PROBLEM AS resolve_local_vod, one layer down.
    `clips.file_path` is written by whichever machine rendered, so an
    n8n-rendered clip points at the SERVER filesystem and is unreachable here.
    `drive_sync_path` is a full local Drive path from the Mac deliverer but a
    BARE FILENAME from the n8n lane -- and a bare name was previously skipped
    outright, which is why a delivered, Drive-synced clip still reported "not
    reachable from this machine" with the file sitting in the operator's own
    Drive mount.

    Order: any absolute path that exists, then the basename of either candidate
    looked up in the local clip directories.
    """
    for raw in (file_path, drive_sync_path):
        if not raw:
            continue
        p = Path(str(raw))
        if p.is_absolute() and p.is_file():
            return p
    dirs = local_clip_dirs()
    for raw in (drive_sync_path, file_path):
        if not raw:
            continue
        hit = find_by_name(Path(str(raw)).name, dirs)
        if hit is not None:
            return hit
    return None


CLIP_CACHE_DIR = Path(
    os.environ.get('CLPR_CLIP_CACHE',
                   str(Path.home() / '.cache' / 'clpr' / 'clips')))
SSH_HOST = os.environ.get('CLPR_SSH_HOST', 'n8nserver')

# B4: one lock per candidate id, so two concurrent requests for the SAME clip
# serialise instead of both writing `fetch_clip_from_server`'s cache path at
# once. That race is what punched a NUL hole into a "successfully" cached
# file under ThreadingHTTPServer -- the second thread's O_TRUNC landed inside
# the window the first thread's write was still filling. Access only through
# `_clip_fetch_lock`, never directly: the outer guard is what makes the
# lazy-create of a per-id Lock itself race-free (two threads racing to be
# the FIRST fetch of a given candidate must not each build a different Lock
# object, which would defeat the serialisation entirely).
_clip_fetch_locks: 'defaultdict[int, threading.Lock]' = defaultdict(threading.Lock)
_clip_fetch_locks_guard = threading.Lock()


def _clip_fetch_lock(candidate_id: int) -> threading.Lock:
    with _clip_fetch_locks_guard:
        return _clip_fetch_locks[candidate_id]


# Find-clips (2026-08-09 operator ask: "the button to find clips should
# actually live on the reviewer"). One global run at a time -- a
# non-blocking acquire so a second overlapping POST refuses LOUDLY (409)
# instead of silently firing a second finder run in n8n. The client already
# guards against a fast double-click (its own in-flight flag), so this is
# the backstop for any other path that reaches the handler concurrently.
_find_clips_lock = threading.Lock()


def _parse_iso_utc(text: str) -> datetime:
    """Parse the project's ISO-8601 'Z'-suffixed UTC timestamp format
    (see utc_now_iso, and render_from_slice.py's identical helper that writes
    clips.created_at on every render) into an AWARE datetime. Never returns a
    naive one, so it can only ever be compared against another aware value
    (charter §11 gate 21: one instant, one format -- a naive/aware mix is
    exactly the kind of comparison that silently returns a wrong answer
    instead of raising)."""
    return datetime.fromisoformat(text.replace('Z', '+00:00'))


def _clip_file_is_stale(cached_path: Path, row_created_at: str | None) -> bool:
    """True when the clips row was (re-)rendered more recently than the file
    at `cached_path` was written -- i.e. a re-render happened after this
    file was cached or synced, so serving it would show the operator stale
    bytes while he believes he is reviewing the current render.

    `clips.created_at` is rewritten on every render, INCLUDING a re-render
    of an already-rendered clip (render_from_slice.py's `ON CONFLICT ...
    DO UPDATE SET created_at = EXCLUDED.created_at`), so it is the one
    signal that actually changes when the underlying bytes change --
    `deliver_approved.delivered_name()` does not (it is deterministic in
    session_label/start_s/category/candidate_id), which is exactly why the
    cache key stays byte-identical across a caption-only re-render and the
    cache itself cannot self-detect staleness.

    Absent or unparseable created_at is NOT treated as proof of staleness
    (the charter's conservative default: under-claim rather than assert a
    guess) -- a row with no readable timestamp does not refuse to play a
    file that otherwise resolved.
    """
    if not row_created_at:
        return False
    try:
        row_dt = _parse_iso_utc(str(row_created_at))
    except ValueError:
        return False
    try:
        file_dt = datetime.fromtimestamp(cached_path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return True  # the file vanished under us -- treat as a miss, not a crash
    return row_dt > file_dt


def fetch_clip_from_server(server_path: str | None, candidate_id: int,
                            row_created_at: str | None = None) -> Path | None:
    """Pull a clip off the origin server and cache it locally, or None.

    WHY THIS EXISTS RATHER THAN JUST SEARCHING HARDER LOCALLY. Local resolution
    is machine-specific by construction: it depends on this Mac's Drive mount
    path, and on the clip having been delivered at all. Both assumptions fail --
    the first on the laptop, the second for a clip that is rendered but not yet
    synced. Fetching from the server depends only on ssh, which is the same
    thing every other server operation in this project already needs.

    Cached under CLPR_CLIP_CACHE so the transfer happens once per clip and every
    byte-range request after that is served from disk, which is also what makes
    seeking in the player work.

    `row_created_at` (clips.created_at) gates the cache-hit: a re-render
    writes a byte-identical cache KEY (see `_clip_file_is_stale`), so a plain
    is-file-non-empty check would keep serving pre-rerender bytes forever.
    When the cached copy is stale this function re-fetches and OVERWRITES it
    via the same atomic write used for a first-time fetch.

    The fetch-and-write is serialised per candidate id (`_clip_fetch_lock`)
    and the temp file is created with `tempfile.mkstemp` (unique, exclusive)
    so two concurrent callers for the same clip cannot interleave writes to
    one shared `.part` path -- the final rename onto `cached` is always
    `os.replace`, an atomic single syscall.

    Returns None rather than raising: the caller already has a 404 path that the
    UI renders as "not reachable from this machine", and a review surface must
    not 500 because a side channel is unavailable.
    """
    if not server_path:
        return None
    name = Path(str(server_path)).name
    if not name:
        return None
    cached = CLIP_CACHE_DIR / f'c{candidate_id}_{name}'

    with _clip_fetch_lock(candidate_id):
        if (cached.is_file() and cached.stat().st_size > 0
                and not _clip_file_is_stale(cached, row_created_at)):
            return cached
        CLIP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=CLIP_CACHE_DIR, prefix=f'.c{candidate_id}_', suffix='.part')
        os.close(tmp_fd)
        tmp = Path(tmp_name)
        try:
            container = subprocess.run(
                ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', SSH_HOST,
                 "docker ps --format '{{.Names}}' | grep '^n8n-'"],
                capture_output=True, text=True, timeout=30)
            names = [n for n in container.stdout.split() if n.startswith('n8n-')]
            if container.returncode != 0 or len(names) != 1:
                tmp.unlink(missing_ok=True)
                return None
            with open(tmp, 'wb') as fh:
                proc = subprocess.run(
                    ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', SSH_HOST,
                     f'docker exec {names[0]} cat {shlex.quote(str(server_path))}'],
                    stdout=fh, stderr=subprocess.DEVNULL, timeout=600)
            if proc.returncode != 0 or tmp.stat().st_size == 0:
                tmp.unlink(missing_ok=True)
                return None
            os.replace(tmp, cached)
            return cached
        except Exception:  # noqa: BLE001 - a side channel must never 500 the review UI
            tmp.unlink(missing_ok=True)
            return None


def invalidate_clip_cache(candidate_id: int) -> int:
    """Remove every cached copy of this candidate's clip. Returns the count
    removed, so the caller can PRINT proof the sweep actually ran rather than
    assume it did (charter §1.5 gate 2: a check that cannot fail loudly is
    not a check).

    Cache keys are `c<candidate_id>_<basename>` (fetch_clip_from_server); a
    re-render writes a byte-identical KEY for a caption-only change
    (deliver_approved.delivered_name() is deterministic in
    session_label/start_s/category/candidate_id), so without this sweep the
    review server would keep serving the pre-rerender bytes forever -- the
    serving-side mtime/created_at check (`_clip_file_is_stale`) is a second,
    independent line of defense against the same failure, not a substitute
    for actually clearing the stale copy.
    """
    removed = 0
    for f in CLIP_CACHE_DIR.glob(f'c{candidate_id}_*'):
        try:
            f.unlink()
            removed += 1
        except OSError:
            pass
    return removed


# ---------------------------------------------------------------- preview proxies
# Preview proxies for the review UI (operator-approved 2026-08-10, verbatim:
# "approved - just make sure the final output is full quality. let's go with
# eager"). MEASURED PROBLEM: /clipmedia/<id> was serving the full-quality
# source slice over a Tailscale DERP relay -- a 47s preview measured
# 1920x1080 @ 6.0 Mbps = 36.8 MB with atom order ftyp/free/mdat/moov (the
# index at the END), so the player stalled before the first frame. Preview
# quality does not need to match output quality: the final render is always
# built SERVER-SIDE from the original staged slice (render_from_slice.py,
# untouched by this change), so a light preview costs nothing in the
# delivered clip.
#
# THE INVARIANT THIS WHOLE ADDITION IS JUDGED AGAINST: a proxy must never be
# reachable as a render or delivery source. That is a structural property,
# not a promise -- rendering reads /home/node/.n8n/clpr/media/slices/<...>
# ON THE SERVER, proxies live only in CLPR_PROXY_CACHE on THIS machine, and
# CLPR_PROXY_CACHE is a directory CLIP_CACHE_DIR's own single-level glob
# (invalidate_clip_cache, above) cannot see -- a proxy is never written into
# CLIP_CACHE_DIR and never uploaded anywhere.
CLPR_PROXY_CACHE = Path(
    os.environ.get('CLPR_PROXY_CACHE', str(Path.home() / '.cache' / 'clpr' / 'proxies')))

# THE GUARD IS ASYMMETRIC ON PURPOSE (revised 2026-08-10, operator
# escalation from the first cut's symmetric 0.05s equality check -- read
# this before "fixing" it back to equality).
#
# Temporal alignment between proxy and source is guaranteed STRUCTURALLY,
# not by matching durations: FFMPEG_PROXY_ARGS performs no temporal
# operation whatsoever -- no -ss, no -t, no setpts, no rate change, only a
# spatial scale and a re-encode. Every input frame is encoded in order at
# its original presentation time; there is no code path by which the proxy
# could drift out of sync with the source frame-for-frame. MEASURED, not
# assumed: whole-clip SSIM of the proxy against the scaled source on the
# real c3 file = 0.979376; a 2-frame-offset negative control drops that to
# 0.961665, so the metric is sensitive enough to catch real drift. Worst
# case checked at the clip's TAIL (t=45s, where any drift would have had
# the most time to accumulate) -- scoring the source frame at 45.0s against
# proxy frames at 44.4/44.7/45.0/45.3/45.6s: SSIM 0.792/0.837/**0.952**
# /0.882/0.855, a clean peak at ZERO offset. No drift, even at the tail.
#
# So exact duration equality was never the right property to assert, and
# is unattainable against these sources in general: they are cut with
# `-c copy` and land on irregular timestamps, so several are genuinely
# variable-frame-rate despite a nominal fps tag -- c3 itself claims
# nb_frames=1430 over duration=47.233984s (≈30.27fps effective), while this
# module's CFR re-encode produces a self-consistent 1414 frames /
# 47.133333s. Two correct, alignment-preserving files can legitimately
# report different container durations; a symmetric equality check with
# ANY tolerance tight enough to mean something will misfire on real VFR
# input (measured: c3 delta 0.100651s, c4 delta 0.066667s, both rejected by
# the original 0.05s check despite the proxy being frame-accurate).
#
# What the guard actually needs to catch is a BROKEN encode, and only two
# shapes of broken are possible given the above:
#   - a proxy LONGER than its source: FFMPEG_PROXY_ARGS has no way to add
#     time, so this implies padding or an unintended rate change -- exactly
#     the case that COULD misalign trim points. Zero tolerance beyond
#     ordinary probe rounding.
#   - a proxy dramatically SHORTER than its source: a died-halfway or
#     truncated encode. The threshold is generous (0.5s) specifically to
#     admit the ≤0.11s container/VFR artifact measured across every real
#     clip probed, while still catching a genuine truncation (which loses
#     seconds, not tenths).
PROXY_MAX_LONGER_S = 0.05
PROXY_MAX_SHORTER_S = 0.5

# Operator-approved encode: scale to 720p, fast preset, faststart so the
# index is at the FRONT of the file (the fix for the measured defect above).
FFMPEG_PROXY_ARGS = (
    '-vf', 'scale=-2:720',
    '-c:v', 'libx264',
    '-crf', '28',
    '-preset', 'veryfast',
    '-g', '30',
    '-keyint_min', '30',
    '-sc_threshold', '0',
    '-c:a', 'aac',
    '-b:a', '96k',
    '-movflags', '+faststart',
)


def _proxy_path_for(candidate_id: int) -> Path:
    """The one proxy path for a candidate. Keyed only by candidate_id (unlike
    the clip cache, which also keys on the source basename) because there is
    only ever one CURRENT preview for a candidate at a time; staleness
    (`_valid_proxy` below) is what keeps this filename from ever serving the
    wrong generation of it -- including across the pre-approval-slice ->
    rendered-clip transition, where the underlying source changes identity
    but the proxy path does not."""
    return CLPR_PROXY_CACHE / f'c{candidate_id}.proxy.mp4'


def _valid_proxy(candidate_id: int, row_created_at: str | None) -> Path | None:
    """An existing, non-empty, non-stale proxy for this candidate, or None.

    Reuses `_clip_file_is_stale` -- the SAME staleness gate the clip cache
    uses against `clips.created_at` -- rather than inventing a second
    signal (build brief point 5, and charter §1.5 gate 1: one truth). A
    re-render (or a first render following a pre-approval preview) writes a
    NEWER created_at than any proxy built before it, so the same >
    comparison that protects the clip cache from serving pre-rerender bytes
    also protects the proxy from showing the operator stale video while the
    UI tells him it is current.
    """
    path = _proxy_path_for(candidate_id)
    if not (path.is_file() and path.stat().st_size > 0):
        return None
    if _clip_file_is_stale(path, row_created_at):
        return None
    return path


def _ffprobe_duration(path: Path) -> float | None:
    """`path`'s real duration in seconds per ffprobe, or None on any
    failure/unparseable output -- never raises, this is a side channel for
    the duration-invariant guard below, which itself treats None as
    'refuse, don't guess' (see _finalize_proxy)."""
    try:
        proc = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', str(path)],
            capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            return None
        return float(proc.stdout.strip())
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def _finalize_proxy(tmp: Path, final: Path, source_duration: float | None,
                     candidate_id: int) -> Path | None:
    """THE BROKEN-ENCODE GATE (see PROXY_MAX_LONGER_S / PROXY_MAX_SHORTER_S
    above for why it is asymmetric, not an equality check), isolated from
    the ffmpeg call so it can be exercised directly against a
    deliberately-wrong `source_duration` (build brief verification: 'a
    guard you assert but never watch fail is not a guard'). `tmp` must
    already be a complete, successfully-encoded proxy file sitting in
    CLPR_PROXY_CACHE.

    Alignment is guaranteed structurally (no temporal op in the encode);
    this gate exists only to catch a proxy that is not what a correct
    encode of `source` could ever produce -- DELETE and refuse (return
    None) rather than promote one. The caller (`_generate_proxy`) falls
    back to serving the untouched source file on a None return -- refusing
    here never breaks preview, it only forfeits the speed-up for this one
    clip.
    """
    proxy_duration = _ffprobe_duration(tmp)
    if source_duration is None or proxy_duration is None:
        print(f'PROXY_DURATION_UNKNOWN candidate_id={candidate_id} '
              f'source_duration={source_duration} proxy_duration={proxy_duration} '
              f'-- refusing proxy, serving source', file=sys.stderr)
        tmp.unlink(missing_ok=True)
        return None
    if proxy_duration > source_duration + PROXY_MAX_LONGER_S:
        print(f'PROXY_DURATION_LONGER_THAN_SOURCE candidate_id={candidate_id} '
              f'source_duration={source_duration} proxy_duration={proxy_duration} '
              f'over_by={proxy_duration - source_duration} > {PROXY_MAX_LONGER_S}s -- '
              f'refusing proxy, serving source', file=sys.stderr)
        tmp.unlink(missing_ok=True)
        return None
    if source_duration - proxy_duration > PROXY_MAX_SHORTER_S:
        print(f'PROXY_DURATION_TRUNCATED candidate_id={candidate_id} '
              f'source_duration={source_duration} proxy_duration={proxy_duration} '
              f'short_by={source_duration - proxy_duration} > {PROXY_MAX_SHORTER_S}s -- '
              f'refusing proxy, serving source', file=sys.stderr)
        tmp.unlink(missing_ok=True)
        return None
    os.replace(tmp, final)
    return final


def _generate_proxy(source: Path, candidate_id: int) -> Path | None:
    """Encode a lightweight, faststart preview proxy from `source` (an
    ALREADY-FETCHED full-quality clip on this machine -- never a remote
    path) into CLPR_PROXY_CACHE, or None on any failure. A proxy is an
    optimization, never a hard dependency: every caller falls back to
    `source` on a None return, exactly today's pre-proxy behaviour.

    Write-then-rename via `_finalize_proxy` (`os.replace`), the same
    atomic-write discipline `fetch_clip_from_server`'s cache already uses --
    a reader must never observe a partially-written proxy.

    MUST be called with `candidate_id`'s `_clip_fetch_lock` already held by
    the caller (serialises with a concurrent on-demand fetch/serve for the
    same candidate, same pattern as the clip cache).
    """
    CLPR_PROXY_CACHE.mkdir(parents=True, exist_ok=True)
    final = _proxy_path_for(candidate_id)
    source_duration = _ffprobe_duration(source)
    tmp_fd, tmp_name = tempfile.mkstemp(
        dir=CLPR_PROXY_CACHE, prefix=f'.c{candidate_id}_', suffix='.proxy.mp4.part')
    os.close(tmp_fd)
    tmp = Path(tmp_name)
    try:
        # -f mp4 forces the muxer explicitly: `tmp`'s real extension is
        # `.part` (tempfile.mkstemp's suffix, so a crashed encode can never
        # leave a `.mp4`-named partial file lying around for a stale glob to
        # pick up), and ffmpeg's own format auto-detection keys off the
        # extension it actually sees -- without this it refuses to guess an
        # output format at all (measured: "Unable to choose an output format
        # for '...proxy.mp4.part'").
        proc = subprocess.run(
            ['ffmpeg', '-y', '-i', str(source), *FFMPEG_PROXY_ARGS, '-f', 'mp4', str(tmp)],
            capture_output=True, text=True, timeout=300)
        if proc.returncode != 0 or not tmp.is_file() or tmp.stat().st_size == 0:
            print(f'PROXY_ENCODE_FAILED candidate_id={candidate_id} source={source} '
                  f'rc={proc.returncode} stderr={proc.stderr[-2000:]!r}', file=sys.stderr)
            tmp.unlink(missing_ok=True)
            return None
        return _finalize_proxy(tmp, final, source_duration, candidate_id)
    except Exception as exc:  # noqa: BLE001 -- a side channel must never break preview
        print(f'PROXY_GENERATE_FAILED candidate_id={candidate_id} source={source} '
              f'error={exc!r}', file=sys.stderr)
        tmp.unlink(missing_ok=True)
        return None


def _pending_candidates_for_eager() -> list[dict]:
    """The exact population `_serve_candidates('candidate')` shows the
    operator (D-056's delivery-gated pending WHERE clause), duplicated here
    for the same reason `_serve_run_progress`'s pending_count duplicates it:
    this addition must never touch that already-verified, live endpoint.
    Carries clips.file_path/drive_sync_path/created_at (LEFT JOIN, null when
    no clips row yet) so eager generation can resolve a source exactly the
    way `_serve_clip_media` does, for both the pre-approval-slice and the
    rendered-clip case."""
    conn = db.connect()
    try:
        cur = dict_cursor(conn)
        cur.execute(
            '''
            SELECT c.id AS candidate_id, cl.file_path, cl.drive_sync_path, cl.created_at
            FROM clip_candidates c
            LEFT JOIN clips cl ON cl.candidate_id = c.id
            WHERE (c.state = 'candidate' OR (c.state = 'approved' AND cl.drive_synced_at IS NULL))
            '''
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# One eager pass at a time -- a run finishing right as the server also
# starts must collapse into one pass, not two racing ones. Per-candidate
# work still serialises through `_clip_fetch_lock` exactly like an on-demand
# request would, so this can never race a live /clipmedia request either.
_eager_proxy_lock = threading.Lock()


def eager_generate_pending_proxies() -> None:
    """Background pass: build a proxy for every pending candidate that does
    not already have a valid one (operator ruling 2026-08-10: "let's go with
    eager" -- ready before he opens it, rather than generated on his first
    request). Triggered on server start (`main`) and after a LOCAL
    find-clips run completes (`_start_local_find_clips`'s watcher) -- the
    two triggers named in the build brief.

    Never raises out to its caller: eager generation is a pure optimization
    and must never take the server down or block a preview if a single
    candidate's fetch/encode fails (build brief point 6). Each candidate's
    own exception is caught and logged; the pass moves on to the next.
    """
    if not _eager_proxy_lock.acquire(blocking=False):
        print('EAGER_PROXY_PASS_SKIPPED already running', file=sys.stderr)
        return
    try:
        try:
            pending = _pending_candidates_for_eager()
        except Exception as exc:  # noqa: BLE001
            print(f'EAGER_PROXY_PASS_QUERY_FAILED error={exc!r}', file=sys.stderr)
            return
        built = 0
        for row in pending:
            candidate_id = int(row['candidate_id'])
            try:
                row_created_at = row.get('created_at')

                # FAST EARLY-OUT, NO LOCK: a candidate that already has a
                # valid proxy needs neither a resolve/fetch nor a lock at
                # all -- this is the function's own stated contract ("does
                # not already have a valid one"), and skipping here is what
                # keeps a repeated eager pass from re-fetching a multi-MB
                # source clip over ssh for a candidate that is already
                # done. Safe without the lock: `_generate_proxy` only ever
                # promotes a proxy via `os.replace` (atomic), so a read here
                # can observe an old-but-complete file or a new-but-complete
                # one, never a torn one.
                if _valid_proxy(candidate_id, row_created_at) is not None:
                    continue

                # RESOLVE/FETCH THE FULL CLIP -- DELIBERATELY OUTSIDE
                # `_clip_fetch_lock`. `fetch_clip_from_server` takes that
                # SAME per-candidate lock internally (it is how it
                # serialises its own cache write against a concurrent
                # on-demand request for the same candidate) -- `Lock` is
                # non-reentrant, so holding it here before calling into
                # `fetch_clip_from_server` deadlocks the calling thread
                # against itself on the very first candidate that needs a
                # fetch (confirmed by direct reproduction: the eager pass
                # printed its pending count and then produced no ffmpeg,
                # no ssh, no proxy dir, and never reached
                # EAGER_PROXY_PASS_DONE). `fetch_clip_from_server` needs no
                # help serialising here -- that is the whole reason it
                # takes the lock itself.
                if row.get('file_path') or row.get('drive_sync_path'):
                    source = resolve_local_clip(row.get('file_path'), row.get('drive_sync_path'))
                    if source is not None and _clip_file_is_stale(source, row_created_at):
                        source = None
                    if source is None:
                        source = fetch_clip_from_server(
                            row.get('file_path'), candidate_id, row_created_at)
                else:
                    # No clips row yet -- the pre-approval slice, same
                    # fallback _serve_clip_media uses (row_created_at is
                    # None there too: no clips row means no created_at
                    # to gate on).
                    slice_path = f'/home/node/.n8n/clpr/media/slices/c{candidate_id}.mp4'
                    source = fetch_clip_from_server(slice_path, candidate_id, None)
                    row_created_at = None
                if source is None:
                    continue

                # THE LOCK NOW GUARDS ONLY THE PROXY WORK -- exactly the
                # `_generate_proxy` contract ("MUST be called with
                # candidate_id's `_clip_fetch_lock` already held"), and
                # nothing this function calls inside the `with` below ever
                # takes `_clip_fetch_lock` itself, so there is no
                # reentrancy needed and none is used (an RLock here would
                # only paper over a lock-SCOPE error and leave the next
                # caller free to repeat it somewhere that does real harm).
                # Re-check `_valid_proxy` inside the lock: another thread
                # (a concurrent eager pass, once one exists) could have
                # built a proxy for this candidate in the window between
                # the early-out check above and acquiring the lock here.
                with _clip_fetch_lock(candidate_id):
                    if _valid_proxy(candidate_id, row_created_at) is not None:
                        continue
                    if _generate_proxy(source, candidate_id) is not None:
                        built += 1
            except Exception as exc:  # noqa: BLE001 -- one candidate must never stop the pass
                print(f'EAGER_PROXY_CANDIDATE_FAILED candidate_id={candidate_id} '
                      f'error={exc!r}', file=sys.stderr)
                continue
        print(f'EAGER_PROXY_PASS_DONE candidates={len(pending)} built={built}', file=sys.stderr)
    finally:
        _eager_proxy_lock.release()


def _spawn_eager_proxy_pass() -> None:
    """Fire-and-forget: run `eager_generate_pending_proxies` on a daemon
    thread so the caller (server startup, or a completed local run) never
    blocks on it (build brief point 6: run off the request thread)."""
    threading.Thread(target=eager_generate_pending_proxies, daemon=True).start()


def _watch_local_run_and_generate_proxies(proc: 'subprocess.Popen[bytes]') -> None:
    """Block (on a background thread, never the request thread) until the
    local find-clips child `proc` exits, then run the eager proxy pass --
    "after a run completes" (build brief point 6) for the local engine.
    Called from `_start_local_find_clips` right after it spawns `proc`."""
    try:
        proc.wait()
    except Exception as exc:  # noqa: BLE001 -- a side channel must never break anything else
        print(f'EAGER_PROXY_RUN_WATCH_FAILED error={exc!r}', file=sys.stderr)
        return
    _spawn_eager_proxy_pass()


def resolve_local_vod(stored_path: str) -> Path | None:
    """A locally-readable VIDEO for this recording, or None.

    Tries the stored path first (the local lane's rows are already correct and
    must keep working unchanged), then falls back to a stem search.
    """
    p = Path(stored_path)
    if p.suffix.lower() in VIDEO_EXTS and p.is_file():
        return p
    stem = p.stem
    for d in local_vod_dirs():
        for ext in VIDEO_EXTS:
            cand = d / f'{stem}{ext}'
            if cand.is_file():
                return cand
    return None
UI_PATH = Path(__file__).resolve().parent / 'review_ui.html'
POST_ACTION_RE = re.compile(r'^/api/candidates/(\d+)/(approve|reject|maybe)$')
POST_WINDOW_RE = re.compile(r'^/api/candidates/(\d+)/window$')
# D-061 post kit (2026-08-07 follow-ups). Each of these is its own route so a
# typo can never fall through into the verdict routes above.
POST_GENERATE_RE = re.compile(r'^/api/candidates/(\d+)/generate$')
# D-063 captions toggle. Its own route, for the same reason as the others: a
# typo must 404, never fall through into a verdict.
POST_CAPTIONS_RE = re.compile(r'^/api/candidates/(\d+)/captions-toggle$')
# D-074 amendment. Its own route, same reason as every other one here: a typo
# must 404, never fall through into a verdict.
POST_STYLE_RE = re.compile(r'^/api/candidates/(\d+)/render-style$')
POST_RERENDER_RE = re.compile(r'^/api/candidates/(\d+)/rerender$')
POST_KIT_RE = re.compile(r'^/api/candidates/(\d+)/kit$')
POST_KIT_REGEN_RE = re.compile(r'^/api/candidates/(\d+)/kit/regenerate$')
POST_SUBJECT_RE = re.compile(r'^/api/recordings/(\d+)/subject$')
GET_KITS_RE = re.compile(r'^/api/candidates/(\d+)/kits$')
GET_CAPTIONS_RE = re.compile(r'^/api/candidates/(\d+)/captions$')
GET_CAPTIONS_SRT_RE = re.compile(r'^/api/candidates/(\d+)/captions\.srt$')
# Reactive editor (2026-08-09 operator ask): live caption cues for the pending
# screen's overlay preview. Its own route, same reason as every other one
# here: a typo must 404, never fall through into a verdict.
GET_CUES_RE = re.compile(r'^/api/candidates/(\d+)/cues$')
GET_CLIP_MEDIA_RE = re.compile(r'^/clipmedia/(\d+)$')

# One truth for the candidate payload columns (list endpoints AND the window
# endpoint's 200 body use exactly this shape — the UI re-renders from either).
# D-055: adjusted_start_s/adjusted_end_s ride along in every candidate payload
# (null when unset); originals start_s/end_s are immutable. `state` rides
# along too (D-055 fixer) so the UI's editable backstop (c.state !==
# 'approved') keys on a value that actually exists in the payload.
# D-056: clip_state (the clips row's state, null when no row) and
# drive_synced_at (null when unset/no row) ride along additively — the
# delivery witness the UI badges on. clips has UNIQUE(candidate_id), so the
# LEFT JOIN in CANDIDATE_PAYLOAD_FROM can never fan a candidate into two rows.
# D-063: FOUR caption fields ride along, and they are four because they answer
# four different questions that are allowed to disagree (006's own reasoning):
#   burn_captions      — what the operator wants NOW, on the candidate. The
#                        toggle's state, and the ONLY one the toggle reflects.
#   captions_requested — what the render that made the existing file was asked.
#   captions_burned    — whether that file really carries captions. The ONLY
#                        field any surface may render as "this clip has
#                        captions", and it is null when no clip exists yet.
#   captions_cue_count — 0 with requested=1 is the honest "asked, but nobody
#                        spoke in this window" case, which is not a failure.
# A surface that showed the candidate flag on a delivered clip would be
# claiming a property of a FILE from a field about an INTENTION.
#
# D-074: three more intent fields ride along the same way burn_captions does
# (migration 010) — burn_hook, caption_color, hook_color. All three are
# candidate-level INTENT ONLY, same as burn_captions; there is no clips-table
# fact column for "was the hook really burned" (unlike captions_burned),
# because the migration deliberately did not add one — see 010's own header.
CANDIDATE_PAYLOAD_COLUMNS = '''
          c.id,
          c.recording_id AS vod_id,
          r.path AS vod_path,
          r.session_label,
          r.display_name,
          c.start_s,
          c.end_s,
          c.adjusted_start_s,
          c.adjusted_end_s,
          c.state,
          c.score,
          c.signal_audio,
          c.signal_transcript,
          c.signal_chat,
          c.signal_beat_boost,
          c.created_at,
          cl.state AS clip_state,
          cl.drive_synced_at,
          c.post_kit_enabled,
          c.burn_captions,
          cl.captions_requested,
          cl.captions_burned,
          cl.captions_cue_count,
          c.burn_hook,
          c.caption_color,
          c.hook_color'''

# One truth for the payload FROM clause (every query that SELECTs
# CANDIDATE_PAYLOAD_COLUMNS uses exactly these joins).
CANDIDATE_PAYLOAD_FROM = '''
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        LEFT JOIN clips cl ON cl.candidate_id = c.id'''

# D-061/D-062: the ACTIVE post kit for a candidate, the newest regenerate
# request, and the per-recording context.
#
# NONE of these joins can fan a candidate into two rows, and that is enforced
# by the database rather than by care: 003's `idx_post_kits_active` is a
# partial UNIQUE index on (candidate_id) WHERE is_active = 1, and
# `idx_recording_context_active` is the same shape on (recording_id). The
# request join is a LATERAL ... LIMIT 1. The delivered query asserts the row
# count against an independent count anyway (see _serve_delivered).
#
# Appended AFTER CANDIDATE_PAYLOAD_FROM, which stays the one truth for the
# base joins, and used only by the post-kit queries.
KIT_PAYLOAD_FROM = '''
        LEFT JOIN post_kits k ON k.candidate_id = c.id AND k.is_active = 1
        LEFT JOIN recording_context rc ON rc.recording_id = c.recording_id AND rc.is_active = 1
        LEFT JOIN LATERAL (
            SELECT q.id, q.state, q.error, q.requested_at, q.active_version_at_request,
                   q.force_over_operator_edit, q.satisfied_kit_version
            FROM post_kit_requests q
            WHERE q.candidate_id = c.id
            ORDER BY q.id DESC
            LIMIT 1
        ) req ON true'''

# The kit half of a post-kit payload. Every kit field is null when the
# candidate has no ACTIVE kit, which is exactly how the UI tells MISSING from
# EMPTY: kit_version null means no kit exists, and the UI renders that as "not
# generated yet", never as a kit whose fields happen to be blank.
#
# The three products, in the operator's own vocabulary:
#   on-video text -> hook_withheld / hook_domain / hook_payoff
#   captions      -> srt_text (built by workers/build_srt.py, the one truth for
#                    the rebasing geometry — this server never re-derives it)
#   video caption -> video_caption + hashtags
KIT_PAYLOAD_COLUMNS = ''',
          k.id AS kit_id,
          k.version AS kit_version,
          k.origin AS kit_origin,
          k.edited_from_version,
          k.hook_withheld,
          k.hook_domain,
          k.hook_payoff,
          k.video_caption,
          k.hashtags,
          k.quoted_line,
          k.srt_text,
          k.srt_segment_count,
          k.srt_basis,
          k.srt_clip_t0_abs_s,
          k.srt_clip_duration_s,
          k.scene_description,
          k.subject_kind AS kit_subject_kind,
          k.recording_context_version AS kit_recording_context_version,
          k.profile_version AS kit_profile_version,
          k.vision_model,
          k.writer_model,
          k.passthrough_degraded,
          k.created_at AS kit_created_at,
          rc.version AS context_version,
          rc.subject_kind,
          rc.subject_text,
          rc.context_notes,
          req.id AS request_id,
          req.state AS request_state,
          req.error AS request_error,
          req.requested_at AS request_requested_at,
          req.active_version_at_request,
          req.force_over_operator_edit,
          cl.duration_s AS clip_duration_s,
          cl.file_path AS clip_file_path,
          cl.drive_sync_path,
          cl.created_by_run AS clip_created_by_run'''


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def json_bytes(obj: object) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode('utf-8')


# D-074: the SAME regex migration 010's CHECK constraints enforce on
# caption_color/hook_color. Validating it here too means a malformed value
# 400s with a plain message instead of surfacing as a raw psycopg2
# IntegrityError from the database CHECK.
HEX_COLOR_RE = re.compile(r'^#?[0-9A-Fa-f]{6}$')


def _validate_hex_or_null(value: object) -> tuple[bool, Optional[str]]:
    """(True, None) for a JSON null (clears the color); (True, value.strip())
    for a string matching HEX_COLOR_RE; (False, None) for anything else."""
    if value is None:
        return True, None
    if not isinstance(value, str):
        return False, None
    stripped = value.strip()
    if not HEX_COLOR_RE.match(stripped):
        return False, None
    return True, stripped


def fire_verdict_webhook(candidate_id: int, recording_id: int, old_state: str, new_state: str) -> str:
    """POST the verdict to CLPR_VERDICT_WEBHOOK_URL (D-053). Fire-and-forget:
    a webhook failure NEVER fails the verdict HTTP response (the verdict is the
    money action; the webhook is bookkeeping) but is logged loudly to stderr.
    Returns 'ok' | 'failed' | 'unconfigured' for the response JSON."""
    url = os.environ.get('CLPR_VERDICT_WEBHOOK_URL', '').strip()
    if not url:
        return 'unconfigured'
    payload = {
        'candidate_id': candidate_id,
        'recording_id': recording_id,
        'old_state': old_state,
        'new_state': new_state,
        'ts': datetime.now(timezone.utc).isoformat(),
    }
    try:
        req = urllib.request.Request(
            url,
            data=json_bytes(payload),
            headers={
                'Content-Type': 'application/json',
                # The edge in front of n8n (Cloudflare) 403s urllib's default
                # `Python-urllib/<ver>` User-Agent as a bot signature. Measured
                # 2026-08-07 against the live endpoint: identical POST bodies,
                # UA=Python-urllib/3.9 -> 403, UA=curl -> 200. Every verdict
                # webhook silently "failed" until this header existed.
                'User-Agent': 'clpr-review-server/1.0',
            },
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=3):
            pass
        return 'ok'
    except Exception as exc:  # noqa: BLE001 — any failure is bookkeeping, never blocks the verdict
        print(
            f'WEBHOOK_FAILED candidate_id={candidate_id} recording_id={recording_id} '
            f'old_state={old_state} new_state={new_state} error={exc!r}',
            file=sys.stderr,
        )
        return 'failed'


def fire_find_clips_webhook() -> tuple[str, int]:
    """POST an empty form submission to CLPR_FINDER_WEBHOOK_URL -- the n8n
    Form Trigger that starts a new clip-finding run. UNLIKE
    fire_verdict_webhook this is NOT fire-and-forget: the operator clicked a
    button and is watching for the result, so the raw outcome (the upstream
    status code, or 0 when no response was ever received) is returned for
    the caller to put straight in the HTTP response, never just swallowed
    into a generic 'failed'.

    Same UA lesson as fire_verdict_webhook (D-053/2026-08-07): the edge in
    front of n8n (Cloudflare) 403s urllib's default `Python-urllib/<ver>`
    User-Agent as a bot signature, so the identical header override is used
    here.

    Returns ('unconfigured', 0) | ('ok', <2xx status>) |
    ('failed', <status, or 0 if the request never got a response at all>).
    """
    url = os.environ.get('CLPR_FINDER_WEBHOOK_URL', '').strip()
    if not url:
        return 'unconfigured', 0
    req = urllib.request.Request(
        url,
        data=b'',
        headers={
            # An empty form's default HTML encoding: no fields, no boundary.
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'clpr-review-server/1.0',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 'ok', resp.status
    except urllib.error.HTTPError as exc:
        return 'failed', exc.code
    except Exception as exc:  # noqa: BLE001 -- report loudly, start nothing
        print(f'FIND_CLIPS_WEBHOOK_FAILED error={exc!r}', file=sys.stderr)
        return 'failed', 0


# ---------------------------------------------------------------- local engine
# The LOCAL clip-finding engine (2026-08-09 operator ruling, verbatim: "the
# option to either trigger the portable or local engine... the result should
# be the same... the routing will just be different." DEFAULT = LOCAL.)
# Unlike the portable lane (an n8n webhook, fire-and-forget), the local lane
# is a real child process on THIS Mac -- app/workers/find_clips_local.py,
# which itself execs run_vod.py. This server only spawns it, remembers the
# pid in its OWN module state (this is in-memory: it does not survive a
# server restart -- find_clips_local.py's own file lock is the durable
# backstop for that case, see that module's docstring), and tails its log
# for honest status. It never re-implements any of find_clips_local's logic.
LOCAL_RUN_LOG = Path(os.environ.get(
    'CLPR_LOCAL_RUN_LOG', str(Path.home() / 'Library' / 'Logs' / 'clpr-local-run.log')))
FIND_CLIPS_LOCAL_PATH = Path(__file__).resolve().parent / 'workers' / 'find_clips_local.py'

_local_run_state_lock = threading.Lock()
_local_run_proc: Optional['subprocess.Popen[bytes]'] = None


def tail_last_nonempty_line(path: Path, tail_bytes: int = 65536) -> str:
    """The last non-empty line of `path`, reading only its final `tail_bytes`
    -- the logfile is appended forever across runs (never truncated), so a
    full read on every /api/run-progress poll would grow unboundedly slower
    over the project's life. Bounded, not exact-from-EOF: if the last
    `tail_bytes` happen to contain no non-empty line at all (essentially
    impossible given how short every line these workers print is), this
    returns '' rather than growing the read -- a smaller honest answer, not
    a hang."""
    if not path.exists():
        return ''
    try:
        with path.open('rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - tail_bytes))
            data = f.read()
    except OSError:
        return ''
    lines = [ln for ln in data.split(b'\n') if ln.strip()]
    if not lines:
        return ''
    return lines[-1].decode('utf-8', errors='replace')


def local_run_status() -> dict:
    """The local engine child's honest state for GET /api/run-progress
    (local_run: {alive, exit_code, last_line}). `alive`/`exit_code` come from
    THIS server process's own module state (see the module note above);
    `last_line` is read from the shared logfile regardless of module state,
    so it can carry a stale historical line when alive=False and
    exit_code=None (no run this server process knows about) -- callers
    (the UI) must treat that specific combination as "nothing to show",
    never trust last_line on its own."""
    with _local_run_state_lock:
        proc = _local_run_proc
    alive = False
    exit_code: Optional[int] = None
    if proc is not None:
        rc = proc.poll()
        if rc is None:
            alive = True
        else:
            exit_code = int(rc)
    return {
        'alive': alive,
        'exit_code': exit_code,
        'last_line': tail_last_nonempty_line(LOCAL_RUN_LOG),
    }


def fetch_candidate_payload(cur, candidate_id: int) -> Optional[dict]:
    """The candidate row in EXACTLY the list-endpoint shape (state included —
    CANDIDATE_PAYLOAD_COLUMNS carries it since the D-055 fixer)."""
    cur.execute(
        f'''
        SELECT{CANDIDATE_PAYLOAD_COLUMNS}{CANDIDATE_PAYLOAD_FROM}
        WHERE c.id = %s
        ''',
        (candidate_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_delivered_payload(cur, candidate_id: int) -> Optional[dict]:
    """One delivered-candidate row in EXACTLY the /api/candidates/delivered
    shape (candidate payload + latest kit + newest failed attempt), so every
    mutating kit endpoint can return the same object the list endpoint serves
    and the UI re-renders from either. No delivery filter here: a mutation
    response must describe the row it just changed, whatever its state."""
    cur.execute(
        f'''
        SELECT{CANDIDATE_PAYLOAD_COLUMNS}{KIT_PAYLOAD_COLUMNS}
        {CANDIDATE_PAYLOAD_FROM}{KIT_PAYLOAD_FROM}
        WHERE c.id = %s
        ''',
        (candidate_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def utc_now_iso() -> str:
    """The project's timestamp format, identical to the workers' own
    (cut_clip.utc_now_iso): ISO-8601 UTC, second precision, 'Z' suffix. One
    format everywhere is what makes the text columns comparable at all
    (charter §11 gate 21)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def fetch_profile(cur) -> Optional[dict]:
    """The ACTIVE global creator profile (003's creator_profile, versioned,
    one active row enforced by a partial unique index). None when the operator
    has never written one — reported as absent, never as an empty profile."""
    cur.execute(
        'SELECT id, version, channel_name, handle, platforms, style_notes, do_nots, '
        'extra_context, created_by, created_at '
        'FROM creator_profile WHERE is_active = 1'
    )
    row = cur.fetchone()
    return dict(row) if row else None


def is_finite_number(v: object) -> bool:
    """True for finite int/float; False for bool (a JSON true/false is not a
    number here) and for NaN/Infinity (json.loads accepts those literals)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


# ---------------------------------------------------------------- run progress
# GET /api/run-progress (2026-08-09 operator ask: "anyway to add a progress
# bar to shpw real dynamic progressiom" — REAL progress from real witnesses,
# never a fake timer). compute_run_stages is the one place the four-stage
# pipeline model lives, so the HTTP handler and this project's own test
# harness can both exercise the identical function.
RUN_STAGES = ('fetch', 'transcribe', 'detect', 'score')

# Membership sets, NOT a lexicographic '>=' compare on recordings.state text.
# Alphabetically 'detected' < 'transcribed', so a naive string '>=' would
# call a detected-or-later recording NOT yet transcribed — exactly the trap
# charter gate 21 (one instant, one format) warns about. Same shape as this
# codebase's own zebra_detect.is_recording_ready() (app/workers/zebra_detect.py).
TRANSCRIBED_OR_LATER = {'transcribed', 'detected', 'done'}
# score_fusion.py (app/workers/score_fusion.py) writes state='detected' once
# fusion/scoring finishes, WHETHER OR NOT it produced any candidates — 'done'
# included for forward-compatibility though nothing in this codebase writes
# it today. cand_count alone would leave 'score' stuck 'active' forever on a
# recording that legitimately scored zero.
SCORED_STATES = {'detected', 'done'}


def compute_run_stages(recording_state: str, seg_count: int, signal_count: int,
                        cand_count: int) -> dict:
    """The four-stage pipeline model, in pipeline order. Each stage's DONE
    condition is evidence-first (a real row count) with a state-vocabulary
    fallback for the legitimate-zero case (see SCORED_STATES above). The
    FIRST stage that is not done becomes 'active'; every stage after it is
    'waiting' regardless of its own condition — the ordering rule is applied
    literally, left to right, not per-stage independently."""
    done = {
        # The caller only reaches this function once it has confirmed a
        # recording row exists (that is what makes it non-idle), so fetch —
        # "a recording row exists" — is always already satisfied here.
        'fetch': True,
        'transcribe': recording_state in TRANSCRIBED_OR_LATER or seg_count > 0,
        'detect': signal_count > 0,
        'score': cand_count > 0 or recording_state in SCORED_STATES,
    }
    stages: dict = {}
    active_assigned = False
    for name in RUN_STAGES:
        if active_assigned:
            stages[name] = 'waiting'
        elif done[name]:
            stages[name] = 'done'
        else:
            stages[name] = 'active'
            active_assigned = True
    return stages


class ReviewHandler(BaseHTTPRequestHandler):
    server_version = 'clpr-review/0.1'

    def _send_json(self, status: int, payload: object) -> None:
        body = json_bytes(payload)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, text: str, content_type: str = 'text/plain; charset=utf-8',
                    head_only: bool = False) -> None:
        body = text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _serve_ui(self) -> None:
        if not UI_PATH.exists():
            self._send_text(HTTPStatus.NOT_FOUND, f'UI not found: {UI_PATH}')
            return
        body = UI_PATH.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def _serve_candidates(self, state: str = 'candidate') -> None:
        # D-056 ruling (operator verbatim 2026-08-06): "an approved clip
        # should not be removed from pending unless the webhook workflow
        # successfully saves it to drive." The delivery witness is a clips row
        # with drive_synced_at NOT NULL, so pending serves state='candidate'
        # rows PLUS state='approved' rows whose clips row (if any) has
        # drive_synced_at NULL. With the LEFT JOIN, cl.drive_synced_at IS NULL
        # covers both "no clips row" and "row present, witness unset".
        # Maybe/rejected queues are unchanged. Approve itself stays instant
        # and terminal (D-050); only this queue VIEW is delivery-gated.
        if state == 'candidate':
            where = ("(c.state = 'candidate' OR "
                     "(c.state = 'approved' AND cl.drive_synced_at IS NULL))")
            params: tuple = ()
        else:
            where = 'c.state = %s'
            params = (state,)
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                f'''
                SELECT{CANDIDATE_PAYLOAD_COLUMNS}{CANDIDATE_PAYLOAD_FROM}
                WHERE {where}
                ORDER BY c.score DESC NULLS LAST, c.id ASC
                ''',
                params,
            )
            rows = cur.fetchall()
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, [dict(r) for r in rows])

    def _serve_media(self, recording_id_text: str) -> None:
        try:
            recording_id = int(recording_id_text)
        except ValueError:
            self._send_text(HTTPStatus.BAD_REQUEST, 'Invalid vod_id')
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT path FROM recordings WHERE id = %s', (recording_id,))
            row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            self._send_text(HTTPStatus.NOT_FOUND, f'vod_id not found: {recording_id}')
            return

        file_path = resolve_local_vod(str(row['path']))
        if file_path is None:
            searched = ' ; '.join(str(d) for d in local_vod_dirs())
            self._send_text(
                HTTPStatus.NOT_FOUND,
                f'VOD not playable on this machine.\n'
                f'recordings.path = {row["path"]}\n'
                f'Searched for a video named "{Path(str(row["path"])).stem}.*" in: {searched}\n'
                f'Set CLPR_LOCAL_VOD_DIRS (colon-separated) to the folder holding the '
                f'source videos.')
            return

        self._serve_file_range(file_path)

    def _serve_clip_media(self, candidate_id_text: str, head_only: bool = False) -> None:
        """D-061: byte-range media for the RENDERED CLIP (not the source VOD),
        which is what the post-kit mockup plays.

        The clip is not always reachable from this machine, and that is a real
        state rather than an error to paper over: `clips.file_path` is written
        by whichever machine rendered, so an n8n-rendered clip's path points at
        the SERVER's filesystem; `drive_sync_path` carries two incompatible
        formats (a full local Drive-mount path from the Mac deliverer, a bare
        filename from the n8n lane) plus NULLs. So resolve in order, and 404
        with a distinct message when nothing resolves — the UI renders that as
        "clip file not reachable from this machine", never as a broken player.

        B4: a locally-resolved file (either a stale review-server cache, or a
        `resolve_local_clip` hit against a not-yet-re-synced Drive copy) is
        REJECTED as a hit when `clips.created_at` is newer than the file's own
        mtime -- that ordering can only happen when the clip was re-rendered
        after this copy was written, i.e. exactly the caption-only-re-render
        case that produces a byte-identical cache key. A rejected local hit
        falls through to `fetch_clip_from_server`, which re-pulls (and, for
        its own cache, overwrites) the current server-side render.

        PRE-APPROVAL FALLBACK (probed live 2026-08-09): a candidate with no
        `clips` row yet -- nothing has been rendered/approved -- still has a
        staged SLICE on the server at
        `/home/node/.n8n/clpr/media/slices/c<candidate_id>.mp4`, written by
        the scorer for every scored candidate. When the clips SELECT finds no
        row, and ONLY then, fall through to that slice via the same
        `fetch_clip_from_server` machinery (ssh + docker cat + atomic local
        cache, already per-candidate locked) instead of 404ing outright. The
        rendered-clip path above is untouched when a clips row DOES exist --
        even if it fails to resolve, this fallback does not fire, so an
        already-rendered clip that is merely unreachable still reports that
        distinctly rather than silently showing a stale pre-render slice.

        The unknown-candidate 404 stays distinct from "no clip yet": before
        falling back to the slice we check clip_candidates for the id, so a
        candidate id that never existed still 404s as "no candidate", not as
        a (nonexistent) slice-fetch failure.

        Cache key for the slice fetch: `fetch_clip_from_server` derives its
        cache filename as `c<candidate_id>_<basename(server_path)>`; the
        slice's basename is already `c<candidate_id>.mp4`, so the actual key
        on disk is `c<candidate_id>_c<candidate_id>.mp4` (e.g. `c1_c1.mp4`
        for candidate 1) -- NOT a separately-invented prefix. That key still
        falls inside `invalidate_clip_cache`'s `c<candidate_id>_*` glob, so a
        real render (which calls that sweep) still evicts a cached slice
        preview along with everything else cached for this candidate; the
        wipe procedure's cache-dir clear covers it too. A stale slice can
        therefore only be served in the narrow window between a re-render and
        its own cache invalidation, same as every other cached copy here.

        PREVIEW PROXIES (2026-08-10): checks for a valid proxy (`_valid_proxy`)
        FIRST in both branches, BEFORE any local resolve / ssh fetch of the
        full clip -- so a proxy already on disk is served even in the moment
        the full clip is unreachable (Drive unmounted, ssh down), and a
        reachable full clip is never paid for just to be thrown away in
        favour of the proxy. Generation itself never happens on this request
        path, only on the eager background pass
        (`eager_generate_pending_proxies`), so this handler never blocks on
        ffmpeg. A missing/stale/failed proxy is not an error: it falls
        straight through to resolving the full file, i.e. today's pre-proxy
        behaviour, unchanged. `_serve_file_range` (the Range/206/
        Accept-Ranges handling the operator's seeking depends on) is
        untouched either way."""
        try:
            candidate_id = int(candidate_id_text)
        except ValueError:
            self._send_text(HTTPStatus.BAD_REQUEST, 'Invalid candidate id', head_only=head_only)
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                'SELECT file_path, drive_sync_path, created_at FROM clips '
                'WHERE candidate_id = %s',
                (candidate_id,),
            )
            row = cur.fetchone()
            candidate_exists = None
            if not row:
                cur.execute('SELECT id FROM clip_candidates WHERE id = %s', (candidate_id,))
                candidate_exists = cur.fetchone()
        finally:
            conn.close()

        if not row:
            if not candidate_exists:
                self._send_text(
                    HTTPStatus.NOT_FOUND, f'no candidate {candidate_id}', head_only=head_only)
                return
            slice_path = f'/home/node/.n8n/clpr/media/slices/c{candidate_id}.mp4'
            proxy = _valid_proxy(candidate_id, None)
            if proxy is not None:
                self._serve_file_range(proxy, head_only=head_only)
                return
            resolved = fetch_clip_from_server(slice_path, candidate_id, None)
            if resolved is not None:
                self._serve_file_range(resolved, head_only=head_only)
                return
            self._send_text(
                HTTPStatus.NOT_FOUND,
                f'clip slice not reachable from this machine: candidate {candidate_id}',
                head_only=head_only,
            )
            return

        # Proxy-first, same as the no-row branch above: check BEFORE paying
        # for a local resolve / ssh fetch of the full clip, and BEFORE that
        # full-clip resolution can fail. A valid, non-stale proxy already on
        # disk (eager-built earlier) must be servable even in the moment the
        # full clip is unreachable (Drive unmounted, ssh down) -- checking
        # only after a successful full-clip resolve would 404 a candidate
        # whose proxy is sitting right there.
        proxy = _valid_proxy(candidate_id, row['created_at'])
        if proxy is not None:
            self._serve_file_range(proxy, head_only=head_only)
            return

        resolved = resolve_local_clip(row['file_path'], row['drive_sync_path'])
        if resolved is not None and _clip_file_is_stale(resolved, row['created_at']):
            resolved = None
        if resolved is None:
            # THE DURABLE FALLBACK: fetch it from the origin server.
            #
            # Everything above depends on a local copy existing, which makes it
            # machine-specific: it breaks on a laptop whose Drive mount differs,
            # and on a clip that is rendered but not yet delivered. Pulling from
            # the server removes both dependencies -- the clip is reachable
            # wherever ssh is, delivered or not. It also re-fetches when the
            # local copy above was rejected as stale.
            resolved = fetch_clip_from_server(row['file_path'], candidate_id, row['created_at'])
        if resolved is not None:
            self._serve_file_range(resolved, head_only=head_only)
            return

        self._send_text(
            HTTPStatus.NOT_FOUND,
            f'clip file not reachable from this machine: candidate {candidate_id}',
            head_only=head_only,
        )

    def _serve_file_range(self, file_path: Path, content_type: str = 'video/mp4',
                           head_only: bool = False) -> None:
        """HTTP byte-range serving. Extracted VERBATIM from _serve_media so the
        VOD route and the clip route have ONE implementation (charter §1.5
        gate 1) — the behaviour below is unchanged from the shipped, live
        version and is exercised by both callers.

        `head_only` (default False, additive — every existing caller is
        unaffected): send the identical status/headers a GET would send for
        this same Range, but skip writing the body, for a do_HEAD caller."""
        file_size = file_path.stat().st_size
        range_header = self.headers.get('Range', '').strip()
        start = 0
        end = file_size - 1
        partial = False

        if range_header:
            m = re.match(r'^bytes=(\d*)-(\d*)$', range_header)
            if not m:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header('Content-Range', f'bytes */{file_size}')
                self.end_headers()
                return

            start_text, end_text = m.groups()
            if start_text == '' and end_text == '':
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header('Content-Range', f'bytes */{file_size}')
                self.end_headers()
                return

            if start_text == '':
                suffix_len = int(end_text)
                if suffix_len <= 0:
                    self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                    self.send_header('Content-Range', f'bytes */{file_size}')
                    self.end_headers()
                    return
                start = max(file_size - suffix_len, 0)
                end = file_size - 1
            else:
                start = int(start_text)
                end = int(end_text) if end_text != '' else (file_size - 1)

            if start > end or start >= file_size:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header('Content-Range', f'bytes */{file_size}')
                self.end_headers()
                return

            end = min(end, file_size - 1)
            partial = True

        content_length = (end - start) + 1
        status = HTTPStatus.PARTIAL_CONTENT if partial else HTTPStatus.OK

        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Length', str(content_length))
        if partial:
            self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
        self.end_headers()

        if head_only:
            return

        with file_path.open('rb') as f:
            f.seek(start)
            remaining = content_length
            while remaining > 0:
                chunk = f.read(min(64 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

    def _transition_candidate(self, candidate_id: int, target_state: str) -> None:
        # D-050 ruling (operator verbatim): statuses upgradeable any time —
        # rejected can move back up to maybe/approved. approved stays terminal
        # (the publish gate): no entry, so every transition off it 409s.
        allowed = {
            'candidate': {'approved', 'rejected', 'maybe'},
            'maybe': {'approved', 'rejected'},
            'rejected': {'maybe', 'approved'},
        }

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT state FROM clip_candidates WHERE id = %s', (candidate_id,))
            row = cur.fetchone()
            if not row:
                self._send_json(HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'})
                return
            observed_state = str(row['state'])
            allowed_targets = allowed.get(observed_state, set())
            if target_state not in allowed_targets:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'candidate already decided',
                        'id': candidate_id,
                        'state': observed_state,
                        'requested_state': target_state,
                    },
                )
                return

            # ATOMIC APPROVE LOCK (B6 fixer, same pattern as _edit_window's
            # D-055 fixer, "ATOMIC APPROVE LOCK (D-055 fixer)" above): the
            # guard lives IN the UPDATE, binding the state THIS handler
            # observed above, not in a preceding SELECT the UPDATE trusts
            # blindly. Two concurrent approves both pass the allowed-targets
            # check (both observed 'candidate'), but only the WHERE-matched
            # UPDATE can ever change a row -- Postgres re-evaluates the WHERE
            # clause against the newly-committed row for whichever request
            # arrives second (READ COMMITTED's EvalPlanQual), so a genuine
            # loser always gets rowcount 0, never a second successful write.
            try:
                cur.execute(
                    'UPDATE clip_candidates SET state = %s WHERE id = %s AND state = %s',
                    (target_state, candidate_id, observed_state),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    cur.execute('SELECT state FROM clip_candidates WHERE id = %s', (candidate_id,))
                    row = cur.fetchone()
                    if not row:
                        self._send_json(
                            HTTPStatus.NOT_FOUND,
                            {'error': f'candidate not found: {candidate_id}'},
                        )
                        return
                    self._send_json(
                        HTTPStatus.CONFLICT,
                        {
                            'error': 'candidate already decided',
                            'id': candidate_id,
                            'state': str(row['state']),
                            'requested_state': target_state,
                        },
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = fetch_candidate_payload(cur, candidate_id)
        finally:
            conn.close()

        # This point is reached ONLY when the UPDATE above actually matched a
        # row (rowcount == 1) -- every rowcount == 0 path returns from inside
        # the try block (via the 404/409 branches) before falling through
        # here, so the loser of a race never reaches this call. Fires only
        # AFTER the commit succeeded (the verdict is durable); its outcome
        # rides along in the response so the UI could surface it.
        webhook_status = fire_verdict_webhook(
            candidate_id,
            int(updated['vod_id']) if updated else -1,
            observed_state,
            target_state,
        )
        if updated is not None:
            updated['webhook'] = webhook_status
        self._send_json(HTTPStatus.OK, updated)

    def _edit_window(self, candidate_id: int, body_raw: bytes) -> None:
        # D-055: operator window edit. Originals start_s/end_s are IMMUTABLE;
        # this endpoint only ever writes adjusted_start_s/adjusted_end_s.
        # NO webhook fires here — the webhook is a verdict signal only.
        try:
            body = json.loads(body_raw.decode('utf-8')) if body_raw else None
        except (ValueError, UnicodeDecodeError):
            self._send_json(HTTPStatus.BAD_REQUEST, {'error': 'body must be valid JSON'})
            return
        if not isinstance(body, dict) or 'start_s' not in body or 'end_s' not in body:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must be a JSON object with keys start_s and end_s'},
            )
            return

        start_s = body['start_s']
        end_s = body['end_s']
        if start_s is None and end_s is None:
            new_start: Optional[float] = None
            new_end: Optional[float] = None
        else:
            if not (is_finite_number(start_s) and is_finite_number(end_s)):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {'error': 'start_s and end_s must both be finite numbers, or both null to reset'},
                )
                return
            if not (0 <= start_s < end_s):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {'error': 'window must satisfy 0 <= start_s < end_s',
                     'start_s': start_s, 'end_s': end_s},
                )
                return
            new_start = float(start_s)
            new_end = float(end_s)

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            # ATOMIC APPROVE LOCK (D-055 fixer): the guard lives IN the UPDATE,
            # not in a preceding SELECT — a concurrent approve landing between
            # a check and the write can no longer be edited past. approved is
            # terminal (D-050, the publish gate), so a rowcount of 0 means the
            # row is either absent (404) or approved (409); re-SELECT to answer
            # honestly.
            try:
                cur.execute(
                    'UPDATE clip_candidates SET adjusted_start_s = %s, adjusted_end_s = %s '
                    "WHERE id = %s AND state <> 'approved'",
                    (new_start, new_end, candidate_id),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    cur.execute('SELECT state FROM clip_candidates WHERE id = %s', (candidate_id,))
                    row = cur.fetchone()
                    if not row:
                        self._send_json(
                            HTTPStatus.NOT_FOUND,
                            {'error': f'candidate not found: {candidate_id}'},
                        )
                        return
                    self._send_json(
                        HTTPStatus.CONFLICT,
                        {
                            'error': 'candidate already decided',
                            'id': candidate_id,
                            'state': str(row['state']),
                        },
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = fetch_candidate_payload(cur, candidate_id)
        finally:
            conn.close()

        self._send_json(HTTPStatus.OK, updated)

    def _serve_run_progress(self) -> None:
        """GET /api/run-progress: the pipeline's honest state, for the
        operator's progress bar (2026-08-09 ask: "anyway to add a progress
        bar to shpw real dynamic progressiom" — REAL witnesses, never a fake
        timer).

        Every number comes from ONE REPEATABLE READ read-only snapshot (same
        pattern as _serve_delivered below), so the counts in one response
        can never straddle a concurrent write mid-request — e.g. transcribe
        finishing and writing segments between two counts taken under READ
        COMMITTED, which could report 'detect active' with a segment count
        that never actually existed at any single instant.

        idle=true means the recordings table has no row at all: no run has
        ever started. A recording that finished (state='detected') is NOT
        idle — it is fully done, every stage 'done', none 'active'.

        pending_count is GLOBAL (every recording's queue), not scoped to the
        latest recording — it is the signal the UI uses to know this run
        produced reviewable output. Its WHERE clause MUST stay byte-identical
        to _serve_candidates('candidate')'s (D-056 delivery-gated pending);
        duplicated here rather than shared so this addition never touches
        that already-verified, live endpoint."""
        conn = db.connect()
        try:
            conn.set_session(isolation_level='REPEATABLE READ', readonly=True)
            cur = dict_cursor(conn)
            cur.execute('SELECT id, state, ingested_at FROM recordings ORDER BY id DESC LIMIT 1')
            latest = cur.fetchone()

            cur.execute(
                '''
                SELECT count(*) AS n
                FROM clip_candidates c
                LEFT JOIN clips cl ON cl.candidate_id = c.id
                WHERE (c.state = 'candidate' OR (c.state = 'approved' AND cl.drive_synced_at IS NULL))
                '''
            )
            pending_count = int(cur.fetchone()['n'])

            if latest is None:
                self._send_json(HTTPStatus.OK, {
                    'idle': True,
                    'recording_id': None,
                    'recording_state': None,
                    'started_at': None,
                    'stages': None,
                    'counts': None,
                    'pending_count': pending_count,
                    'local_run': local_run_status(),
                })
                return

            recording_id = int(latest['id'])
            recording_state = str(latest['state'])

            cur.execute(
                'SELECT count(*) AS n FROM transcript_segments WHERE recording_id = %s',
                (recording_id,),
            )
            seg_count = int(cur.fetchone()['n'])

            cur.execute(
                'SELECT count(*) AS n FROM llm_signal_candidates WHERE recording_id = %s',
                (recording_id,),
            )
            signal_count = int(cur.fetchone()['n'])

            cur.execute(
                'SELECT count(*) AS n FROM clip_candidates WHERE recording_id = %s',
                (recording_id,),
            )
            cand_count = int(cur.fetchone()['n'])

            cur.execute(
                '''
                SELECT count(*) AS n
                FROM clips cl
                JOIN clip_candidates c ON c.id = cl.candidate_id
                WHERE c.recording_id = %s
                ''',
                (recording_id,),
            )
            clip_count = int(cur.fetchone()['n'])
        finally:
            conn.close()

        self._send_json(HTTPStatus.OK, {
            'idle': False,
            'recording_id': recording_id,
            'recording_state': recording_state,
            'started_at': latest['ingested_at'],
            'stages': compute_run_stages(recording_state, seg_count, signal_count, cand_count),
            'counts': {
                'transcript_segments': seg_count,
                'llm_signal_candidates': signal_count,
                'clip_candidates': cand_count,
                'clips': clip_count,
            },
            'pending_count': pending_count,
            'local_run': local_run_status(),
        })

    # ---------------------------------------------------------------- D-061
    # THE POST KIT endpoints. None of them touches the verdict webhook or the
    # transition logic above: no verdict is fired, read, or altered anywhere
    # below this line.

    def _serve_delivered(self) -> None:
        """The POST KIT queue: candidates whose Drive delivery is WITNESSED
        (clips.drive_synced_at NOT NULL — D-056's unforgeable witness, the same
        one the pending queue uses in the opposite direction), each carrying
        its ACTIVE kit, the context that kit was written against, and its
        newest regenerate request.

        This also closes a structural hole: once drive_synced_at lands the card
        leaves Pending and the review UI has nowhere to show that candidate
        ever again.

        The row count is asserted against an INDEPENDENT count over the same
        predicate. A LEFT JOIN that fanned out would inflate this query and
        leave the plain count alone, and a silent duplicate would mean the
        operator edits one copy of a kit while looking at another.

        Both reads are taken in ONE REPEATABLE READ snapshot. Under READ
        COMMITTED (the default) each statement gets its own snapshot, so a
        BENIGN concurrent write -- `Mark Delivered` landing between the two
        SELECTs -- could move the plain count without moving the joined
        query (or vice versa) and trip this guard on nothing wrong at all.
        Pinning both reads to one snapshot means a mismatch can only mean
        the join itself actually fanned out or dropped a row."""
        conn = db.connect()
        try:
            conn.set_session(isolation_level='REPEATABLE READ', readonly=True)
            cur = dict_cursor(conn)
            cur.execute(
                f'''
                SELECT{CANDIDATE_PAYLOAD_COLUMNS}{KIT_PAYLOAD_COLUMNS}
                {CANDIDATE_PAYLOAD_FROM}{KIT_PAYLOAD_FROM}
                WHERE cl.drive_synced_at IS NOT NULL
                ORDER BY cl.drive_synced_at DESC, c.id DESC
                '''
            )
            rows = [dict(r) for r in cur.fetchall()]
            cur.execute(
                'SELECT count(*) AS n FROM clips WHERE drive_synced_at IS NOT NULL'
            )
            expected = int(cur.fetchone()['n'])
        finally:
            conn.close()

        if len(rows) != expected:
            # Same guard, opposite failure modes: too MANY rows is the LEFT
            # JOIN fanning out (more than one kit/context/request row per
            # candidate); too FEW is the join silently dropping a delivered
            # clip (a missing join match). Collapsing them into one message
            # pointed the operator at the wrong bug.
            kind = 'fanned out' if len(rows) > expected else 'missing join'
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    'error': f'delivered query {kind}',
                    'rows': len(rows),
                    'delivered_clips': expected,
                },
            )
            return
        self._send_json(HTTPStatus.OK, rows)

    def _serve_kits(self, candidate_id: int) -> None:
        """Full kit lineage for one candidate: every version, newest first,
        plus every regenerate request. Nothing is ever deleted from post_kits,
        so this is the audit trail that makes 'never silently overwrite the
        operator's own edits' checkable rather than asserted."""
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT id FROM clip_candidates WHERE id = %s', (candidate_id,))
            if cur.fetchone() is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                )
                return
            cur.execute(
                'SELECT id, version, origin, is_active, edited_from_version, '
                'hook_withheld, hook_domain, hook_payoff, video_caption, hashtags, '
                'quoted_line, srt_segment_count, srt_basis, subject_kind, '
                'recording_context_version, profile_version, vision_model, writer_model, '
                'passthrough_degraded, created_by_run, created_at '
                'FROM post_kits WHERE candidate_id = %s ORDER BY version DESC',
                (candidate_id,),
            )
            versions = [dict(r) for r in cur.fetchall()]
            cur.execute(
                'SELECT id, state, error, requested_by, requested_at, '
                'active_version_at_request, force_over_operator_edit, satisfied_kit_version '
                'FROM post_kit_requests WHERE candidate_id = %s ORDER BY id DESC',
                (candidate_id,),
            )
            requests = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
        self._send_json(
            HTTPStatus.OK,
            {'candidate_id': candidate_id, 'versions': versions, 'requests': requests},
        )

    def _serve_captions(self, candidate_id: int, as_file: bool) -> None:
        """CAPTIONS: the real speech transcription, as a subtitle file.

        THIS SERVER DOES NOT BUILD THE SRT AND MUST NOT. The rebasing geometry
        lives in workers/build_srt.py, which selects its basis from the
        RENDERER WITNESS (clips.created_by_run) — sidecar arithmetic for a
        server render, the clamp formula for a Mac render — and cross-checks
        the result against the ffprobe'd clips.duration_s. A second
        implementation here would be a second truth (charter §1.5 gate 1) and
        the two would disagree on exactly the clips where it matters. So this
        endpoint serves the stored bytes, and says plainly when there are
        none."""
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT id FROM clip_candidates WHERE id = %s', (candidate_id,))
            if cur.fetchone() is None:
                msg = f'candidate not found: {candidate_id}'
                if as_file:
                    self._send_text(HTTPStatus.NOT_FOUND, msg)
                else:
                    self._send_json(HTTPStatus.NOT_FOUND, {'error': msg})
                return
            cur.execute(
                'SELECT version, origin, srt_text, srt_segment_count, srt_basis, '
                'srt_clip_t0_abs_s, srt_clip_duration_s '
                'FROM post_kits WHERE candidate_id = %s AND is_active = 1',
                (candidate_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()

        if not row or not row['srt_text']:
            # Two different absences, said differently, because they need
            # different actions: no kit at all versus a kit whose window held
            # no transcript (under-claim — build_srt writes no SRT rather than
            # an empty one).
            reason = ('no post kit for this candidate yet'
                      if not row else
                      'this kit has no captions: the window held no transcript segments')
            if as_file:
                self._send_text(HTTPStatus.NOT_FOUND, reason)
            else:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {'error': reason, 'candidate_id': candidate_id,
                     'kit_version': (int(row['version']) if row else None)},
                )
            return

        srt_text = str(row['srt_text'])
        if as_file:
            data = srt_text.encode('utf-8')
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'application/x-subrip; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.send_header(
                'Content-Disposition',
                f'attachment; filename="candidate_{candidate_id}.srt"',
            )
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)
            return
        self._send_json(
            HTTPStatus.OK,
            {
                'candidate_id': candidate_id,
                'kit_version': int(row['version']),
                'kit_origin': str(row['origin']),
                'srt': srt_text,
                'segment_count': row['srt_segment_count'],
                'basis': row['srt_basis'],
                'clip_t0_abs_s': row['srt_clip_t0_abs_s'],
                'clip_duration_s': row['srt_clip_duration_s'],
            },
        )

    def _serve_cues(self, candidate_id: int, query: dict) -> None:
        """GET /api/candidates/<id>/cues -- LIVE preview cues for the pending
        screen's reactive editor (2026-08-09 operator ask), in ABSOLUTE
        (recording-relative) seconds -- the same domain as c.start_s/end_s
        and playerToAbs()/absToPlayer() in review_ui.html, NOT the
        clip-relative 0-based domain build_srt.py's own SRT output uses.

        THIS DOES NOT CONTRADICT _serve_captions's 'THIS SERVER DOES NOT
        BUILD THE SRT' rule just above -- that rule is about never RE-DERIVING
        the stored, versioned kit product build_srt.py already computed for a
        RENDERED clip (a second implementation would be a second truth).
        There is no stored product here to disagree with: this candidate has
        no clips row yet (see the !hasClip gate in review_ui.html's
        wireSlider -- a candidate with a real rendered clip does not reach
        this code path from the UI), and the whole point of a reactive
        preview is to recompute on every unsaved trim edit, before a kit
        (or even a render) exists. So this calls build_srt's OWN functions
        (rebase_segments, its MIN_CUE_S/MIN_SEGMENT_OVERLAP_S boundary rule)
        rather than reimplementing them -- one truth preserved, not broken.

        Geometry: the 'formula' basis every renderer converges to before a
        renderer-specific witness exists (slice_geometry.effective_window +
        render_pad_s, clamped to the recording's own duration) -- because a
        pending candidate never has a clips row, so build_srt's
        renderer-witness path (basis_for_run/resolve_clip_zero, which
        requires one) does not apply and is not called here.

        Query params start=/end= (both required together, floats): the UI's
        UNSAVED draft trim window, so cues react before Save. Passing them is
        itself the "operator edit" signal slice_geometry.render_pad_s() keys
        on, so the pad is 0.0 whenever they are present -- identical to the
        rule an actual saved adjusted_start_s/adjusted_end_s would trigger.
        """
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                '''
                SELECT c.recording_id, c.start_s, c.end_s,
                       c.adjusted_start_s, c.adjusted_end_s,
                       r.duration_s
                FROM clip_candidates c
                JOIN recordings r ON r.id = c.recording_id
                WHERE c.id = %s
                ''',
                (candidate_id,),
            )
            row = cur.fetchone()
            if not row:
                self._send_json(
                    HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                )
                return
            rec_dur = row['duration_s']
            if rec_dur is None:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {'error': f'recording_id={row["recording_id"]} has no duration_s yet, '
                              'so the preview window cannot be clamped'},
                )
                return
            rec_dur = float(rec_dur)

            raw_start = query.get('start', [None])[0]
            raw_end = query.get('end', [None])[0]
            if (raw_start is None) != (raw_end is None):
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {'error': 'start and end must be given together, or neither'},
                )
                return

            if raw_start is not None:
                try:
                    draft_start = float(raw_start)
                    draft_end = float(raw_end)
                except ValueError:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {'error': 'start and end must be finite numbers'},
                    )
                    return
                if not (math.isfinite(draft_start) and math.isfinite(draft_end)):
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {'error': 'start and end must be finite numbers'},
                    )
                    return
                eff_start = ts.clamp(draft_start, 0.0, rec_dur)
                eff_end = ts.clamp(draft_end, 0.0, rec_dur)
                if eff_end <= eff_start:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {'error': 'end must be after start once clamped to the recording',
                         'start': eff_start, 'end': eff_end},
                    )
                    return
                # An explicit draft window IS the operator-edit signal
                # render_pad_s() keys on (see the docstring above) -- 0.0 pad,
                # matching what saving this draft as adjusted_*_s would do.
                pad = 0.0
            else:
                eff_start, eff_end = slice_geometry.effective_window(
                    row['start_s'], row['end_s'],
                    row['adjusted_start_s'], row['adjusted_end_s'],
                )
                pad = slice_geometry.render_pad_s(row['adjusted_start_s'], row['adjusted_end_s'])

            t0 = ts.clamp(eff_start - pad, 0.0, rec_dur)
            t1 = ts.clamp(eff_end + pad, 0.0, rec_dur)
            duration = t1 - t0
            if duration <= 0:
                self._send_json(HTTPStatus.OK, [])
                return

            # transcript_signal.fetch_segments unpacks each row as a plain
            # (start_s, end_s, text) TUPLE -- the shape every build_srt.py
            # caller passes it (conn.cursor(), never a RealDictCursor). A
            # dict_cursor row here would iterate its KEYS on unpack, not its
            # values (measured: 'could not convert string to float: '
            # 'start_s'' the one time this used `cur` directly), so this
            # reuses the real function against the cursor shape it actually
            # expects rather than adapting the function to a dict row.
            plain_cur = conn.cursor()
            segments = ts.fetch_segments(plain_cur, int(row['recording_id']))
        finally:
            conn.close()

        window_segments = ts.transcript_slice_for_window(segments, t0, t1)
        # The REAL build_srt functions -- rebase_segments returns
        # clip-relative (0-based) cues, clamped/filtered per its own
        # MIN_SEGMENT_OVERLAP_S/MIN_CUE_S boundary rule; t0 is added back
        # here ONLY to translate the coordinate origin back to ABSOLUTE
        # (recording) seconds for the UI's playerToAbs() domain -- the
        # boundary/clamp logic itself is never reimplemented.
        rel_cues = build_srt.rebase_segments(window_segments, t0, duration)
        cues = [{'start': t0 + s, 'end': t0 + e, 'text': text} for s, e, text in rel_cues]

        self._send_json(HTTPStatus.OK, cues)

    def _serve_profile(self) -> None:
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            payload = fetch_profile(cur)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, {'profile': payload})

    def _parse_json_body(self, body_raw: bytes):
        """Returns (ok, value). A malformed body is rejected here so no
        endpoint below ever has to guess what the operator meant."""
        try:
            body = json.loads(body_raw.decode('utf-8')) if body_raw else None
        except (ValueError, UnicodeDecodeError):
            self._send_json(HTTPStatus.BAD_REQUEST, {'error': 'body must be valid JSON'})
            return False, None
        if not isinstance(body, dict):
            self._send_json(HTTPStatus.BAD_REQUEST, {'error': 'body must be a JSON object'})
            return False, None
        return True, body

    def _text_or_none(self, value: object):
        """Unknown stays null: a blank box is an ABSENT value, never an empty
        string masquerading as written copy."""
        if value is None:
            return None
        if not isinstance(value, str):
            return False  # sentinel: wrong type
        return value if value.strip() != '' else None

    def _set_generate(self, candidate_id: int, body_raw: bytes) -> None:
        """The per-clip GENERATE TOGGLE (clip_candidates.post_kit_enabled).
        Records INTENT ONLY — no generation fires from here.
        generate_post_kit.py reads this column and skips when it is 0.

        Deliberately NOT locked on approved candidates. The ruling grants
        flipping "any time before or at approval": it states a permission, not
        a prohibition, and this control has no publish consequence to protect
        (unlike a verdict, D-050). Locking it would leave an approved clip's
        toggle permanently unfixable, which is the worse failure."""
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return
        value = body.get('post_kit_enabled')
        if value is None:
            value = body.get('generate_kit')  # the UI's plainer name
        if not isinstance(value, bool):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must be a JSON object with boolean key post_kit_enabled'},
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            try:
                cur.execute(
                    'UPDATE clip_candidates SET post_kit_enabled = %s WHERE id = %s',
                    (1 if value else 0, candidate_id),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    self._send_json(
                        HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            updated = fetch_delivered_payload(cur, candidate_id)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, updated)

    def _set_captions(self, candidate_id: int, body_raw: bytes) -> None:
        """The per-clip BURN-CAPTIONS TOGGLE (clip_candidates.burn_captions).

        D-063, the operator's own shape: "C" (on demand, not always) plus "UI
        option while approving". So it is OFF by default, ticking it is the
        opt-in, and it is settable any time before or at approval.

        Records INTENT ONLY. No render fires from here and no existing file is
        touched: the burn happens inside whichever render runs next, in the
        same pass as the clip. It is deliberately NOT locked after approval,
        for the identical reason _set_generate is not (see there): the ruling
        grants a permission, it does not impose a prohibition, and locking it
        would leave an approved clip's toggle permanently unfixable.

        THE ONE THING THIS ENDPOINT MUST NOT DO is change any clips column.
        Flipping the toggle cannot make an already-rendered file gain or lose
        captions, so the fields that describe that file (captions_requested /
        captions_burned) are left exactly as the render wrote them. That gap is
        not an inconsistency to be tidied away, it is the operator asking for
        something the current file does not have — and the Mac-side deliverer
        refuses on exactly that signal.
        """
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return
        value = body.get('burn_captions')
        if value is None:
            value = body.get('captions')  # the UI's plainer name
        if not isinstance(value, bool):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must be a JSON object with boolean key burn_captions'},
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            try:
                cur.execute(
                    'UPDATE clip_candidates SET burn_captions = %s WHERE id = %s',
                    (1 if value else 0, candidate_id),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    self._send_json(
                        HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            updated = fetch_delivered_payload(cur, candidate_id)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, updated)

    def _set_render_style(self, candidate_id: int, body_raw: bytes) -> None:
        """The per-clip RENDER STYLE (clip_candidates.burn_hook /
        caption_color / hook_color, migration 010), D-074's amendment to
        D-063 and D-061.

        Same shape as _set_captions in every way that matters: records
        INTENT ONLY, fires no render, touches no clips column, and is
        deliberately NOT locked after approval or delivery for the identical
        reason _set_captions and _set_generate are not (see there) — the
        ruling grants a permission, it does not impose a prohibition, and
        locking it would leave an approved or delivered clip's style
        permanently unfixable. Flipping this cannot change a file that has
        already shipped; the review UI says so with the same "applies to the
        next render" wording _set_captions's own docstring explains.

        All three fields travel together in ONE request because the review
        UI saves them as one unit (the toggle plus both color pickers via a
        single "Save style" action, the window editor's dirty/save
        convention — see review_ui.html), not three independent PATCHes. So
        this endpoint requires all three keys present, not a partial update:
        a request missing one is a client bug, not a legitimate partial
        write, and 400s rather than guessing what was meant.
        """
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return

        if 'burn_hook' not in body or not isinstance(body['burn_hook'], bool):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must be a JSON object with boolean key burn_hook'},
            )
            return
        burn_hook = body['burn_hook']

        if 'caption_color' not in body:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must include key caption_color (a 6-hex-digit color string or null)'},
            )
            return
        cap_ok, caption_color = _validate_hex_or_null(body['caption_color'])
        if not cap_ok:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'caption_color must be null or a 6-hex-digit color, e.g. "#FFE600"'},
            )
            return

        if 'hook_color' not in body:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must include key hook_color (a 6-hex-digit color string or null)'},
            )
            return
        hook_ok, hook_color = _validate_hex_or_null(body['hook_color'])
        if not hook_ok:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'hook_color must be null or a 6-hex-digit color, e.g. "#00E676"'},
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            try:
                cur.execute(
                    'UPDATE clip_candidates SET burn_hook = %s, caption_color = %s, hook_color = %s '
                    'WHERE id = %s',
                    (1 if burn_hook else 0, caption_color, hook_color, candidate_id),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    self._send_json(
                        HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            updated = fetch_delivered_payload(cur, candidate_id)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, updated)

    def _save_kit(self, candidate_id: int, body_raw: bytes) -> None:
        """Save an operator edit as a NEW VERSION with origin='operator_edit'.

        Nothing is ever UPDATEd in place except the is_active flag, so his
        words cannot be overwritten by a later regenerate and every earlier
        version stays readable forever. From this point on the generator
        refuses to supersede it without an explicit --force.

        base_version is REQUIRED and must still be the ACTIVE version. That is
        the charter's bound-to-what-was-DISPLAYED rule made mechanical: if a
        regenerate landed while this form was open, the save 409s with the
        current version instead of silently burying it.

        The deactivate and the insert are ONE transaction. 003's partial unique
        index (candidate_id) WHERE is_active = 1 means a half-done save cannot
        leave two active kits — it would fail the index and roll back."""
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return

        base_version = body.get('base_version')
        if not isinstance(base_version, int) or isinstance(base_version, bool):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'body must carry integer base_version (the version you edited)'},
            )
            return

        # 003 declares the three hooks and the caption NOT NULL, so an edit
        # must carry all four. Refusing here is honest: the alternative is
        # inventing a value for a field the operator cleared.
        required = {}
        for field in ('hook_withheld', 'hook_domain', 'hook_payoff', 'video_caption'):
            v = body.get(field)
            if not isinstance(v, str) or v.strip() == '':
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {'error': f'{field} is required and must be a non-empty string',
                     'field': field},
                )
                return
            required[field] = v

        hashtags = body.get('hashtags')
        if hashtags is None:
            tags = None
        elif isinstance(hashtags, list) and all(isinstance(t, str) for t in hashtags):
            tags = [t.strip() for t in hashtags if t.strip() != ''] or None
        elif isinstance(hashtags, str):
            tags = [t for t in hashtags.replace(',', ' ').split() if t] or None
        else:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'hashtags must be a list of strings, a string, or null'},
            )
            return

        srt_text = self._text_or_none(body.get('srt_text'))
        if srt_text is False:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {'error': 'srt_text must be a string or null'}
            )
            return
        quoted_line = self._text_or_none(body.get('quoted_line'))
        if quoted_line is False:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {'error': 'quoted_line must be a string or null'}
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT id FROM clip_candidates WHERE id = %s', (candidate_id,))
            if cur.fetchone() is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                )
                return

            cur.execute(
                'SELECT * FROM post_kits WHERE candidate_id = %s AND is_active = 1',
                (candidate_id,),
            )
            active = cur.fetchone()
            if not active:
                # Nothing to edit. Refusing is the honest answer: an operator
                # edit SUPERSEDES a version, and superseding nothing would
                # fabricate a kit the generator never produced. 003's
                # post_kits_edit_has_parent CHECK says the same thing in SQL.
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {'error': f'no active post kit for candidate {candidate_id}, nothing to edit'},
                )
                return
            current_version = int(active['version'])
            if base_version != current_version:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'kit changed since you opened it',
                        'id': candidate_id,
                        'base_version': base_version,
                        'current_version': current_version,
                    },
                )
                return

            cur.execute(
                'SELECT COALESCE(MAX(version), 0) AS v FROM post_kits WHERE candidate_id = %s',
                (candidate_id,),
            )
            new_version = int(cur.fetchone()['v']) + 1

            # Carry forward the parts of the kit an edit does not author:
            # the captions witnesses (built by build_srt, still true), the
            # scene description (the input the copy came from), and the
            # context provenance (which context this lineage was written
            # against). Model identifiers are deliberately NOT carried: his
            # words are not a model's output and must never be labelled as one.
            try:
                # DEACTIVATE FIRST. 003's `idx_post_kits_active` is a partial
                # UNIQUE index on (candidate_id) WHERE is_active = 1, so an
                # INSERT of a second active row while the old one is still
                # active violates it — the save then fails with a duplicate-key
                # error that reads like a lost race but is really just the
                # wrong statement order. (Found by the harness on the first
                # real save: every operator edit 409'd with "a newer kit
                # version landed while saving" when nothing had landed at all.
                # The comment here was right and the code was backwards.)
                # Both statements are in ONE transaction, so a failure leaves
                # the original kit active and untouched.
                cur.execute(
                    'UPDATE post_kits SET is_active = 0 '
                    'WHERE candidate_id = %s AND is_active = 1',
                    (candidate_id,),
                )
                cur.execute(
                    '''
                    INSERT INTO post_kits
                        (candidate_id, version, origin, is_active, edited_from_version,
                         hook_withheld, hook_domain, hook_payoff, video_caption, hashtags,
                         quoted_line, srt_text, srt_segment_count, srt_basis,
                         srt_clip_t0_abs_s, srt_clip_duration_s, scene_description,
                         subject_kind, recording_context_version, profile_version,
                         prompt_version, created_by_run, created_at)
                    VALUES (%s, %s, 'operator_edit', 1, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s)
                    RETURNING id
                    ''',
                    (
                        candidate_id, new_version, current_version,
                        required['hook_withheld'], required['hook_domain'],
                        required['hook_payoff'], required['video_caption'], tags,
                        quoted_line if quoted_line is not None else active['quoted_line'],
                        srt_text if srt_text is not None else active['srt_text'],
                        active['srt_segment_count'], active['srt_basis'],
                        active['srt_clip_t0_abs_s'], active['srt_clip_duration_s'],
                        active['scene_description'],
                        active['subject_kind'], active['recording_context_version'],
                        active['profile_version'], active['prompt_version'],
                        'review_ui_operator_edit', utc_now_iso(),
                    ),
                )
                # Assert the end state rather than trusting two statements to
                # have composed: exactly one active kit, and it is the new
                # version. Charter gate 22 — a write is proven by a witness,
                # never by the driver not throwing.
                cur.execute(
                    'SELECT count(*) AS n FROM post_kits '
                    'WHERE candidate_id = %s AND is_active = 1 AND version = %s',
                    (candidate_id, new_version),
                )
                if int(cur.fetchone()['n']) != 1:
                    conn.rollback()
                    self._send_json(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {'error': 'the saved version did not become the single active kit'},
                    )
                    return
                conn.commit()
            except psycopg2.IntegrityError as exc:
                # UNIQUE (candidate_id, version) and the partial active index
                # are the race backstops: two saves racing on the same base
                # computed the same new version and exactly one wins.
                conn.rollback()
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'a newer kit version landed while saving',
                        'id': candidate_id,
                        'attempted_version': new_version,
                        'detail': str(exc).strip(),
                    },
                )
                return
            except Exception:
                conn.rollback()
                raise

            updated = fetch_delivered_payload(cur, candidate_id)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, updated)

    def _find_clips(self, body_raw: bytes) -> None:
        """POST /api/find-clips: start a new clip-finding run, either engine
        (2026-08-09 operator ruling, verbatim: "the option to either trigger
        the portable or local engine... the result should be the same...
        the routing will just be different." DEFAULT = LOCAL).

        engine resolution:
          - no body, or a body whose "engine" key is absent/empty -> local
            (the ruling's default)
          - {"engine": "local"} -> local (workers/find_clips_local.py, this
            Mac)
          - {"engine": "portable"} -> UNCHANGED below: the existing n8n Form
            Trigger webhook relay. No DB row here either -- n8n owns
            everything about that run itself; this endpoint only starts it
            and reports whether the start succeeded.
          - any other engine value -> refused (400), naming the bad value
        """
        engine = 'local'
        if body_raw:
            try:
                payload = json.loads(body_raw.decode('utf-8'))
            except Exception:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    'started': False,
                    'error': 'Request body is not valid JSON.',
                })
                return
            if not isinstance(payload, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    'started': False,
                    'error': 'Request body must be a JSON object.',
                })
                return
            raw_engine = payload.get('engine')
            if raw_engine:  # a non-empty value was explicitly given
                if raw_engine not in ('local', 'portable'):
                    self._send_json(HTTPStatus.BAD_REQUEST, {
                        'started': False,
                        'error': f'Unknown engine "{raw_engine}"; expected "local" or "portable".',
                    })
                    return
                engine = raw_engine
            # engine key omitted, None, or '' -> falls through, stays 'local'

        if not _find_clips_lock.acquire(blocking=False):
            self._send_json(HTTPStatus.CONFLICT, {
                'started': False,
                'error': 'A find-clips run is already starting. Wait a moment and try again.',
            })
            return
        try:
            if engine == 'local':
                self._start_local_find_clips()
                return
            result, status = fire_find_clips_webhook()
            if result == 'unconfigured':
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                    'started': False,
                    'error': 'CLPR_FINDER_WEBHOOK_URL is not set, so nothing would run the '
                             'clip search. Set it in the environment before using this button.',
                })
                return
            if result == 'ok' and 200 <= status < 300:
                self._send_json(HTTPStatus.OK, {'started': True})
                return
            self._send_json(HTTPStatus.BAD_GATEWAY, {
                'started': False,
                'error': f'The find-clips webhook did not succeed (status={status}).',
                'status': status,
            })
        finally:
            _find_clips_lock.release()

    def _start_local_find_clips(self) -> None:
        """The LOCAL half of _find_clips: spawn workers/find_clips_local.py
        detached (fire-and-forget -- this HTTP request does not wait for the
        run), remembering its pid so a second overlapping POST can be
        refused loudly (409) without even spawning a redundant process.
        Called ONLY from inside _find_clips's _find_clips_lock critical
        section, same as the portable branch above.
        """
        sa_json = os.environ.get('CLPR_GDRIVE_SA_JSON', '').strip()
        if not sa_json:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {
                'started': False,
                'error': 'CLPR_GDRIVE_SA_JSON is not set, so the local engine has no '
                         'service-account key to read the to_clip Drive folder with. Set it '
                         'in the environment before using this button.',
            })
            return

        global _local_run_proc
        with _local_run_state_lock:
            if _local_run_proc is not None and _local_run_proc.poll() is None:
                self._send_json(HTTPStatus.CONFLICT, {
                    'started': False,
                    'error': 'A local find-clips run is already in progress. Wait for it to finish.',
                })
                return

            LOCAL_RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
            # CLPR_GDRIVE_SA_JSON (checked above) and every other CLPR_* override
            # (CLPR_DB_URL, CLPR_LOCAL_LOCK_FILE, CLPR_SSH_HOST, ...) reach the
            # child by ordinary environment inheritance -- no `env=` override
            # here, so nothing is filtered out ("read at spawn and pass
            # through").
            logf = open(LOCAL_RUN_LOG, 'a', buffering=1, encoding='utf-8')
            try:
                proc = subprocess.Popen(
                    [sys.executable or 'python3', str(FIND_CLIPS_LOCAL_PATH)],
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            finally:
                # The child already holds its own duplicated fd to the file
                # (Popen dup2's it before this returns); closing OUR handle
                # does not affect the child's ability to keep writing.
                logf.close()
            _local_run_proc = proc

            # Preview proxies (2026-08-10, eager): "after a run completes" for
            # the LOCAL engine, whose completion this server can observe
            # directly by waiting on its own child -- unlike the portable/n8n
            # lane, which finishes on n8n's machine with no signal back here
            # (the on-server-start pass in main() is that lane's coverage).
            # Runs on its own daemon thread so this HTTP response is never
            # delayed by it.
            threading.Thread(
                target=_watch_local_run_and_generate_proxies, args=(proc,), daemon=True,
            ).start()

        self._send_json(HTTPStatus.OK, {'started': True, 'engine': 'local'})

    def _rerender_clip(self, candidate_id: int, body_raw: bytes) -> None:
        """RE-RENDER an already-rendered clip, so a caption decision taken after
        delivery can actually reach Drive.

        WHY THIS EXISTS. `burn_captions` is a decision on the CANDIDATE and is
        read at render time, so un-ticking it after delivery recorded the intent
        and changed nothing: the delivered file keeps whatever was burned into
        it, and neither verdict buttons nor `Regenerate` (which rebuilds the
        post-kit COPY, never the video) re-render. The operator's decision was
        being stored and ignored, which is worse than refusing it.

        WHAT IT DOES, and what it deliberately does not. It re-fires the
        verdict webhook, which re-runs the SAME approved chain (render ->
        upload -> mark delivered) rather than introducing a second rendering
        path that could drift from it, and clears the D-056 delivery witness
        ONLY once that webhook comes back 'ok'. The witness-clear is what
        re-opens the clip: the pending queue is `state='approved' AND
        drive_synced_at IS NULL`. Firing before clearing means a webhook
        failure leaves the DB untouched -- n8n re-renders from the DB, so
        firing again on retry is safe -- rather than clearing the witness for
        a chain that never ran and stranding the clip: badged for an upload
        nothing will start, while the review UI reports "queued".

        It refuses on a candidate that is not approved -- there is nothing to
        re-render -- and it refuses when the webhook is unconfigured rather than
        clearing the witness and stranding the clip as undelivered with no
        chain to deliver it.
        """
        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                'SELECT c.state, c.recording_id, c.burn_captions, cl.id AS clip_id, '
                '       cl.drive_sync_path '
                'FROM clip_candidates c LEFT JOIN clips cl ON cl.candidate_id = c.id '
                'WHERE c.id = %s',
                (candidate_id,),
            )
            row = cur.fetchone()
            if row is None:
                self._send_json(HTTPStatus.NOT_FOUND,
                                {'error': f'no candidate {candidate_id}'})
                return
            if row['state'] != 'approved':
                self._send_json(HTTPStatus.CONFLICT, {
                    'error': f'candidate {candidate_id} is {row["state"]}, not approved. '
                             'Only an approved clip can be re-rendered.'})
                return
            if row['clip_id'] is None:
                self._send_json(HTTPStatus.CONFLICT, {
                    'error': f'candidate {candidate_id} has no clip row yet — '
                             'nothing has been rendered to re-render.'})
                return
            if not os.environ.get('CLPR_VERDICT_WEBHOOK_URL', '').strip():
                self._send_json(HTTPStatus.CONFLICT, {
                    'error': 'CLPR_VERDICT_WEBHOOK_URL is not set, so nothing would run the '
                             're-render. Refusing rather than clearing the delivery witness '
                             'and stranding the clip.'})
                return

            previous_drive_name = row['drive_sync_path']
            recording_id = row['recording_id']
            burn = row['burn_captions']
        finally:
            conn.close()

        # Fire FIRST. Only on 'ok' do we touch the DB -- clearing the witness
        # for a webhook that did not run would strand the clip: it drops out
        # of the pending queue with nothing left to actually re-render it.
        hook = fire_verdict_webhook(candidate_id, recording_id, 'approved', 'approved')
        if hook != 'ok':
            self._send_json(HTTPStatus.BAD_GATEWAY, {
                'candidate_id': candidate_id,
                'rerender': 'failed',
                'webhook': hook,
                'error': f'The re-render webhook did not succeed (webhook={hook}). Nothing '
                         'changed: the delivery witness was NOT cleared, the clip is still '
                         'recorded as delivered, and no re-render was started. Retry once the '
                         'webhook endpoint is reachable.',
            })
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                'UPDATE clips SET drive_synced_at = NULL, drive_sync_path = NULL '
                'WHERE candidate_id = %s',
                (candidate_id,),
            )
            conn.commit()
            # B4: the re-render is about to write NEW bytes under the SAME
            # cache key (deliver_approved.delivered_name() is deterministic
            # in session_label/start_s/category/candidate_id, so a
            # caption-only change does not change the filename). Sweep every
            # cached copy of this candidate now, so nothing in the review
            # server's own cache -- or a stale `resolve_local_clip` local
            # copy the mtime check has not yet seen replaced -- can be
            # served as if it were the fresh render.
            removed = invalidate_clip_cache(candidate_id)
            print(f'RERENDER_CACHE_INVALIDATED candidate={candidate_id} removed={removed}')
        finally:
            conn.close()

        self._send_json(HTTPStatus.OK, {
            'candidate_id': candidate_id,
            'rerender': 'queued',
            'burn_captions': burn,
            'webhook': hook,
            # The Drive node uploads rather than replaces, so a copy left in the
            # folder becomes a same-named duplicate. Say so plainly instead of
            # letting the operator discover two files.
            'previous_drive_file': previous_drive_name,
            'note': ('The clip re-renders with the CURRENT caption setting and re-uploads. '
                     'Delete the previous copy from Drive if it is still there — the upload '
                     'creates a new file rather than replacing one.'),
        })

    def _request_regenerate(self, candidate_id: int, body_raw: bytes) -> None:
        """Ask for a fresh machine kit. Records INTENT ONLY into
        post_kit_requests (004): it writes NOTHING to post_kits, deletes
        nothing, and deactivates nothing, so an existing kit — the operator's
        especially — survives this call untouched no matter what.

        THE OPERATOR-EDIT GUARD LIVES HERE, IN THE API, not only in the
        browser dialog: if the active version is an operator edit, this 409s
        unless the request carries confirm=true, and only then does the
        recorded request carry force_over_operator_edit=1, which is the flag
        generate_post_kit.py needs before it will supersede that edit. A rule
        enforced only by a browser button is not enforced (charter §11 gate
        17: the two exits must reach genuinely different outcomes)."""
        if body_raw:
            ok, body = self._parse_json_body(body_raw)
            if not ok:
                return
        else:
            body = {}
        confirm = body.get('confirm') is True

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT id FROM clip_candidates WHERE id = %s', (candidate_id,))
            if cur.fetchone() is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND, {'error': f'candidate not found: {candidate_id}'}
                )
                return

            cur.execute(
                'SELECT version, origin FROM post_kits WHERE candidate_id = %s AND is_active = 1',
                (candidate_id,),
            )
            active = cur.fetchone()
            is_operator_edit = bool(active and str(active['origin']) == 'operator_edit')
            if is_operator_edit and not confirm:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'the active kit is your own edit, regenerating would supersede it',
                        'id': candidate_id,
                        'current_version': int(active['version']),
                        'requires_confirm': True,
                    },
                )
                return

            cur.execute(
                'SELECT COALESCE(MAX(version), 0) AS v FROM post_kits WHERE candidate_id = %s',
                (candidate_id,),
            )
            active_version = int(cur.fetchone()['v'])

            try:
                cur.execute(
                    '''
                    INSERT INTO post_kit_requests
                        (candidate_id, active_version_at_request, force_over_operator_edit,
                         state, requested_by, requested_at)
                    VALUES (%s, %s, %s, 'requested', 'review_ui', %s)
                    RETURNING id
                    ''',
                    (
                        candidate_id, active_version,
                        1 if (is_operator_edit and confirm) else 0,
                        utc_now_iso(),
                    ),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    self._send_json(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {'error': f'request insert touched {cur.rowcount} rows, expected 1'},
                    )
                    return
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = fetch_delivered_payload(cur, candidate_id)
        finally:
            conn.close()
        if updated is not None:
            # The request is now a real queued intent that generate_post_kit.py
            # CONSUMES: running the post-kit lane for this candidate answers it,
            # marks it satisfied, and on failure marks it failed with the reason
            # verbatim. The hand command is kept for the case where the operator
            # wants to answer it himself at a terminal — it needs no flags,
            # because the request already carries the regenerate and, when he
            # confirmed it, the force.
            updated['regenerate_command'] = (
                'python3 app/workers/generate_post_kit.py --candidate-id '
                f'{candidate_id}'
            )
        self._send_json(HTTPStatus.OK, updated)

    def _save_profile(self, body_raw: bytes) -> None:
        """The global creator profile. VERSIONED, never overwritten: saving
        deactivates the current active row and inserts a new version in the
        same transaction, so every profile the copy was ever written against
        stays readable (post_kits.profile_version points straight at it).

        Optimistic concurrency on the version the client displayed: a mismatch
        409s rather than burying an edit made elsewhere."""
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return

        fields = {}
        for field in ('channel_name', 'handle', 'platforms', 'style_notes',
                      'do_nots', 'extra_context'):
            v = self._text_or_none(body.get(field))
            if v is False:
                self._send_json(
                    HTTPStatus.BAD_REQUEST, {'error': f'{field} must be a string or null'}
                )
                return
            fields[field] = v
        if all(v is None for v in fields.values()):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': 'a profile with every field empty is not a profile, fill at least one'},
            )
            return

        base_version = body.get('base_version')
        if base_version is not None and (
            not isinstance(base_version, int) or isinstance(base_version, bool)
        ):
            self._send_json(
                HTTPStatus.BAD_REQUEST, {'error': 'base_version must be an integer or null'}
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            current = fetch_profile(cur)
            current_version = int(current['version']) if current else None
            if base_version != current_version:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'profile changed since you opened it',
                        'base_version': base_version,
                        'current_version': current_version,
                    },
                )
                return

            cur.execute('SELECT COALESCE(MAX(version), 0) AS v FROM creator_profile')
            new_version = int(cur.fetchone()['v']) + 1
            try:
                cur.execute(
                    'UPDATE creator_profile SET is_active = 0 WHERE is_active = 1'
                )
                cur.execute(
                    '''
                    INSERT INTO creator_profile
                        (version, channel_name, handle, platforms, style_notes, do_nots,
                         extra_context, is_active, created_by, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 'operator', %s)
                    ''',
                    (
                        new_version, fields['channel_name'], fields['handle'],
                        fields['platforms'], fields['style_notes'], fields['do_nots'],
                        fields['extra_context'], utc_now_iso(),
                    ),
                )
                conn.commit()
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {'error': 'a newer profile version landed while saving',
                     'detail': str(exc).strip()},
                )
                return
            except Exception:
                conn.rollback()
                raise
            payload = fetch_profile(cur)
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, {'profile': payload})

    def _set_subject(self, recording_id: int, body_raw: bytes) -> None:
        """The per-recording SUBJECT (003's recording_context, versioned).

        subject_kind is 'me' or 'other' and there is no third value and no
        default, by design: absence of a row means UNKNOWN, and the generator
        says so out loud rather than assuming the operator is on screen.
        'other' REQUIRES subject_text, which the schema also enforces — a
        named other with no description would leave the writer model to invent
        who the person is."""
        ok, body = self._parse_json_body(body_raw)
        if not ok:
            return
        subject_kind = body.get('subject_kind')
        if subject_kind not in ('me', 'other'):
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': "subject_kind must be 'me' or 'other'", 'got': subject_kind},
            )
            return
        subject_text = self._text_or_none(body.get('subject_text'))
        if subject_text is False:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {'error': 'subject_text must be a string or null'}
            )
            return
        if subject_kind == 'other' and subject_text is None:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {'error': "subject_kind 'other' requires subject_text describing who is on screen"},
            )
            return
        if subject_kind == 'me':
            subject_text = None  # the schema's pairing CHECK requires this
        context_notes = self._text_or_none(body.get('context_notes'))
        if context_notes is False:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {'error': 'context_notes must be a string or null'}
            )
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute('SELECT id FROM recordings WHERE id = %s', (recording_id,))
            if cur.fetchone() is None:
                self._send_json(
                    HTTPStatus.NOT_FOUND, {'error': f'recording not found: {recording_id}'}
                )
                return
            cur.execute(
                'SELECT COALESCE(MAX(version), 0) AS v FROM recording_context '
                'WHERE recording_id = %s',
                (recording_id,),
            )
            new_version = int(cur.fetchone()['v']) + 1
            try:
                cur.execute(
                    'UPDATE recording_context SET is_active = 0 '
                    'WHERE recording_id = %s AND is_active = 1',
                    (recording_id,),
                )
                cur.execute(
                    '''
                    INSERT INTO recording_context
                        (recording_id, version, subject_kind, subject_text, context_notes,
                         is_active, created_by, created_at)
                    VALUES (%s, %s, %s, %s, %s, 1, 'operator', %s)
                    ''',
                    (recording_id, new_version, subject_kind, subject_text,
                     context_notes, utc_now_iso()),
                )
                conn.commit()
            except psycopg2.IntegrityError as exc:
                conn.rollback()
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {'error': 'a newer context version landed while saving',
                     'detail': str(exc).strip()},
                )
                return
            except Exception:
                conn.rollback()
                raise
            cur.execute(
                'SELECT recording_id, version, subject_kind, subject_text, context_notes, '
                'created_at FROM recording_context WHERE recording_id = %s AND is_active = 1',
                (recording_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        self._send_json(HTTPStatus.OK, dict(row) if row else None)

    def do_HEAD(self) -> None:
        """HEAD support, scoped to /clipmedia/<id> only (the probe that found
        the pre-approval-preview defect also found HEAD 501ing there; iOS
        sometimes preflights a video URL with HEAD before requesting bytes).
        BaseHTTPRequestHandler has no do_HEAD by default, and every route
        besides this one 501s exactly as it did before this fix -- giving
        every route HEAD parity means threading head_only through
        _send_json/_serve_ui/_serve_media and every other handler too, which
        is out of scope here. The message below matches BaseHTTPRequestHandler's
        own default 501 body verbatim, so the behaviour for any other path is
        byte-identical to before this method existed."""
        parsed = urlparse(self.path)
        m = GET_CLIP_MEDIA_RE.match(parsed.path)
        if m:
            self._serve_clip_media(m.group(1), head_only=True)
            return
        self.send_error(HTTPStatus.NOT_IMPLEMENTED, 'Unsupported method (%r)' % self.command)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/':
            self._serve_ui()
            return
        if path == '/api/candidates':
            self._serve_candidates('candidate')
            return
        if path == '/api/candidates/maybe':
            self._serve_candidates('maybe')
            return
        if path == '/api/candidates/rejected':
            self._serve_candidates('rejected')
            return
        # D-061 post kit routes. `delivered` is matched BEFORE the numeric
        # routes below, and it cannot collide with them: the regexes require
        # digits.
        if path == '/api/candidates/delivered':
            self._serve_delivered()
            return
        if path == '/api/run-progress':
            self._serve_run_progress()
            return
        if path == '/api/profile':
            self._serve_profile()
            return
        m = GET_KITS_RE.match(path)
        if m:
            self._serve_kits(int(m.group(1)))
            return
        m = GET_CAPTIONS_SRT_RE.match(path)
        if m:
            self._serve_captions(int(m.group(1)), as_file=True)
            return
        m = GET_CAPTIONS_RE.match(path)
        if m:
            self._serve_captions(int(m.group(1)), as_file=False)
            return
        m = GET_CUES_RE.match(path)
        if m:
            self._serve_cues(int(m.group(1)), parse_qs(parsed.query))
            return
        m = GET_CLIP_MEDIA_RE.match(path)
        if m:
            self._serve_clip_media(m.group(1))
            return
        if path.startswith('/media/'):
            self._serve_media(path.split('/media/', 1)[1])
            return

        self._send_text(HTTPStatus.NOT_FOUND, 'Not found')

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        wm = POST_WINDOW_RE.match(parsed.path)
        if wm:
            content_len = int(self.headers.get('Content-Length', '0') or '0')
            body_raw = self.rfile.read(content_len) if content_len > 0 else b''
            self._edit_window(int(wm.group(1)), body_raw)
            return

        # D-061 post kit mutations. Each reads its own body and returns; the
        # verdict route below is untouched. `/kit/regenerate` is matched BEFORE
        # `/kit` so the longer path can never be swallowed by the shorter one.
        for regex, handler in (
            (POST_KIT_REGEN_RE, self._request_regenerate),
            (POST_KIT_RE, self._save_kit),
            (POST_GENERATE_RE, self._set_generate),
            (POST_CAPTIONS_RE, self._set_captions),
            (POST_STYLE_RE, self._set_render_style),
            (POST_RERENDER_RE, self._rerender_clip),
            (POST_SUBJECT_RE, self._set_subject),
        ):
            km = regex.match(parsed.path)
            if km:
                content_len = int(self.headers.get('Content-Length', '0') or '0')
                body_raw = self.rfile.read(content_len) if content_len > 0 else b''
                handler(int(km.group(1)), body_raw)
                return

        if parsed.path == '/api/profile':
            content_len = int(self.headers.get('Content-Length', '0') or '0')
            body_raw = self.rfile.read(content_len) if content_len > 0 else b''
            self._save_profile(body_raw)
            return

        if parsed.path == '/api/find-clips':
            content_len = int(self.headers.get('Content-Length', '0') or '0')
            body_raw = self.rfile.read(content_len) if content_len > 0 else b''
            self._find_clips(body_raw)
            return

        m = POST_ACTION_RE.match(parsed.path)
        if not m:
            self._send_json(HTTPStatus.NOT_FOUND, {'error': 'Not found'})
            return

        candidate_id = int(m.group(1))
        action = m.group(2)
        if action == 'approve':
            target_state = 'approved'
        elif action == 'reject':
            target_state = 'rejected'
        else:
            target_state = 'maybe'

        content_len = int(self.headers.get('Content-Length', '0') or '0')
        if content_len > 0:
            _ = self.rfile.read(content_len)

        self._transition_candidate(candidate_id, target_state)

    def log_message(self, fmt: str, *args: object) -> None:
        # Keep logs concise but present for verification.
        print(f'{self.address_string()} - - [{self.log_date_time_string()}] {fmt % args}')


def main() -> int:
    # Validate the URL is set at startup (fail loudly now, not per-request);
    # never print the URL itself — it may carry credentials.
    db.get_db_url()
    print(f'Starting review server on http://{HOST}:{PORT} using CLPR_DB_URL from environment')
    # Preview proxies (2026-08-10, eager): "on server start" -- build a proxy
    # for every already-pending candidate before the operator opens the UI.
    # Backgrounded (build brief point 6): server startup must never block on
    # ffmpeg encodes.
    _spawn_eager_proxy_pass()
    with ThreadingHTTPServer((HOST, PORT), ReviewHandler) as httpd:
        httpd.serve_forever()


if __name__ == '__main__':
    raise SystemExit(main())
