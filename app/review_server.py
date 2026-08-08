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
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent / 'workers'))
import db  # noqa: E402  (app/workers/db.py — the shared adapter)

HOST = '127.0.0.1'
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


def fetch_clip_from_server(server_path: str | None, candidate_id: int) -> Path | None:
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
    if cached.is_file() and cached.stat().st_size > 0:
        return cached
    CLIP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = cached.with_suffix(cached.suffix + '.part')
    try:
        container = subprocess.run(
            ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', SSH_HOST,
             "docker ps --format '{{.Names}}' | grep '^n8n-'"],
            capture_output=True, text=True, timeout=30)
        names = [n for n in container.stdout.split() if n.startswith('n8n-')]
        if container.returncode != 0 or len(names) != 1:
            return None
        with open(tmp, 'wb') as fh:
            proc = subprocess.run(
                ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', SSH_HOST,
                 f'docker exec {names[0]} cat {shlex.quote(str(server_path))}'],
                stdout=fh, stderr=subprocess.DEVNULL, timeout=600)
        if proc.returncode != 0 or tmp.stat().st_size == 0:
            tmp.unlink(missing_ok=True)
            return None
        tmp.replace(cached)
        return cached
    except Exception:  # noqa: BLE001 - a side channel must never 500 the review UI
        tmp.unlink(missing_ok=True)
        return None


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
POST_KIT_RE = re.compile(r'^/api/candidates/(\d+)/kit$')
POST_KIT_REGEN_RE = re.compile(r'^/api/candidates/(\d+)/kit/regenerate$')
POST_SUBJECT_RE = re.compile(r'^/api/recordings/(\d+)/subject$')
GET_KITS_RE = re.compile(r'^/api/candidates/(\d+)/kits$')
GET_CAPTIONS_RE = re.compile(r'^/api/candidates/(\d+)/captions$')
GET_CAPTIONS_SRT_RE = re.compile(r'^/api/candidates/(\d+)/captions\.srt$')
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
          cl.captions_cue_count'''

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

    def _send_text(self, status: int, text: str, content_type: str = 'text/plain; charset=utf-8') -> None:
        body = text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
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

    def _serve_clip_media(self, candidate_id_text: str) -> None:
        """D-061: byte-range media for the RENDERED CLIP (not the source VOD),
        which is what the post-kit mockup plays.

        The clip is not always reachable from this machine, and that is a real
        state rather than an error to paper over: `clips.file_path` is written
        by whichever machine rendered, so an n8n-rendered clip's path points at
        the SERVER's filesystem; `drive_sync_path` carries two incompatible
        formats (a full local Drive-mount path from the Mac deliverer, a bare
        filename from the n8n lane) plus NULLs. So resolve in order, and 404
        with a distinct message when nothing resolves — the UI renders that as
        "clip file not reachable from this machine", never as a broken player."""
        try:
            candidate_id = int(candidate_id_text)
        except ValueError:
            self._send_text(HTTPStatus.BAD_REQUEST, 'Invalid candidate id')
            return

        conn = db.connect()
        try:
            cur = dict_cursor(conn)
            cur.execute(
                'SELECT file_path, drive_sync_path FROM clips WHERE candidate_id = %s',
                (candidate_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            self._send_text(HTTPStatus.NOT_FOUND, f'no clip row for candidate {candidate_id}')
            return

        resolved = resolve_local_clip(row['file_path'], row['drive_sync_path'])
        if resolved is None:
            # THE DURABLE FALLBACK: fetch it from the origin server.
            #
            # Everything above depends on a local copy existing, which makes it
            # machine-specific: it breaks on a laptop whose Drive mount differs,
            # and on a clip that is rendered but not yet delivered. Pulling from
            # the server removes both dependencies -- the clip is reachable
            # wherever ssh is, delivered or not.
            resolved = fetch_clip_from_server(row['file_path'], candidate_id)
        if resolved is not None:
            self._serve_file_range(resolved)
            return

        self._send_text(
            HTTPStatus.NOT_FOUND,
            f'clip file not reachable from this machine: candidate {candidate_id}',
        )

    def _serve_file_range(self, file_path: Path, content_type: str = 'video/mp4') -> None:
        """HTTP byte-range serving. Extracted VERBATIM from _serve_media so the
        VOD route and the clip route have ONE implementation (charter §1.5
        gate 1) — the behaviour below is unchanged from the shipped, live
        version and is exercised by both callers."""
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
            current_state = str(row['state'])
            allowed_targets = allowed.get(current_state, set())
            if target_state not in allowed_targets:
                self._send_json(
                    HTTPStatus.CONFLICT,
                    {
                        'error': 'candidate already decided',
                        'id': candidate_id,
                        'state': current_state,
                        'requested_state': target_state,
                    },
                )
                return

            try:
                cur.execute(
                    'UPDATE clip_candidates SET state = %s WHERE id = %s',
                    (target_state, candidate_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = fetch_candidate_payload(cur, candidate_id)
        finally:
            conn.close()

        # Webhook fires only AFTER the commit succeeded (the verdict is durable);
        # its outcome rides along in the response so the UI could surface it.
        webhook_status = fire_verdict_webhook(
            candidate_id,
            int(updated['vod_id']) if updated else -1,
            current_state,
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
        operator edits one copy of a kit while looking at another."""
        conn = db.connect()
        try:
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
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    'error': 'delivered query fanned out',
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
    with ThreadingHTTPServer((HOST, PORT), ReviewHandler) as httpd:
        httpd.serve_forever()


if __name__ == '__main__':
    raise SystemExit(main())
