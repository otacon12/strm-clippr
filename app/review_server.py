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
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent / 'workers'))
import db  # noqa: E402  (app/workers/db.py — the shared adapter)

HOST = '127.0.0.1'
PORT = int(os.environ.get('CLPR_REVIEW_PORT', '8737'))
UI_PATH = Path(__file__).resolve().parent / 'review_ui.html'
POST_ACTION_RE = re.compile(r'^/api/candidates/(\d+)/(approve|reject|maybe)$')
POST_WINDOW_RE = re.compile(r'^/api/candidates/(\d+)/window$')

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
          cl.drive_synced_at'''

# One truth for the payload FROM clause (every query that SELECTs
# CANDIDATE_PAYLOAD_COLUMNS uses exactly these joins).
CANDIDATE_PAYLOAD_FROM = '''
        FROM clip_candidates c
        JOIN recordings r ON r.id = c.recording_id
        LEFT JOIN clips cl ON cl.candidate_id = c.id'''


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

        file_path = Path(str(row['path']))
        if not file_path.exists() or not file_path.is_file():
            self._send_text(HTTPStatus.NOT_FOUND, f'VOD file not found: {file_path}')
            return

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
        self.send_header('Content-Type', 'video/mp4')
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
