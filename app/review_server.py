#!/usr/bin/env python3
"""review_server: local operator-only review surface for clip candidates.
Binds to loopback by default; CLPR_REVIEW_HOST / CLPR_REVIEW_PORT can override.
Reads CLPR_DB_PATH. No external dependencies.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import socket
import sqlite3
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8737
LOOPBACK_HOSTS = frozenset({'127.0.0.1', 'localhost'})

HOST = os.environ.get('CLPR_REVIEW_HOST', '').strip() or DEFAULT_HOST
PORT = int(os.environ.get('CLPR_REVIEW_PORT', '').strip() or DEFAULT_PORT)
UI_PATH = Path(__file__).resolve().parent / 'review_ui.html'
POST_ACTION_RE = re.compile(r'^/api/candidates/(\d+)/(approve|reject|maybe)$')


def is_loopback_bind(host: str) -> bool:
    return host.strip().lower() in LOOPBACK_HOSTS


def detect_lan_ip() -> Optional[str]:
    """Best-effort LAN IPv4 for this machine, derived at runtime.

    Returns None when no usable address can be determined; the caller must say
    so plainly rather than print a placeholder.
    """
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
    except OSError:
        return None

    for info in infos:
        addr = info[4][0]
        try:
            parsed = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if parsed.is_loopback or parsed.is_link_local or parsed.is_unspecified or parsed.is_multicast:
            continue
        return str(parsed)
    return None


def print_open_surface_warning(port: int) -> None:
    banner = '*' * 72
    for line in (
        '',
        banner,
        'WARNING: this review surface is NOT bound to loopback.',
        '',
        '  It is now reachable by OTHER DEVICES on this network.',
        '  It has NO AUTHENTICATION WHATSOEVER.',
        '  Anyone who can reach this address can approve or reject clip',
        '  candidates and can stream the source VODs in full.',
        '',
        '  Return it to loopback (unset CLPR_REVIEW_HOST) when not in use.',
        banner,
        '',
    ):
        print(line, flush=True)


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn


def json_bytes(obj: object) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode('utf-8')


def fetch_candidate_row(conn: sqlite3.Connection, candidate_id: int) -> Optional[dict]:
    row = conn.execute(
        '''
        SELECT
          c.id,
          c.vod_id,
          v.path AS vod_path,
          v.session_label,
          c.start_s,
          c.end_s,
          c.score,
          c.signal_audio,
          c.signal_transcript,
          c.signal_chat,
          c.signal_beat_boost,
          c.state,
          c.created_at
        FROM candidates c
        JOIN vods v ON v.id = c.vod_id
        WHERE c.id = ?
        ''',
        (candidate_id,),
    ).fetchone()
    return dict(row) if row else None


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
        with db_connect() as conn:
            rows = conn.execute(
                '''
                SELECT
                  c.id,
                  c.vod_id,
                  v.path AS vod_path,
                  v.session_label,
                  c.start_s,
                  c.end_s,
                  c.score,
                  c.signal_audio,
                  c.signal_transcript,
                  c.signal_chat,
                  c.signal_beat_boost,
                  c.created_at
                FROM candidates c
                JOIN vods v ON v.id = c.vod_id
                WHERE c.state = ?
                ORDER BY c.score DESC, c.id ASC
                ''',
                (state,),
            ).fetchall()
        self._send_json(HTTPStatus.OK, [dict(r) for r in rows])

    def _serve_media(self, vod_id_text: str) -> None:
        try:
            vod_id = int(vod_id_text)
        except ValueError:
            self._send_text(HTTPStatus.BAD_REQUEST, 'Invalid vod_id')
            return

        with db_connect() as conn:
            row = conn.execute('SELECT path FROM vods WHERE id = ?', (vod_id,)).fetchone()

        if not row:
            self._send_text(HTTPStatus.NOT_FOUND, f'vod_id not found: {vod_id}')
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
        allowed = {
            'candidate': {'approved', 'rejected', 'maybe'},
            'maybe': {'approved', 'rejected'},
        }

        with db_connect() as conn:
            row = conn.execute('SELECT state FROM candidates WHERE id = ?', (candidate_id,)).fetchone()
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

            conn.execute('BEGIN;')
            try:
                conn.execute('UPDATE candidates SET state = ? WHERE id = ?', (target_state, candidate_id))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            updated = fetch_candidate_row(conn, candidate_id)

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
        if path.startswith('/media/'):
            self._serve_media(path.split('/media/', 1)[1])
            return

        self._send_text(HTTPStatus.NOT_FOUND, 'Not found')

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
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
    print(f'Starting review server on http://{HOST}:{PORT} using CLPR_DB_PATH={get_db_path()}', flush=True)
    if not is_loopback_bind(HOST):
        print_open_surface_warning(PORT)
        lan_ip = detect_lan_ip()
        if lan_ip:
            print(f'On another device on this network (e.g. your phone), open: http://{lan_ip}:{PORT}', flush=True)
        else:
            print(
                'Could not determine this machine\'s LAN IP address. Look it up manually '
                f'(System Settings > Network) and open http://<that-address>:{PORT}',
                flush=True,
            )
    with ThreadingHTTPServer((HOST, PORT), ReviewHandler) as httpd:
        httpd.serve_forever()


if __name__ == '__main__':
    raise SystemExit(main())
