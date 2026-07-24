#!/usr/bin/env python3
"""chat_capture: capture Twitch chat messages live into chat_raw.
Reads CLPR_DB_PATH; exits non-zero on unexpected startup failure.
Runs until killed; reconnects with backoff; commits each message row.
"""

from __future__ import annotations

import datetime as dt
import os
import random
import socket
import sqlite3
import ssl
import sys
import time
from pathlib import Path
from typing import Optional, BinaryIO, cast

try:
    from migrations import apply_migrations
except ModuleNotFoundError:
    from .migrations import apply_migrations

IRC_HOST = 'irc.chat.twitch.tv'
IRC_PORT = 6697
CHANNEL = '#fif4pres'
CAP_REQ = 'CAP REQ :twitch.tv/membership twitch.tv/tags twitch.tv/commands'


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def today_session_label() -> str:
    return dt.datetime.now().strftime('%Y-%m-%d')


def make_run_id() -> str:
    return f'chat_capture_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'


def parse_irc_line(line: str) -> tuple[dict[str, str], str, list[str], Optional[str]]:
    tags: dict[str, str] = {}
    rest = line

    if rest.startswith('@'):
        tag_part, rest = rest.split(' ', 1)
        for kv in tag_part[1:].split(';'):
            if '=' in kv:
                k, v = kv.split('=', 1)
                tags[k] = v
            else:
                tags[kv] = ''

    prefix = ''
    if rest.startswith(':'):
        prefix, rest = rest[1:].split(' ', 1)

    trailing = None
    if ' :' in rest:
        rest, trailing = rest.split(' :', 1)

    parts = rest.split()
    command = parts[0] if parts else ''
    params = parts[1:] if len(parts) > 1 else []

    if trailing is not None:
        params.append(trailing)

    return tags, prefix, [command] + params, trailing


def extract_author(tags: dict[str, str], prefix: str) -> str:
    display = (tags.get('display-name') or '').strip()
    if display:
        return display
    if '!' in prefix:
        return prefix.split('!', 1)[0]
    return prefix or 'unknown'


def extract_privmsg_text(parts: list[str]) -> str:
    if len(parts) < 3:
        return ''
    return parts[-1]


def ensure_schema(conn: sqlite3.Connection) -> None:
    migrations_dir = Path(__file__).resolve().parent.parent / 'migrations'
    apply_migrations(conn, migrations_dir)


def open_irc_socket() -> tuple[socket.socket, BinaryIO]:
    raw = socket.create_connection((IRC_HOST, IRC_PORT), timeout=30)
    ctx = ssl.create_default_context()
    sock = ctx.wrap_socket(raw, server_hostname=IRC_HOST)
    sock.settimeout(None)
    stream = cast(BinaryIO, sock.makefile('rwb', buffering=0))
    return sock, stream


def send_irc(stream: BinaryIO, line: str) -> None:
    data = (line + '\r\n').encode('utf-8')
    stream.write(data)
    print(f'IRC>> {line}', flush=True)


def recv_line(stream: BinaryIO) -> str:
    raw = stream.readline()
    if raw == b'':
        raise ConnectionError('socket closed by remote')
    return raw.decode('utf-8', errors='replace').rstrip('\r\n')


def insert_message(conn: sqlite3.Connection, run_id: str, author: str, text: str) -> None:
    conn.execute(
        '''
        INSERT INTO chat_raw(session_label, ts_utc, author, text, captured_by_run)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (today_session_label(), utc_now_iso(), author, text, run_id),
    )
    conn.commit()


def run_capture_loop() -> int:
    db_path = get_db_path()
    run_id = make_run_id()
    print(f'CHAT_CAPTURE_START db_path="{db_path}" run_id="{run_id}" channel="{CHANNEL}"', flush=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        ensure_schema(conn)

        backoff_s = 1.0
        while True:
            sock: Optional[socket.socket] = None
            stream: Optional[BinaryIO] = None
            try:
                print(f'IRC_CONNECT host="{IRC_HOST}" port={IRC_PORT}', flush=True)
                sock, stream = open_irc_socket()
                nick = f'justinfan{random.randint(10000, 99999)}'
                send_irc(stream, CAP_REQ)
                send_irc(stream, f'NICK {nick}')
                send_irc(stream, f'JOIN {CHANNEL}')
                print(f'IRC_CONNECTED nick="{nick}" channel="{CHANNEL}"', flush=True)
                backoff_s = 1.0

                while True:
                    line = recv_line(stream)
                    print(f'IRC<< {line}', flush=True)

                    if line.startswith('PING '):
                        payload = line.split(' ', 1)[1]
                        send_irc(stream, f'PONG {payload}')
                        continue

                    tags, prefix, parts, _ = parse_irc_line(line)
                    if not parts:
                        continue
                    command = parts[0]

                    if command == 'PRIVMSG':
                        author = extract_author(tags, prefix)
                        text = extract_privmsg_text(parts)
                        if text.strip() == '':
                            continue
                        insert_message(conn, run_id, author, text)
                        print(f'CHAT_RAW_INSERTED author="{author}" text="{text}"', flush=True)
            except KeyboardInterrupt:
                print('CHAT_CAPTURE_STOP reason="keyboard interrupt"', flush=True)
                return 0
            except Exception as exc:
                print(f'IRC_DISCONNECT reason="{exc}" backoff_s={backoff_s:.1f}', flush=True)
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 2.0, 30.0)
            finally:
                try:
                    if stream is not None:
                        stream.close()
                except Exception:
                    pass
                try:
                    if sock is not None:
                        sock.close()
                except Exception:
                    pass


def main() -> int:
    return run_capture_loop()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
