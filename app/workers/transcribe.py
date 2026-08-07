#!/usr/bin/env python3
"""transcribe: run whisper.cpp for one VOD and persist transcript segments.
Reads CLPR_DB_URL, WHISPER_CLI_PATH, WHISPER_MODEL_PATH by env var name.
Exits non-zero on any failure and prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods->recordings,
vod_id->recording_id). The --vod-id CLI flag is an external contract and
stays; it binds to recording_id internally.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from obs_guard import require_obs_idle_or_raise
except ModuleNotFoundError:
    from .obs_guard import require_obs_idle_or_raise

import db

TS_RE = re.compile(r'^(\d{2}):(\d{2}):(\d{2}),(\d{3})$')


def require_env(name: str) -> str:
    v = os.environ.get(name, '').strip()
    if not v:
        raise RuntimeError(f'Required env var missing: {name}')
    return v


def ts_to_seconds(ts: str) -> float:
    m = TS_RE.match(ts)
    if not m:
        raise ValueError(f'Invalid timestamp format: {ts}')
    hh, mm, ss, ms = map(int, m.groups())
    return hh * 3600 + mm * 60 + ss + ms / 1000.0


def fetch_recording_path(cur, recording_id: int) -> str:
    cur.execute('SELECT path FROM recordings WHERE id = %s', (recording_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'recording_id not found: {recording_id}')
    return row[0]


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


def persist_segments(cur, recording_id: int, segments: list[tuple[float, float, str]]) -> None:
    """Replace this recording's transcript segments and mark it transcribed.

    Extracted so the DB-write conversion is testable without running whisper
    (delete-then-insert is idempotent per recording; caller owns commit).
    """
    cur.execute('DELETE FROM transcript_segments WHERE recording_id = %s', (recording_id,))
    cur.executemany(
        '''
        INSERT INTO transcript_segments(recording_id, start_s, end_s, text)
        VALUES (%s, %s, %s, %s)
        ''',
        [(recording_id, s, e, t) for s, e, t in segments],
    )
    cur.execute("UPDATE recordings SET state = 'transcribed' WHERE id = %s", (recording_id,))


def transcribe(recording_id: int) -> int:
    require_obs_idle_or_raise('transcribe')

    whisper_cli = require_env('WHISPER_CLI_PATH')
    whisper_model = require_env('WHISPER_MODEL_PATH')

    conn = db.connect()
    try:
        cur = conn.cursor()
        recording_path = fetch_recording_path(cur, recording_id)

        with tempfile.TemporaryDirectory(prefix='clpr_transcribe_') as tmpdir:
            tmp_base = Path(tmpdir) / f'vod_{recording_id}'
            wav_path = tmp_base.with_suffix('.wav')
            json_path = Path(str(tmp_base) + '.json')

            ffmpeg_cmd = [
                'ffmpeg', '-y', '-i', recording_path,
                '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le',
                str(wav_path),
            ]
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            whisper_cmd = [
                whisper_cli,
                '-m', whisper_model,
                '-f', str(wav_path),
                '-oj',
                '-of', str(tmp_base),
                '-ng',
            ]
            started = time.monotonic()
            whisper_proc = run_capture(whisper_cmd)
            elapsed_s = time.monotonic() - started
            print(f'WHISPER_EXIT_CODE {whisper_proc.returncode}')

            if not json_path.exists():
                raise RuntimeError(f'Expected whisper output json not found: {json_path}')

            payload = json.loads(json_path.read_text(encoding='utf-8'))
            transcription = payload.get('transcription')
            if not isinstance(transcription, list):
                raise RuntimeError('whisper json missing transcription array')

            segments = []
            for entry in transcription:
                ts = entry.get('timestamps') or {}
                from_ts = ts.get('from')
                to_ts = ts.get('to')
                text = (entry.get('text') or '').strip()
                if from_ts is None or to_ts is None:
                    raise RuntimeError(f'missing timestamps in entry: {entry}')
                start_s = ts_to_seconds(from_ts)
                end_s = ts_to_seconds(to_ts)
                segments.append((start_s, end_s, text))

            # autocommit is OFF: the reads above already opened the transaction
            # implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
            persist_segments(cur, recording_id, segments)
            conn.commit()

            print(
                f'RESULT transcribe recording={recording_id} ok=1 segments={len(segments)} '
                f'elapsed_s={elapsed_s:.3f} vod_path="{recording_path}"'
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Transcribe one VOD with whisper.cpp and persist segments')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return transcribe(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
