#!/usr/bin/env python3
"""transcribe: run whisper.cpp for one VOD and persist transcript segments.
Reads CLPR_DB_URL, WHISPER_CLI_PATH, WHISPER_MODEL_PATH by env var name.
Exits non-zero on any failure and prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods->recordings,
vod_id->recording_id). The --vod-id CLI flag is an external contract and
stays; it binds to recording_id internally.

CHUNKED TRANSCRIPTION (D-028, 2026-08-04, made the default here): whisper.cpp
on this build degrades into repetition loops on long audio (measured: a
138.4-minute VOD produced 59.4% of its speech segments inside >=3-long
identical runs and was refused by the D-028 quality gate). D-028 proved a
15-minute slice of the exact same region returns clean output at 0% blank,
so chunking to bounded input lengths is the default path; the pre-D-028
single-shot behaviour is kept reachable via --whole-file for comparison.
Per-chunk whisper output is cached in a persistent work dir (see
CLPR_TRANSCRIBE_WORK_DIR below) so a crash mid-run costs only the chunk in
flight, not the whole VOD.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NamedTuple, Optional

try:
    from obs_guard import require_obs_idle_or_raise
except ModuleNotFoundError:
    from .obs_guard import require_obs_idle_or_raise

import db

TS_RE = re.compile(r'^(\d{2}):(\d{2}):(\d{2}),(\d{3})$')

# D-028 (2026-08-04) measured 15-minute (900s) chunks as the input length
# whisper.cpp handles cleanly on this build, and a 15s overlap as enough
# decode context that a sentence spanning a chunk boundary is not clipped.
# Both numbers came from that measurement; do not retune them without a new
# one.
CHUNK_LEN_S = 900.0
CHUNK_OVERLAP_S = 15.0

# Overridable persistent work dir for per-chunk whisper json (resumability).
# Default: ~/.clpr/transcribe_work/<recording_id>/
WORK_DIR_ENV = 'CLPR_TRANSCRIBE_WORK_DIR'


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


def parse_transcription_entries(transcription: list) -> list[tuple[float, float, str]]:
    """Convert one whisper json `transcription[]` array into (start_s, end_s, text)."""
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
    return segments


def run_whisper(whisper_cli: str, whisper_model: str, wav_path: Path, out_base: Path) -> tuple[dict, float]:
    """Run whisper-cli over wav_path, return (parsed json payload, elapsed_s)."""
    whisper_cmd = [
        whisper_cli,
        '-m', whisper_model,
        '-f', str(wav_path),
        '-oj',
        '-of', str(out_base),
        '-ng',
    ]
    started = time.monotonic()
    whisper_proc = run_capture(whisper_cmd)
    elapsed_s = time.monotonic() - started
    print(f'WHISPER_EXIT_CODE {whisper_proc.returncode}')

    json_path = Path(str(out_base) + '.json')
    if not json_path.exists():
        raise RuntimeError(f'Expected whisper output json not found: {json_path}')

    payload = json.loads(json_path.read_text(encoding='utf-8'))
    transcription = payload.get('transcription')
    if not isinstance(transcription, list):
        raise RuntimeError('whisper json missing transcription array')
    return payload, elapsed_s


def probe_duration_s(wav_path: Path) -> float:
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(wav_path),
    ]
    proc = run_capture(cmd)
    raw = proc.stdout.strip()
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f'could not parse ffprobe duration output: {raw!r}') from exc


class ChunkPlan(NamedTuple):
    index: int
    start_s: float          # decode window start (absolute, seconds)
    decode_len_s: float     # decode window length fed to ffmpeg -t
    accept_start_s: float   # segments accepted only if midpoint >= this
    accept_end_s: Optional[float]  # ... and midpoint < this; None on the
                                    # final chunk means "accept to EOF"


def compute_chunk_plan(duration_s: float) -> list[ChunkPlan]:
    """Plan non-overlapping ACCEPTANCE windows over overlapping DECODE windows.

    Chunk i decodes [i*step, i*step + CHUNK_LEN_S] (clipped to duration) but
    is only credited with the segments whose midpoint falls in
    [i*step, i*step + step) -- the final chunk is credited everything to the
    end of file. step = CHUNK_LEN_S - CHUNK_OVERLAP_S, so the overlap exists
    purely as decode context and is never double-counted in the output.
    """
    if duration_s <= 0:
        raise RuntimeError(f'invalid duration_s={duration_s} for chunk planning')

    step_s = CHUNK_LEN_S - CHUNK_OVERLAP_S
    plan: list[ChunkPlan] = []
    index = 0
    start_s = 0.0
    while start_s < duration_s:
        decode_len_s = min(CHUNK_LEN_S, duration_s - start_s)
        is_last = (start_s + CHUNK_LEN_S) >= duration_s
        accept_end_s = None if is_last else (start_s + step_s)
        plan.append(ChunkPlan(
            index=index,
            start_s=start_s,
            decode_len_s=decode_len_s,
            accept_start_s=start_s,
            accept_end_s=accept_end_s,
        ))
        if is_last:
            break
        start_s += step_s
        index += 1
    return plan


def get_work_dir(recording_id: int) -> Path:
    override = os.environ.get(WORK_DIR_ENV, '').strip()
    base = Path(override) if override else (Path.home() / '.clpr' / 'transcribe_work')
    return base / str(recording_id)


def process_chunk(
    chunk: ChunkPlan,
    full_wav_path: Path,
    whisper_cli: str,
    whisper_model: str,
    work_dir: Path,
) -> tuple[dict, float, bool]:
    """Return (whisper json payload, elapsed_s, reused).

    Reuses a persisted chunk json if present and it parses to a valid
    transcription array; otherwise extracts the chunk's audio, runs whisper,
    and persists the json (atomically) so a crash mid-run costs only this
    chunk on the next invocation.
    """
    json_path = work_dir / f'chunk_{chunk.index:03d}.json'
    if json_path.exists():
        try:
            payload = json.loads(json_path.read_text(encoding='utf-8'))
            if isinstance(payload.get('transcription'), list):
                return payload, 0.0, True
        except (json.JSONDecodeError, OSError):
            pass  # corrupt/partial from an earlier crash: fall through and recompute

    with tempfile.TemporaryDirectory(prefix=f'clpr_chunk_{chunk.index:03d}_') as tmpdir:
        chunk_wav = Path(tmpdir) / f'chunk_{chunk.index:03d}.wav'
        extract_cmd = [
            'ffmpeg', '-y',
            '-ss', f'{chunk.start_s:.3f}',
            '-t', f'{chunk.decode_len_s:.3f}',
            '-i', str(full_wav_path),
            '-c', 'copy',
            str(chunk_wav),
        ]
        run_capture(extract_cmd)

        chunk_base = Path(tmpdir) / f'chunk_{chunk.index:03d}'
        payload, elapsed_s = run_whisper(whisper_cli, whisper_model, chunk_wav, chunk_base)

        # Write-then-rename: a crash mid-write must never leave a json that
        # falsely "parses" as a complete chunk on the next run.
        work_dir.mkdir(parents=True, exist_ok=True)
        tmp_json_path = work_dir / f'chunk_{chunk.index:03d}.json.tmp'
        tmp_json_path.write_text(json.dumps(payload), encoding='utf-8')
        os.replace(tmp_json_path, json_path)

    return payload, elapsed_s, False


def transcribe_chunked(
    recording_id: int,
    wav_path: Path,
    whisper_cli: str,
    whisper_model: str,
) -> tuple[list[tuple[float, float, str]], float, str]:
    """Chunk the full-recording wav per D-028's proven parameters, transcribe
    each chunk (crash-resumable via a persistent per-recording work dir), and
    stitch the accepted segments back into absolute VOD time.

    Returns (stitched segments, total whisper elapsed_s, a summary line).
    """
    duration_s = probe_duration_s(wav_path)
    plan = compute_chunk_plan(duration_s)

    work_dir = get_work_dir(recording_id)
    work_dir.mkdir(parents=True, exist_ok=True)

    all_segments: list[tuple[float, float, str]] = []
    total_elapsed_s = 0.0
    reused_count = 0
    computed_count = 0
    per_chunk_counts: list[int] = []

    for chunk in plan:
        payload, elapsed_s, reused = process_chunk(
            chunk, wav_path, whisper_cli, whisper_model, work_dir,
        )
        total_elapsed_s += elapsed_s
        if reused:
            reused_count += 1
        else:
            computed_count += 1

        local_segments = parse_transcription_entries(payload['transcription'])
        accepted = 0
        for local_start, local_end, text in local_segments:
            abs_start = chunk.start_s + local_start
            abs_end = chunk.start_s + local_end
            midpoint = (abs_start + abs_end) / 2.0
            if midpoint < chunk.accept_start_s:
                continue
            if chunk.accept_end_s is not None and midpoint >= chunk.accept_end_s:
                continue
            all_segments.append((abs_start, abs_end, text))
            accepted += 1
        per_chunk_counts.append(accepted)
        print(
            f'CHUNK {chunk.index} {"REUSED" if reused else "COMPUTED"} '
            f'start_s={chunk.start_s:.1f} decode_len_s={chunk.decode_len_s:.1f} '
            f'raw_segments={len(local_segments)} accepted_segments={accepted}'
        )

    all_segments.sort(key=lambda s: (s[0], s[1]))

    chunk_summary = (
        f'RESULT transcribe_chunks recording={recording_id} chunk_count={len(plan)} '
        f'reused={reused_count} computed={computed_count} '
        f'per_chunk_segments="{",".join(str(c) for c in per_chunk_counts)}"'
    )
    return all_segments, total_elapsed_s, chunk_summary


def transcribe(recording_id: int, whole_file: bool = False) -> int:
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

            ffmpeg_cmd = [
                'ffmpeg', '-y', '-i', recording_path,
                '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le',
                str(wav_path),
            ]
            ffmpeg_proc = run_capture(ffmpeg_cmd)
            print(f'FFMPEG_EXIT_CODE {ffmpeg_proc.returncode}')

            chunk_summary = None
            if whole_file:
                payload, elapsed_s = run_whisper(whisper_cli, whisper_model, wav_path, tmp_base)
                segments = parse_transcription_entries(payload['transcription'])
            else:
                segments, elapsed_s, chunk_summary = transcribe_chunked(
                    recording_id, wav_path, whisper_cli, whisper_model,
                )

            if not segments:
                raise RuntimeError(
                    f'whisper produced 0 transcript segments for recording_id={recording_id} '
                    f'vod_path="{recording_path}"; refusing to write, existing transcript '
                    f'(if any) left untouched'
                )

            # autocommit is OFF: the reads above already opened the transaction
            # implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
            persist_segments(cur, recording_id, segments)
            conn.commit()

            print(
                f'RESULT transcribe recording={recording_id} ok=1 segments={len(segments)} '
                f'elapsed_s={elapsed_s:.3f} vod_path="{recording_path}"'
            )
            if chunk_summary is not None:
                print(chunk_summary)
                # Success only: a raised exception above skips this, so a
                # crashed run's cached chunk jsons survive for the next
                # invocation to reuse.
                shutil.rmtree(get_work_dir(recording_id), ignore_errors=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Transcribe one VOD with whisper.cpp and persist segments')
    parser.add_argument('--vod-id', type=int, required=True)
    parser.add_argument(
        '--whole-file', action='store_true',
        help='Transcribe the full recording in a single whisper.cpp call '
             '(pre-D-028 behaviour; degrades on long audio). Default is '
             'chunked transcription (D-028).',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return transcribe(args.vod_id, whole_file=args.whole_file)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
