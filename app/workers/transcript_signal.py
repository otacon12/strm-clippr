#!/usr/bin/env python3
"""transcript_signal: produce transcript + zebra boundary signal candidates for one VOD.
Poison gate is enforced first. Exits non-zero on any failure.
Writes intermediate rows to transcript_signal_candidates.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional

CATEGORIES = ['funny', 'inspirational', 'educational', 'showing-AI-off', 'context']
CHUNK_CHAR_LIMIT = 12000
CHUNK_OVERLAP_CHARS = 1500


@dataclass
class Segment:
    start_s: float
    end_s: float
    text: str


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def require_env(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Required env var missing: {name}')
    return value


def ensure_intermediate_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS transcript_signal_candidates (
            id INTEGER PRIMARY KEY,
            vod_id INTEGER NOT NULL REFERENCES vods(id),
            start_s REAL NOT NULL,
            end_s REAL NOT NULL,
            category TEXT NOT NULL,
            reason TEXT NOT NULL,
            confidence REAL NOT NULL,
            source TEXT NOT NULL,
            trigger_offset_s REAL,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    conn.execute(
        '''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tsc_unique
        ON transcript_signal_candidates(vod_id, start_s, end_s, category, source, IFNULL(trigger_offset_s, -1))
        '''
    )


def fetch_vod(conn: sqlite3.Connection, vod_id: int) -> tuple[float, str]:
    row = conn.execute('SELECT duration_s, state FROM vods WHERE id = ?', (vod_id,)).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    duration_s, state = row
    if duration_s is None:
        raise RuntimeError(f'vod_id has null duration_s: {vod_id}')
    if str(state) not in {'transcribed', 'detected', 'done'}:
        raise RuntimeError(f'vod_id={vod_id} must be transcribed or later; got state={state}')
    return float(duration_s), str(state)


def fetch_segments(conn: sqlite3.Connection, vod_id: int) -> list[Segment]:
    rows = conn.execute(
        'SELECT start_s, end_s, text FROM transcript_segments WHERE vod_id = ? ORDER BY start_s, id',
        (vod_id,),
    ).fetchall()
    return [Segment(float(s), float(e), str(t or '')) for s, e, t in rows]


def chunk_segments(segments: list[Segment]) -> list[list[Segment]]:
    chunks: list[list[Segment]] = []
    current: list[Segment] = []
    current_chars = 0

    for seg in segments:
        seg_line = f'[{seg.start_s:.3f},{seg.end_s:.3f}] {seg.text}\n'
        seg_chars = len(seg_line)
        if current and current_chars + seg_chars > CHUNK_CHAR_LIMIT:
            chunks.append(current)
            overlap: list[Segment] = []
            overlap_chars = 0
            for prev in reversed(current):
                prev_line = f'[{prev.start_s:.3f},{prev.end_s:.3f}] {prev.text}\n'
                prev_chars = len(prev_line)
                if overlap_chars + prev_chars > CHUNK_OVERLAP_CHARS and overlap:
                    break
                overlap.insert(0, prev)
                overlap_chars += prev_chars
            current = overlap[:]
            current_chars = sum(len(f'[{x.start_s:.3f},{x.end_s:.3f}] {x.text}\n') for x in current)

        current.append(seg)
        current_chars += seg_chars

    if current:
        chunks.append(current)
    return chunks


def build_transcript_payload(segments: list[Segment]) -> str:
    return ''.join(f'[{s.start_s:.3f},{s.end_s:.3f}] {s.text}\n' for s in segments)


def extract_json_payload(raw: str) -> str:
    s = raw.strip()
    if '```' in s:
        parts = s.split('```')
        for part in parts:
            candidate = part.strip()
            if candidate.startswith('json'):
                candidate = candidate[4:].strip()
            if candidate.startswith('{') or candidate.startswith('['):
                return candidate
    return s


def call_claude_structured(prompt: str) -> dict:
    api_key = require_env('OPENROUTER_API_KEY')
    model = os.environ.get('OPENROUTER_MODEL', 'anthropic/claude-haiku-4.5').strip() or 'anthropic/claude-haiku-4.5'

    body = {
        'model': model,
        'max_tokens': 2200,
        'temperature': 0,
        'messages': [
            {
                'role': 'user',
                'content': prompt,
            }
        ],
    }

    cmd = [
        'curl', '-sS', 'https://openrouter.ai/api/v1/chat/completions',
        '-H', f'Authorization: Bearer {api_key}',
        '-H', 'content-type: application/json',
        '-d', json.dumps(body),
    ]

    debug_cmd = [seg if str(seg).startswith('Authorization: Bearer') is False else 'Authorization: Bearer ***REDACTED***' for seg in cmd[:6]]
    print(f'LLM_CMD {debug_cmd} ...')
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f'LLM call failed exit={proc.returncode} stderr="{(proc.stderr or "").strip()}"')

    resp = json.loads(proc.stdout)
    if 'error' in resp:
        raise RuntimeError(f'LLM API error: {resp["error"]}')

    choices = resp.get('choices') or []
    if not choices:
        raise RuntimeError('LLM response missing choices')
    message = (choices[0] or {}).get('message') or {}
    raw_text = str(message.get('content') or '').strip()
    if not raw_text:
        raise RuntimeError('LLM returned no text content')

    payload = extract_json_payload(raw_text)
    return json.loads(payload)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def normalize_window(start_s: float, end_s: float, duration_s: float) -> tuple[float, float]:
    s = clamp(float(start_s), 0.0, duration_s)
    e = clamp(float(end_s), 0.0, duration_s)
    if e <= s:
        e = clamp(s + 1.0, 0.0, duration_s)
    if e <= s:
        raise RuntimeError(f'invalid normalized window start={s} end={e} duration={duration_s}')
    return s, e


def insert_signal_rows(conn: sqlite3.Connection, vod_id: int, rows: list[tuple[float, float, str, str, float, str, Optional[float]]], run_id: str) -> int:
    inserted = 0
    now_iso = utc_now_iso()
    for start_s, end_s, category, reason, confidence, source, trigger_offset_s in rows:
        cur = conn.execute(
            '''
            INSERT OR IGNORE INTO transcript_signal_candidates(
                vod_id, start_s, end_s, category, reason, confidence, source, trigger_offset_s, run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (vod_id, start_s, end_s, category, reason, confidence, source, trigger_offset_s, run_id, now_iso),
        )
        if cur.rowcount == 1:
            inserted += 1
    return inserted


def build_scan_prompt(chunk_index: int, chunk_total: int, transcript_text: str) -> str:
    return (
        'You are labeling transcript moments for clip candidacy.\n'
        'Allowed categories only: funny, inspirational, educational, showing-AI-off, context.\n'
        'Return JSON only with shape: {"candidates":[{"start_s":number,"end_s":number,"category":string,"reason":string,"confidence":number}]}.\n'
        'Rules: use only evidence present in transcript lines, do not invent quotes or timestamps, confidence 0.0-1.0.\n'
        f'Chunk index: {chunk_index} of {chunk_total}.\n'
        f'Transcript chunk:\n{transcript_text}'
    )


def build_zebra_prompt(outer_start: float, outer_end: float, trigger_offset: float, transcript_text: str) -> str:
    return (
        'Operator flagged a clip-worthy moment via trigger word at END of this window.\n'
        'Find where this specific story/topic/bit actually began.\n'
        'Return JSON only: {"start_s":number,"confidence":number,"reason":string}.\n'
        'Rules: choose a start inside the provided window; do not invent text or timestamps.\n'
        f'Window start={outer_start:.3f} end={outer_end:.3f} trigger_offset={trigger_offset:.3f}.\n'
        f'Transcript slice:\n{transcript_text}'
    )


def iter_scan_items(segments: list[Segment]):
    """Yield (meta, prompt) for every chunk-scan LLM call this VOD requires.

    Single source of truth for scan prompt construction: the LOCAL lane
    (detect_transcript_categories) and the split lane (transcript_signal_prepare)
    both consume this iterator.
    """
    chunks = chunk_segments(segments)
    for idx, chunk in enumerate(chunks, start=1):
        transcript_text = build_transcript_payload(chunk)
        meta = {
            'chunk_index': idx,
            'chunk_total': len(chunks),
            'window_start_s': chunk[0].start_s,
            'window_end_s': chunk[-1].end_s,
        }
        yield meta, build_scan_prompt(idx, len(chunks), transcript_text)


def fetch_zebra_triggers(conn: sqlite3.Connection, vod_id: int) -> list[float]:
    rows = conn.execute(
        '''
        SELECT offset_s
        FROM beats
        WHERE vod_id = ?
          AND source = 'zebra_trigger'
          AND offset_s IS NOT NULL
        ORDER BY offset_s
        ''',
        (vod_id,),
    ).fetchall()
    return [float(offset_raw) for (offset_raw,) in rows]


def iter_zebra_items(triggers: list[float], duration_s: float, segments: list[Segment]):
    """Yield (meta, prompt) for EVERY zebra trigger, in offset order.

    prompt is None when the transcript slice is empty: no LLM call is made and
    the consumer must emit the fallback row from meta alone. Single source of
    truth for zebra prompt construction (LOCAL lane + split lane).
    """
    for trigger_offset in triggers:
        outer_start = clamp(trigger_offset - 300.0, 0.0, duration_s)
        outer_end = clamp(trigger_offset + 15.0, 0.0, duration_s)
        if outer_end <= outer_start:
            outer_end = clamp(outer_start + 1.0, 0.0, duration_s)

        slice_segments = transcript_slice_for_window(segments, outer_start, outer_end)
        transcript_text = build_transcript_payload(slice_segments)

        meta = {
            'trigger_offset_s': trigger_offset,
            'outer_start_s': outer_start,
            'outer_end_s': outer_end,
            'fallback_start_s': clamp(trigger_offset - 60.0, 0.0, duration_s),
        }
        prompt = None
        if transcript_text.strip() != '':
            prompt = build_zebra_prompt(outer_start, outer_end, trigger_offset, transcript_text)
        yield meta, prompt


def detect_transcript_categories(segments: list[Segment], duration_s: float) -> list[tuple[float, float, str, str, float, str, Optional[float]]]:
    out: list[tuple[float, float, str, str, float, str, Optional[float]]] = []
    seen: set[tuple[int, int, str]] = set()

    for _meta, prompt in iter_scan_items(segments):
        data = call_claude_structured(prompt)
        candidates = data.get('candidates')
        if not isinstance(candidates, list):
            raise RuntimeError('LLM response missing candidates list')

        for item in candidates:
            if not isinstance(item, dict):
                continue
            category = str(item.get('category', '')).strip()
            if category not in CATEGORIES:
                continue
            start_s, end_s = normalize_window(float(item['start_s']), float(item['end_s']), duration_s)
            reason = str(item.get('reason', '')).strip() or 'model-selected transcript moment'
            confidence = clamp(float(item.get('confidence', 0.0)), 0.0, 1.0)

            key = (int(round(start_s * 1000)), int(round(end_s * 1000)), category)
            if key in seen:
                continue
            seen.add(key)
            out.append((start_s, end_s, category, reason, confidence, 'transcript_scan', None))

    return out


def transcript_slice_for_window(segments: list[Segment], w_start: float, w_end: float) -> list[Segment]:
    return [s for s in segments if not (s.end_s < w_start or s.start_s > w_end)]


def detect_zebra_boundaries(
    conn: sqlite3.Connection,
    vod_id: int,
    duration_s: float,
    segments: list[Segment],
) -> list[tuple[float, float, str, str, float, str, Optional[float]]]:
    out: list[tuple[float, float, str, str, float, str, Optional[float]]] = []

    triggers = fetch_zebra_triggers(conn, vod_id)

    for meta, prompt in iter_zebra_items(triggers, duration_s, segments):
        trigger_offset = meta['trigger_offset_s']
        outer_start = meta['outer_start_s']
        fallback_start = meta['fallback_start_s']
        chosen_start = fallback_start
        chosen_conf = 0.0
        reason = 'fallback 60s lookback from zebra trigger'

        if prompt is not None:
            try:
                data = call_claude_structured(prompt)
                conf = clamp(float(data.get('confidence', 0.0)), 0.0, 1.0)
                model_start = clamp(float(data.get('start_s', fallback_start)), outer_start, trigger_offset)
                model_reason = str(data.get('reason', '')).strip() or 'model-selected zebra boundary'
                if conf >= 0.5:
                    chosen_start = model_start
                    chosen_conf = conf
                    reason = model_reason
            except Exception as exc:
                reason = f'fallback 60s lookback from zebra trigger (model error: {exc})'

        end_s = clamp(trigger_offset + 15.0, 0.0, duration_s)
        if end_s <= chosen_start:
            end_s = clamp(chosen_start + 1.0, 0.0, duration_s)

        out.append((chosen_start, end_s, 'context', reason, chosen_conf, 'zebra_boundary', trigger_offset))

    return out


def run(vod_id: int) -> int:
    db_path = get_db_path()
    run_id = f'transcript_signal_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        # D-050: pre-detection poison gate removed — the operator's clip review (D-002) is the poison gate; M5 auto-publish must reinstate a mandatory mechanism.
        duration_s, _ = fetch_vod(conn, vod_id)
        segments = fetch_segments(conn, vod_id)
        ensure_intermediate_table(conn)

        transcript_rows = detect_transcript_categories(segments, duration_s)
        zebra_rows = detect_zebra_boundaries(conn, vod_id, duration_s, segments)

        conn.execute('BEGIN;')
        try:
            inserted_transcript = insert_signal_rows(conn, vod_id, transcript_rows, run_id)
            inserted_zebra = insert_signal_rows(conn, vod_id, zebra_rows, run_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    total_inserted = inserted_transcript + inserted_zebra
    print(
        f'RESULT transcript_signal vod={vod_id} '
        f'transcript_candidates={inserted_transcript} zebra_candidates={inserted_zebra} total_inserted={total_inserted}'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build transcript/zebra signal candidates for one VOD')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
