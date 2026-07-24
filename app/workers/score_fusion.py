#!/usr/bin/env python3
"""score_fusion: merge transcript/audio/chat signals into final candidates for one VOD.
Poison gate enforced first. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional

try:
    from poison_gate import require_poison_reviewed_or_raise, is_poisoned
except ModuleNotFoundError:
    from .poison_gate import require_poison_reviewed_or_raise, is_poisoned


@dataclass
class CandidateInput:
    start_s: float
    end_s: float
    signal_transcript: float
    category: str
    reason: str
    source: str
    trigger_offset_s: Optional[float]


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def fetch_vod(conn: sqlite3.Connection, vod_id: int) -> tuple[float, str]:
    row = conn.execute('SELECT duration_s, state FROM vods WHERE id = ?', (vod_id,)).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    duration_s, state = row
    if duration_s is None:
        raise RuntimeError(f'vod_id has null duration_s: {vod_id}')
    return float(duration_s), str(state)


def fetch_intermediate_candidates(conn: sqlite3.Connection, vod_id: int) -> list[CandidateInput]:
    rows = conn.execute(
        '''
        SELECT start_s, end_s, confidence, category, reason, source, trigger_offset_s
        FROM transcript_signal_candidates
        WHERE vod_id = ?
        ORDER BY start_s, end_s, id
        ''',
        (vod_id,),
    ).fetchall()
    out: list[CandidateInput] = []
    for start_s, end_s, conf, category, reason, source, trigger in rows:
        out.append(
            CandidateInput(
                start_s=float(start_s),
                end_s=float(end_s),
                signal_transcript=clamp(float(conf), 0.0, 1.0),
                category=str(category),
                reason=str(reason),
                source=str(source),
                trigger_offset_s=(None if trigger is None else float(trigger)),
            )
        )
    return out


def list_audio_energy(conn: sqlite3.Connection, vod_id: int) -> list[tuple[float, float, float]]:
    return [
        (float(s), float(e), float(l))
        for s, e, l in conn.execute(
            'SELECT start_s, end_s, loudness_lufs FROM audio_energy WHERE vod_id = ? ORDER BY start_s',
            (vod_id,),
        ).fetchall()
    ]


def audio_signal_for_window(audio_rows: list[tuple[float, float, float]], start_s: float, end_s: float) -> float:
    vals: list[float] = []
    for a_s, a_e, lufs in audio_rows:
        if a_e <= start_s or a_s >= end_s:
            continue
        vals.append(lufs)
    if not vals:
        return 0.0

    # Normalize LUFS range [-60, -10] to [0,1] and average.
    norms = [clamp((v + 60.0) / 50.0, 0.0, 1.0) for v in vals]
    return sum(norms) / len(norms)


def fetch_chat_offsets(conn: sqlite3.Connection, vod_id: int) -> list[float]:
    return [
        float(r[0])
        for r in conn.execute('SELECT offset_s FROM chat_messages WHERE vod_id = ? ORDER BY offset_s', (vod_id,)).fetchall()
    ]


def count_in_window(offsets: list[float], start_s: float, end_s: float) -> int:
    return sum(1 for o in offsets if start_s <= o <= end_s)


def chat_signal_for_window(offsets: list[float], start_s: float, end_s: float) -> float:
    if not offsets:
        return 0.0

    window_count = count_in_window(offsets, start_s, end_s)
    width = max(end_s - start_s, 1.0)

    baseline_start = max(0.0, start_s - 300.0)
    baseline_end = start_s
    baseline_width = max(baseline_end - baseline_start, 1.0)
    baseline_count = count_in_window(offsets, baseline_start, baseline_end)

    rate_window = window_count / width
    rate_baseline = baseline_count / baseline_width
    burst_ratio = 0.0 if rate_baseline <= 0 else (rate_window / rate_baseline)

    if rate_baseline <= 0:
        # If no baseline activity, small boost for any in-window chat.
        return 1.0 if window_count > 0 else 0.0

    # ratio 1.0 -> 0.0, ratio >=3.0 -> 1.0 linearly
    return clamp((burst_ratio - 1.0) / 2.0, 0.0, 1.0)


def is_beat_sourced(c: CandidateInput, conn: sqlite3.Connection, vod_id: int) -> bool:
    if c.source == 'zebra_boundary':
        return True

    row = conn.execute(
        '''
        SELECT 1
        FROM beats
        WHERE vod_id = ?
          AND offset_s IS NOT NULL
          AND offset_s >= ?
          AND offset_s <= ?
        LIMIT 1
        ''',
        (vod_id, c.start_s, c.end_s),
    ).fetchone()
    return row is not None


def run(vod_id: int) -> int:
    db_path = get_db_path()
    run_id = f'score_fusion_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        require_poison_reviewed_or_raise(conn, vod_id)
        duration_s, _ = fetch_vod(conn, vod_id)

        raw_candidates = fetch_intermediate_candidates(conn, vod_id)
        audio_rows = list_audio_energy(conn, vod_id)
        chat_offsets = fetch_chat_offsets(conn, vod_id)

        cap_count = round(15.0 * duration_s / 3600.0)
        if cap_count < 0:
            cap_count = 0

        beat_rows: list[tuple[CandidateInput, float, float, float, float]] = []
        signal_rows: list[tuple[CandidateInput, float, float, float, float]] = []

        for c in raw_candidates:
            sig_audio = audio_signal_for_window(audio_rows, c.start_s, c.end_s)
            sig_chat = chat_signal_for_window(chat_offsets, c.start_s, c.end_s)
            avg = (sig_audio + sig_chat + c.signal_transcript) / 3.0
            beat_src = is_beat_sourced(c, conn, vod_id)
            score = max(0.9, avg) if beat_src else avg
            row = (c, sig_audio, sig_chat, c.signal_transcript, score)
            if beat_src:
                beat_rows.append(row)
            else:
                signal_rows.append(row)

        signal_rows_sorted = sorted(signal_rows, key=lambda x: x[4], reverse=True)
        kept_signal = signal_rows_sorted[:cap_count]

        candidates_written = 0
        beat_written = 0
        signal_written = 0
        poisoned_excluded = 0

        conn.execute('BEGIN;')
        try:
            for row_group, is_beat in ((beat_rows, True), (kept_signal, False)):
                for c, sig_audio, sig_chat, sig_transcript, score in row_group:
                    poisoned = is_poisoned(conn, vod_id, c.start_s, c.end_s)
                    if poisoned:
                        poisoned_excluded += 1
                        continue

                    cur = conn.execute(
                        '''
                        INSERT OR IGNORE INTO candidates(
                            vod_id, start_s, end_s, score,
                            signal_audio, signal_transcript, signal_chat, signal_beat_boost,
                            state, created_by_run, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?)
                        ''',
                        (
                            vod_id,
                            c.start_s,
                            c.end_s,
                            score,
                            sig_audio,
                            sig_transcript,
                            sig_chat,
                            1.0 if is_beat else 0.0,
                            run_id,
                            utc_now_iso(),
                        ),
                    )
                    if cur.rowcount == 1:
                        candidates_written += 1
                        if is_beat:
                            beat_written += 1
                        else:
                            signal_written += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(
        f'RESULT score_fusion vod={vod_id} candidates_written={candidates_written} '
        f'beat_sourced={beat_written} signal_only={signal_written} poisoned_excluded={poisoned_excluded}'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fuse transcript/audio/chat into final candidates for one VOD')
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
