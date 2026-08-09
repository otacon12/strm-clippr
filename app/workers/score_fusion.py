#!/usr/bin/env python3
"""score_fusion: merge transcript/audio/chat signals into final candidates for one VOD.
Poison gate enforced first. Exits non-zero on any failure.
Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods->recordings,
beats->trigger_beats, audio_energy->audio_energy_buckets,
transcript_signal_candidates->llm_signal_candidates, candidates->clip_candidates,
vod_id->recording_id). The --vod-id CLI flag is an external contract and stays;
it binds to recording_id internally.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from dataclasses import dataclass
from typing import Optional

import db

try:
    from poison_gate import is_poisoned
except ModuleNotFoundError:
    from .poison_gate import is_poisoned


@dataclass
class CandidateInput:
    start_s: float
    end_s: float
    signal_transcript: float
    category: str
    reason: str
    source: str
    trigger_offset_s: Optional[float]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def fetch_recording(cur, recording_id: int) -> tuple[float, str]:
    cur.execute('SELECT duration_s, state FROM recordings WHERE id = %s', (recording_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'recording_id not found: {recording_id}')
    duration_s, state = row
    if duration_s is None:
        raise RuntimeError(f'recording_id has null duration_s: {recording_id}')
    return float(duration_s), str(state)


def fetch_intermediate_candidates(cur, recording_id: int) -> list[CandidateInput]:
    cur.execute(
        '''
        SELECT start_s, end_s, confidence, category, reason, source, trigger_offset_s
        FROM llm_signal_candidates
        WHERE recording_id = %s
        ORDER BY start_s, end_s, id
        ''',
        (recording_id,),
    )
    rows = cur.fetchall()
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


def list_audio_energy(cur, recording_id: int) -> list[tuple[float, float, float]]:
    cur.execute(
        'SELECT start_s, end_s, loudness_lufs FROM audio_energy_buckets WHERE recording_id = %s ORDER BY start_s',
        (recording_id,),
    )
    return [(float(s), float(e), float(l)) for s, e, l in cur.fetchall()]


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


def fetch_chat_offsets(cur, recording_id: int) -> list[float]:
    cur.execute('SELECT offset_s FROM chat_messages WHERE recording_id = %s ORDER BY offset_s', (recording_id,))
    return [float(r[0]) for r in cur.fetchall()]


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


def is_beat_sourced(c: CandidateInput, cur, recording_id: int) -> bool:
    if c.source == 'zebra_boundary':
        return True

    cur.execute(
        '''
        SELECT 1
        FROM trigger_beats
        WHERE recording_id = %s
          AND offset_s IS NOT NULL
          AND offset_s >= %s
          AND offset_s <= %s
        LIMIT 1
        ''',
        (recording_id, c.start_s, c.end_s),
    )
    return cur.fetchone() is not None


def run(recording_id: int) -> int:
    run_id = f'score_fusion_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    conn = db.connect()
    try:
        cur = conn.cursor()
        # D-050: pre-detection poison gate removed — the operator's clip review (D-002) is the poison gate; M5 auto-publish must reinstate a mandatory mechanism.
        duration_s, _ = fetch_recording(cur, recording_id)

        raw_candidates = fetch_intermediate_candidates(cur, recording_id)
        audio_rows = list_audio_energy(cur, recording_id)
        chat_offsets = fetch_chat_offsets(cur, recording_id)

        cap_count = round(15.0 * duration_s / 3600.0)
        if cap_count < 0:
            cap_count = 0
        # ID-04 (golden-review F21): round(15*d/3600) rounds DOWN to 0 for any
        # recording <= 120s (banker's rounding on round()), so a short test VOD
        # silently discards every non-beat candidate via the cap with no signal
        # that the cap itself -- not a detection failure -- produced the empty
        # output.
        if cap_count == 0 and duration_s > 0:
            print(
                f'NOTE score_fusion recording={recording_id} cap=0 duration_s={duration_s:.1f} '
                'reason=short_recording_rounds_to_zero_cap (round(15*duration_s/3600) rounds '
                'down to 0 for any recording <= 120s; every non-beat candidate is discarded by '
                'the cap, not by detection)'
            )

        beat_rows: list[tuple[CandidateInput, float, float, float, float]] = []
        signal_rows: list[tuple[CandidateInput, float, float, float, float]] = []

        for c in raw_candidates:
            sig_audio = audio_signal_for_window(audio_rows, c.start_s, c.end_s)
            sig_chat = chat_signal_for_window(chat_offsets, c.start_s, c.end_s)
            avg = (sig_audio + sig_chat + c.signal_transcript) / 3.0
            beat_src = is_beat_sourced(c, cur, recording_id)
            score = max(0.9, avg) if beat_src else avg
            row = (c, sig_audio, sig_chat, c.signal_transcript, score)
            if beat_src:
                beat_rows.append(row)
            else:
                signal_rows.append(row)

        signal_rows_sorted = sorted(signal_rows, key=lambda x: x[4], reverse=True)
        kept_signal = signal_rows_sorted[:cap_count]
        # ID-04: named for the RESULT line so a capped-out run is distinguishable
        # from a run that genuinely found nothing.
        signal_raw = len(signal_rows_sorted)
        cap = cap_count
        capped_out = max(0, signal_raw - len(kept_signal))

        candidates_written = 0
        beat_written = 0
        signal_written = 0
        poisoned_excluded = 0
        # ID-08 (golden-review F21): ON CONFLICT ... DO NOTHING means a full
        # re-run of an already-fused recording reports candidates_written=0,
        # identical to the found-nothing case. rows_seen/rows_skipped_existing
        # separate "nothing was found" from "everything already existed".
        rows_seen = 0
        rows_skipped_existing = 0

        # autocommit is OFF: the statements above already opened the
        # transaction implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        for row_group, is_beat in ((beat_rows, True), (kept_signal, False)):
            for c, sig_audio, sig_chat, sig_transcript, score in row_group:
                poisoned = is_poisoned(cur, recording_id, c.start_s, c.end_s)
                if poisoned:
                    poisoned_excluded += 1
                    continue

                rows_seen += 1
                cur.execute(
                    '''
                    INSERT INTO clip_candidates(
                        recording_id, start_s, end_s, score,
                        signal_audio, signal_transcript, signal_chat, signal_beat_boost,
                        state, created_by_run, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'candidate', %s, %s)
                    ON CONFLICT (recording_id, start_s, end_s) DO NOTHING
                    ''',
                    (
                        recording_id,
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
                else:
                    rows_skipped_existing += 1

        # R6/ID-12: state never left 'transcribed' — advance it here, in the
        # same transaction as the candidate writes above, so a rollback on
        # failure also rolls back the state (CHECK constraint: 001 allows
        # 'detected').
        cur.execute("UPDATE recordings SET state = 'detected' WHERE id = %s", (recording_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        f'RESULT score_fusion recording={recording_id} candidates_written={candidates_written} '
        f'beat_sourced={beat_written} signal_only={signal_written} poisoned_excluded={poisoned_excluded} '
        f'signal_raw={signal_raw} cap={cap} capped_out={capped_out} '
        f'rows_seen={rows_seen} rows_skipped_existing={rows_skipped_existing}'
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
