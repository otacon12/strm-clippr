#!/usr/bin/env python3
"""quality_gate: D-028 transcript quality gate — refuse detection on a bad transcript.

Classifies every transcript segment as blank / non_speech / speech, computes
transcript-health metrics, and FAILS (exit 1) when the transcript is unusable
(no segments, majority BLANK_AUDIO, or majority repetition-loop in the speech).

Bracketed markers ([music], [MUSIC], [INAUDIBLE], [APPLAUSE], ...) and lines
containing the musical note are FAITHFUL transcriptions of singing/music on
stream (operator-verified 2026-08-05), NOT hallucinations — they never fail
the gate on their own.

Prints machine-parseable RESULT line to stdout always; on failure, prints the
RESULT line and the full verdict to stderr too (n8n's Execute Command discards
a failing child's stdout — only stderr reaches the error). Exits non-zero on
any failure.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys

NON_SPEECH_BRACKET_RE = re.compile(r'^\[[A-Za-z_ ]+\]$')
MUSIC_NOTE = '♪'  # ♪


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def fetch_duration_s(conn: sqlite3.Connection, vod_id: int) -> float:
    row = conn.execute('SELECT duration_s FROM vods WHERE id = ?', (vod_id,)).fetchone()
    if not row:
        raise RuntimeError(f'vod_id not found: {vod_id}')
    return float(row[0]) if row[0] is not None else 0.0


def classify(text: str) -> str:
    """Classify one segment's text (already stripped) as blank / non_speech / speech."""
    if 'BLANK_AUDIO' in text:
        return 'blank'
    if NON_SPEECH_BRACKET_RE.match(text) or MUSIC_NOTE in text:
        return 'non_speech'
    return 'speech'


def repetition_count(speech_texts: list[str]) -> int:
    """Count segments inside runs of >= 3 consecutive identical texts.

    Operates over the speech subsequence ONLY (order preserved, non-speech
    already filtered out). Identity is after strip().lower().
    """
    rep = 0
    i = 0
    n = len(speech_texts)
    while i < n:
        j = i
        key = speech_texts[i].strip().lower()
        while j < n and speech_texts[j].strip().lower() == key:
            j += 1
        run_len = j - i
        if run_len >= 3:
            rep += run_len
        i = j
    return rep


def gate(vod_id: int) -> int:
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        duration_s = fetch_duration_s(conn, vod_id)
        rows = conn.execute(
            '''
            SELECT text
            FROM transcript_segments
            WHERE vod_id = ?
            ORDER BY start_s, id
            ''',
            (vod_id,),
        ).fetchall()

    texts = [str(t or '').strip() for (t,) in rows]
    total = len(texts)

    blank = 0
    non_speech = 0
    speech_texts: list[str] = []
    for text in texts:
        kind = classify(text)
        if kind == 'blank':
            blank += 1
        elif kind == 'non_speech':
            non_speech += 1
        else:
            speech_texts.append(text)

    speech = len(speech_texts)
    blank_pct = (blank / total * 100.0) if total else 0.0
    non_speech_pct = (non_speech / total * 100.0) if total else 0.0
    rep = repetition_count(speech_texts)
    rep_pct = (rep / speech * 100.0) if speech else 0.0
    segments_per_min = (total / (duration_s / 60.0)) if duration_s > 0 else 0.0

    failures: list[str] = []
    if total == 0:
        failures.append('transcript has ZERO segments')
    else:
        if blank_pct > 50.0:
            failures.append(f'blank_pct={blank_pct:.1f}% of segments are BLANK_AUDIO (>50%)')
        if rep_pct > 50.0:
            failures.append(f'repetition_pct={rep_pct:.1f}% of speech segments sit in >=3-long identical runs (>50%)')

    verdict = 'FAIL' if failures else 'PASS'
    result_line = (
        f'RESULT quality_gate {verdict} vod_id={vod_id} total={total} '
        f'blank_pct={blank_pct:.1f} non_speech_pct={non_speech_pct:.1f} speech={speech} '
        f'repetition_pct={rep_pct:.1f} segments_per_min={segments_per_min:.1f} duration_s={duration_s:.1f}'
    )

    # Metrics line always goes to stdout.
    print(result_line)

    if failures:
        # n8n's Execute Command DISCARDS a failing child's stdout; only stderr
        # reaches the error output — so the failure path repeats everything there.
        print(result_line, file=sys.stderr)
        print(f'QUALITY GATE FAILED (D-028) for vod_id={vod_id}:', file=sys.stderr)
        for reason in failures:
            print(f'  - {reason}', file=sys.stderr)
        print(
            'Detection must NOT run on this transcript: scoring garbage segments '
            'would produce garbage candidates and burn review time downstream (D-028).',
            file=sys.stderr,
        )
        print(
            'Known fix for genuine transcription failures: re-transcribe in chunks '
            '(chunked transcription beat the whole-file run on the same VOD, D-028).',
            file=sys.stderr,
        )
        return 1

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='D-028 transcript quality gate: fail on unusable transcripts before detection')
    parser.add_argument('--vod-id', type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return gate(args.vod_id)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
