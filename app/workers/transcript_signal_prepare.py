#!/usr/bin/env python3
"""transcript_signal_prepare: emit every LLM prompt the transcript signal lane needs for one VOD.

Split lane, first half (n8n Basic LLM Chain redesign): this worker builds the exact
prompts the LOCAL lane (transcript_signal.py) would send, using the SAME iterators
(iter_scan_items / iter_zebra_items) so prompt construction exists exactly once.
n8n executes the LLM calls; transcript_signal_ingest.py validates and lands responses.

No API calls. No DB writes beyond ensure_intermediate_table (CREATE IF NOT EXISTS).
stdout is exactly ONE JSON object (sorted keys, deterministic — the downstream node
parses stdout as JSON, so no RESULT line is printed on success):
  {"vod_id": N, "items": [{"call_id", "kind": "scan"|"zebra", "prompt", "meta"}], "count": M}
call_ids are deterministic (kind + chunk index / trigger offset) so ingest can
recompute them independently. Zebra triggers whose transcript slice is empty need
no LLM call and are NOT emitted; ingest recomputes their fallback rows itself.

Exits non-zero on any failure; failure verdict prints to stderr (D-047).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys

import transcript_signal as ts


def scan_call_id(chunk_index: int) -> str:
    return f'scan:{chunk_index:04d}'


def zebra_call_id(trigger_offset_s: float) -> str:
    return f'zebra:{trigger_offset_s:.3f}'


def build_items(conn: sqlite3.Connection, vod_id: int) -> list[dict]:
    duration_s, _ = ts.fetch_vod(conn, vod_id)
    segments = ts.fetch_segments(conn, vod_id)
    triggers = ts.fetch_zebra_triggers(conn, vod_id)

    items: list[dict] = []
    for meta, prompt in ts.iter_scan_items(segments):
        items.append({
            'call_id': scan_call_id(meta['chunk_index']),
            'kind': 'scan',
            'prompt': prompt,
            'meta': meta,
        })
    for meta, prompt in ts.iter_zebra_items(triggers, duration_s, segments):
        if prompt is None:
            continue  # empty transcript slice: no LLM call; ingest emits the fallback row
        items.append({
            'call_id': zebra_call_id(meta['trigger_offset_s']),
            'kind': 'zebra',
            'prompt': prompt,
            'meta': meta,
        })
    return items


def run(vod_id: int) -> int:
    db_path = ts.get_db_path()
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        ts.ensure_intermediate_table(conn)
        items = build_items(conn, vod_id)

    payload = {'vod_id': vod_id, 'items': items, 'count': len(items)}
    json.dump(payload, sys.stdout, sort_keys=True)
    sys.stdout.write('\n')
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Emit every transcript-signal LLM prompt for one VOD as JSON')
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
