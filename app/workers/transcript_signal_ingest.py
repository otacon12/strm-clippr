#!/usr/bin/env python3
"""transcript_signal_ingest: validate n8n LLM responses and land signal candidate rows for one VOD.

Split lane, second half (n8n Basic LLM Chain redesign). Reads a responses file shaped
  {"vod_id": N, "items": [{"call_id", "kind", "meta", "response_text"}], "count": M}

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (rows land in
llm_signal_candidates, schema-owned by 001_consolidated_schema.sql). The
--vod-id CLI flag and the "vod_id" JSON key are external contracts (n8n) and
stay; both bind to recording_id internally.

HARD GATES before any write (any failure -> loud error, ZERO rows land):
  - file exists and parses as a JSON object
  - file vod_id == --vod-id
  - count == len(items)
  - the multiset of call_ids exactly matches the ids this worker RECOMPUTES from the DB
    via the same iterators prepare used (deterministic ids; file meta is never trusted —
    all validation meta is recomputed from the DB).

Then per item, validation/normalization mirrors transcript_signal.py exactly:
  - scan: extract_json_payload + json.loads (malformed -> whole run fails, matching the
    monolith where a bad payload raises out of call_claude_structured, uncaught in
    detect_transcript_categories); candidates-list check; category whitelist skip;
    normalize_window; reason default; confidence clamp; cross-chunk millisecond dedup.
  - zebra: per-trigger fallback defaults; parse errors are caught per item and produce
    the fallback row with a model-error reason (matching detect_zebra_boundaries'
    try/except); confidence >= 0.5 gate; empty-slice triggers (no LLM item) produce
    the pure fallback row.

Rows land via the same insert path (insert_signal_rows, ON CONFLICT ... DO NOTHING)
with a fresh run_id. RESULT line last. Exits non-zero on any failure; failure verdict
prints to stderr (D-047).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Optional

import db
import transcript_signal as ts
from transcript_signal_prepare import scan_call_id, zebra_call_id

Row = tuple[float, float, str, str, float, str, Optional[float]]


def load_responses(path: str) -> dict:
    if not os.path.isfile(path):
        raise RuntimeError(f'responses file not found: {path}')
    with open(path, encoding='utf-8') as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f'responses file is not valid JSON: {exc}')
    if not isinstance(data, dict):
        raise RuntimeError('responses file must be a JSON object')
    return data


def collect_scan_rows(scan_items: list, responses: dict[str, str], duration_s: float) -> list[Row]:
    """Mirror detect_transcript_categories' per-response handling exactly."""
    out: list[Row] = []
    seen: set[tuple[int, int, str]] = set()

    for meta, _prompt in scan_items:
        raw = responses[scan_call_id(meta['chunk_index'])]
        # Malformed payload raises -> whole run fails, zero rows (monolith parity:
        # json.loads raises inside call_claude_structured, uncaught in the scan loop).
        data = json.loads(ts.extract_json_payload(raw))
        candidates = data.get('candidates')
        if not isinstance(candidates, list):
            raise RuntimeError('LLM response missing candidates list')

        for item in candidates:
            if not isinstance(item, dict):
                continue
            category = str(item.get('category', '')).strip()
            if category not in ts.CATEGORIES:
                continue
            start_s, end_s = ts.normalize_window(float(item['start_s']), float(item['end_s']), duration_s)
            reason = str(item.get('reason', '')).strip() or 'model-selected transcript moment'
            confidence = ts.clamp(float(item.get('confidence', 0.0)), 0.0, 1.0)

            key = (int(round(start_s * 1000)), int(round(end_s * 1000)), category)
            if key in seen:
                continue
            seen.add(key)
            out.append((start_s, end_s, category, reason, confidence, 'transcript_scan', None))

    return out


def collect_zebra_rows(zebra_items: list, responses: dict[str, str], duration_s: float) -> list[Row]:
    """Mirror detect_zebra_boundaries' per-trigger handling exactly."""
    out: list[Row] = []

    for meta, prompt in zebra_items:
        trigger_offset = meta['trigger_offset_s']
        outer_start = meta['outer_start_s']
        fallback_start = meta['fallback_start_s']
        chosen_start = fallback_start
        chosen_conf = 0.0
        reason = 'fallback 60s lookback from zebra trigger'

        if prompt is not None:
            raw = responses[zebra_call_id(trigger_offset)]
            try:
                data = json.loads(ts.extract_json_payload(raw))
                conf = ts.clamp(float(data.get('confidence', 0.0)), 0.0, 1.0)
                model_start = ts.clamp(float(data.get('start_s', fallback_start)), outer_start, trigger_offset)
                model_reason = str(data.get('reason', '')).strip() or 'model-selected zebra boundary'
                if conf >= 0.5:
                    chosen_start = model_start
                    chosen_conf = conf
                    reason = model_reason
            except Exception as exc:
                reason = f'fallback 60s lookback from zebra trigger (model error: {exc})'

        end_s = ts.clamp(trigger_offset + 15.0, 0.0, duration_s)
        if end_s <= chosen_start:
            end_s = ts.clamp(chosen_start + 1.0, 0.0, duration_s)

        out.append((chosen_start, end_s, 'context', reason, chosen_conf, 'zebra_boundary', trigger_offset))

    return out


def run(recording_id: int, responses_path: str) -> int:
    payload = load_responses(responses_path)

    file_vod = payload.get('vod_id')
    if file_vod != recording_id:
        raise RuntimeError(f'vod_id mismatch: --vod-id {recording_id} but responses file has vod_id={file_vod!r}')

    items = payload.get('items')
    if not isinstance(items, list):
        raise RuntimeError('responses file missing items list')
    count = payload.get('count')
    if count != len(items):
        raise RuntimeError(f'count mismatch: count={count!r} but len(items)={len(items)}')
    for item in items:
        if not isinstance(item, dict) or 'call_id' not in item:
            raise RuntimeError('every responses item must be an object with a call_id')

    run_id = f'transcript_signal_ingest_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    conn = db.connect()
    try:
        cur = conn.cursor()
        duration_s, _ = ts.fetch_recording(cur, recording_id)
        segments = ts.fetch_segments(cur, recording_id)
        triggers = ts.fetch_zebra_triggers(cur, recording_id)
        # llm_signal_candidates is schema-owned (001_consolidated_schema.sql).

        scan_items = list(ts.iter_scan_items(segments))
        zebra_items = list(ts.iter_zebra_items(triggers, duration_s, segments))

        expected_ids = [scan_call_id(meta['chunk_index']) for meta, _p in scan_items]
        expected_ids += [zebra_call_id(meta['trigger_offset_s']) for meta, p in zebra_items if p is not None]

        got_ids = [str(item['call_id']) for item in items]
        if sorted(got_ids) != sorted(expected_ids):
            missing = sorted(set(expected_ids) - set(got_ids))
            unexpected = sorted(set(got_ids) - set(expected_ids))
            raise RuntimeError(
                f'call_id set mismatch: expected {len(expected_ids)} ids, got {len(got_ids)}; '
                f'missing={missing} unexpected={unexpected}'
            )

        responses = {str(item['call_id']): str(item.get('response_text') or '') for item in items}

        transcript_rows = collect_scan_rows(scan_items, responses, duration_s)
        zebra_rows = collect_zebra_rows(zebra_items, responses, duration_s)

        # autocommit is OFF: the reads above already opened the transaction
        # implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        inserted_transcript = ts.insert_signal_rows(cur, recording_id, transcript_rows, run_id)
        inserted_zebra = ts.insert_signal_rows(cur, recording_id, zebra_rows, run_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    attempted = len(transcript_rows) + len(zebra_rows)
    inserted = inserted_transcript + inserted_zebra
    print(
        f'RESULT transcript_signal_ingest recording={recording_id} items={len(items)} '
        f'rows_inserted={inserted} rows_skipped_duplicate={attempted - inserted} '
        f'scan_rows={len(transcript_rows)} scan_inserted={inserted_transcript} '
        f'zebra_rows={len(zebra_rows)} zebra_inserted={inserted_zebra}'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Validate LLM responses and land transcript-signal rows for one VOD')
    parser.add_argument('--vod-id', type=int, required=True)
    parser.add_argument('--responses', type=str, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(args.vod_id, args.responses)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
