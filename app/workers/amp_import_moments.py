#!/usr/bin/env python3
"""amp_import_moments: import the operator's approved moments into the amp
clipper database (CLPR-AMP-02 items 4 and 5; ruling
clpr-amp-mode-eight-changes-approved-amp-project-only-2026-10-07).

Amp mode only (CLPR_AMP_MODE=1, so db.connect() is the amp database). No
transcription, scoring, LLM or chat stage runs: the candidates ARE the approved
windows and the transcript IS the episode's caption file.

Every input is pinned by a sha256 given on the command line (never hard-coded
here): the moments JSON, the source video and the caption SRT. All three
hashes, the six primary windows and every caption cue are validated BEFORE the
first database write; the writes run in one transaction committed last.

  - Only windows with kind == "primary" are imported (alternates are not
    approved); anything other than exactly 6 is refused.
  - Every caption cue overlapping a window must lie wholly inside it, and the
    cue numbers inside must equal that window's own "cues" list; a cue crossing
    a window edge is refused.
  - Idempotent: the recording is keyed by its path (recordings.path is
    UNIQUE; the file's sha is verified on every run), candidates by
    (recording_id, start_s, end_s) with ON CONFLICT DO NOTHING, and the
    caption cues are inserted once and verified equal on every later run.
  - Per candidate it stages, in CLPR_SLICES_DIR: c<id>.mp4 (a symlink to the
    source, so the render reads the original frames), c<id>.json (the schema-2
    sidecar covering the whole source, plus amp_clip_prefix
    <episode>-SOCIAL-<NN> that the amp render names the file by), and
    c<id>.preview.mp4 (the review page's pre-approval preview: the source cut
    to the slice bounds the page assumes, slice_geometry.slice_start/end).

Prints a RESULT line last; ERROR to stderr and exit 1 on any refusal.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path

import db
import render_from_slice
import slice_geometry

SRT_TIME_RE = re.compile(r'^(\d+):(\d{2}):(\d{2}),(\d{3}) --> (\d+):(\d{2}):(\d{2}),(\d{3})$')
PRIMARY_COUNT = 6


def fail(msg: str) -> None:
    raise RuntimeError(msg)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for block in iter(lambda: fh.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def require_sha(label: str, path: Path, expected: str) -> None:
    if not path.is_file():
        fail(f'{label}_MISSING: {path}')
    actual = sha256_of(path)
    print(f'SHA {label} path="{path}" sha256={actual} expected={expected}')
    if actual != expected.strip().lower():
        fail(f'{label}_SHA_MISMATCH: {path} is sha256 {actual}, expected {expected}; refusing')


def parse_srt(text: str) -> list[tuple[int, float, float, str]]:
    """(number, start_s, end_s, text) per cue, text byte for byte (its own
    line breaks kept)."""
    cues = []
    blocks = [b for b in re.split(r'\r?\n\r?\n', text.strip('\r\n')) if b.strip()]
    for block in blocks:
        lines = block.split('\n')
        if len(lines) < 3 or not lines[0].strip().isdigit():
            fail(f'CAPTIONS_UNPARSEABLE: block {block!r}')
        m = SRT_TIME_RE.match(lines[1].strip())
        if not m:
            fail(f'CAPTIONS_UNPARSEABLE: timing line {lines[1]!r}')
        g = [int(x) for x in m.groups()]
        start = g[0] * 3600 + g[1] * 60 + g[2] + g[3] / 1000
        end = g[4] * 3600 + g[5] * 60 + g[6] + g[7] / 1000
        cues.append((int(lines[0].strip()), start, end, '\n'.join(lines[2:])))
    return cues


def primary_windows(moments: dict) -> list[dict]:
    windows = [w for w in moments.get('windows', []) if w.get('kind') == 'primary']
    skipped = [w.get('n') for w in moments.get('windows', []) if w.get('kind') != 'primary']
    print(f'MOMENTS primary={[w.get("n") for w in windows]} not_imported={skipped}')
    if len(windows) != PRIMARY_COUNT:
        fail(f'MOMENTS_PRIMARY_COUNT: {len(windows)} primary windows, expected {PRIMARY_COUNT}')
    for w in windows:
        if not isinstance(w.get('n'), int):
            fail(f'MOMENTS_BAD_WINDOW: n={w.get("n")!r}')
        for key in ('start_s', 'end_s'):
            if not math.isfinite(float(w[key])):
                fail(f'MOMENTS_BAD_WINDOW: n={w["n"]} {key}={w[key]!r}')
        if not float(w['start_s']) < float(w['end_s']):
            fail(f'MOMENTS_BAD_WINDOW: n={w["n"]} start_s >= end_s')
    return windows


def cues_for_window(cues, w) -> list[int]:
    """Cue numbers wholly inside [start_s, end_s]; a crossing cue refuses."""
    ws, we = float(w['start_s']), float(w['end_s'])
    inside = []
    for number, start, end, text in cues:
        if end <= ws or start >= we:
            continue
        if start < ws or end > we:
            fail(f'CAPTION_CROSSES_WINDOW: window {w["n"]} [{ws:.3f}..{we:.3f}]s cuts cue '
                 f'{number} [{start:.3f}..{end:.3f}]s {text!r}; refusing')
        inside.append(number)
    if inside != list(w.get('cues', [])):
        fail(f'WINDOW_CUES_MISMATCH: window {w["n"]} holds cues {inside}, '
             f'MOMENTS lists {w.get("cues")}; refusing')
    return inside


def probe_duration(path: Path) -> float:
    return render_from_slice.measure_duration_s(path)


def stage(slices_dir: Path, candidate_id: int, source: Path, duration_s: float,
          prefix: str, start_s: float, end_s: float, ffmpeg_bin: str) -> None:
    link = slices_dir / f'c{candidate_id}.mp4'
    if link.is_symlink() or link.exists():
        if not link.is_symlink() or Path(os.readlink(link)) != source:
            fail(f'SLICE_CONFLICT: {link} exists and is not a symlink to {source}')
    else:
        link.symlink_to(source)
    sidecar = {
        'schema': slice_geometry.SIDECAR_SCHEMA,
        'candidate_id': candidate_id,
        'abs_start_s': 0.0,
        'abs_end_s': duration_s,
        'actual_duration_s': duration_s,
        'source_path': str(source),
        'source_size_bytes': source.stat().st_size,
        'source_duration_s': duration_s,
        render_from_slice.AMP_SIDECAR_CLIP_PREFIX: prefix,
    }
    (slices_dir / f'c{candidate_id}.json').write_text(json.dumps(sidecar, indent=1) + '\n',
                                                      encoding='utf-8')
    preview = slices_dir / f'c{candidate_id}.preview.mp4'
    if preview.exists():
        return
    lo = slice_geometry.slice_start(start_s)
    hi = slice_geometry.slice_end(end_s, duration_s)
    # The review server's own preview-proxy encode, imported, not copied.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from review_server import FFMPEG_PROXY_ARGS
    tmp = slices_dir / f'c{candidate_id}.preview.part.mp4'
    render_from_slice.run_capture([
        ffmpeg_bin, '-y', '-ss', f'{lo:.3f}', '-t', f'{hi - lo:.3f}', '-i', str(source),
        *FFMPEG_PROXY_ARGS, str(tmp),
    ])
    os.replace(tmp, preview)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--moments', required=True, type=Path)
    ap.add_argument('--moments-sha256', required=True)
    ap.add_argument('--source', required=True, type=Path)
    ap.add_argument('--source-sha256', required=True)
    ap.add_argument('--captions', required=True, type=Path)
    ap.add_argument('--captions-sha256', required=True)
    ap.add_argument('--episode', required=True, help='session label, e.g. EP1')
    a = ap.parse_args()

    if not db.amp_mode():
        fail('AMP_MODE_OFF: amp_import_moments runs only with CLPR_AMP_MODE=1')
    slices_dir = Path(render_from_slice.require_env('CLPR_SLICES_DIR'))
    ffmpeg_bin = render_from_slice.resolve_amp_ffmpeg()
    source = a.source.resolve()

    # ---- validate everything before the first write -----------------------
    require_sha('MOMENTS', a.moments, a.moments_sha256)
    require_sha('SOURCE', source, a.source_sha256)
    require_sha('CAPTIONS', a.captions, a.captions_sha256)
    moments = json.loads(a.moments.read_text(encoding='utf-8'))
    windows = primary_windows(moments)
    cues = parse_srt(a.captions.read_text(encoding='utf-8'))
    per_window = {w['n']: cues_for_window(cues, w) for w in windows}
    duration_s = probe_duration(source)
    for w in windows:
        if float(w['end_s']) > duration_s:
            fail(f'MOMENTS_BAD_WINDOW: n={w["n"]} ends after the source ({duration_s:.3f}s)')
    print(f'VALIDATED windows={len(windows)} cues={len(cues)} '
          f'cues_per_window={ {n: len(v) for n, v in per_window.items()} }')

    run_id = f'amp_import_moments_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'
    now = render_from_slice.utc_now_iso()
    slices_dir.mkdir(parents=True, exist_ok=True)
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT id, session_label FROM recordings WHERE path = %s', (str(source),))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                'INSERT INTO recordings(path, session_label, duration_s, ingested_at) '
                'VALUES (%s, %s, %s, %s) RETURNING id',
                (str(source), a.episode, duration_s, now),
            )
            recording_id = int(cur.fetchone()[0])
            recording_new = 1
        else:
            recording_id, label = int(row[0]), str(row[1])
            if label != a.episode:
                fail(f'RECORDING_CONFLICT: {source} is recording {recording_id} labelled '
                     f'{label!r}, not {a.episode!r}')
            recording_new = 0

        cur.execute('SELECT start_s, end_s, text FROM transcript_segments '
                    'WHERE recording_id = %s ORDER BY start_s, id', (recording_id,))
        stored = [(float(s), float(e), str(t)) for s, e, t in cur.fetchall()]
        wanted = [(s, e, t) for _, s, e, t in cues]
        if not stored:
            cur.executemany(
                'INSERT INTO transcript_segments(recording_id, start_s, end_s, text) '
                'VALUES (%s, %s, %s, %s)',
                [(recording_id, s, e, t) for s, e, t in wanted],
            )
            cues_inserted = len(wanted)
        elif stored != wanted:
            fail(f'CAPTIONS_CONFLICT: recording {recording_id} already holds '
                 f'{len(stored)} cues that differ from {a.captions}; refusing')
        else:
            cues_inserted = 0

        inserted = 0
        for w in windows:
            start_s, end_s = float(w['start_s']), float(w['end_s'])
            cur.execute(
                'INSERT INTO clip_candidates(recording_id, start_s, end_s, state, created_by_run, '
                'created_at, burn_captions, post_kit_enabled) '
                "VALUES (%s, %s, %s, 'candidate', %s, %s, 1, 0) "
                'ON CONFLICT (recording_id, start_s, end_s) DO NOTHING RETURNING id',
                (recording_id, start_s, end_s, run_id, now),
            )
            got = cur.fetchone()
            if got is None:
                cur.execute('SELECT id FROM clip_candidates WHERE recording_id = %s '
                            'AND start_s = %s AND end_s = %s', (recording_id, start_s, end_s))
                got = cur.fetchone()
            else:
                inserted += 1
            candidate_id = int(got[0])
            prefix = f'{a.episode}-SOCIAL-{w["n"]:02d}'
            stage(slices_dir, candidate_id, source, duration_s, prefix, start_s, end_s, ffmpeg_bin)
            print(f'WINDOW n={w["n"]} candidate_id={candidate_id} window={start_s:.3f}-{end_s:.3f} '
                  f'cues={per_window[w["n"]]} prefix={prefix}')
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f'RESULT amp_import_moments ok=1 recording_id={recording_id} recording_new={recording_new} '
          f'candidates_inserted={inserted} cues_inserted={cues_inserted}')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
