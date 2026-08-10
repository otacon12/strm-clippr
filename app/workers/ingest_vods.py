#!/usr/bin/env python3
"""ingest_vods: scan VOD directory and register eligible files in recordings.
Reads CLPR_DB_URL; exits non-zero on any failure.
Prints machine-parseable RESULT line last.

PostgreSQL port (D-052 P3): connects via the shared adapter app/workers/db.py
(CLPR_DB_URL); tables per app/docs/naming-map.md (vods -> recordings). The
worker name (and its RESULT/VODS_* output labels) are an external contract
and stay as-is.

Two modes, identical per-file semantics (MIN_AGE_SECONDS guard, idempotent
find-or-create, per-file failure isolation, RESULT line shape):
  scan mode (default, no --video): iterate --root (or $CLPR_VIDEO_ROOT, or
    the VIDEO_ROOT constant if neither is given -- byte-unchanged for every
    existing caller) for files matching VIDEO_EXTS.
  --video <path>: ingest EXACTLY that one file, regardless of scan root --
    closes the local-engine ingest gap (an operator-ruled to_clip source
    that lives outside VIDEO_ROOT; see DECISIONS.md).

MIN_AGE_SECONDS BYPASS -- opt-in, off by default (--skip-age-check, or the
env var CLPR_SKIP_AGE_CHECK=1; either engages it -- see resolve_skip_age_check
below). The guard exists to catch a file whose mtime is still moving because
it is a Google Drive DESKTOP sync in progress -- an in-progress recording or
a half-synced file. That reasoning does not hold for a caller that downloaded
the file itself via the Drive API and already verified, byte-for-byte, that
what landed on disk matches Drive's own reported size: for that caller the
file's mtime is simply the download-completion time (always seconds old),
never a signal of an in-progress write, and the guard would reject a
genuinely-complete file forever. Absent the flag/env var, behaviour is
UNCHANGED: the 30-minute guard applies exactly as before to every existing
caller (scan mode and --video alike).
"""

import argparse
import datetime as dt
import os
import re
import subprocess
import sys
from pathlib import Path

import db

try:
    from migrations import apply_migrations
except ModuleNotFoundError:
    from .migrations import apply_migrations

VIDEO_ROOT = Path('/Volumes/GOLDMINE/vibecoder-recordings/')
VIDEO_EXTS = {'.mov', '.mp4', '.mkv'}
MIN_AGE_SECONDS = 30 * 60


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def redacted_db_url() -> str:
    """CLPR_DB_URL with any password redacted (userinfo or ?password= form) —
    this string is printed into worker logs and must never leak a secret."""
    url = os.environ.get('CLPR_DB_URL', '')
    url = re.sub(r'://([^:/@]*):[^@]*@', r'://\1:***@', url)
    url = re.sub(r'([?&]password=)[^&]*', r'\1***', url)
    return url


def ensure_schema(conn) -> None:
    migrations_dir = Path(__file__).resolve().parent.parent / 'migrations_pg'
    apply_migrations(conn, migrations_dir)


def parse_session_label(filename: str) -> str:
    # Expected like: 2026-07-20 10-44-08.mov -> 2026-07-20
    return filename.split(' ')[0]


def probe_duration_seconds(path: Path) -> str:
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(path),
    ]
    print(f'FFPROBE_CMD {cmd}')
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, check=True)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or '').rstrip('\n')
        raise RuntimeError(
            f'ffprobe failed for {path} exit={exc.returncode} stderr={stderr}'
        ) from exc

    raw = proc.stdout.strip()
    if raw == '':
        raise RuntimeError(f'ffprobe returned empty duration output for {path}')
    print(f'FFPROBE_RAW path="{path}" output="{raw}"')
    return raw


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='Scan a VOD directory (or ingest one named file) and '
                     'register eligible files in recordings.',
    )
    p.add_argument(
        '--video', default=None,
        help='ingest EXACTLY this one file (find-or-create its recordings '
             'row), regardless of VIDEO_ROOT/--root/$CLPR_VIDEO_ROOT -- '
             'bypasses the directory scan entirely. All per-file semantics '
             '(the MIN_AGE_SECONDS guard, idempotent find-or-create, failure '
             'isolation) still apply to this one file.',
    )
    p.add_argument(
        '--root', default=None,
        help='override the scan-mode VOD directory (default: VIDEO_ROOT, '
             f'i.e. {VIDEO_ROOT}, or $CLPR_VIDEO_ROOT if set). Ignored when '
             '--video is given.',
    )
    p.add_argument(
        '--skip-age-check', action='store_true',
        help='bypass the MIN_AGE_SECONDS (30m) guard for every file in this '
             'invocation. Off by default -- the guard applies exactly as '
             'today unless this is passed (or $CLPR_SKIP_AGE_CHECK=1 is '
             'set; either engages it). Only safe for a caller that has '
             'independently verified the file is completely written (e.g. '
             'on-disk bytes verified against Drive-reported metadata '
             'bytes) -- this flag does not perform that verification '
             'itself, it only trusts the caller already did.',
    )
    return p.parse_args()


def resolve_skip_age_check(cli_flag: bool) -> bool:
    """--skip-age-check wins if passed; otherwise CLPR_SKIP_AGE_CHECK=1 (the
    established CLPR_* env-var pattern this codebase already uses elsewhere,
    e.g. CLPR_ALLOW_DURING_STREAM) engages the same bypass. Absent both, this
    returns False and behaviour is byte-unchanged for every existing caller.
    """
    if cli_flag:
        return True
    return os.environ.get('CLPR_SKIP_AGE_CHECK', '').strip() == '1'


def resolve_video_root(root_arg) -> Path:
    """--root wins, then $CLPR_VIDEO_ROOT, then the VIDEO_ROOT constant --
    so a caller that passes neither (every existing caller, before this
    change) is byte-unchanged: it still scans VIDEO_ROOT exactly as before.
    """
    if root_arg:
        return Path(root_arg)
    env_root = os.environ.get('CLPR_VIDEO_ROOT', '').strip()
    if env_root:
        return Path(env_root)
    return VIDEO_ROOT


def main() -> int:
    args = parse_args()
    skip_age_check = resolve_skip_age_check(args.skip_age_check)

    if args.video:
        # Single-file mode: ingest EXACTLY this file, regardless of scan
        # root. os.path.abspath(os.path.expanduser(...)) matches run_vod.py's
        # own normalization of --video (see its main()), so the path string
        # written to recordings.path here is byte-identical to the one
        # run_vod.py's post-ingest lookup_recording() queries for.
        targets = [Path(os.path.abspath(os.path.expanduser(args.video)))]
    else:
        root = resolve_video_root(args.root)
        if not root.exists():
            raise FileNotFoundError(f'VOD root not found: {root}')
        targets = [
            p for p in sorted(root.iterdir())
            if p.is_file() and p.suffix.lower() in VIDEO_EXTS
        ]

    now_epoch = dt.datetime.now(dt.timezone.utc).timestamp()
    inserted = 0
    skipped_recent = 0
    existing = 0
    failed = 0

    conn = db.connect()
    try:
        cur = conn.cursor()
        ensure_schema(conn)

        # autocommit is OFF: the first statement below opens the transaction
        # implicitly (no explicit BEGIN in PostgreSQL/psycopg2).
        for p in targets:
            try:
                # Per-file savepoint: a failed SQL statement aborts a PG
                # transaction, so without this the FILE_FAILED-and-continue
                # contract could not survive a SQL error on one file.
                cur.execute('SAVEPOINT ingest_file')
                mtime_epoch = p.stat().st_mtime
                age_seconds = now_epoch - mtime_epoch
                # This guard applies identically in scan mode and single-file
                # (--video) mode: a file still mid-Drive-sync (mtime moving)
                # is exactly the in-progress-recording case it exists to
                # catch, regardless of which directory it lives in -- a
                # to_clip file synced in from Google Drive desktop is not
                # exempt just because it arrived via --video. skip_age_check
                # is the one opt-in exception: a caller that has already
                # verified this exact file is completely written by a means
                # other than mtime (byte-count match against Drive metadata)
                # bypasses this proxy in favour of that direct witness -- see
                # the module docstring.
                if age_seconds < MIN_AGE_SECONDS:
                    if skip_age_check:
                        print(
                            f'AGE_CHECK_BYPASSED file="{p.name}" age_seconds={age_seconds:.1f} '
                            'reason="MIN_AGE_SECONDS guard bypassed: completeness verified by '
                            'byte-count match against Drive metadata (the mtime-staleness proxy '
                            'this guard checks does not apply -- the file was downloaded via the '
                            'Drive API, not synced in place, so its mtime is the download-'
                            'completion time, not a signal of an in-progress write)"'
                        )
                    else:
                        skipped_recent += 1
                        print(f'SKIP_RECENT file="{p.name}" age_seconds={age_seconds:.1f} reason="modified <30m"')
                        continue

                session_label = parse_session_label(p.name)
                duration_raw = probe_duration_seconds(p)
                duration_s = float(duration_raw)
                ingested_at = utc_now_iso()

                cur.execute(
                    '''
                    INSERT INTO recordings(path, session_label, duration_s, ingested_at, state)
                    VALUES (%s, %s, %s, %s, 'ingested')
                    ON CONFLICT (path) DO NOTHING
                    ''',
                    (str(p), session_label, duration_s, ingested_at),
                )
                inserted_now = cur.rowcount == 1

                cur.execute('SELECT id FROM recordings WHERE path = %s', (str(p),))
                recording_id_row = cur.fetchone()
                recording_id = recording_id_row[0] if recording_id_row else None

                if inserted_now:
                    inserted += 1
                    print(
                        'INGESTED '
                        f'id={recording_id} '
                        f'path="{p}" '
                        f'session_label="{session_label}" '
                        f'duration_s={duration_s}'
                    )
                else:
                    existing += 1
                    print(f'ALREADY_EXISTS id={recording_id} path="{p}"')
            except Exception as exc:
                failed += 1
                print(f'FILE_FAILED path="{p}" reason="{exc}"')
                cur.execute('ROLLBACK TO SAVEPOINT ingest_file')
                continue

        conn.commit()

        cur.execute('SELECT id, path, session_label, duration_s, ingested_at, state FROM recordings ORDER BY id')
        rows = cur.fetchall()
        print(f'VODS_TOTAL {len(rows)}')
        for r in rows:
            print(f'VODS_ROW {r}')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f'RESULT ingest_vods ok=1 inserted={inserted} existing={existing} skipped_recent={skipped_recent} failed={failed} db_path="{redacted_db_url()}"')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
