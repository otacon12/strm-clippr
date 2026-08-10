#!/usr/bin/env python3
"""find_clips_local: the LOCAL clip-finding engine driver.

Operator ruling (2026-08-09, verbatim, law): under Find clips, "the option to
either trigger the portable or local engine... the result should be the
same... the routing will just be different." Both engines read the SAME
to_clip directory via Drive desktop sync. DEFAULT = LOCAL.

This mirrors the portable (n8n) finder's front half -- resolve the newest
candidate, refuse honestly when there is nothing new to do -- then hands the
VOD to this Mac's own pipeline driver (workers/run_vod.py) and relays its
output and exit code VERBATIM. It never re-implements pipeline logic itself
(run_vod.py already owns "resumable, idempotent, nothing partial" -- see its
own docstring) and it never writes a clip_candidates/recordings row itself;
this driver's own writes are limited to its lock file.

COUPLING TO run_vod.py IS DELIBERATELY SHALLOW: CLI args in, stdout/stderr
streamed through, exit code read back. Nothing here imports run_vod.py or
calls its functions directly -- run_vod.py is being edited by a concurrent,
unrelated build pass right now, so importing its internals would coincidence
this file to a moving target. Where the same SQL shape is genuinely needed
(the near-miss/dedupe lookup), it is COPIED with a comment naming the source,
not imported.

KNOWN GAP -- NOT FIXED HERE, FLAGGED FOR THE OPERATOR/DESIGN AGENT.
CLPR_TO_CLIP_DIR (this driver's video source, per the ruling above) and
workers/ingest_vods.VIDEO_ROOT (currently hardcoded to
/Volumes/GOLDMINE/vibecoder-recordings/, the ONLY directory run_vod.py's
`ingest` stage ever scans -- no CLI flag exists to point it elsewhere) are
TWO DIFFERENT DIRECTORIES. On the day this file was written, to_clip held
zero video files and GOLDMINE held 15. So: a genuinely new recording that
lands in to_clip will be picked by find_newest_video() below, handed to
`run_vod.py --video <to_clip-path>`, sail through preflight, and then fail
at the `ingest` stage -- ingest_vods.py will not register a to_clip path
(it never scans that directory), so run_vod's own post-ingest lookup will
raise a StageFailure ("no recordings row exists for path=..."). run_vod.py
already half-anticipates this (see its own NOTE at the "ingest will not
register it" check), but the actual failure is real and reproducible; fixing
it means changing ingest_vods.py's scan root or adding a single-file ingest
path, both out of this file's scope (ingest_vods.py and run_vod.py are not
files this brief authorized touching) and both a design decision, not a
build one. Historically to_clip held only extracted AUDIO (.m4a) for the
n8n/portable lane (see DECISIONS.md: "upload the .m4a to to_clip"), which is
a different vocabulary again. This driver is built exactly to the brief
regardless: the failure, when it happens, is run_vod's own honest
StageFailure, propagated verbatim below, never swallowed or worked around.

Exit codes
----------
    0  pipeline complete (run_vod exit 0), OR NO_NEW_VOD (nothing to do --
       an empty to_clip dir, or the newest file is already processed; see
       dedupe_skip_reason() for the exact rule shipped)
    1  run_vod.py exited 1 (a stage failed for a reason other than the three
       classified below), or an unexpected error in this driver itself
    2  usage/config problem in THIS driver, before run_vod.py is ever
       invoked: CLPR_TO_CLIP_DIR unset, or the directory it names is missing
    3  passthrough of run_vod's EXIT_OBS_GATE (D-009/D-019) -- NOT a
       failure; the RESULT line below carries marker=OBS_GUARD_REFUSED
    4  passthrough of run_vod's EXIT_QUALITY_GATE (D-028) -- marker=QUALITY_GATE_FAILED
    5  passthrough of run_vod's EXIT_EXTRACT_REFUSED (no/silent audio) --
       marker=AUDIO_GUARD_REFUSED
    6  another local run is already live (this driver's own file lock) --
       marker=LOCAL_LOCK_BUSY
    7  the PORTABLE (server) engine is mid-run (cross-engine busy check,
       operator-approved) -- marker=PORTABLE_ENGINE_BUSY

Codes 0/1/3/4/5 are run_vod.py's OWN exit codes, read back unchanged
("classified failures verbatim"); 2/6/7 are this driver's own and never
overlap with a run_vod invocation.

RESULT LINE, LAST (matches this codebase's worker convention -- AGENTS.md).
Unlike most workers here, this file's stdout is also tailed by
review_server.py for direct operator display (GET /api/run-progress's
local_run.last_line), so the final RESULT line carries a machine-parseable
marker= token (grep-able, e.g. "OBS_GUARD_REFUSED") AND a quoted, readable
note="..." field in the SAME line, so whichever consumer reads it gets both.
note text is authored to never contain a literal double-quote character (the
UI's parser assumes none) and never an em dash.

Non-interactive by construction, same discipline as run_vod.py: no prompts,
stdin=DEVNULL on every child, safe under nohup / a server-spawned detached
Popen.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import db

APP_DIR = Path(__file__).resolve().parent.parent
WORKERS_DIR = APP_DIR / 'workers'

# Kept in sync with ingest_vods.VIDEO_EXTS by reading it, never by copying it
# (charter: never describe existing code from memory) -- ingest_vods.py is
# NOT on the concurrent-edit list this brief named, so importing this one
# stable constant from it is safe (unlike run_vod.py, deliberately not
# imported anywhere in this file -- see the module docstring).
try:
    from ingest_vods import VIDEO_EXTS
except Exception:  # pragma: no cover - only if ingest_vods cannot be imported
    VIDEO_EXTS = {'.mov', '.mp4', '.mkv'}

ENV_TO_CLIP_DIR = 'CLPR_TO_CLIP_DIR'
ENV_LOCAL_LOCK_FILE = 'CLPR_LOCAL_LOCK_FILE'
ENV_RUN_VOD_PATH = 'CLPR_RUN_VOD_PATH'
ENV_SSH_HOST = 'CLPR_SSH_HOST'  # same name review_server.py already uses -- one truth
ENV_N8N_CONTAINER = 'CLPR_N8N_CONTAINER'
ENV_PORTABLE_LOCK_PATH = 'CLPR_PORTABLE_LOCK_PATH'

DEFAULT_LOCAL_LOCK_FILE = Path.home() / 'Library' / 'Logs' / 'clpr-local-run.lock'
DEFAULT_RUN_VOD_PATH = WORKERS_DIR / 'run_vod.py'
DEFAULT_SSH_HOST = 'n8nserver'
DEFAULT_N8N_CONTAINER = '9c780efb632f'
DEFAULT_PORTABLE_LOCK_PATH = '/home/node/.n8n/clpr/run.lock'
PORTABLE_LOCK_FRESH_MINUTES = 30
SSH_TIMEOUT_S = 10
LOCAL_LOCK_GRACE_SECONDS = 5.0

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_USAGE = 2
EXIT_OBS_GATE = 3
EXIT_QUALITY_GATE = 4
EXIT_EXTRACT_REFUSED = 5
EXIT_LOCAL_LOCK_BUSY = 6
EXIT_PORTABLE_ENGINE_BUSY = 7

# run_vod.py's own exit-code vocabulary (app/workers/run_vod.py) -- read back
# here as PLAIN INTEGERS, never imported, so this file cannot break if that
# module's internals shift (see the coupling note in the module docstring).
RUN_VOD_EXIT_OK = 0
RUN_VOD_EXIT_FAIL = 1
RUN_VOD_EXIT_OBS_GATE = 3
RUN_VOD_EXIT_QUALITY_GATE = 4
RUN_VOD_EXIT_EXTRACT_REFUSED = 5


def say(msg: str) -> None:
    """Print immediately to stdout. A local run can run for hours; buffering
    is not an option (same reasoning as run_vod.say)."""
    print(msg, flush=True)


def utc_now_iso() -> str:
    """This project's timestamp format (ISO-8601 UTC, second precision, 'Z'
    suffix) -- same shape as ingest_vods.utc_now_iso and review_server's, a
    small enough helper that every module in this codebase defines its own
    rather than importing one (the existing project norm)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


# ---------------------------------------------------------------------------
# Step 1: resolve the newest candidate video in CLPR_TO_CLIP_DIR
# ---------------------------------------------------------------------------

def resolve_to_clip_dir() -> tuple[Optional[Path], Optional[str]]:
    """(dir, None) on success, (None, refusal message) otherwise. Refuses
    loudly, naming the env var, when it is unset; refuses when the directory
    it names does not exist."""
    raw = os.environ.get(ENV_TO_CLIP_DIR, '').strip()
    if not raw:
        return None, f'{ENV_TO_CLIP_DIR} is not set. Point it at the to_clip Drive-synced directory.'
    d = Path(raw)
    if not d.is_dir():
        return None, f'{ENV_TO_CLIP_DIR} names a directory that does not exist: {d}'
    return d, None


def find_newest_video(directory: Path) -> Optional[Path]:
    """The newest (by mtime) video file directly in `directory`, or None.

    Matches node 0b's semantics: exactly one candidate video is expected at
    a time in normal operation, so "pick the newest" is the whole rule --
    older files are silently ignored (not an error), and an empty directory
    is not an error either (see NO_NEW_VOD in main())."""
    candidates = [
        p for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() in VIDEO_EXTS
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ---------------------------------------------------------------------------
# Step 2: dedupe against the database (mirrors run_vod.lookup_recording /
# near_miss_paths -- app/workers/run_vod.py -- COPIED, not imported: that
# file is on the concurrent-edit do-not-touch list for this brief, so its
# function surface may shift before this lands)
# ---------------------------------------------------------------------------

def matching_recording_rows(video_path: str) -> list[tuple[int, str, str]]:
    """Rows whose path matches exactly, OR whose basename matches but the
    full path does not (a near-miss: a different mount point, a trailing
    slash, a renamed file -- the same signal run_vod.near_miss_paths reports
    diagnostically). Returns (id, path, state) tuples."""
    conn = db.connect()
    try:
        cur = conn.cursor()
        base = os.path.basename(video_path)
        cur.execute(
            'SELECT id, path, state FROM recordings WHERE path = %s OR path LIKE %s ORDER BY id',
            (video_path, '%' + base),
        )
        return [(int(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()]
    finally:
        conn.close()


def dedupe_skip_reason(video_path: str) -> Optional[str]:
    """None => this video is new, proceed. A string => the honest NO_NEW_VOD
    reason to report instead of running anything.

    DEDUPE RULE SHIPPED: the newest file is treated as ALREADY PROCESSED
    (skip) when a recordings row matches it (exact path, or a near-miss on
    basename) AND that row's state is anything OTHER than 'ingested'.
    state == 'ingested' means the row was only just registered and nothing
    further has run yet -- that is NOT treated as done; it falls through and
    this video is still handed to run_vod.py, which resumes it safely (its
    own docstring: "RESUMABLE AND IDEMPOTENT"). Only a row that has moved
    PAST 'ingested' (transcribed / detected / done) counts as "dealt with
    already" for this button's purpose -- re-running a fully- or
    partially-scored recording from scratch on every click would be wrong
    (and, for the `signal` stage, would re-spend real OpenRouter money for
    nothing new, per run_vod's own --force-respend guard).
    """
    rows = matching_recording_rows(video_path)
    done_rows = [r for r in rows if r[2] != 'ingested']
    if not done_rows:
        return None
    described = '; '.join(f'id={r[0]} path="{r[1]}" state={r[2]}' for r in done_rows)
    return f'the newest video is already processed: {described}'


# ---------------------------------------------------------------------------
# Step 3: this driver's own local lock (refuse a second overlapping local run)
# ---------------------------------------------------------------------------

def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else -- the safe direction is "alive"
    except OSError:
        return False
    return True


def acquire_local_lock(lock_path: Path) -> Optional[str]:
    """None => acquired; this process now owns the lock file. A non-None
    string => refusal reason; the lock was NOT acquired and the caller must
    not proceed.

    SHAPE mirrors the portable engine's own busy-check (fresh mtime), plus a
    live-pid check this local check can make that a remote check cannot (it
    runs on the same machine as the process it is checking). Correctness is
    carried by the PID check, not a time window: as long as the process that
    holds the lock (this one, for the run's full duration) stays alive, the
    pid check alone catches an overlap no matter how long the run takes, so
    a multi-hour transcription cannot go "stale" and let a second run start
    underneath it -- a pure freshness-window check would have exactly that
    bug. LOCAL_LOCK_GRACE_SECONDS is a narrow, non-load-bearing extra: it
    only matters in the sliver between the lock file being written and the
    OS reporting that pid as visible to os.kill.
    """
    if lock_path.exists():
        held_pid = -1
        held_started = '?'
        try:
            payload = json.loads(lock_path.read_text(encoding='utf-8'))
            held_pid = int(payload.get('pid', -1))
            held_started = str(payload.get('started_at', '?'))
        except Exception:
            pass
        age_s: Optional[float] = None
        try:
            age_s = time.time() - lock_path.stat().st_mtime
        except OSError:
            pass
        alive = held_pid > 0 and _pid_alive(held_pid)
        fresh = age_s is not None and age_s < LOCAL_LOCK_GRACE_SECONDS
        if alive or (held_pid <= 0 and fresh):
            return (f'another local find-clips run is already live (pid={held_pid}, '
                     f'started {held_started}). Wait for it to finish.')
        say(f'  local lock at {lock_path} is stale (pid={held_pid} not alive); breaking it.')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({'pid': os.getpid(), 'started_at': utc_now_iso()}),
        encoding='utf-8',
    )
    return None


def release_local_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Step 4: cross-engine busy check (operator-approved) -- is the PORTABLE
# (server/n8n) engine mid-run right now?
# ---------------------------------------------------------------------------

def portable_engine_busy() -> tuple[bool, str]:
    """(True, note) only on a DEFINITIVE non-empty-stdout signal that the
    portable engine's own run.lock is fresh. Anything else -- the file does
    not exist (find's own empty stdout + nonzero exit), the container is
    unreachable, ssh cannot connect, ssh times out, the ssh binary is
    missing -- returns (False, note), i.e. "proceed", with `note` carrying a
    WARN line for the transport-failure case and '' for the ordinary
    not-busy case. Never blocks local work on an unreachable server."""
    ssh_host = os.environ.get(ENV_SSH_HOST, DEFAULT_SSH_HOST)
    container = os.environ.get(ENV_N8N_CONTAINER, DEFAULT_N8N_CONTAINER)
    lock_path = os.environ.get(ENV_PORTABLE_LOCK_PATH, DEFAULT_PORTABLE_LOCK_PATH)
    remote_cmd = (
        f"docker exec {shlex.quote(container)} sh -lc "
        f"'find {shlex.quote(lock_path)} -mmin -{PORTABLE_LOCK_FRESH_MINUTES}'"
    )
    cmd = ['ssh', ssh_host, remote_cmd]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=SSH_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 -- any transport failure: WARN, never block
        return False, (f'WARN: could not reach {ssh_host} to check the portable engine '
                        f'({exc!r}); continuing with the local run.')
    # ssh's OWN connection-failure exit code is 255 (documented ssh(1)
    # convention), distinct from the REMOTE command's exit status passing
    # through unchanged on a successful connection -- `find` legitimately
    # exits 1 with empty stdout when run.lock does not exist, and that is
    # the ordinary not-busy case, not a transport failure. Only 255 means
    # ssh itself never reached the host.
    if proc.returncode == 255:
        detail = proc.stderr.strip() or f'ssh exited 255 with no stderr'
        return False, (f'WARN: could not reach {ssh_host} to check the portable engine '
                        f'({detail}); continuing with the local run.')
    if proc.stdout.strip():
        return True, (f'the portable engine (n8n, container {container}) is mid-run: '
                       f'{proc.stdout.strip()}')
    return False, ''


# ---------------------------------------------------------------------------
# Step 5: hand off to run_vod.py, streaming its output through verbatim
# ---------------------------------------------------------------------------

def run_vod_path() -> Path:
    raw = os.environ.get(ENV_RUN_VOD_PATH, '').strip()
    return Path(raw) if raw else DEFAULT_RUN_VOD_PATH


def run_run_vod(video: Path) -> int:
    """Exec run_vod.py --video <video> as a subprocess, streaming ITS stdout
    to this process's stdout and ITS stderr to this process's stderr live
    (same "no long quiet bout" reasoning as run_vod.run_child), and return
    its exit code UNCHANGED."""
    py = sys.executable or 'python3'
    cmd = [py, str(run_vod_path()), '--video', str(video)]
    say(f'  $ {" ".join(cmd)}')
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )

    def pump(stream, sink) -> None:
        for line in iter(stream.readline, ''):
            sink.write(line if line.endswith('\n') else line + '\n')
            sink.flush()
        stream.close()

    t_out = threading.Thread(target=pump, args=(proc.stdout, sys.stdout), daemon=True)
    t_err = threading.Thread(target=pump, args=(proc.stderr, sys.stderr), daemon=True)
    t_out.start()
    t_err.start()
    rc = proc.wait()
    t_out.join()
    t_err.join()
    return rc


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    say('=' * 78)
    say('find_clips_local -- LOCAL engine driver (operator default, 2026-08-09)')
    say('=' * 78)

    to_clip_dir, dir_problem = resolve_to_clip_dir()
    if dir_problem:
        print(f'ERROR: {dir_problem}', file=sys.stderr)
        say(f'RESULT find_clips_local ok=0 video="" run_vod_exit=none marker=USAGE_ERROR '
            f'note="{dir_problem}"')
        return EXIT_USAGE
    say(f'  to_clip dir    {to_clip_dir}')

    video = find_newest_video(to_clip_dir)
    if video is None:
        note = f'No new recording to process: {to_clip_dir} has no video files.'
        say(f'NO_NEW_VOD reason="{note}"')
        say(f'RESULT find_clips_local ok=1 video="" run_vod_exit=none marker=NO_NEW_VOD note="{note}"')
        return EXIT_OK
    say(f'  newest video   {video}  (mtime {time.ctime(video.stat().st_mtime)})')

    skip_reason = dedupe_skip_reason(str(video))
    if skip_reason:
        note = skip_reason[:1].upper() + skip_reason[1:] + '.'
        say(f'NO_NEW_VOD reason="{skip_reason}"')
        say(f'RESULT find_clips_local ok=1 video="{video}" run_vod_exit=none marker=NO_NEW_VOD '
            f'note="{note}"')
        return EXIT_OK

    lock_path = Path(os.environ.get(ENV_LOCAL_LOCK_FILE, str(DEFAULT_LOCAL_LOCK_FILE)))
    refusal = acquire_local_lock(lock_path)
    if refusal:
        print(f'ERROR: {refusal}', file=sys.stderr)
        say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit=none marker=LOCAL_LOCK_BUSY '
            f'note="{refusal}"')
        return EXIT_LOCAL_LOCK_BUSY

    try:
        busy, note = portable_engine_busy()
        if busy:
            print(f'ERROR: {note}', file=sys.stderr)
            say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit=none '
                f'marker=PORTABLE_ENGINE_BUSY note="{note}"')
            return EXIT_PORTABLE_ENGINE_BUSY
        if note:
            say(f'  WARN  {note}')

        rc = run_run_vod(video)

        if rc == RUN_VOD_EXIT_OK:
            say(f'Local engine run complete for {video.name}.')
            say(f'RESULT find_clips_local ok=1 video="{video}" run_vod_exit=0 marker=none '
                f'note="Local engine run complete."')
            return EXIT_OK

        if rc == RUN_VOD_EXIT_OBS_GATE:
            note = ('OBS is live, so the local engine refused to run (D-009/D-019). Try again '
                     'once you are off air; nothing was lost, re-running is safe.')
            say(f'OBS_GUARD_REFUSED: {note}')
            say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit=3 marker=OBS_GUARD_REFUSED '
                f'note="{note}"')
            return EXIT_OBS_GATE

        if rc == RUN_VOD_EXIT_QUALITY_GATE:
            note = ('The transcript for this recording is unusable (D-028), so detection was '
                     'refused on purpose. The transcript itself was kept; nothing downstream ran.')
            say(f'QUALITY_GATE_FAILED: {note}')
            say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit=4 marker=QUALITY_GATE_FAILED '
                f'note="{note}"')
            return EXIT_QUALITY_GATE

        if rc == RUN_VOD_EXIT_EXTRACT_REFUSED:
            note = 'This recording has no usable audio, so the local engine refused to run.'
            say(f'AUDIO_GUARD_REFUSED: {note}')
            say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit=5 marker=AUDIO_GUARD_REFUSED '
                f'note="{note}"')
            return EXIT_EXTRACT_REFUSED

        note = f'The local engine run failed (run_vod exit={rc}). See the log above for the real error.'
        say(note)
        say(f'RESULT find_clips_local ok=0 video="{video}" run_vod_exit={rc} marker=none note="{note}"')
        return EXIT_FAIL
    finally:
        release_local_lock(lock_path)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('ERROR: interrupted', file=sys.stderr)
        sys.exit(EXIT_FAIL)
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(EXIT_FAIL)
