#!/usr/bin/env python3
"""run_vod: drive ONE VOD through the whole local clip-detection pipeline, then STOP.

Why this exists
---------------
34 hours of VOD across 15 real streams are about to be re-run through a freshly
wiped database, ONE AT A TIME, earliest first, with the operator validating each
one before the next begins. The only orchestrator today lives on a stale branch
and predates the PostgreSQL port. This is the driver: one invocation, one VOD, a
report, and exit. It NEVER advances to another VOD.

The pipeline, in the order the workers require:

    1 ingest       workers/ingest_vods.py        register the recording
    2 audio_guard  scripts/extract_audio.sh      silence/no-audio check ONLY — no persisted
                                                  artifact (D-074 ruling 9 / LO-03, see below)
    3 transcribe   workers/transcribe.py         whisper.cpp            [OBS-gated]
    4 quality      workers/quality_gate.py       D-028 transcript gate
    5 zebra        workers/zebra_detect.py       trigger-word beats
    6 energy       workers/audio_energy.py       ebur128 buckets        [OBS-gated]
    7 signal       workers/transcript_signal.py  LLM signal candidates  [costs money]
    8 fusion       workers/score_fusion.py       final clip candidates

Guarantees this driver provides
-------------------------------
* IT STOPS. One VOD per invocation. There is no loop over recordings anywhere
  in this file.
* RESUMABLE AND IDEMPOTENT, WITH ONE NAMED EXCEPTION. Every stage has a
  done-check against the database — except `audio_guard`, which by design has
  no persisted witness to check (its output is discarded every run; see the
  LO-03 note below) and therefore always re-runs. Every other stage's work
  already done is SKIPPED and said so out loud. A 4-to-8 hour transcription is
  never repeated because a later stage failed.
* NOTHING PARTIAL. This driver writes ZERO rows itself; it only SELECTs. Every
  write happens inside a worker's own transaction, and every worker rolls back
  on failure (verified by reading them). A failed stage stops the run before any
  downstream stage can build on it, so candidates can never come from a
  half-transcribed VOD.
* THE OBS GATE IS SURFACED AS A GATE, NOT A CRASH. transcribe and audio_energy
  enforce D-009/D-019 themselves; a refusal is reported as "not a failure, come
  back when off air" with its own exit code.
* EVERY STAGE IS TIMED, and transcription is reported as a realtime multiple —
  the number that decides whether the 34-hour plan is feasible on this Mac.

AUDIO_GUARD — WHY IT DISCARDS ITS OWN OUTPUT (D-074 ruling 9 / LO-03)
----------------------------------------------------------------------
Before this fix, stage 2 was called `extract` and its .m4a was kept on disk
as a persisted artifact, checked on the next invocation to decide whether to
skip. That .m4a is consumed by NOTHING downstream: transcribe.py and
audio_energy.py both ffmpeg the SOURCE VIDEO directly, never the .m4a. 15
already-completed VODs each paid a full extract-and-verify pass (including,
on some codecs, a real aac re-encode) for a file nobody ever reads.

The valuable HALF of that stage is not the file, it is the GUARD: the
silence/no-audio refusal that catches a vod-6-shaped failure (a whole
session that recorded with no live audio) before hours are sunk into
transcribing it. That refusal logic lives inside extract_audio.sh's own
volumedetect passes, and extract_audio.sh has no check-only flag to run
that logic without also producing and verifying an output file (adding one
is out of scope here: this fix touches run_vod.py only). So the honest
choice, and the one this driver takes: run the script exactly as before
(same command, same cost), then immediately DELETE the .m4a it produced.
The refusal path (`is_extract_refusal`, exit code 5 below) is unaffected —
it is driven off stderr, not off the file — so a genuinely silent or
audio-less recording is refused exactly as before. Because nothing is left
on disk to check, `audio_guard` has no done-check and always re-runs (see
the guarantees section above); this is a deliberate, named exception, not
an oversight.

SIGNAL STAGE TRANSPORT — LOCAL vs PORTABLE LANE (D-074 ruling 10 / LO-06)
--------------------------------------------------------------------------
This driver's `signal` stage runs the LOCAL lane exclusively: it invokes
workers/transcript_signal.py, which calls OpenRouter directly via a
blocking `curl` subprocess, one call per chunk/trigger
(call_claude_structured), decoding each response's JSON body in the same
Python process immediately before issuing the next call.

A SEPARATE, portable lane exists for the live n8n workflow and is NEVER
invoked by this driver: workers/transcript_signal_prepare.py emits every
prompt this VOD needs as one JSON document up front (no API calls, no DB
writes); n8n's own LLM Chain node performs the actual HTTP calls
server-side, on its own transport, retry policy, and response-decoding
path; workers/transcript_signal_ingest.py then validates and lands
whatever responses it is handed back.

This divergence is confined to TRANSPORT — who calls the LLM API, over
what protocol, and how the raw text is unwrapped into JSON — and never
touches CANDIDATE-SELECTION LOGIC: both lanes build prompts from the
identical iterators (iter_scan_items / iter_zebra_items, defined once in
transcript_signal.py) and validate/normalize responses with the identical
functions (normalize_window, the category whitelist, the confidence
clamp, the cross-chunk dedup — transcript_signal_ingest.py imports these
from transcript_signal.py rather than reimplementing them). It is
acceptable because the two lanes never run against the same recording in
the same invocation: this driver (run_vod.py) only ever drives the local
lane, on this Mac; the split lane runs only inside the n8n workflow,
server-side, and this driver never touches it. A transport bug confined to
one lane (a different default model, a different retry count) could only
ever produce a discrepancy WITHIN that lane — it cannot silently diverge
the two lanes' candidate-selection algorithm, because that algorithm is
one shared module.

Non-interactive by construction: there is no prompt anywhere in this file and
every child is spawned with stdin=DEVNULL, so it is safe under nohup, in a
background shell, or piped to tee. That is why there is no --yes flag: a flag
that disables prompts that do not exist would be a lie about the design.

Conventions: stdlib + psycopg2 via app/workers/db.py, argparse, RESULT line
last, ERROR to stderr, honest exit codes.

Exit codes
----------
    0  pipeline complete (or nothing left to do)
    1  a stage failed, or preflight failed
    2  usage / bad arguments (argparse) — INCLUDING --force covering `signal`
       without --force-respend (D-074 ruling 2 / LO-07, see parse_args())
    3  OBS gate refusal — NOT a failure; come back when off air
    4  D-028 quality gate FAILED — the transcript is unusable
    5  audio_guard REFUSED (extract_audio.sh) — no audio, or every audio
       stream is silent
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import db
import slice_candidates
import stage_slices_remote

APP_DIR = Path(__file__).resolve().parent.parent
WORKERS_DIR = APP_DIR / 'workers'
SCRIPTS_DIR = APP_DIR / 'scripts'
ENV_FILE = APP_DIR / '.env'

# Kept in sync with ingest_vods.VIDEO_ROOT by reading it, never by copying it
# (charter: never describe existing code from memory).
try:
    from ingest_vods import VIDEO_ROOT as INGEST_VIDEO_ROOT
except Exception:  # pragma: no cover - only if ingest_vods cannot be imported
    INGEST_VIDEO_ROOT = None

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_OBS_GATE = 3
EXIT_QUALITY_GATE = 4
EXIT_EXTRACT_REFUSED = 5

STAGE_ORDER = [
    'ingest',
    'audio_guard',
    'transcribe',
    'quality',
    'zebra',
    'energy',
    'signal',
    'fusion',
    'slice',
    'push',
]

STAGE_BLURB = {
    'ingest': 'register the recording (ingest_vods.py)',
    'audio_guard': 'silence/no-audio check (extract_audio.sh) — output discarded, LO-03',
    'transcribe': 'whisper.cpp transcription (transcribe.py) [OBS-gated, the long one]',
    'quality': 'D-028 transcript quality gate (quality_gate.py)',
    'zebra': 'zebra trigger-word beats (zebra_detect.py)',
    'energy': 'ebur128 5s loudness buckets (audio_energy.py) [OBS-gated]',
    'signal': 'LLM signal candidates (transcript_signal.py) [costs OpenRouter money]',
    'fusion': 'score fusion into clip candidates (score_fusion.py)',
    'slice': 'stream-copy per-candidate slices for the server lane (slice_candidates.py)',
    'push': 'push staged slices+sidecars to the n8n server (stage_slices_remote.py) — a '
            'failed push is a loud stage failure, not a skip: the server drain needs '
            'these slices to generate post kits',
}


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def say(msg: str) -> None:
    """Print immediately. He watches this for hours; buffering is not an option."""
    print(msg, flush=True)


def hms(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    if h:
        return f'{h}h{m:02d}m{s:04.1f}s'
    if m:
        return f'{m}m{s:04.1f}s'
    return f'{s:.1f}s'


def load_env_file(path: Path) -> tuple[list[str], list[str]]:
    """Load KEY=VALUE lines from .env, SET-IF-ABSENT ONLY.

    The existing environment ALWAYS wins. This matters more than it looks:
    app/.env carries a CLPR_DB_URL pointing at the LIVE database, and this
    driver is expected to run under app/scripts/pg_test_harness.sh, which
    exports a throwaway CLPR_DB_URL. If .env could overwrite it, every worker
    in a "safe" harness run would write to the live database instead.

    Returns (names_loaded, names_already_set). Never returns or prints a value.
    """
    loaded: list[str] = []
    already: list[str] = []
    if not path.exists():
        return loaded, already

    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        name, value = line.split('=', 1)
        name = name.strip()
        if not name:
            continue
        value = value.strip().strip('"').strip("'")
        if os.environ.get(name, '').strip():
            already.append(name)
            continue
        os.environ[name] = value
        loaded.append(name)
    return loaded, already


class StageFailure(Exception):
    """A stage failed. `exit_code` carries the classified reason.

    LO-10 (golden-review F21): `stage` names WHICH of the 8 STAGE_ORDER
    entries failed, so the final RESULT line can report the stage NAME
    (failed_stage=transcribe) rather than only the classified exit code
    (an integer that collapses several different stages onto the same
    EXIT_FAIL=1).
    """

    def __init__(self, message: str, exit_code: int = EXIT_FAIL, stage: str = ''):
        super().__init__(message)
        self.exit_code = exit_code
        self.stage = stage


class StageResult:
    def __init__(self, name: str):
        self.name = name
        self.status = 'pending'   # ran | skipped | not-run | failed
        self.detail = ''
        self.elapsed_s = 0.0
        self.result_line = ''
        self.stdout_lines: list[str] = []
        self.stderr_lines: list[str] = []


def run_child(label: str, cmd: list[str], env: Optional[dict] = None) -> tuple[int, list[str], list[str]]:
    """Run a child, STREAMING its output live, and return (rc, stdout, stderr).

    Live streaming is not a nicety. A silent multi-hour gap is the "long quiet
    bout" failure by construction: the operator cannot tell a working whisper
    from a hung one. Every line is relayed the moment it arrives, prefixed with
    the stage so a long log stays readable, and retained for RESULT parsing.

    stdin is DEVNULL on every child: ffmpeg without -nostdin swallows keystrokes
    from the terminal the operator is watching.

    env=None (every existing stage) inherits this process's environment
    unchanged, exactly as before. Only the `slice` stage passes a real dict
    (a copy of os.environ with CLPR_SLICES_DIR set) — see main()'s stage
    loop — so slice_candidates.py stages into the same directory `push`
    (stage_slices_remote.py, which computes that directory itself) will
    look for it in.
    """
    say(f'    $ {" ".join(cmd)}')
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        env=env,
    )

    out_lines: list[str] = []
    err_lines: list[str] = []

    def pump(stream, sink: list[str], prefix: str) -> None:
        for line in iter(stream.readline, ''):
            line = line.rstrip('\n')
            sink.append(line)
            say(f'    [{label}{prefix}] {line}')
        stream.close()

    t_out = threading.Thread(target=pump, args=(proc.stdout, out_lines, ''), daemon=True)
    t_err = threading.Thread(target=pump, args=(proc.stderr, err_lines, ':err'), daemon=True)
    t_out.start()
    t_err.start()
    rc = proc.wait()
    t_out.join()
    t_err.join()
    return rc, out_lines, err_lines


def find_result_line(lines: list[str]) -> str:
    for line in reversed(lines):
        if line.startswith('RESULT '):
            return line
    return ''


def parse_kv(result_line: str) -> dict[str, str]:
    """Parse `key=value` tokens out of a RESULT line (quoted values kept whole)."""
    out: dict[str, str] = {}
    for token in result_line.split():
        if '=' not in token:
            continue
        k, v = token.split('=', 1)
        out[k] = v.strip('"')
    return out


def is_obs_refusal(err_lines: list[str]) -> bool:
    """Match obs_guard's own refusal wording (obs_guard.require_obs_idle_or_raise)."""
    blob = '\n'.join(err_lines)
    return 'OBS is actively' in blob and 'Refusing' in blob


def is_extract_refusal(err_lines: list[str]) -> bool:
    return any(line.startswith('REFUSED:') for line in err_lines)


# ---------------------------------------------------------------------------
# database reads (this driver NEVER writes)
# ---------------------------------------------------------------------------

def db_scalar(sql: str, params: tuple) -> Optional[object]:
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()


def db_rows(sql: str, params: tuple) -> list[tuple]:
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def lookup_recording(video_path: str) -> Optional[tuple[int, float, str]]:
    rows = db_rows(
        'SELECT id, duration_s, state FROM recordings WHERE path = %s',
        (video_path,),
    )
    if not rows:
        return None
    rid, duration_s, state = rows[0]
    return int(rid), (0.0 if duration_s is None else float(duration_s)), str(state)


def near_miss_paths(video_path: str) -> list[str]:
    """Rows whose basename matches but whose full path does not.

    A path mismatch (trailing slash, /Volumes vs symlink, a renamed file) is the
    most likely reason a lookup misses, and a bare 'not found' is undebuggable.
    """
    base = os.path.basename(video_path)
    rows = db_rows(
        'SELECT path FROM recordings WHERE path LIKE %s AND path <> %s ORDER BY path',
        ('%' + base, video_path),
    )
    return [str(r[0]) for r in rows]


def count_for(table: str, recording_id: int, extra_sql: str = '') -> int:
    sql = f'SELECT COUNT(*) FROM {table} WHERE recording_id = %s {extra_sql}'
    return int(db_scalar(sql, (recording_id,)) or 0)


def recording_state(recording_id: int) -> Optional[str]:
    """The authoritative done-witness for stages gated on state, not row count.

    A row count cannot distinguish "genuinely zero segments" from "never ran" —
    both are 0. The state column is written inside the SAME transaction as the
    stage's real work (transcribe.persist_segments sets state='transcribed' in
    the same commit as the segment INSERT — verified by reading transcribe.py),
    so it is a witness a partial or failed run cannot forge (golden-review F15).
    """
    val = db_scalar('SELECT state FROM recordings WHERE id = %s', (recording_id,))
    return None if val is None else str(val)


# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------

def preflight(video: str, want_stages: list[str]) -> list[str]:
    """Check EVERYTHING before stage 1. Returns the list of problems found.

    A missing OPENROUTER_API_KEY discovered after a six-hour transcription is
    exactly the failure this driver exists to prevent, so every requirement of
    every stage that is going to run is checked up front.
    """
    problems: list[str] = []

    # --- the input file -----------------------------------------------------
    try:
        st = os.stat(video)
        say(f'  input          OK    {video} ({st.st_size / 1048576:.1f} MB)')
    except OSError as exc:
        problems.append(f'input file not readable: {video} -> {exc}')
        say(f'  input          FAIL  {video} -> {exc}')

    # --- external binaries --------------------------------------------------
    for binary in ('ffmpeg', 'ffprobe'):
        found = shutil.which(binary)
        if found:
            say(f'  {binary:<14} OK    {found}')
        else:
            problems.append(f'{binary} not found on PATH')
            say(f'  {binary:<14} FAIL  not on PATH')

    # --- database -----------------------------------------------------------
    url = os.environ.get('CLPR_DB_URL', '').strip()
    if not url:
        problems.append('CLPR_DB_URL is not set')
        say('  CLPR_DB_URL    FAIL  not set')
    else:
        try:
            one = db_scalar('SELECT 1', ())
            if one != 1:
                raise RuntimeError(f'SELECT 1 returned {one!r}')
            say('  database       OK    connected, SELECT 1 = 1')
        except Exception as exc:
            problems.append(f'database connection failed: {exc}')
            say(f'  database       FAIL  {exc}')

    # --- whisper (only if transcription is actually going to run) -----------
    if 'transcribe' in want_stages:
        for name in ('WHISPER_CLI_PATH', 'WHISPER_MODEL_PATH'):
            value = os.environ.get(name, '').strip()
            if not value:
                problems.append(f'{name} is not set (required by transcribe.py)')
                say(f'  {name:<14} FAIL  not set')
                continue
            p = Path(value)
            if not p.exists():
                problems.append(f'{name} points at a missing file: {value}')
                say(f'  {name:<14} FAIL  missing file {value}')
            elif name == 'WHISPER_CLI_PATH' and not os.access(value, os.X_OK):
                problems.append(f'{name} is not executable: {value}')
                say(f'  {name:<14} FAIL  not executable {value}')
            else:
                say(f'  {name:<14} OK    {value}')

    # --- OpenRouter (only if the LLM stage is actually going to run) --------
    if 'signal' in want_stages:
        if os.environ.get('OPENROUTER_API_KEY', '').strip():
            say('  OPENROUTER_API_KEY OK  set (value never printed)')
        else:
            problems.append('OPENROUTER_API_KEY is not set (required by transcript_signal.py)')
            say('  OPENROUTER_API_KEY FAIL  not set')

    # --- obsws-python (only if an OBS-gated stage is actually going to run) -
    # transcribe.py and audio_energy.py each call
    # obs_guard.require_obs_idle_or_raise, which imports obsws_python BEFORE
    # it ever probes the OBS port (read obs_guard.py: the import happens
    # first, the port check second) -- so a missing module previously
    # surfaced only once that stage's subprocess was already running, as a
    # generic stage failure rather than a named missing-dependency refusal
    # here at preflight. Skipped when CLPR_ALLOW_DURING_STREAM=1, exactly as
    # obs_guard.py itself skips the import in that case (same override, same
    # env var, checked first there too).
    obs_gated_stages = [s for s in ('transcribe', 'energy') if s in want_stages]
    if obs_gated_stages and os.environ.get('CLPR_ALLOW_DURING_STREAM') != '1':
        try:
            import obsws_python  # noqa: F401
        except ModuleNotFoundError as exc:
            problems.append(
                'obsws-python is not installed, required by obs_guard for the D-009 OBS gate '
                f'ahead of {"/".join(obs_gated_stages)}: {exc}. Install it with: '
                'python3 -m pip install obsws-python. If this run is intentional, set '
                'CLPR_ALLOW_DURING_STREAM=1 to override D-009.'
            )
            say(f'  obsws_python   FAIL  not installed (needed by {"/".join(obs_gated_stages)})')
        else:
            say('  obsws_python   OK    importable')

    # --- bash (only if audio_guard is actually going to run) ----------------
    # stage_command() invokes extract_audio.sh via ['bash', ...]; a missing
    # bash on PATH would otherwise surface as a generic stage-launch failure
    # rather than a named missing-dependency refusal here.
    if 'audio_guard' in want_stages:
        found = shutil.which('bash')
        if found:
            say(f'  bash           OK    {found}')
        else:
            problems.append('bash not found on PATH (required to run extract_audio.sh)')
            say('  bash           FAIL  not on PATH')

    return problems


# ---------------------------------------------------------------------------
# stage done-checks (the resumability contract)
# ---------------------------------------------------------------------------

def audio_artifact_path(video: str) -> Path:
    """The path extract_audio.sh writes its .m4a to — used ONLY to unlink it.

    Before D-074 ruling 9 (LO-03) this path was also the `extract` stage's
    persisted done-witness (checked for existence/size/duration on the next
    invocation). That witness is gone by design: `audio_guard` now deletes
    this file immediately after every run (nothing downstream reads it — see
    the LO-03 note in the module docstring), so there is nothing here to
    check on a later invocation. This helper survives only so the post-run
    unlink in main() and extract_audio.sh's own layout agree on the path.
    """
    p = Path(video)
    return p.with_suffix('.m4a')


def stage_done(stage: str, recording_id: Optional[int], video: str, duration_s: float) -> tuple[bool, str]:
    """Return (already_done, human reason). Cheap stages are never 'done'."""
    if stage == 'ingest':
        if recording_id is None:
            return False, 'not registered yet'
        return True, f'already registered as recording id={recording_id}'

    if recording_id is None:
        return False, 'recording not registered yet'

    if stage == 'audio_guard':
        # D-074 ruling 9 / LO-03: this stage's own output (.m4a) is deleted
        # immediately after every run because nothing downstream reads it, so
        # there is no persisted witness left to check for a skip.
        # extract_audio.sh has no check-only flag (adding one is out of scope
        # for this driver), so the honest choice is to always re-run the full
        # script and discard its output afterward — never to fake a skip off
        # a file that no longer exists by design.
        return False, 'no persisted witness by design (output discarded each run) — always re-run'

    if stage == 'transcribe':
        # Gated on recordings.state, NOT on transcript_segments row count: a
        # legitimately zero-segment transcript (silent VOD) is done, and a
        # count-based check would re-run a 4-to-8 hour whisper pass forever on
        # exactly that recording (golden-review F15). See recording_state()
        # for why this witness cannot be forged by a partial run.
        state = recording_state(recording_id)
        n = count_for('transcript_segments', recording_id)
        if state in ('transcribed', 'detected', 'done'):
            return True, f'{n} transcript segments, state={state} (done-check gates on state, not row count)'
        return False, f'{n} transcript segments, state={state or "?"} — not transcribed yet'

    if stage in ('quality', 'zebra'):
        # Deliberately never skipped. quality_gate is a read-only verdict that
        # belongs in every report; zebra_detect is a cheap regex scan whose
        # writes are guarded by its own beat_exists() dedupe. Re-running both
        # costs a second and keeps the report honest.
        return False, 'cheap and idempotent, always re-run'

    if stage == 'energy':
        n = count_for('audio_energy_buckets', recording_id)
        if n > 0:
            return True, f'{n} loudness buckets already present'
        return False, 'no loudness buckets'

    if stage == 'signal':
        n = count_for('llm_signal_candidates', recording_id)
        if n > 0:
            return True, f'{n} LLM signal candidates already present (re-running would spend money again)'
        return False, 'no LLM signal candidates'

    if stage == 'fusion':
        n = count_for('clip_candidates', recording_id)
        if n > 0:
            return True, f'{n} clip candidates already present'
        return False, 'no clip candidates'

    if stage == 'slice':
        ids = slice_candidate_ids(recording_id)
        if not ids:
            return True, 'no candidates in slice-eligible states (candidate/maybe/approved) — nothing to stage'
        staging = slice_staging_dir(recording_id)
        missing = [cid for cid in ids if not candidate_has_valid_staged_slice(staging, cid, video)]
        if missing:
            return False, (
                f'{len(missing)}/{len(ids)} slice-eligible candidate(s) missing a valid, '
                f'source-matched staged slice+sidecar under {staging}: {missing}'
            )
        return True, f'all {len(ids)} slice-eligible candidates already staged (slice+sidecar) at {staging}'

    if stage == 'push':
        # stage_slices_remote.py already does its own per-file remote verify
        # (size + ownership, its own module docstring step 5) and its own
        # idempotent size-compare skip on mp4s (sidecars always resend,
        # ~200 bytes each) on EVERY invocation — duplicating a remote-side
        # done-check here would be a second, unverified copy of exactly what
        # that script already proves for real. Honest choice, same shape as
        # quality/zebra above: always re-run. It is cheap (a fully-up-to-date
        # push is all remote-size skips plus tiny sidecar resends) and the
        # server drain needs the push's OWN verify to have actually run, not
        # a local guess that it would agree.
        return False, ('stage_slices_remote does its own remote verify + idempotent '
                        'size-compare skip on every run; always re-run (cheap)')

    raise ValueError(f'unknown stage: {stage}')


def slice_candidate_ids(recording_id: int) -> list[int]:
    """clip_candidates in a slice-eligible state for this recording, taken
    from stage_slices_remote.SLICE_STATES BY REFERENCE (itself a re-export of
    slice_candidates.SLICE_STATES) so this driver's done-check can never
    disagree with which candidates the stagers themselves will act on."""
    rows = db_rows(
        'SELECT id FROM clip_candidates WHERE recording_id = %s AND state IN %s ORDER BY id',
        (recording_id, stage_slices_remote.SLICE_STATES),
    )
    return [int(r[0]) for r in rows]


def slice_staging_dir(recording_id: int):
    """The exact local staging directory the portable lane's own stager uses
    (stage_slices_remote.staging_dir_for), reused BY REFERENCE — never a
    second copy of that <base>/<recording_id> formula — so `slice` and `push`
    always agree on where staged slices live."""
    return stage_slices_remote.staging_dir_for(recording_id)


def candidate_has_valid_staged_slice(staging_dir, candidate_id: int, video: str) -> bool:
    """Is candidate_id already staged at `staging_dir` with a slice+sidecar
    slice_candidates.py would itself SKIP_EXISTING rather than restage?

    Replicates slice_candidates.run()'s own skip predicate exactly (mp4
    present and non-empty; sidecar present and schema/candidate/coordinate
    valid via slice_candidates.load_valid_sidecar; AND the sidecar's SOURCE
    WITNESS — source_path/source_size_bytes — matches the CURRENT video) —
    not a weaker approximation of it. Skipping the source-witness check would
    accept a valid-but-WRONG-video sidecar (this happened live: recording 19
    was repointed to a different local file, commit 36c6e91) as "already
    staged", so this stage would report done, slice_candidates would never
    get the chance to RESTAGE it, and `push` would then ship a slice cut from
    the wrong source video as verified-good.
    """
    mp4 = staging_dir / f'c{candidate_id}.mp4'
    if not mp4.is_file() or mp4.stat().st_size == 0:
        return False
    sidecar_data = slice_candidates.load_valid_sidecar(
        slice_candidates.sidecar_path_for(mp4), candidate_id
    )
    if sidecar_data is None:
        return False
    try:
        video_size_bytes = os.stat(video).st_size
    except OSError:
        return False
    return (
        sidecar_data.get('source_path') == str(video)
        and sidecar_data.get('source_size_bytes') == video_size_bytes
    )


# ---------------------------------------------------------------------------
# stage runners
# ---------------------------------------------------------------------------

def stage_command(stage: str, video: str, recording_id: Optional[int]) -> list[str]:
    py = sys.executable or 'python3'
    if stage == 'ingest':
        return [py, str(WORKERS_DIR / 'ingest_vods.py')]
    if stage == 'audio_guard':
        return ['bash', str(SCRIPTS_DIR / 'extract_audio.sh'), video]
    if stage == 'slice':
        # The exact invocation the portable lane's own stager uses
        # (stage_slices_remote.run_slice_candidates): slice_candidates.py
        # --vod-id <id> --video <path>, with CLPR_SLICES_DIR pointed at
        # stage_slices_remote.staging_dir_for(recording_id) — see the env=
        # built for this stage in main()'s stage loop below, so `push`
        # (stage_slices_remote.py itself) finds these slices already staged.
        return [py, str(WORKERS_DIR / 'slice_candidates.py'),
                '--vod-id', str(recording_id), '--video', video]
    if stage == 'push':
        # Reused as-is (stage_slices_remote.py's own CLI uses --recording-id,
        # not --vod-id like every sibling worker here).
        return [py, str(WORKERS_DIR / 'stage_slices_remote.py'),
                '--recording-id', str(recording_id), '--video', video]
    worker = {
        'transcribe': 'transcribe.py',
        'quality': 'quality_gate.py',
        'zebra': 'zebra_detect.py',
        'energy': 'audio_energy.py',
        'signal': 'transcript_signal.py',
        'fusion': 'score_fusion.py',
    }[stage]
    return [py, str(WORKERS_DIR / worker), '--vod-id', str(recording_id)]


def require_registered(stage: str, recording_id: Optional[int], video: str) -> None:
    """Every stage but ingest needs a real recording id.

    Passing recording_id=None through stage_command() produces the literal
    string 'None' as --vod-id (golden-review F16a: reachable via --from,
    --skip ingest, or --only on a later stage) instead of failing loudly
    here, before a child is even spawned with garbage arguments.
    """
    if stage == 'ingest' or recording_id is not None:
        return
    raise StageFailure(
        f'stage {stage}: no recording is registered for "{video}" yet, so this stage cannot '
        'run — there is no recording id to pass it. Register it first with `--only ingest` '
        '(or run the pipeline from the start). If you expected this video to already be '
        'registered, check near_miss_paths() / the NOTE printed above for a path mismatch '
        '(trailing slash, /Volumes vs symlink, a renamed file).',
        EXIT_FAIL,
        stage=stage,
    )


def execute_stage(stage: str, cmd: list[str], res: StageResult, env: Optional[dict] = None) -> None:
    started = time.monotonic()
    rc, out_lines, err_lines = run_child(stage, cmd, env=env)
    res.elapsed_s = time.monotonic() - started
    res.stdout_lines = out_lines
    res.stderr_lines = err_lines
    res.result_line = find_result_line(out_lines) or find_result_line(err_lines)

    if rc == 0:
        res.status = 'ran'
        return

    res.status = 'failed'

    # Three distinct non-success presentations, because they mean three
    # completely different things to the operator.
    if is_obs_refusal(err_lines):
        raise StageFailure(
            f'stage {stage}: OBS GATE REFUSAL (D-009/D-019). This is NOT a failure and '
            'nothing is broken: compute during streaming crashed the machine once, so the '
            'worker refuses while OBS is live. Come back when you are off air and re-run '
            'the exact same command; everything already done will be skipped.',
            EXIT_OBS_GATE,
            stage=stage,
        )

    if stage == 'quality' and res.result_line.startswith('RESULT quality_gate') and ' FAIL ' in res.result_line:
        raise StageFailure(
            f'stage {stage}: D-028 QUALITY GATE FAILED (exit {rc}). The transcript is '
            'unusable, so detection is refused on purpose — scoring garbage segments would '
            'produce garbage candidates. The transcript is left in the database (a 4-to-8 '
            'hour job is not thrown away); the gate stops everything downstream of it. '
            'The gate\'s own guidance is quoted verbatim above.',
            EXIT_QUALITY_GATE,
            stage=stage,
        )
    # A non-zero exit from `quality` WITHOUT a genuine quality_gate RESULT/FAIL
    # verdict line (e.g. an argparse usage error, an unhandled crash before the
    # gate ever ran) is NOT "the transcript is unusable" — asserting that would
    # be a false D-028 verdict (golden-review F16b). Falls through to the
    # generic branch below, with the real stderr tail.

    if stage == 'audio_guard' and is_extract_refusal(err_lines):
        raise StageFailure(
            f'stage {stage}: extract_audio.sh REFUSED (exit {rc}). This is the vod-6 '
            'signature (D-028): the recording has no audio, or every audio stream is '
            'digitally silent. Nothing was extracted and nothing was written. Check the '
            'recorder\'s audio source. Its refusal text is quoted verbatim above.',
            EXIT_EXTRACT_REFUSED,
            stage=stage,
        )

    tail = err_lines[-1] if err_lines else (out_lines[-1] if out_lines else '(no output)')
    raise StageFailure(f'stage {stage}: exit {rc}. Last output line: {tail}', EXIT_FAIL, stage=stage)


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

def score_distribution(scores: list[float]) -> list[str]:
    """Describe the SHAPE of the ranking, not just its size.

    A bare candidate count cannot tell the operator whether the ranking
    separates anything. Today every score clustered in a narrow band because
    only one of three signals was populated, and a count of N looked identical
    to a healthy run. So: spread, quartiles, and a histogram.
    """
    lines: list[str] = []
    if not scores:
        lines.append('    (no candidates, so no distribution)')
        return lines

    lo = min(scores)
    hi = max(scores)
    spread = hi - lo
    ordered = sorted(scores)
    if len(ordered) >= 4:
        q = statistics.quantiles(ordered, n=4, method='inclusive')
        q1, q2, q3 = q[0], q[1], q[2]
    else:
        q1 = q2 = q3 = statistics.median(ordered)

    lines.append(f'    min {lo:.3f}   p25 {q1:.3f}   median {q2:.3f}   p75 {q3:.3f}   max {hi:.3f}')
    lines.append(f'    spread {spread:.3f} over {len(scores)} candidates, {len(set(round(s, 3) for s in scores))} distinct scores')
    if spread < 0.05:
        lines.append('    WARNING: the scores span less than 0.05. This ranking separates almost')
        lines.append('             nothing — check the per-signal contribution below before you')
        lines.append('             trust the ordering.')

    bins: dict[int, int] = {}
    for s in scores:
        idx = min(19, max(0, int(s / 0.05)))
        bins[idx] = bins.get(idx, 0) + 1
    widest = max(bins.values())
    lines.append('    histogram (0.05 bins):')
    for idx in sorted(bins):
        bar = '#' * max(1, int(round(bins[idx] * 40 / widest)))
        lines.append(f'      {idx * 0.05:.2f}-{(idx + 1) * 0.05:.2f}  {bins[idx]:>4}  {bar}')
    return lines


def next_vod_hint(video: str) -> str:
    """The next VOD, earliest first, from the same directory — when it is readable."""
    try:
        here = Path(video).resolve()
        siblings = sorted(
            p for p in here.parent.iterdir()
            if p.is_file() and p.suffix.lower() in {'.mov', '.mp4', '.mkv'}
        )
        names = [str(p) for p in siblings]
        if str(here) in names:
            idx = names.index(str(here))
            if idx + 1 < len(names):
                return names[idx + 1]
            return ''
        return ''
    except Exception:
        return ''


def final_report(
    video: str,
    recording_id: Optional[int],
    duration_s: float,
    results: dict[str, StageResult],
    total_elapsed: float,
) -> None:
    say('')
    say('=' * 78)
    say('FINAL REPORT — one VOD, complete. This driver STOPS here by design.')
    say('=' * 78)
    say(f'  video          {video}')
    say(f'  recording id   {recording_id}')
    if duration_s > 0:
        say(f'  duration       {hms(duration_s)}  ({duration_s:.1f}s)')

    # ---- per-stage timings -------------------------------------------------
    say('')
    say('STAGE TIMINGS')
    for stage in STAGE_ORDER:
        res = results.get(stage)
        if res is None:
            say(f'  {stage:<11} not-run     -')
            continue
        if res.status == 'skipped':
            say(f'  {stage:<11} SKIPPED     {res.detail}')
        elif res.status == 'ran':
            say(f'  {stage:<11} ran         {hms(res.elapsed_s):>12}   {res.detail}')
        elif res.status == 'failed':
            say(f'  {stage:<11} FAILED      {hms(res.elapsed_s):>12}   {res.detail}')
        else:
            say(f'  {stage:<11} not-run     {res.detail}')
    say(f'  {"TOTAL":<11}             {hms(total_elapsed):>12}')

    # ---- the number he actually needs: whisper speed on THIS Mac ----------
    say('')
    say('TRANSCRIPTION SPEED (the number that decides whether the 34-hour plan is feasible)')
    tres = results.get('transcribe')
    if tres is None or tres.status != 'ran':
        reason = 'skipped (transcript already existed)' if (tres and tres.status == 'skipped') else 'did not run'
        say(f'  not measured this run — transcribe {reason}.')
    elif duration_s <= 0:
        say('  cannot compute: the recording has no duration.')
    else:
        kv = parse_kv(tres.result_line)
        whisper_s = float(kv['elapsed_s']) if 'elapsed_s' in kv else None
        stage_mult = duration_s / tres.elapsed_s if tres.elapsed_s > 0 else 0.0
        say(f'  stage wall time      {hms(tres.elapsed_s)}  (ffmpeg wav extract + whisper)')
        say(f'  stage realtime mult  {stage_mult:.2f}x   (audio seconds per wall second)')
        if whisper_s:
            say(f'  whisper only         {hms(whisper_s)}  -> {duration_s / whisper_s:.2f}x realtime')
        if stage_mult > 0:
            say(f'  at this rate, 34 hours of VOD would take about {hms(34 * 3600 / stage_mult)} of wall time.')
        say('  NOTE: transcribe.py invokes whisper-cli with -ng, which DISABLES GPU')
        say('        acceleration. This number is CPU-only; Metal is not being used.')

    if recording_id is None:
        say('')
        say('  (no recording id — nothing further to report)')
        return

    # ---- transcript + gate -------------------------------------------------
    say('')
    say('TRANSCRIPT')
    segments = count_for('transcript_segments', recording_id)
    say(f'  segments       {segments}')
    qres = results.get('quality')
    if qres is not None and qres.result_line:
        kv = parse_kv(qres.result_line)
        verdict = 'PASS' if ' PASS ' in qres.result_line else ('FAIL' if ' FAIL ' in qres.result_line else '?')
        say(f'  D-028 gate     {verdict}')
        say(f'    blank           {kv.get("blank_pct", "?")}%   (>50% fails)')
        say(f'    repetition      {kv.get("repetition_pct", "?")}%   (>50% of SPEECH segments fails)')
        say(f'    non_speech      {kv.get("non_speech_pct", "?")}%   ([music], the note glyph, etc.)')
        say('                    non-speech markers are FAITHFUL transcription of you singing')
        say('                    on stream, not hallucinations. They never fail the gate.')
        say(f'    speech segs     {kv.get("speech", "?")}')
        say(f'    segments/min    {kv.get("segments_per_min", "?")}')
    else:
        say('  D-028 gate     not run this invocation')

    # ---- what each signal contributed --------------------------------------
    say('')
    say('SIGNAL CONTRIBUTION (what the ranking actually had to work with)')
    beats = count_for('trigger_beats', recording_id, "AND source = 'zebra_trigger'")
    buckets = count_for('audio_energy_buckets', recording_id)
    chat = count_for('chat_messages', recording_id)
    say(f'  zebra beats            {beats}')
    say(f'  audio energy buckets   {buckets}')
    # LO-12: this driver has no 'chat' stage in STAGE_ORDER at all -- chat_messages
    # is never populated by run_vod.py, so 0 here is not evidence of anything
    # missing or broken, it is the honest, structural, EVERY-run value. The old
    # wording ("EMPTY: the signal contributes nothing") read as a defect report.
    say(f'  chat messages          {chat}' +
        ('   <- 0 by construction (no chat stage in this driver)' if chat == 0 else ''))
    for source, n in db_rows(
        'SELECT source, COUNT(*) FROM llm_signal_candidates WHERE recording_id = %s GROUP BY source ORDER BY source',
        (recording_id,),
    ):
        say(f'  llm candidates         {int(n):>4}  from {source}')
    if not db_rows('SELECT 1 FROM llm_signal_candidates WHERE recording_id = %s LIMIT 1', (recording_id,)):
        say('  llm candidates            0   <- the transcript signal contributes nothing')

    # ---- candidates + the distribution -------------------------------------
    say('')
    say('CANDIDATES')
    rows = db_rows(
        '''
        SELECT score, signal_audio, signal_transcript, signal_chat, signal_beat_boost
        FROM clip_candidates WHERE recording_id = %s ORDER BY score DESC
        ''',
        (recording_id,),
    )
    say(f'  total          {len(rows)}')
    if rows:
        beat_sourced = sum(1 for r in rows if float(r[4] or 0.0) >= 1.0)
        say(f'  beat-sourced   {beat_sourced}   (zebra: floored at 0.9)')
        say(f'  signal-only    {len(rows) - beat_sourced}')
        say('  candidates carrying a NON-ZERO value per signal:')
        say(f'    signal_audio      {sum(1 for r in rows if float(r[1] or 0.0) > 0):>4} / {len(rows)}')
        say(f'    signal_transcript {sum(1 for r in rows if float(r[2] or 0.0) > 0):>4} / {len(rows)}')
        say(f'    signal_chat       {sum(1 for r in rows if float(r[3] or 0.0) > 0):>4} / {len(rows)}')
        say('  score distribution:')
        for line in score_distribution([float(r[0] or 0.0) for r in rows]):
            say(line)

    # ---- what is next ------------------------------------------------------
    say('')
    say('=' * 78)
    say('WHAT IS NEXT')
    say('=' * 78)
    say('  1. REVIEW this VOD (the local review surface, reads CLPR_DB_URL from the env):')
    say('')
    say(f'       cd {APP_DIR.parent}')
    say(f'       set -a; . {ENV_FILE}; set +a')
    say(f'       {sys.executable or "python3"} {APP_DIR / "review_server.py"}')
    say('       then open  http://127.0.0.1:8737')
    say('')
    say('     Or straight from SQL:')
    say('')
    say('       psql "$CLPR_DB_URL" -c "SELECT id, start_s, end_s, ROUND(score::numeric,3) AS score,')
    say('                                      signal_audio, signal_transcript, signal_chat, signal_beat_boost')
    say(f'                               FROM clip_candidates WHERE recording_id = {recording_id}')
    say('                               ORDER BY score DESC"')
    say('')
    nxt = next_vod_hint(video)
    say('  2. ONLY WHEN YOU ARE SATISFIED, the next VOD (this driver will not do it for you):')
    say('')
    if nxt:
        say(f'       {sys.executable or "python3"} {Path(__file__).resolve()} --video "{nxt}"')
    else:
        say(f'       {sys.executable or "python3"} {Path(__file__).resolve()} --video "<next-vod-path>"')
        say('')
        say('     (the next path could not be listed from here — take the next file by name,')
        say('      earliest first, from the recordings directory)')


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='Drive ONE VOD through the whole clip-detection pipeline, then stop.',
        epilog='Non-interactive by design: no prompts, children get stdin=DEVNULL. Safe under nohup.',
    )
    p.add_argument('--video', required=True, help='absolute path to the VOD file')
    p.add_argument('--dry-run', action='store_true',
                   help='print the full stage plan and touch nothing')
    p.add_argument('--from', dest='from_stage', choices=STAGE_ORDER, default=None,
                   help='resume from this stage; every earlier stage is not run')
    p.add_argument('--only', choices=STAGE_ORDER, default=None,
                   help='run exactly one stage and nothing else')
    p.add_argument('--skip', action='append', choices=STAGE_ORDER, default=[],
                   help='skip this stage (repeatable)')
    p.add_argument('--force', action='append', default=[],
                   help='re-run a stage even if its work already exists; "all" forces every stage (repeatable)')
    p.add_argument('--force-respend', action='store_true',
                   help='required alongside --force (or --force all) to actually re-run `signal`: '
                        'ON CONFLICT DO NOTHING at temperature 0 means a same-input re-run re-pays the '
                        'full OpenRouter LLM pass and writes nothing new (D-074 ruling 2 / LO-07)')
    return p.parse_args()


def selected_stages(args: argparse.Namespace) -> list[str]:
    if args.only:
        return [args.only]
    stages = list(STAGE_ORDER)
    if args.from_stage:
        stages = stages[STAGE_ORDER.index(args.from_stage):]
    return [s for s in stages if s not in set(args.skip)]


def main() -> int:
    args = parse_args()

    for f in args.force:
        if f != 'all' and f not in STAGE_ORDER:
            print(f'ERROR: --force expects a stage name or "all", got: {f}', file=sys.stderr)
            return 2
    forced = set(STAGE_ORDER) if 'all' in args.force else set(args.force)

    # D-074 ruling 2 / LO-07: `signal` writes via ON CONFLICT DO NOTHING at
    # temperature 0, so forcing a re-run of an already-populated recording
    # re-pays the ENTIRE OpenRouter LLM pass and then writes zero new rows —
    # every candidate collides with what is already there. Refuse outright
    # unless the operator also passes --force-respend, naming the cost and
    # the flag so the refusal is actionable, not just a wall.
    if 'signal' in forced and not args.force_respend:
        print(
            'ERROR: --force covers the `signal` stage, which would re-pay the full OpenRouter '
            'LLM pass and then write NOTHING new (ON CONFLICT DO NOTHING at temperature 0 means '
            'every candidate collides with what already exists). Refusing. Pass --force-respend '
            'as well if you really mean to spend that money again.',
            file=sys.stderr,
        )
        return 2

    video = os.path.abspath(os.path.expanduser(args.video))
    stages = selected_stages(args)
    run_started = time.monotonic()

    say('=' * 78)
    say('run_vod — ONE VOD, then STOP')
    say('=' * 78)
    say(f'  video   {video}')
    say(f'  stages  {" -> ".join(stages)}')
    if forced:
        say(f'  forced  {", ".join(sorted(forced))} (re-run even if already done)')

    # --- environment --------------------------------------------------------
    loaded, already = load_env_file(ENV_FILE)
    say('')
    say(f'ENVIRONMENT (from {ENV_FILE}, set-if-absent only — the existing environment always wins)')
    say(f'  loaded from .env : {", ".join(loaded) if loaded else "(none)"}')
    say(f'  already set, kept: {", ".join(already) if already else "(none)"}')
    if 'CLPR_DB_URL' in already:
        say('  CLPR_DB_URL came from the environment, NOT from .env — this run targets')
        say('  whatever database that URL names (e.g. the pg_test_harness throwaway).')

    # --- preflight ----------------------------------------------------------
    say('')
    say('PREFLIGHT (everything is checked before stage 1 — a missing key found after a')
    say('           six-hour transcription is the failure this driver exists to prevent)')
    problems = preflight(video, stages)
    if problems and not args.dry_run:
        say('')
        for pr in problems:
            print(f'ERROR: preflight: {pr}', file=sys.stderr)
        say('RESULT run_vod ok=0 stage=preflight problems=%d' % len(problems))
        return EXIT_FAIL
    if problems:
        # A dry run is a PLANNING tool, so it still prints the whole plan even
        # when preflight failed — knowing which stages would run is exactly what
        # you want when something in the environment is broken. The exit code
        # stays honest (non-zero), and the failures are repeated at the end.
        say('')
        say(f'  {len(problems)} preflight problem(s) — the plan below is printed anyway, but a')
        say('  real run would refuse. See the ERROR lines at the end.')

    # --- current state ------------------------------------------------------
    found = lookup_recording(video)
    recording_id = found[0] if found else None
    duration_s = found[1] if found else 0.0
    if found:
        say('')
        say(f'  already registered: recording id={recording_id} state={found[2]} duration={hms(duration_s)}')
    if recording_id is None and INGEST_VIDEO_ROOT is not None:
        try:
            if Path(video).parent.resolve() != Path(str(INGEST_VIDEO_ROOT)).resolve():
                say('')
                say(f'  NOTE: ingest_vods.py scans ONLY {INGEST_VIDEO_ROOT} (hardcoded, no CLI flag).')
                say(f'        This video lives in {Path(video).parent}, so ingest will not register it.')
        except Exception:
            pass

    # --- plan ---------------------------------------------------------------
    say('')
    say('PLAN')
    plan: list[tuple[str, bool, str]] = []
    for stage in stages:
        try:
            done, why = stage_done(stage, recording_id, video, duration_s)
        except Exception as exc:
            done, why = False, f'done-check inconclusive ({exc})'
        if stage in forced and done:
            done, why = False, f'FORCED re-run (would otherwise skip: {why})'
        plan.append((stage, done, why))
        mark = 'SKIP' if done else 'RUN '
        say(f'  [{mark}] {stage:<11} {STAGE_BLURB[stage]}')
        say(f'         {why}')

    if args.dry_run:
        say('')
        say('DRY RUN: nothing was executed, nothing was written.')
        for pr in problems:
            print(f'ERROR: preflight: {pr}', file=sys.stderr)
        say(f'RESULT run_vod ok={0 if problems else 1} dry_run=1 recording={recording_id} '
            f'stages_would_run={sum(1 for _, d, _ in plan if not d)} '
            f'stages_would_skip={sum(1 for _, d, _ in plan if d)} '
            f'preflight_problems={len(problems)}')
        return EXIT_FAIL if problems else EXIT_OK

    # --- execute ------------------------------------------------------------
    results: dict[str, StageResult] = {s: StageResult(s) for s in STAGE_ORDER}
    extract_streams: Optional[int] = None
    extract_selected: Optional[str] = None
    total = len(plan)

    try:
        for idx, (stage, done, why) in enumerate(plan, start=1):
            res = results[stage]
            say('')
            say('-' * 78)
            if done:
                res.status = 'skipped'
                res.detail = why
                say(f'[{idx}/{total}] {stage:<11} SKIPPED — {why}')
                continue

            require_registered(stage, recording_id, video)

            say(f'[{idx}/{total}] {stage:<11} RUNNING — {STAGE_BLURB[stage]}')
            cmd = stage_command(stage, video, recording_id)
            stage_env = None
            if stage == 'slice':
                # The exact env construction stage_slices_remote.
                # run_slice_candidates uses for its own subprocess call to
                # this same worker: a copy of the environment with
                # CLPR_SLICES_DIR pointed at the shared staging directory, so
                # `push` (which computes that directory itself) finds these
                # slices already there.
                staging = slice_staging_dir(recording_id)
                stage_env = os.environ.copy()
                stage_env['CLPR_SLICES_DIR'] = str(staging)
                say(f'         CLPR_SLICES_DIR={staging}')
            execute_stage(stage, cmd, res, env=stage_env)
            res.detail = res.result_line or 'ok'
            say(f'[{idx}/{total}] {stage:<11} DONE in {hms(res.elapsed_s)}')

            if stage == 'ingest':
                found = lookup_recording(video)
                if not found:
                    misses = near_miss_paths(video)
                    msg = (
                        f'ingest_vods.py ran and exited 0, but no recordings row exists for '
                        f'path="{video}". ingest_vods scans only {INGEST_VIDEO_ROOT} and matches '
                        f'on the exact path string.'
                    )
                    if misses:
                        msg += ' Rows with the same filename but a different path: ' + '; '.join(misses)
                    raise StageFailure(msg, EXIT_FAIL, stage=stage)
                recording_id, duration_s = found[0], found[1]
                # D-074 ruling 12 / LO-09: ingest_vods.py scans its whole
                # directory and can report ok=1 while some OTHER file in that
                # scan failed (its own RESULT line already carries failed=N).
                # This run_vod invocation only targets `video`, and the lookup
                # above already failed loudly if THAT specific file did not
                # register — but a nonzero failed count here means something
                # ELSE in the scan is broken, and that must not go unreported
                # just because it was not the file this run cared about.
                ingest_kv = parse_kv(res.result_line)
                ingest_failed = ingest_kv.get('failed', '?')
                res.detail = f'recording id={recording_id} duration={hms(duration_s)} failed={ingest_failed}'
                say(f'         registered as recording id={recording_id}, duration {hms(duration_s)}, '
                    f'failed={ingest_failed}')
                if ingest_failed not in ('0', '?'):
                    say('')
                    say(f'  NOTE: ingest_vods reported failed={ingest_failed} for its directory scan '
                        '(ok=1 covers the whole scan, not just this file). This video registered fine, '
                        'but check ingest_vods.py stderr above for which other file(s) failed.')

            if stage == 'audio_guard':
                for line in res.stdout_lines:
                    s = line.strip()
                    if s.startswith('audio streams :'):
                        try:
                            extract_streams = int(s.split(':', 1)[1].strip())
                        except ValueError:
                            pass
                    if s.startswith('SELECTED audio stream '):
                        extract_selected = s.split()[3]
                if extract_streams and extract_streams > 1:
                    say('')
                    say('  WARNING — MULTIPLE AUDIO STREAMS')
                    say(f'    extract_audio.sh measured {extract_streams} audio streams and selected '
                        f'{extract_selected}.')
                    say('    transcribe.py and audio_energy.py invoke ffmpeg WITHOUT an explicit -map,')
                    say('    so ffmpeg applies its own default stream selection there, which is NOT')
                    say('    guaranteed to be the stream extract_audio.sh just chose. If the transcript')
                    say('    comes back mostly BLANK_AUDIO on a recording whose audio is fine, this is')
                    say('    the first thing to suspect. Reported, not worked around: fixing it means')
                    say('    changing verified workers, which is a design call.')

                # D-074 ruling 9 / LO-03: nothing downstream reads this .m4a
                # (transcribe.py and audio_energy.py ffmpeg the source video
                # directly), so it is discarded the moment this stage is done
                # running — success or not, whatever extract_audio.sh left
                # behind is removed. extract_audio.sh has no check-only flag
                # (adding one is out of scope: this fix touches run_vod.py
                # only), so the full extract-and-verify pass still runs; only
                # its OUTPUT FILE is not kept. The guard's refusal verdict
                # (is_extract_refusal, above) is read from stderr, not from
                # this file, so it is unaffected by the deletion.
                guard_out = audio_artifact_path(video)
                if guard_out.exists():
                    try:
                        guard_out.unlink()
                        res.detail += f'; .m4a discarded ({guard_out.name}, LO-03: nothing reads it)'
                        say(f'         .m4a discarded ({guard_out.name}) — nothing downstream reads it (LO-03)')
                    except OSError as exc:
                        say(f'         WARNING: could not remove discarded .m4a artifact {guard_out}: {exc}')

    except StageFailure as exc:
        say('')
        say('!' * 78)
        print(f'ERROR: {exc}', file=sys.stderr)
        say('!' * 78)
        say('')
        say('THE RUN STOPPED HERE. Nothing downstream of this stage ran, so no candidates')
        say('were produced from incomplete inputs. Every worker owns its own transaction and')
        say('rolls back on failure, so this stage persisted nothing partial.')
        say('Re-running the exact same command is safe: completed stages will be skipped.')
        # LO-15 (golden-review F21): final_report opens fresh DB connections
        # (count_for/db_rows) to build the report, and it is being called
        # HERE, inside the failure handler, where the DB itself may be the
        # very thing that just failed. Without this try/except, a DB-caused
        # StageFailure would have final_report raise a SECOND time trying to
        # read the DB for the report, and that second exception would escape
        # this handler uncaught -- crashing before the RESULT line below ever
        # printed, so the classified exit code (exc.exit_code) would never
        # reach the operator or a caller parsing this driver's output.
        try:
            final_report(video, recording_id, duration_s, results, time.monotonic() - run_started)
        except Exception as report_exc:  # noqa: BLE001 - the ORIGINAL failure is what matters
            say('')
            say(f'  WARNING: final_report itself failed ({report_exc!r}) while reporting a stage '
                'failure -- possibly the same DB outage that caused it. Continuing to the RESULT '
                'line so the classified exit code below is not swallowed by a report-time crash.')
        say('')
        # LO-10: the STAGE NAME (e.g. "transcribe"), not the classified exit
        # code integer -- exit= right after it already carries that integer,
        # and several different stages can share one exit code (EXIT_FAIL=1
        # covers require_registered's refusal AND execute_stage's generic
        # fallback), which collapsed "which stage" into "what kind of exit".
        say(f'RESULT run_vod ok=0 recording={recording_id} failed_stage={exc.stage or "unknown"} '
            f'exit={exc.exit_code} video="{video}"')
        return exc.exit_code

    total_elapsed = time.monotonic() - run_started
    final_report(video, recording_id, duration_s, results, total_elapsed)

    ran = sum(1 for s in STAGE_ORDER if results[s].status == 'ran')
    skipped = sum(1 for s in STAGE_ORDER if results[s].status == 'skipped')
    candidates = count_for('clip_candidates', recording_id) if recording_id else 0
    say('')
    say(f'RESULT run_vod ok=1 recording={recording_id} stages_ran={ran} stages_skipped={skipped} '
        f'candidates={candidates} elapsed_s={total_elapsed:.3f} video="{video}"')
    return EXIT_OK


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('ERROR: interrupted', file=sys.stderr)
        sys.exit(EXIT_FAIL)
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(EXIT_FAIL)
