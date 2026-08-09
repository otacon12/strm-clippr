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

    1 ingest      workers/ingest_vods.py        register the recording
    2 extract     scripts/extract_audio.sh      audio artifact + silence/track guard
    3 transcribe  workers/transcribe.py         whisper.cpp            [OBS-gated]
    4 quality     workers/quality_gate.py       D-028 transcript gate
    5 zebra       workers/zebra_detect.py       trigger-word beats
    6 energy      workers/audio_energy.py       ebur128 buckets        [OBS-gated]
    7 signal      workers/transcript_signal.py  LLM signal candidates  [costs money]
    8 fusion      workers/score_fusion.py       final clip candidates

Guarantees this driver provides
-------------------------------
* IT STOPS. One VOD per invocation. There is no loop over recordings anywhere
  in this file.
* RESUMABLE AND IDEMPOTENT. Every stage has a done-check against the database
  (or, for `extract`, against the artifact on disk). Work already done is
  SKIPPED and said so out loud. A 4-to-8 hour transcription is never repeated
  because a later stage failed.
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
    2  usage / bad arguments (argparse)
    3  OBS gate refusal — NOT a failure; come back when off air
    4  D-028 quality gate FAILED — the transcript is unusable
    5  extract_audio REFUSED — no audio, or every audio stream is silent
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
    'extract',
    'transcribe',
    'quality',
    'zebra',
    'energy',
    'signal',
    'fusion',
]

STAGE_BLURB = {
    'ingest': 'register the recording (ingest_vods.py)',
    'extract': 'extract audio + silence/track guard (extract_audio.sh)',
    'transcribe': 'whisper.cpp transcription (transcribe.py) [OBS-gated, the long one]',
    'quality': 'D-028 transcript quality gate (quality_gate.py)',
    'zebra': 'zebra trigger-word beats (zebra_detect.py)',
    'energy': 'ebur128 5s loudness buckets (audio_energy.py) [OBS-gated]',
    'signal': 'LLM signal candidates (transcript_signal.py) [costs OpenRouter money]',
    'fusion': 'score fusion into clip candidates (score_fusion.py)',
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
    """A stage failed. `exit_code` carries the classified reason."""

    def __init__(self, message: str, exit_code: int = EXIT_FAIL):
        super().__init__(message)
        self.exit_code = exit_code


class StageResult:
    def __init__(self, name: str):
        self.name = name
        self.status = 'pending'   # ran | skipped | not-run | failed
        self.detail = ''
        self.elapsed_s = 0.0
        self.result_line = ''
        self.stdout_lines: list[str] = []
        self.stderr_lines: list[str] = []


def run_child(label: str, cmd: list[str]) -> tuple[int, list[str], list[str]]:
    """Run a child, STREAMING its output live, and return (rc, stdout, stderr).

    Live streaming is not a nicety. A silent multi-hour gap is the "long quiet
    bout" failure by construction: the operator cannot tell a working whisper
    from a hung one. Every line is relayed the moment it arrives, prefixed with
    the stage so a long log stays readable, and retained for RESULT parsing.

    stdin is DEVNULL on every child: ffmpeg without -nostdin swallows keystrokes
    from the terminal the operator is watching.
    """
    say(f'    $ {" ".join(cmd)}')
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
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

    return problems


# ---------------------------------------------------------------------------
# stage done-checks (the resumability contract)
# ---------------------------------------------------------------------------

def audio_artifact_path(video: str) -> Path:
    p = Path(video)
    return p.with_suffix('.m4a')


def extract_already_done(video: str, duration_s: float) -> tuple[bool, str]:
    """extract_audio.sh's output is a file on disk, so the done-check is too.

    Checked the same way the script itself verifies its output: the artifact
    exists, is not suspiciously small, and its duration matches the recording
    within 2 seconds. A truncated leftover therefore does NOT count as done.
    """
    out = audio_artifact_path(video)
    try:
        if not out.exists():
            return False, 'no .m4a artifact next to the video'
        size = out.stat().st_size
        if size <= 10240:
            return False, f'.m4a exists but is only {size} bytes (extract_audio.sh calls that suspiciously small)'
        proc = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=nw=1:nk=1', str(out)],
            capture_output=True, text=True, stdin=subprocess.DEVNULL,
        )
        raw = (proc.stdout or '').strip()
        if proc.returncode != 0 or not raw:
            return False, '.m4a exists but ffprobe could not read its duration'
        out_dur = float(raw)
        if duration_s > 0 and abs(out_dur - duration_s) > 2.0:
            return False, f'.m4a duration {out_dur:.1f}s does not match recording {duration_s:.1f}s'
        return True, f'{out.name} present, {size / 1048576:.1f} MB, {out_dur:.1f}s'
    except Exception as exc:
        return False, f'.m4a check inconclusive ({exc})'


def stage_done(stage: str, recording_id: Optional[int], video: str, duration_s: float) -> tuple[bool, str]:
    """Return (already_done, human reason). Cheap stages are never 'done'."""
    if stage == 'ingest':
        if recording_id is None:
            return False, 'not registered yet'
        return True, f'already registered as recording id={recording_id}'

    if recording_id is None:
        return False, 'recording not registered yet'

    if stage == 'extract':
        return extract_already_done(video, duration_s)

    if stage == 'transcribe':
        n = count_for('transcript_segments', recording_id)
        if n > 0:
            return True, f'{n} transcript segments already present'
        return False, 'no transcript segments'

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

    raise ValueError(f'unknown stage: {stage}')


# ---------------------------------------------------------------------------
# stage runners
# ---------------------------------------------------------------------------

def stage_command(stage: str, video: str, recording_id: Optional[int]) -> list[str]:
    py = sys.executable or 'python3'
    if stage == 'ingest':
        return [py, str(WORKERS_DIR / 'ingest_vods.py')]
    if stage == 'extract':
        return ['bash', str(SCRIPTS_DIR / 'extract_audio.sh'), video]
    worker = {
        'transcribe': 'transcribe.py',
        'quality': 'quality_gate.py',
        'zebra': 'zebra_detect.py',
        'energy': 'audio_energy.py',
        'signal': 'transcript_signal.py',
        'fusion': 'score_fusion.py',
    }[stage]
    return [py, str(WORKERS_DIR / worker), '--vod-id', str(recording_id)]


def execute_stage(stage: str, cmd: list[str], res: StageResult) -> None:
    started = time.monotonic()
    rc, out_lines, err_lines = run_child(stage, cmd)
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
        )

    if stage == 'quality':
        raise StageFailure(
            f'stage {stage}: D-028 QUALITY GATE FAILED (exit {rc}). The transcript is '
            'unusable, so detection is refused on purpose — scoring garbage segments would '
            'produce garbage candidates. The transcript is left in the database (a 4-to-8 '
            'hour job is not thrown away); the gate stops everything downstream of it. '
            'The gate\'s own guidance is quoted verbatim above.',
            EXIT_QUALITY_GATE,
        )

    if stage == 'extract' and is_extract_refusal(err_lines):
        raise StageFailure(
            f'stage {stage}: extract_audio.sh REFUSED (exit {rc}). This is the vod-6 '
            'signature (D-028): the recording has no audio, or every audio stream is '
            'digitally silent. Nothing was extracted and nothing was written. Check the '
            'recorder\'s audio source. Its refusal text is quoted verbatim above.',
            EXIT_EXTRACT_REFUSED,
        )

    tail = err_lines[-1] if err_lines else (out_lines[-1] if out_lines else '(no output)')
    raise StageFailure(f'stage {stage}: exit {rc}. Last output line: {tail}', EXIT_FAIL)


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
    say(f'  chat messages          {chat}' + ('   <- EMPTY: the chat signal contributes nothing' if chat == 0 else ''))
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

            say(f'[{idx}/{total}] {stage:<11} RUNNING — {STAGE_BLURB[stage]}')
            cmd = stage_command(stage, video, recording_id)
            execute_stage(stage, cmd, res)
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
                    raise StageFailure(msg, EXIT_FAIL)
                recording_id, duration_s = found[0], found[1]
                res.detail = f'recording id={recording_id} duration={hms(duration_s)}'
                say(f'         registered as recording id={recording_id}, duration {hms(duration_s)}')

            if stage == 'extract':
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
        final_report(video, recording_id, duration_s, results, time.monotonic() - run_started)
        say('')
        say(f'RESULT run_vod ok=0 recording={recording_id} failed_stage={exc.exit_code} '
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
