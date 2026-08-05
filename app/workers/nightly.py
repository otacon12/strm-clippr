#!/usr/bin/env python3
"""nightly: LOCAL lane nightly loop. Drives ingest -> transcribe -> (human fence) -> detection.

The loop has a PERMANENT HUMAN CHECKPOINT in the middle and that is deliberate (D-005).
Four of the five detection stages call require_poison_reviewed_or_raise(), and
ingest_vods.py leaves a new VOD at poison_reviewed=0, so detection CANNOT proceed until
a human has poison-reviewed the VOD. This worker never sets poison_reviewed, never infers
it, and never routes around it: a VOD that has not been cleared is SKIPPED and reported in
the digest under "WAITING ON YOUR POISON REVIEW". It is not handed to a detection stage
just to watch that stage refuse.

Phase 1 (automatic, no fence):
  1. ingest_vods.py once (it discovers new VODs; it takes no --vod-id)
  2. every VOD at state='ingested'  -> run_pipeline.py --vod-id N --to transcribe
Phase 2 (automatic, only for VODs the human has cleared):
  3. every VOD at state='transcribed' AND poison_reviewed=1 -> run_pipeline.py --vod-id N --from zebra_detect

One VOD failing never aborts the others: each failure is recorded and the loop continues.
An OBS-busy refusal (stderr contains 'Refusing') is an expected outcome, counted and
reported separately from a real failure, and it does not make the run fail.

A SILENT NIGHT IS A FAILED NIGHT: the digest is ALWAYS written, including when nothing
happened at all. It is written to app/digests/nightly_<UTC stamp>.md and printed to stdout.

Exits non-zero if ANY VOD failed. Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

OBS_REFUSAL_MARKER = 'Refusing'
NOTIFY_TITLE = 'clpr nightly'
NOTIFY_TIMEOUT_S = 20
INGESTED_LINE_RE = re.compile(r'^INGESTED id=(\d+)\b')


@dataclass
class VodOutcome:
    """One run_pipeline invocation for one VOD."""
    vod_id: int
    phase: str
    stage_label: str
    cmd: list[str] = field(default_factory=list)
    exit_code: int = 0
    stdout: str = ''
    stderr: str = ''
    result_lines: list[str] = field(default_factory=list)
    ok: bool = False
    obs_busy: bool = False
    failure_reason: Optional[str] = None


@dataclass
class Failure:
    """A failure worth reporting: a VOD run, or the ingest step itself (vod_id None)."""
    vod_id: Optional[int]
    stage: str
    exit_code: int
    error_text: str


@dataclass
class WaitingVod:
    vod_id: int
    duration_s: Optional[float]
    filename: str


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def app_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def digest_path_for(stamp: str) -> Path:
    return app_dir() / 'digests' / f'nightly_{stamp}.md'


def ingest_command() -> list[str]:
    return [sys.executable, os.path.join('workers', 'ingest_vods.py')]


def transcribe_command(vod_id: int) -> list[str]:
    return [
        sys.executable,
        os.path.join('workers', 'run_pipeline.py'),
        '--vod-id', str(vod_id),
        '--to', 'transcribe',
    ]


def detect_command(vod_id: int) -> list[str]:
    return [
        sys.executable,
        os.path.join('workers', 'run_pipeline.py'),
        '--vod-id', str(vod_id),
        '--from', 'zebra_detect',
    ]


def extract_result_lines(stdout: str, stderr: str) -> list[str]:
    """Every RESULT line in the captured output, verbatim from 'RESULT ' onward.

    run_pipeline echoes a stage's own RESULT line inside 'STAGE <name> OK RESULT ...',
    so the marker is found anywhere on the line and the RESULT text is taken from there.
    """
    found: list[str] = []
    for stream in (stdout, stderr):
        for raw_line in stream.splitlines():
            line = raw_line.rstrip('\r')
            idx = line.find('RESULT ')
            if idx >= 0:
                found.append(line[idx:])
    return found


def parse_ingested_ids(stdout: str) -> list[int]:
    """VOD ids newly inserted by ingest_vods, from its 'INGESTED id=<n> ...' lines."""
    ids: list[int] = []
    for raw_line in stdout.splitlines():
        m = INGESTED_LINE_RE.match(raw_line.rstrip('\r'))
        if m:
            ids.append(int(m.group(1)))
    return ids


def run_subprocess(cmd: list[str], cwd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def classify(vod_id: int, phase: str, stage_label: str, cmd: list[str],
             proc: subprocess.CompletedProcess) -> VodOutcome:
    stdout = proc.stdout or ''
    stderr = proc.stderr or ''
    result_lines = extract_result_lines(stdout, stderr)

    failure_reason: Optional[str] = None
    if proc.returncode != 0:
        failure_reason = f'non-zero exit code {proc.returncode}'
    elif not result_lines:
        # Same witness rule run_pipeline applies to its own stages: exit 0 only proves the
        # subprocess did not throw, the RESULT line proves it reached its own end.
        failure_reason = 'exited 0 but produced NO line containing "RESULT " (missing witness)'

    obs_busy = failure_reason is not None and OBS_REFUSAL_MARKER in stderr

    return VodOutcome(
        vod_id=vod_id,
        phase=phase,
        stage_label=stage_label,
        cmd=list(cmd),
        exit_code=proc.returncode,
        stdout=stdout,
        stderr=stderr,
        result_lines=result_lines,
        ok=failure_reason is None,
        obs_busy=obs_busy,
        failure_reason=failure_reason,
    )


def query_ingested_vods(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute("SELECT id FROM vods WHERE state = 'ingested' ORDER BY id").fetchall()
    return [int(r[0]) for r in rows]


def query_detection_ready_vods(conn: sqlite3.Connection) -> list[int]:
    """READ ONLY. VODs the human has explicitly cleared: poison_reviewed = 1."""
    rows = conn.execute(
        """
        SELECT id
        FROM vods
        WHERE state = 'transcribed'
          AND IFNULL(poison_reviewed, 0) = 1
        ORDER BY id
        """
    ).fetchall()
    return [int(r[0]) for r in rows]


def query_waiting_poison_review(conn: sqlite3.Connection) -> list[WaitingVod]:
    """READ ONLY. VODs stalled at the fence: transcribed but NOT poison_reviewed.

    Fail closed exactly like poison_gate.is_poison_reviewed: anything that is not 1
    (including NULL) counts as not reviewed, so this set and the detection-ready set
    partition the transcribed VODs with no gap.
    """
    rows = conn.execute(
        """
        SELECT id, duration_s, path
        FROM vods
        WHERE state = 'transcribed'
          AND IFNULL(poison_reviewed, 0) <> 1
        ORDER BY id
        """
    ).fetchall()
    out: list[WaitingVod] = []
    for vod_id, duration_s, path in rows:
        out.append(
            WaitingVod(
                vod_id=int(vod_id),
                duration_s=(None if duration_s is None else float(duration_s)),
                filename=os.path.basename(str(path)),
            )
        )
    return out


def duration_minutes(duration_s: Optional[float]) -> str:
    if duration_s is None:
        return 'unknown'
    return f'{duration_s / 60.0:.1f}'


def first_error_text(outcome: VodOutcome) -> str:
    """The real error text for the digest: the failure reason plus verbatim stderr."""
    stderr = outcome.stderr.strip()
    reason = outcome.failure_reason or 'unknown failure'
    if stderr:
        return f'{reason}\n{stderr}'
    stdout = outcome.stdout.strip()
    if stdout:
        return f'{reason}\n(stderr empty; stdout follows)\n{stdout}'
    return f'{reason}\n(no output on stdout or stderr)'


def build_digest(
    started_at: str,
    finished_at: str,
    ingested_ids: list[int],
    transcribed: list[VodOutcome],
    detected: list[VodOutcome],
    waiting: list[WaitingVod],
    failures: list[Failure],
    obs_busy: list[VodOutcome],
    ok: bool,
) -> str:
    lines: list[str] = []
    lines.append(f'# clpr nightly digest {started_at}')
    lines.append('')
    lines.append(
        f'SUMMARY: ok={1 if ok else 0} ingested={len(ingested_ids)} '
        f'transcribed={len(transcribed)} detected={len(detected)} '
        f'waiting_poison_review={len(waiting)} failed={len(failures)} obs_busy={len(obs_busy)}'
    )
    lines.append('')
    lines.append(f'- started_at: {started_at}')
    lines.append(f'- finished_at: {finished_at}')
    lines.append(f'- db_path: {get_db_path()}')
    lines.append(f'- cwd: {app_dir()}')
    lines.append('')

    lines.append('## VODs ingested this run')
    lines.append('')
    lines.append(f'count: {len(ingested_ids)}')
    if ingested_ids:
        lines.append(f'ids: {", ".join(str(i) for i in ingested_ids)}')
    else:
        lines.append('ids: none (no new VOD files were registered this run)')
    lines.append('')

    lines.append('## VODs transcribed this run')
    lines.append('')
    if transcribed:
        for o in transcribed:
            lines.append(f'- vod {o.vod_id}:')
            for rl in o.result_lines:
                lines.append(f'  - `{rl}`')
    else:
        lines.append('none.')
    lines.append('')

    lines.append('## VODs detected this run')
    lines.append('')
    if detected:
        for o in detected:
            lines.append(f'- vod {o.vod_id}:')
            for rl in o.result_lines:
                lines.append(f'  - `{rl}`')
    else:
        lines.append('none.')
    lines.append('')

    lines.append('## WAITING ON YOUR POISON REVIEW')
    lines.append('')
    if waiting:
        lines.append(
            f'{len(waiting)} VOD(s) are fully stalled. They are transcribed, but detection '
            'cannot run until you poison-review them. Nothing else can unblock these.'
        )
        lines.append('')
        for w in waiting:
            lines.append(
                f'- vod {w.vod_id} | duration {duration_minutes(w.duration_s)} min | {w.filename}'
            )
    else:
        lines.append('Nothing is waiting on your poison review.')
    lines.append('')

    lines.append('## Failures')
    lines.append('')
    if failures:
        for f in failures:
            vod_label = 'n/a (not a per-VOD step)' if f.vod_id is None else str(f.vod_id)
            lines.append(f'- vod {vod_label} | stage {f.stage} | exit_code {f.exit_code}')
            lines.append('')
            lines.append('```')
            lines.append(f.error_text)
            lines.append('```')
    else:
        lines.append('none.')
    lines.append('')

    lines.append('## OBS-busy skips')
    lines.append('')
    if obs_busy:
        lines.append(
            'These are NOT crashes: OBS was streaming or recording and the stage refused '
            '(the D-009 gate working as designed). They were not retried.'
        )
        lines.append('')
        for o in obs_busy:
            lines.append(f'- vod {o.vod_id} | {o.stage_label} | exit_code {o.exit_code}')
            lines.append('')
            lines.append('```')
            lines.append(o.stderr.strip() or '(stderr empty)')
            lines.append('```')
    else:
        lines.append('none.')
    lines.append('')

    return '\n'.join(lines)


def sanitize_for_osascript(text: str) -> str:
    return text.replace('\\', ' ').replace('"', "'").replace('\n', ' ')


def build_notification_message(
    ingested: int, transcribed: int, detected: int, waiting: int, failed: int, obs_busy: int
) -> str:
    parts = [f'ingested {ingested}, transcribed {transcribed}, detected {detected}']
    parts.append(
        f'{waiting} WAITING on poison review' if waiting else 'nothing waiting on poison review'
    )
    parts.append(f'{failed} FAILED' if failed else 'no failures')
    if obs_busy:
        parts.append(f'{obs_busy} OBS-busy skip(s)')
    return sanitize_for_osascript('; '.join(parts))


def notify(message: str) -> None:
    """Best-effort macOS notification. A notification failure NEVER fails the run."""
    script = f'display notification "{message}" with title "{NOTIFY_TITLE}"'
    cmd = ['osascript', '-e', script]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=NOTIFY_TIMEOUT_S)
    except Exception as exc:
        print(f'NOTIFY_FAILED (ignored, run continues): {exc}', file=sys.stderr)
        return
    if proc.returncode != 0:
        print(
            f'NOTIFY_FAILED (ignored, run continues): exit_code={proc.returncode} '
            f'stderr={(proc.stderr or "").strip()}',
            file=sys.stderr,
        )
        return
    print(f'NOTIFY_SENT {message}')


def do_dry_run() -> int:
    db_path = get_db_path()
    print('DRY RUN nightly: nothing will be executed, no digest written, no notification sent.')
    print(f'cwd: {app_dir()}')
    print(f'CLPR_DB_PATH: {db_path}')
    print('')

    with sqlite3.connect(db_path) as conn:
        ingested = query_ingested_vods(conn)
        ready = query_detection_ready_vods(conn)
        waiting = query_waiting_poison_review(conn)

    step = 0
    print('PHASE 1 (automatic, no fence):')
    step += 1
    print(f'{step}. ingest_vods (once, no --vod-id): {" ".join(ingest_command())}')
    print(
        '   note: VODs this command would newly discover cannot be listed here, because the '
        'plan below is read from the DB as it stands right now.'
    )
    if ingested:
        for vod_id in ingested:
            step += 1
            print(f'{step}. vod {vod_id} (state=ingested) -> {" ".join(transcribe_command(vod_id))}')
    else:
        print('   no VOD is at state=ingested, so no transcribe would run.')
    print('')

    print('PHASE 2 (automatic, ONLY for VODs with poison_reviewed=1):')
    if ready:
        for vod_id in ready:
            step += 1
            print(f'{step}. vod {vod_id} (transcribed, poison_reviewed=1) -> {" ".join(detect_command(vod_id))}')
    else:
        print('   no VOD is cleared for detection, so no detection would run.')
    print('')

    print('WAITING ON YOUR POISON REVIEW (SKIPPED, no worker would be called for these):')
    if waiting:
        for w in waiting:
            print(
                f'- vod {w.vod_id} | duration {duration_minutes(w.duration_s)} min | {w.filename}'
            )
    else:
        print('- none')
    print('')

    print('DRY RUN complete: nothing was executed, no digest written.')
    return 0


def run_nightly() -> int:
    started_at = utc_now_iso()
    stamp = utc_stamp()
    cwd = str(app_dir())
    db_path = get_db_path()

    ingested_ids: list[int] = []
    transcribed: list[VodOutcome] = []
    detected: list[VodOutcome] = []
    failures: list[Failure] = []
    obs_busy: list[VodOutcome] = []

    print(f'NIGHTLY START {started_at} cwd={cwd} CLPR_DB_PATH={db_path}')

    # --- Phase 1, step 1: ingest (never per-VOD; a failure here does not abort the run) ---
    ingest_cmd = ingest_command()
    print(f'INGEST START cmd={" ".join(ingest_cmd)}')
    try:
        proc = run_subprocess(ingest_cmd, cwd)
        ingest_stdout = proc.stdout or ''
        ingest_stderr = proc.stderr or ''
        ingest_results = extract_result_lines(ingest_stdout, ingest_stderr)
        if proc.returncode != 0:
            failures.append(
                Failure(
                    vod_id=None,
                    stage='ingest_vods',
                    exit_code=proc.returncode,
                    error_text=(
                        f'non-zero exit code {proc.returncode}\n'
                        f'{ingest_stderr.strip() or "(stderr empty)"}'
                    ),
                )
            )
            print(f'INGEST FAILED exit_code={proc.returncode}', file=sys.stderr)
        elif not ingest_results:
            failures.append(
                Failure(
                    vod_id=None,
                    stage='ingest_vods',
                    exit_code=proc.returncode,
                    error_text=(
                        'exited 0 but produced NO line containing "RESULT " (missing witness)\n'
                        f'{ingest_stderr.strip() or "(stderr empty)"}'
                    ),
                )
            )
            print('INGEST FAILED exit 0 with no RESULT line', file=sys.stderr)
        else:
            ingested_ids = parse_ingested_ids(ingest_stdout)
            for rl in ingest_results:
                print(f'INGEST OK {rl}')
    except Exception as exc:
        failures.append(
            Failure(vod_id=None, stage='ingest_vods', exit_code=-1, error_text=f'{type(exc).__name__}: {exc}')
        )
        print(f'INGEST FAILED (exception, run continues): {exc}', file=sys.stderr)

    # --- Phase 1, step 2: transcribe every VOD at state='ingested' ---
    with sqlite3.connect(db_path) as conn:
        to_transcribe = query_ingested_vods(conn)

    for vod_id in to_transcribe:
        cmd = transcribe_command(vod_id)
        print(f'TRANSCRIBE START vod={vod_id} cmd={" ".join(cmd)}')
        try:
            proc = run_subprocess(cmd, cwd)
            outcome = classify(vod_id, 'transcribe', 'run_pipeline --to transcribe', cmd, proc)
        except Exception as exc:
            failures.append(
                Failure(vod_id=vod_id, stage='run_pipeline --to transcribe', exit_code=-1,
                        error_text=f'{type(exc).__name__}: {exc}')
            )
            print(f'TRANSCRIBE FAILED vod={vod_id} (exception, run continues): {exc}', file=sys.stderr)
            continue

        if outcome.ok:
            transcribed.append(outcome)
            print(f'TRANSCRIBE OK vod={vod_id}')
        elif outcome.obs_busy:
            obs_busy.append(outcome)
            print(f'TRANSCRIBE OBS-BUSY vod={vod_id} (expected refusal, not a crash)', file=sys.stderr)
        else:
            failures.append(
                Failure(vod_id=vod_id, stage=outcome.stage_label, exit_code=outcome.exit_code,
                        error_text=first_error_text(outcome))
            )
            print(f'TRANSCRIBE FAILED vod={vod_id} exit_code={outcome.exit_code}', file=sys.stderr)

    # --- Phase 2: detection, ONLY for VODs the human has poison-reviewed ---
    with sqlite3.connect(db_path) as conn:
        to_detect = query_detection_ready_vods(conn)

    for vod_id in to_detect:
        cmd = detect_command(vod_id)
        print(f'DETECT START vod={vod_id} cmd={" ".join(cmd)}')
        try:
            proc = run_subprocess(cmd, cwd)
            outcome = classify(vod_id, 'detect', 'run_pipeline --from zebra_detect', cmd, proc)
        except Exception as exc:
            failures.append(
                Failure(vod_id=vod_id, stage='run_pipeline --from zebra_detect', exit_code=-1,
                        error_text=f'{type(exc).__name__}: {exc}')
            )
            print(f'DETECT FAILED vod={vod_id} (exception, run continues): {exc}', file=sys.stderr)
            continue

        if outcome.ok:
            detected.append(outcome)
            print(f'DETECT OK vod={vod_id}')
        elif outcome.obs_busy:
            obs_busy.append(outcome)
            print(f'DETECT OBS-BUSY vod={vod_id} (expected refusal, not a crash)', file=sys.stderr)
        else:
            failures.append(
                Failure(vod_id=vod_id, stage=outcome.stage_label, exit_code=outcome.exit_code,
                        error_text=first_error_text(outcome))
            )
            print(f'DETECT FAILED vod={vod_id} exit_code={outcome.exit_code}', file=sys.stderr)

    # --- The fence report: read AFTER phase 1, so anything transcribed tonight shows up ---
    with sqlite3.connect(db_path) as conn:
        waiting = query_waiting_poison_review(conn)

    finished_at = utc_now_iso()
    ok = not failures

    digest_file = digest_path_for(stamp)
    digest_file.parent.mkdir(parents=True, exist_ok=True)
    digest = build_digest(
        started_at=started_at,
        finished_at=finished_at,
        ingested_ids=ingested_ids,
        transcribed=transcribed,
        detected=detected,
        waiting=waiting,
        failures=failures,
        obs_busy=obs_busy,
        ok=ok,
    )
    with open(digest_file, 'w', encoding='utf-8') as handle:
        handle.write(digest if digest.endswith('\n') else digest + '\n')

    print('')
    print(digest)
    print(f'digest: {digest_file}')

    notify(
        build_notification_message(
            ingested=len(ingested_ids),
            transcribed=len(transcribed),
            detected=len(detected),
            waiting=len(waiting),
            failed=len(failures),
            obs_busy=len(obs_busy),
        )
    )

    print(
        f'RESULT nightly ok={1 if ok else 0} ingested={len(ingested_ids)} '
        f'transcribed={len(transcribed)} detected={len(detected)} '
        f'waiting_poison_review={len(waiting)} failed={len(failures)} '
        f'obs_busy={len(obs_busy)} digest="{digest_file}"'
    )
    return 0 if ok else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'LOCAL lane nightly loop: ingest + transcribe, then detection for VODs a human '
            'has poison-reviewed. Never sets poison_reviewed.'
        )
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='print exactly what would be done per VOD and exit, touching nothing',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.dry_run:
        return do_dry_run()
    return run_nightly()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
