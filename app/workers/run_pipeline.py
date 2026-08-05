#!/usr/bin/env python3
"""run_pipeline: LOCAL lane orchestrator. Runs the detection workers in sequence for one VOD.

Does not reimplement any worker: each stage is invoked as a subprocess with the same
interpreter running this file, with cwd set to the app dir so a relative CLPR_DB_PATH
(default ./clpr.db) resolves exactly as it would for a manual run.

cut_all_approved.py is DELIBERATELY EXCLUDED from this pipeline: cutting requires
candidates at state='approved', which requires human review. The operator is the publish
gate, and a nightly loop that auto-cut would defeat that gate.

Stops on the first failing stage. A stage that exits 0 but prints no line starting with
'RESULT ' is treated as a failure: exit 0 only proves the subprocess did not throw, while
the RESULT line is the witness that the worker actually reached its own end.

Exits non-zero on any failure. Prints machine-parseable RESULT line last.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

STAGES = [
    'transcribe',
    'zebra_detect',
    'audio_energy',
    'transcript_signal',
    'score_fusion',
]

OBS_REFUSAL_MARKER = 'Refusing'
OVERRIDE_ENV_VAR = 'CLPR_ALLOW_DURING_STREAM'


@dataclass
class StageOutcome:
    stage: str
    cmd: list[str]
    cwd: str
    started_at: str
    finished_at: str
    exit_code: int
    stdout: str
    stderr: str
    result_line: Optional[str]
    ok: bool
    failure_reason: Optional[str]
    obs_busy: bool


def get_db_path() -> str:
    return os.environ.get('CLPR_DB_PATH', './clpr.db')


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def app_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def find_result_line(stdout: str, stderr: str) -> Optional[str]:
    """Return the last line starting with 'RESULT ' from the captured output, else None."""
    found: Optional[str] = None
    for stream in (stdout, stderr):
        for raw_line in stream.splitlines():
            line = raw_line.rstrip('\r')
            if line.startswith('RESULT '):
                found = line
    return found


def stage_command(stage: str, vod_id: int) -> list[str]:
    return [sys.executable, os.path.join('workers', f'{stage}.py'), '--vod-id', str(vod_id)]


def run_stage(stage: str, cmd: list[str], cwd: str) -> StageOutcome:
    """Execute one stage and classify the outcome. Never raises on stage failure."""
    started_at = utc_now_iso()
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    finished_at = utc_now_iso()

    stdout = proc.stdout or ''
    stderr = proc.stderr or ''
    result_line = find_result_line(stdout, stderr)

    failure_reason: Optional[str] = None
    if proc.returncode != 0:
        failure_reason = f'non-zero exit code {proc.returncode}'
    elif result_line is None:
        # GATE B: exit 0 is not sufficient. No RESULT line means the worker never
        # reached its own end, so this is a failure exactly like a non-zero exit.
        failure_reason = 'exited 0 but produced NO line starting with "RESULT " (missing witness)'

    obs_busy = failure_reason is not None and OBS_REFUSAL_MARKER in stderr

    return StageOutcome(
        stage=stage,
        cmd=list(cmd),
        cwd=cwd,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=proc.returncode,
        stdout=stdout,
        stderr=stderr,
        result_line=result_line,
        ok=failure_reason is None,
        failure_reason=failure_reason,
        obs_busy=obs_busy,
    )


def resolve_start_index(from_stage: Optional[str]) -> int:
    if from_stage is None:
        return 0
    if from_stage not in STAGES:
        raise RuntimeError(
            f'unknown stage for --from: {from_stage!r}. '
            f'Valid stages, in pipeline order: {", ".join(STAGES)}'
        )
    return STAGES.index(from_stage)


def log_path_for(vod_id: int) -> Path:
    return app_dir() / 'logs' / f'run_vod{vod_id}_{utc_stamp()}.log'


def write_log(handle, text: str) -> None:
    handle.write(text if text.endswith('\n') else text + '\n')
    handle.flush()


def log_stage(handle, outcome: StageOutcome) -> None:
    write_log(handle, '')
    write_log(handle, f'--- stage: {outcome.stage}')
    write_log(handle, f'cmd: {outcome.cmd}')
    write_log(handle, f'cwd: {outcome.cwd}')
    write_log(handle, f'started_at: {outcome.started_at}')
    write_log(handle, f'finished_at: {outcome.finished_at}')
    write_log(handle, f'exit_code: {outcome.exit_code}')
    write_log(handle, f'result_line: {outcome.result_line if outcome.result_line is not None else "<none>"}')
    if not outcome.ok:
        write_log(handle, f'FAILURE: {outcome.failure_reason}')
        write_log(handle, f'obs_busy: {1 if outcome.obs_busy else 0}')
        write_log(handle, '--- stdout (full) ---')
        write_log(handle, outcome.stdout)
        write_log(handle, '--- stderr (full) ---')
        write_log(handle, outcome.stderr)


def report_failure(outcome: StageOutcome) -> None:
    """GATE A + GATE C: print the failing stage, its exit code, and its FULL output verbatim."""
    print('', file=sys.stderr)
    print('=' * 72, file=sys.stderr)
    print(f'PIPELINE FAILED at stage: {outcome.stage}', file=sys.stderr)
    print(f'exit_code: {outcome.exit_code}', file=sys.stderr)
    print(f'reason: {outcome.failure_reason}', file=sys.stderr)
    print(f'cmd: {outcome.cmd}', file=sys.stderr)
    print('--- stage stdout (verbatim, untruncated) ---', file=sys.stderr)
    print(outcome.stdout, file=sys.stderr)
    print('--- stage stderr (verbatim, untruncated) ---', file=sys.stderr)
    print(outcome.stderr, file=sys.stderr)
    print('=' * 72, file=sys.stderr)

    if outcome.obs_busy:
        print('', file=sys.stderr)
        print('OBS BUSY: this is NOT a crash.', file=sys.stderr)
        print(
            f'The pipeline stopped at stage "{outcome.stage}" because OBS is streaming or recording, '
            'and that stage refused to run.',
            file=sys.stderr,
        )
        print('This is the D-009 OBS gate working exactly as designed.', file=sys.stderr)
        print(
            f'To proceed deliberately anyway, the override is {OVERRIDE_ENV_VAR}=1 '
            '(set by a human, on purpose).',
            file=sys.stderr,
        )
        print(
            f'run_pipeline will never set {OVERRIDE_ENV_VAR} for you and will never retry a refused stage.',
            file=sys.stderr,
        )
        print('', file=sys.stderr)

    print(f'NOT RUN (pipeline stopped): stages after {outcome.stage} were skipped.', file=sys.stderr)


def do_dry_run(vod_id: int, start_index: int) -> int:
    planned = STAGES[start_index:]
    print(f'DRY RUN vod={vod_id}: the following {len(planned)} command(s) WOULD run, in this order:')
    print(f'cwd: {app_dir()}')
    print(f'CLPR_DB_PATH: {get_db_path()}')
    if start_index > 0:
        print(f'skipped (before --from): {", ".join(STAGES[:start_index])}')
    for i, stage in enumerate(planned, start=1):
        cmd = stage_command(stage, vod_id)
        print(f'{i}. {stage}: {" ".join(cmd)}')
    print('DRY RUN complete: nothing was executed, no log written.')
    return 0


def run(vod_id: int, from_stage: Optional[str], dry_run: bool) -> int:
    start_index = resolve_start_index(from_stage)

    if dry_run:
        return do_dry_run(vod_id, start_index)

    cwd = str(app_dir())
    planned = STAGES[start_index:]
    skipped = start_index

    log_file = log_path_for(vod_id)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, 'w', encoding='utf-8') as handle:
        write_log(handle, f'run_pipeline vod={vod_id}')
        write_log(handle, f'started_at: {utc_now_iso()}')
        write_log(handle, f'cwd: {cwd}')
        write_log(handle, f'python: {sys.executable}')
        write_log(handle, f'CLPR_DB_PATH: {get_db_path()}')
        write_log(handle, f'stages_planned: {", ".join(planned)}')
        write_log(handle, f'stages_skipped: {", ".join(STAGES[:start_index]) if skipped else "<none>"}')

        stages_run = 0
        for stage in planned:
            cmd = stage_command(stage, vod_id)
            print(f'STAGE {stage} START cmd={" ".join(cmd)}')
            outcome = run_stage(stage, cmd, cwd)
            stages_run += 1
            log_stage(handle, outcome)

            if not outcome.ok:
                write_log(handle, '')
                write_log(handle, f'PIPELINE FAILED at stage {outcome.stage}: {outcome.failure_reason}')
                write_log(handle, f'finished_at: {utc_now_iso()}')
                report_failure(outcome)
                print(f'log: {log_file}', file=sys.stderr)
                print(
                    f'RESULT run_pipeline vod={vod_id} ok=0 failed_stage={outcome.stage} '
                    f'exit_code={outcome.exit_code} log="{log_file}"',
                    file=sys.stderr,
                )
                return outcome.exit_code if outcome.exit_code != 0 else 1

            print(f'STAGE {stage} OK {outcome.result_line}')

        write_log(handle, '')
        write_log(handle, f'PIPELINE OK stages_run={stages_run} stages_skipped={skipped}')
        write_log(handle, f'finished_at: {utc_now_iso()}')

    print(f'log: {log_file}')
    print(
        f'RESULT run_pipeline vod={vod_id} ok=1 stages_run={stages_run} '
        f'stages_skipped={skipped} log="{log_file}"'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Run the LOCAL lane detection workers in sequence for one VOD (does not cut clips)'
    )
    parser.add_argument('--vod-id', type=int, required=True)
    parser.add_argument(
        '--from',
        dest='from_stage',
        type=str,
        default=None,
        help=f'resume at this stage, skipping earlier ones. One of: {", ".join(STAGES)}',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='print the exact commands that would run, in order, and exit without running any',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(args.vod_id, args.from_stage, args.dry_run)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
