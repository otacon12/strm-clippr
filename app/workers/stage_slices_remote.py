#!/usr/bin/env python3
"""stage_slices_remote: cut one recording's candidate slices ON THE MAC and push
them (plus their geometry sidecars) to the n8n server (D-058).

WHY THIS EXISTS (D-057, measured twice): n8n's Google Drive node fetches with
`encoding: 'arraybuffer'` — it materializes the WHOLE file in the JS heap before
storing it, so a 2.5 GB archive video OOM-kills the container at its 4 GB cap no
matter what N8N_DEFAULT_BINARY_DATA_MODE says. The HTTP Request node buffers
identically. NO n8n node can move that video, and trying has repeatedly taken
the operator's production instance down. D-058 ruling (operator, 2026-08-07):
THE VIDEO NEVER TOUCHES n8n. The Mac already holds it; the Mac cuts the slices
and pushes only those. Measured for recording 19: 15 candidates / ~621 padded
seconds / ~357 MB, versus a 2.5 GB download that cannot succeed.

ONE TRUTH FOR GEOMETRY (D-055): this worker does NOT compute any slice bounds.
It shells out to the existing app/workers/slice_candidates.py, which owns the
SLICE_PAD_S (10 s, from the IMMUTABLE ORIGINAL window) staging geometry via
slice_geometry.py and writes the c<id>.json sidecar witness that
render_from_slice.py REQUIRES. Re-deriving any of that here would create a
second copy of the geometry, and two copies always drift.

WHAT IT DOES

1. Resolve the LOCAL video.
   --video PATH  -> used as given; must exist and be readable (the operator is
                    asserting a value, charter 3.5 — no second-guessing it).
   no --video    -> recordings.path for that recording, used ONLY if it exists
                    locally AND ffprobe finds a video stream in it. Otherwise it
                    FAILS LOUD naming --video. Why the extra check: the `path`
                    column can hold a SERVER-side audio path for n8n-analyzed
                    recordings (e.g. .../clpr/media/<name>.wav), and silently
                    slicing an audio file — or a path that means something else
                    on this machine — is exactly the kind of quiet wrong answer
                    the north star forbids. That column's dual meaning is a
                    KNOWN OPEN ITEM; this worker refuses rather than papers
                    over it.

2. Cut slices LOCALLY: run slice_candidates.py as a subprocess with
   CLPR_SLICES_DIR pointed at a local staging dir. Its stdout/stderr are echoed
   line-by-line with a `CHILD ` prefix and its RESULT line is re-surfaced.
   Staging dir = <base>/<recording_id>, base = CLPR_LOCAL_SLICES_DIR when set
   else ~/.clpr/slices_staging. NOTE the override replaces the BASE only; the
   <recording_id> component is always appended, so per-recording isolation
   survives the override.

3. Resolve the server destination DYNAMICALLY over ssh, FROM THE RUNNING
   CONTAINER. The n8n container name changes on every deploy and the Coolify
   volume id is not a constant either, so nothing here is hardcoded. We ask
   docker which container is running (`docker ps`, names starting `n8n-`) and
   read the SOURCE of its actual /home/node/.n8n mount, because that is the
   only thing that answers the real question: "which directory does the n8n
   process READ?". Scanning volumes for a `_data/clpr` directory answers a
   different, weaker question — "which volume happens to contain a clpr dir" —
   and a SECOND n8n stack exists on this host, so a re-home would resolve a
   path nobody reads and every render would then die on SLICE_SIDECAR_MISSING
   with the push reporting a clean success. The volume scan survives only as a
   documented FALLBACK for when no n8n container is running. Both paths fail
   loud on zero and on ambiguity; neither ever guesses.

4. Push with rsync over ssh, then `chown -R 1000:1000` (uid/gid `node` inside
   the container) the whole destination directory.

   CHOWN IS UNCONDITIONAL AND RECURSIVE, NOT PER-PUSHED-FILE. If an earlier run
   rsynced successfully and its chown then failed, a chown limited to
   newly-pushed files could never repair it: every later run sees matching
   sizes, pushes nothing, chowns nothing, and reports success while the files
   stay unreadable by the container forever. So ownership is re-asserted on
   every non-dry run even when the push list is empty, and step 5 VERIFIES it
   rather than assuming it.

   SKIP RULE — MP4s ONLY, BY EXPLICIT SIZE COMPARE, NOT `--ignore-existing`. We
   list the remote dir's file sizes first and skip only mp4s whose remote size
   equals the local size. `--ignore-existing` was rejected because it skips a
   remote file that EXISTS AT ALL, including a truncated/partial one from an
   aborted push — the precise failure that would leave an unplayable slice on
   the server forever while every run reported "already there". Size equality is
   the property; existence is a proxy for it. (rsync writes to a temp name and
   renames, so an interrupted push never leaves a partial under the final
   name; the size compare defends against everything else.)

   SIDECARS ARE PUSHED UNCONDITIONALLY — NEVER SIZE-SKIPPED (sweep finding F1).
   A sidecar is ~200 bytes, so skipping one saves nothing, and its BYTE SIZE IS
   NOT A FINGERPRINT OF ITS GEOMETRY: two sidecars for the same candidate with
   DIFFERENT abs_start_s/abs_end_s are readily both exactly 209 bytes, and the
   real staged set for recording 19 holds 15 sidecars with only 10 distinct
   sizes. Under a size-skip, a RE-CUT slice would be repushed (its mp4 size
   changed) while its sidecar was skipped as "already correct" — pairing the NEW
   slice with the OLD geometry witness. That is precisely the silent wrong
   answer render_from_slice's sidecar check exists to prevent, and it would be
   invisible because every file's size matched.

   ORDER: mp4s first, sidecars second, as two rsync waves. slice_candidates
   writes the sidecar LAST on purpose — it is the commit marker, and
   slice-without-sidecar is the self-healing restage state. A single rsync
   would transfer alphabetically (c102.json before c102.mp4) and invert that,
   so an interrupted push would leave sidecar-without-slice, which reads as
   "done" to nothing and as SLICE_MISSING to the renderer. Two waves preserve
   the local invariant on the server.

5. VERIFY AFTER PUSH — never assume. Re-list the remote dir's OWNER, GROUP and
   SIZE (`stat -c '%u %g %s'`) and require, for EVERY candidate, that the slice
   and its sidecar are both present, both byte-size-identical to the local file,
   and both owned by 1000:1000. Any mismatch, any missing file, any wrong owner
   is counted as a failure and the process exits NONZERO. A partial push must
   never look like a clean one — and neither must an unreadable one, which is
   why ownership is checked here and not merely commanded in step 4.

6. RESULT line last (sibling format), ERROR lines to stderr, honest exit codes.

   RESULT stage_slices_remote recording=N sliced=X skipped_existing=Y pushed=Z \\
       skipped_remote=W bytes_pushed=B failed=F

   UNITS (they are deliberately not all the same): `sliced` and
   `skipped_existing` are CANDIDATE counts taken verbatim from the child's own
   RESULT line. `pushed` is a FILE count (an mp4 and its sidecar are two files).
   `skipped_remote` is a FILE count too, but only MP4s can ever appear in it —
   sidecars are never size-skipped (see step 4, F1), so a fully-up-to-date run
   reports skipped_remote == candidate count and pushed == candidate count.
   `bytes_pushed` is the summed local size of the
   pushed files. `failed` is a CANDIDATE count: candidates that did not end up
   correctly staged AND verified on the server, including the ones the child
   itself failed to cut.

   --dry-run does everything up to the push — it really cuts the slices, really
   resolves the destination over ssh, really reads the remote sizes — and then
   reports what WOULD move on a `DRYRUN would_push=... would_skip_remote=...
   would_push_bytes=...` line. RESULT keeps reporting ACTUALS, so a dry run
   prints pushed=0 bytes_pushed=0: a RESULT line always describes what
   happened, never what was contemplated. A dry run makes no change of any kind
   on the server (it does not even mkdir; it prints WOULD_MKDIR instead).

Env: CLPR_DB_URL (via the shared adapter app/workers/db.py), CLPR_SERVER_SSH
(the ssh host alias, default `n8nserver`), CLPR_LOCAL_SLICES_DIR (optional
staging base). Env comes from the caller — this worker never parses a .env
file, same as every sibling.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

import db
import slice_candidates

# ONE TRUTH PER THING (charter gate 1). This constant decides which candidates
# get pushed; slice_candidates decides which candidates get CUT. A hand-copied
# second tuple here would drift silently and the drift's failure mode is
# candidates that were cut and then never shipped (or shipped and never cut),
# with nothing anywhere reporting an error. Take it from the owner, by
# reference, so the two cannot disagree.
SLICE_STATES = slice_candidates.SLICE_STATES

DEFAULT_SSH_HOST = 'n8nserver'
DEFAULT_STAGING_BASE = Path.home() / '.clpr' / 'slices_staging'

# The container runs as uid/gid 1000 (`node`); pushed files must be readable by it.
REMOTE_UID_GID = '1000:1000'

WORKERS_DIR = Path(__file__).resolve().parent
SLICE_CANDIDATES = WORKERS_DIR / 'slice_candidates.py'


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------------------
# 1. local video resolution
# ---------------------------------------------------------------------------

def has_video_stream(path: Path) -> bool:
    """True iff ffprobe reports a video stream. This is the PROPERTY; a file
    extension is only a proxy for it (a .mp4 container can hold audio only, and
    the `path` column has held audio paths)."""
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_type',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return False
    return proc.stdout.strip() == 'video'


def resolve_local_video(recording_id: int, cli_video: Path | None,
                        db_path: str | None) -> Path:
    """Pure resolver (no I/O against the database) so both branches are directly
    testable. `db_path` is the recordings.path value, or None when --video was
    given and the column was never read.

    --video: the operator supplied it, so it is law (charter 3.5) — we only
    confirm it exists and is readable, and report the failure rather than
    substituting anything.
    """
    if cli_video is not None:
        video = Path(cli_video).expanduser()
        if not video.is_file():
            raise RuntimeError(
                f'--video path does not exist or is not a file: {video}'
            )
        if not os.access(video, os.R_OK):
            raise RuntimeError(f'--video path is not readable: {video}')
        print(f'VIDEO_SOURCE arg path="{video}"')
        return video

    if db_path is None or str(db_path).strip() == '':
        raise RuntimeError(
            f'recording {recording_id} has no path recorded; pass the local '
            'archive video explicitly with --video PATH'
        )

    video = Path(str(db_path)).expanduser()
    if not video.is_file():
        raise RuntimeError(
            f'recordings.path for recording {recording_id} is not a file on '
            f'THIS machine: "{db_path}". That column can hold a SERVER-side '
            'audio path for n8n-analyzed recordings, so it cannot be trusted '
            'as a local video. Pass the local archive video explicitly with '
            '--video PATH.'
        )
    if not has_video_stream(video):
        raise RuntimeError(
            f'recordings.path for recording {recording_id} exists locally but '
            f'ffprobe finds no video stream in it: "{db_path}". That column can '
            'hold a SERVER-side audio path for n8n-analyzed recordings. Pass '
            'the local archive video explicitly with --video PATH.'
        )
    print(f'VIDEO_SOURCE recordings.path path="{video}"')
    return video


# ---------------------------------------------------------------------------
# database reads
# ---------------------------------------------------------------------------

def fetch_recording_and_candidates(recording_id: int) -> tuple[str | None, list[int]]:
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT path FROM recordings WHERE id = %s', (recording_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f'recording not found: recording_id={recording_id}')
        rec_path = str(row[0]) if row[0] is not None else None
        cur.execute(
            'SELECT id FROM clip_candidates WHERE recording_id = %s AND state IN %s '
            'ORDER BY id',
            (recording_id, SLICE_STATES),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()
    return rec_path, ids


# ---------------------------------------------------------------------------
# 2. local slicing (delegated — one truth for geometry)
# ---------------------------------------------------------------------------

def staging_dir_for(recording_id: int) -> Path:
    base = os.environ.get('CLPR_LOCAL_SLICES_DIR', '').strip()
    base_path = Path(base).expanduser() if base else DEFAULT_STAGING_BASE
    return base_path / str(recording_id)


def parse_child_result(line: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for token in line.split():
        if '=' not in token:
            continue
        key, _, value = token.partition('=')
        try:
            out[key] = int(value)
        except ValueError:
            continue
    return out


def run_slice_candidates(recording_id: int, video: Path, staging: Path) -> dict[str, int]:
    """Invoke the existing stager. Geometry is ITS job, not ours."""
    if not SLICE_CANDIDATES.is_file():
        raise RuntimeError(f'slice_candidates.py not found at {SLICE_CANDIDATES}')
    staging.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env['CLPR_SLICES_DIR'] = str(staging)

    cmd = [sys.executable, str(SLICE_CANDIDATES),
           '--vod-id', str(recording_id), '--video', str(video)]
    print(f'CMD {cmd}')
    print(f'CHILD_ENV CLPR_SLICES_DIR="{staging}"')

    proc = subprocess.Popen(
        cmd, cwd=str(WORKERS_DIR), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    result_line = None
    assert proc.stdout is not None
    for raw in proc.stdout:
        line = raw.rstrip('\n')
        print(f'CHILD {line}')
        sys.stdout.flush()
        if line.startswith('RESULT slice_candidates '):
            result_line = line
    rc = proc.wait()
    print(f'CHILD_EXIT_CODE {rc}')

    if result_line is None:
        raise RuntimeError(
            f'slice_candidates produced no RESULT line (exit={rc}) — refusing to '
            'guess what it staged'
        )
    print(f'CHILD_RESULT {result_line}')
    parsed = parse_child_result(result_line)
    for key in ('sliced', 'skipped_existing', 'failed'):
        if key not in parsed:
            raise RuntimeError(
                f'slice_candidates RESULT line is missing {key}: "{result_line}"'
            )
    if rc != 0 and parsed['failed'] == 0:
        raise RuntimeError(
            f'slice_candidates exited {rc} but reported failed=0: "{result_line}"'
        )
    return parsed


# ---------------------------------------------------------------------------
# 3-4. server side
# ---------------------------------------------------------------------------

def ssh_host() -> str:
    return os.environ.get('CLPR_SERVER_SSH', '').strip() or DEFAULT_SSH_HOST


def ssh_run(host: str, script: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20', host, script]
    print(f'SSH {host} :: {script}')
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f'ssh command failed exit={proc.returncode} host={host} '
            f'script="{script}" stdout="{proc.stdout.strip()}" '
            f'stderr="{proc.stderr.strip()}"'
        )
    return proc


VOLUME_PROBE = (
    'for v in $(docker volume ls -q); do '
    'mp=$(docker volume inspect -f "{{.Mountpoint}}" "$v" 2>/dev/null); '
    '[ -n "$mp" ] && [ -d "$mp/clpr" ] && echo "$mp"; '
    'done'
)

# The container's own name for the n8n data directory. Its MOUNT SOURCE on the
# host is the only authoritative answer to "which host directory does the n8n
# process read?".
CONTAINER_N8N_HOME = '/home/node/.n8n'

# Coolify names every service container `<service>-<stack-uuid>-<n>`, so the
# running n8n container always starts with this. Deliberately matched in PYTHON,
# not with a remote `grep`: grep exits 1 when nothing matches, which ssh_run
# would raise on, turning "no n8n container is running" (a fallback condition)
# into a hard crash.
CONTAINER_NAME_PREFIX = 'n8n-'


def resolve_via_container(host: str) -> str | None:
    """Resolve the clpr working directory from the RUNNING n8n container's real
    /home/node/.n8n mount source. Returns None (caller falls back) only when no
    n8n container is running. Fails loud on ambiguity and on a container without
    that mount — a wrong destination here pushes slices somewhere nothing reads,
    which surfaces much later as SLICE_SIDECAR_MISSING at render time while this
    tool reports a clean push."""
    proc = ssh_run(host, "docker ps --format '{{.Names}}'")
    names = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    matches = [n for n in names if n.startswith(CONTAINER_NAME_PREFIX)]
    print(f'N8N_CONTAINERS {matches}')
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        raise RuntimeError(
            f'{len(matches)} running n8n containers on {host} ({matches}) — a '
            'second n8n stack exists on this host, so which one owns the clpr '
            'directory is ambiguous; refusing to guess'
        )
    container = matches[0]
    fmt = ('{{range .Mounts}}{{if eq .Destination "' + CONTAINER_N8N_HOME +
           '"}}{{.Source}}{{println}}{{end}}{{end}}')
    proc = ssh_run(host, f'docker inspect -f {shlex.quote(fmt)} {shlex.quote(container)}')
    sources = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if len(sources) != 1:
        raise RuntimeError(
            f'container {container} on {host} has {len(sources)} mounts at '
            f'{CONTAINER_N8N_HOME} ({sources}) — cannot determine which host '
            'directory n8n actually reads; refusing to guess'
        )
    print(f'CONTAINER_MOUNT container={container} {CONTAINER_N8N_HOME} -> {sources[0]}')
    return sources[0]


def resolve_via_volume_scan(host: str) -> str:
    """FALLBACK ONLY, used when no n8n container is running. Finds the docker
    volume whose _data/clpr exists. This proves "exactly one volume contains a
    clpr directory", which is a PROXY for "this is what n8n reads" — it is
    correct today and would stay correct through a redeploy, but it cannot
    distinguish two n8n stacks. Zero or multiple matches => fail loud."""
    proc = ssh_run(host, VOLUME_PROBE)
    mounts = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    print(f'VOLUME_MATCHES {mounts}')
    if len(mounts) == 0:
        raise RuntimeError(
            f'no docker volume on {host} has a _data/clpr directory — cannot '
            'resolve the clpr working directory; refusing to guess'
        )
    if len(mounts) > 1:
        raise RuntimeError(
            f'{len(mounts)} docker volumes on {host} have a _data/clpr directory '
            f'({mounts}) — ambiguous destination; refusing to guess'
        )
    return mounts[0]


def resolve_remote_slices_dir(host: str) -> str:
    """<n8n data dir>/clpr/media/slices, resolved from the RUNNING container
    first and the volume scan only as a fallback. Nothing is hardcoded: the
    container name changes on every deploy and the Coolify volume id is not a
    constant either."""
    base = resolve_via_container(host)
    if base is None:
        print('RESOLVE_FALLBACK no running n8n- container; scanning docker volumes')
        base = resolve_via_volume_scan(host)
        source = 'volume_scan'
    else:
        source = 'running_container'
    dest = f'{base}/clpr/media/slices'
    print(f'REMOTE_SLICES_DIR {dest} via={source}')
    return dest


def remote_stats(host: str, dest: str) -> dict[str, tuple[int, int, int]]:
    """{filename: (uid, gid, bytes)} for the destination dir. A missing dir
    reads as empty (exit 0, no output) so a first run's skip accounting still
    works and a dry run never has to create anything.

    ONE function serves both the pre-push skip decision and the post-push
    verification on purpose: two listers would be two formats, and the day they
    drift is the day the verify silently checks something the push never set."""
    q = shlex.quote(dest)
    script = (
        f'cd {q} 2>/dev/null || exit 0; '
        'for f in *; do [ -f "$f" ] && echo "$(stat -c \'%u %g %s\' -- "$f") $f"; done'
    )
    proc = ssh_run(host, script)
    stats: dict[str, tuple[int, int, int]] = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(' ', 3)
        if len(parts) != 4:
            continue
        uid_s, gid_s, size_s, name = parts
        try:
            stats[name] = (int(uid_s), int(gid_s), int(size_s))
        except ValueError:
            continue
    return stats


def remote_size_of(stat: tuple[int, int, int] | None) -> int | None:
    return None if stat is None else stat[2]


def rsync_push(host: str, dest: str, files: list[Path], label: str) -> None:
    if not files:
        return
    # NO --info=/--stats: macOS ships openrsync ("rsync version 2.6.9
    # compatible"), which rejects --info= outright and whose flag set differs
    # from modern rsync's. Only the portable core (-a, -e) is used; the
    # post-push stat verification below is what proves the transfer, so no
    # rsync-reported statistic is load-bearing anyway.
    cmd = ['rsync', '-a', '-e', 'ssh -o BatchMode=yes']
    cmd += [str(f) for f in files]
    cmd += [f'{host}:{shlex.quote(dest)}/']
    print(f'RSYNC[{label}] {len(files)} file(s) -> {host}:{dest}/')
    print(f'CMD {cmd}')
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout.strip():
        for line in proc.stdout.strip().splitlines():
            print(f'RSYNC_OUT {line}')
    if proc.returncode != 0:
        raise RuntimeError(
            f'rsync failed exit={proc.returncode} wave={label} '
            f'stderr="{proc.stderr.strip()}"'
        )


def remote_chown(host: str, dest: str) -> None:
    """chown -R the WHOLE destination, on EVERY non-dry run, whether or not
    anything was pushed (sweep finding F3).

    The previous per-pushed-file chown could not repair itself: if an earlier
    run's rsync succeeded and its chown ssh then failed, every later run found
    matching sizes, pushed nothing, chowned nothing, and reported success while
    the container could not read the files. Recursive + unconditional costs one
    ssh round trip and makes the repair automatic. It is still not TRUSTED —
    the post-push verify re-reads the real owner."""
    ssh_run(host, f'chown -R {REMOTE_UID_GID} -- {shlex.quote(dest)}')


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def run(recording_id: int, cli_video: Path | None, dry_run: bool) -> int:
    host = ssh_host()
    print(f'MODE {"dry-run" if dry_run else "push"}')
    print(f'SSH_HOST {host}')

    db_path, candidate_ids = fetch_recording_and_candidates(recording_id)
    print(f'CANDIDATES recording={recording_id} slice_eligible={len(candidate_ids)} '
          f'ids={candidate_ids}')

    video = resolve_local_video(recording_id, cli_video, db_path if cli_video is None else None)

    staging = staging_dir_for(recording_id)
    print(f'STAGING_DIR {staging}')

    child = run_slice_candidates(recording_id, video, staging)
    sliced = child['sliced']
    skipped_existing = child['skipped_existing']
    child_failed = child['failed']

    dest = resolve_remote_slices_dir(host)
    existing = remote_stats(host, dest)
    print(f'REMOTE_EXISTING files={len(existing)}')

    # Build the per-candidate file plan from the DATABASE, not a glob of the
    # staging dir: a candidate rejected since an earlier staging run still has
    # a stale c<id>.mp4 on disk and must not be shipped.
    failed_candidates: list[int] = []
    push_mp4: list[Path] = []
    push_json: list[Path] = []
    skipped_remote = 0
    ok_candidates: list[int] = []

    for cid in candidate_ids:
        mp4 = staging / f'c{cid}.mp4'
        sidecar = staging / f'c{cid}.json'
        if not mp4.is_file() or mp4.stat().st_size == 0:
            eprint(f'ERROR: MISSING_LOCAL_SLICE candidate={cid} path={mp4}')
            failed_candidates.append(cid)
            continue
        if not sidecar.is_file() or sidecar.stat().st_size == 0:
            eprint(
                f'ERROR: MISSING_LOCAL_SIDECAR candidate={cid} path={sidecar} — '
                'the sidecar is the geometry witness render_from_slice REQUIRES; '
                'refusing to push an unwitnessed slice'
            )
            failed_candidates.append(cid)
            continue
        # THE PROPERTY, NOT A PROXY (sweep finding F2). "the file exists and is
        # non-empty" says nothing about whether it WITNESSES THIS SLICE. Reuse
        # slice_candidates' own validator — schema, candidate_id and finite
        # coordinates — so an INVALID sidecar can never be pushed. This state is
        # reachable today: slice_candidates RESTAGES when a sidecar is invalid,
        # so if that re-cut then fails, the stale mp4 and the stale invalid
        # sidecar both survive on disk and would otherwise be shipped as a pair.
        if slice_candidates.load_valid_sidecar(sidecar, cid) is None:
            eprint(
                f'ERROR: INVALID_LOCAL_SIDECAR candidate={cid} path={sidecar} — '
                'it exists but does not validate (schema, candidate_id or '
                'non-finite bounds); it is not a witness for THIS slice, so the '
                'slice is unwitnessed and will not be pushed'
            )
            failed_candidates.append(cid)
            continue
        ok_candidates.append(cid)

        # mp4: size-compare skip (a re-push of ~24 MB is worth avoiding).
        mp4_size = mp4.stat().st_size
        mp4_remote = remote_size_of(existing.get(mp4.name))
        if mp4_remote == mp4_size:
            skipped_remote += 1
            print(f'SKIP_REMOTE file={mp4.name} bytes={mp4_size}')
        else:
            if mp4_remote is not None:
                print(f'REPUSH file={mp4.name} local_bytes={mp4_size} '
                      f'remote_bytes={mp4_remote} reason=size_mismatch')
            push_mp4.append(mp4)

        # sidecar: ALWAYS pushed, never size-skipped (sweep finding F1). Sidecar
        # sizes COLLIDE — two sidecars for the same candidate carrying different
        # geometry are readily both exactly 209 bytes, and recording 19's real
        # staged set has 15 sidecars across only 10 distinct sizes. A size-skip
        # would therefore leave a RE-CUT slice (repushed, its mp4 size moved)
        # paired with the OLD geometry witness, and every size check would agree
        # that everything was fine. ~200 bytes buys that away.
        push_json.append(sidecar)
        print(f'ALWAYS_PUSH_SIDECAR file={sidecar.name} '
              f'bytes={sidecar.stat().st_size} reason=size_is_not_a_geometry_fingerprint')

    planned = push_mp4 + push_json
    planned_bytes = sum(p.stat().st_size for p in planned)
    for path in planned:
        print(f'{"WOULD_PUSH" if dry_run else "PUSH"} file={path.name} '
              f'bytes={path.stat().st_size}')

    pushed = 0
    bytes_pushed = 0

    if dry_run:
        print(f'WOULD_MKDIR {dest}')
        print(f'WOULD_CHOWN -R {REMOTE_UID_GID} {dest}')
        print(f'DRYRUN would_push={len(planned)} would_skip_remote={skipped_remote} '
              f'would_push_bytes={planned_bytes}')
    else:
        ssh_run(host, f'mkdir -p -- {shlex.quote(dest)}')
        # mp4 first, sidecar second: the sidecar is the commit marker, exactly
        # as slice_candidates orders it locally.
        rsync_push(host, dest, push_mp4, 'mp4')
        rsync_push(host, dest, push_json, 'sidecar')
        # Unconditional and recursive, even when `planned` is empty — the empty
        # case IS the repair case for a previous run whose chown failed.
        remote_chown(host, dest)
        pushed = len(planned)
        bytes_pushed = planned_bytes

        # ---- VERIFY AFTER PUSH — do not assume rsync or chown did what they
        # said. Size proves the bytes arrived; OWNERSHIP proves the container
        # can read them. Commanding a chown is not evidence that it happened.
        after = remote_stats(host, dest)
        verify_failed: list[int] = []
        for cid in ok_candidates:
            bad = False
            for path in (staging / f'c{cid}.mp4', staging / f'c{cid}.json'):
                local_size = path.stat().st_size
                stat = after.get(path.name)
                if stat is None:
                    eprint(f'ERROR: VERIFY_MISSING candidate={cid} file={path.name} '
                           f'expected_bytes={local_size}')
                    bad = True
                    continue
                uid, gid, remote_size = stat
                if remote_size != local_size:
                    eprint(f'ERROR: VERIFY_SIZE_MISMATCH candidate={cid} '
                           f'file={path.name} local_bytes={local_size} '
                           f'remote_bytes={remote_size}')
                    bad = True
                if f'{uid}:{gid}' != REMOTE_UID_GID:
                    eprint(f'ERROR: VERIFY_OWNERSHIP candidate={cid} '
                           f'file={path.name} remote_owner={uid}:{gid} '
                           f'expected={REMOTE_UID_GID} — the n8n container runs '
                           'as that uid/gid and cannot read this file')
                    bad = True
            if bad:
                verify_failed.append(cid)
            else:
                print(f'VERIFIED candidate={cid} slice+sidecar sizes and '
                      f'ownership {REMOTE_UID_GID} match')
        failed_candidates.extend(verify_failed)

    failed = len(set(failed_candidates))
    # Candidates the child itself could not cut show up as MISSING_LOCAL_SLICE
    # above, so they are already counted; take the larger of the two views so a
    # child failure can never vanish from the verdict.
    failed = max(failed, child_failed)

    result_line = (
        f'RESULT stage_slices_remote recording={recording_id} '
        f'sliced={sliced} skipped_existing={skipped_existing} pushed={pushed} '
        f'skipped_remote={skipped_remote} bytes_pushed={bytes_pushed} failed={failed}'
    )
    print(result_line)
    if failed > 0:
        print(result_line, file=sys.stderr)
        eprint(f'ERROR: {failed} candidate(s) did not complete for recording={recording_id}')
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Cut a recording\'s candidate slices locally and push them '
                    'to the n8n server (D-058: the video never touches n8n)'
    )
    parser.add_argument('--recording-id', type=int, required=True)
    parser.add_argument('--video', type=Path, default=None,
                        help='local archive video; required whenever '
                             'recordings.path is not a local video file')
    parser.add_argument('--dry-run', action='store_true',
                        help='slice locally and resolve/inspect the destination, '
                             'but change nothing on the server')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run(args.recording_id, args.video, args.dry_run)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
