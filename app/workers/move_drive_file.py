#!/usr/bin/env python3
"""move_drive_file: move ONE Google Drive file from one folder to another.

WHY THIS EXISTS
----------------
Operator ruling (2026-08-10, verbatim): "after the file in to_clip starts
processing it should be moved to: 19mErWEhWlux_Hw8A7hFE4jYwgWm5mU5X" -- so a
VOD that has started processing no longer sits in to_clip, where a re-run of
the "find clips" button (or the portable/n8n lane's own node 0b, which reads
the same folder) could pick it up again.

THIS IS A MOVE, NEVER A DELETE
-------------------------------
The Drive API call is `PATCH .../files/{id}?addParents=...&removeParents=...`
-- the file gains the destination folder as a parent and loses the source
folder as a parent. Nothing is deleted or trashed; the service account this
worker authenticates as reports `canDelete: false`, and no code path here
issues a delete/trash request.

AUTH IS REUSED, NOT DUPLICATED
-------------------------------
The service-account/JWT/token machinery (load_service_account,
get_access_token, auth_headers, http_json, FetchError, and the
register_secret/scrub secret-hygiene discipline that load_service_account /
get_access_token / emit / emit_err already carry) is IMPORTED from
workers/fetch_drive_file.py -- the proven, already-live worker on the
portable lane's critical path -- and never re-implemented here. See that
file's own docstring for the JWT/openssl/scrubbing contract this worker
inherits unchanged. This file registers no secret of its own: a file id and
a folder id are not secrets.

IDEMPOTENT
----------
If the file is ALREADY parented under --dest-folder-id when this worker
starts, that is SUCCESS (idempotent=1 in the RESULT line), not an error --
this worker is called from an automated pipeline that may retry, and a
re-press of a "find clips" button must never fail because a previous run
already moved the file.

USAGE
    python3 move_drive_file.py --file-id 1AbC... \\
        --dest-folder-id <folder-id> --src-folder-id <folder-id> \\
        --sa-json <path-to-service-account-json>

Prints RESULT move_drive_file LAST, on BOTH success (ok=1) and failure
(ok=0). This differs from fetch_drive_file.py, whose RESULT line only
appears on success: this worker's caller (find_clips_local.py's
move_out_of_to_clip()) is required to treat a move failure as non-fatal to
its own run and needs a parseable outcome even when the move fails, not a
bare traceback. ERROR is also printed to stderr on failure. Exit code is 1
on failure, 0 on success (including the idempotent no-op case).
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.parse

from fetch_drive_file import (
    DRIVE_FILES_URL,
    FetchError,
    auth_headers,
    emit,
    emit_err,
    get_access_token,
    http_json,
    load_service_account,
)


def fetch_file_parents(token: str, file_id: str) -> dict:
    """GET id,name,parents for file_id. Raises FetchError/HttpError/
    TransportError (all defined in fetch_drive_file.py, all funnelled
    through http_json) on any failure -- caught by run()'s broad except."""
    url = '{0}/{1}?{2}'.format(
        DRIVE_FILES_URL, urllib.parse.quote(file_id, safe=''),
        urllib.parse.urlencode({
            'fields': 'id,name,parents',
            'supportsAllDrives': 'true',
        }))
    return http_json(url, headers=auth_headers(token))


def move_file(token: str, file_id: str, dest_folder_id: str,
              src_folder_id: str) -> dict:
    """PATCH the move: add dest_folder_id as a parent, remove src_folder_id
    as a parent. Returns the post-move {'id', 'parents'} Google reports back
    in the SAME response (fields=id,parents), so the caller never has to
    re-fetch to confirm what changed."""
    url = '{0}/{1}?{2}'.format(
        DRIVE_FILES_URL, urllib.parse.quote(file_id, safe=''),
        urllib.parse.urlencode({
            'addParents': dest_folder_id,
            'removeParents': src_folder_id,
            'supportsAllDrives': 'true',
            'fields': 'id,parents',
        }))
    headers = dict(auth_headers(token))
    headers['Content-Type'] = 'application/json'
    return http_json(url, method='PATCH', headers=headers, data=b'{}')


def run(args: argparse.Namespace) -> int:
    started = time.monotonic()
    file_id = args.file_id
    dest = args.dest_folder_id
    src = args.src_folder_id
    name = ''
    parents_before = []  # type: list
    parents_after = []  # type: list
    idempotent = 0

    try:
        sa = load_service_account(args.sa_json)
        token = get_access_token(sa)

        before = fetch_file_parents(token, file_id)
        name = before.get('name') or ''
        parents_before = before.get('parents') or []

        if dest in parents_before:
            # Already where it needs to be -- success, not an error. A
            # re-press of the caller's button (or a retry after a warned
            # failure) must not fail here.
            idempotent = 1
            parents_after = list(parents_before)
        else:
            after = move_file(token, file_id, dest, src)
            parents_after = after.get('parents') or []
            if dest not in parents_after:
                raise FetchError(
                    'move reported success but dest_folder_id {0} is not in '
                    'the resulting parents {1} for file_id={2}'.format(
                        dest, parents_after, file_id))

        elapsed = time.monotonic() - started
        emit(
            'RESULT move_drive_file file_id={0} ok=1 idempotent={1} '
            'name="{2}" src_folder_id={3} dest_folder_id={4} '
            'parents_before={5} parents_after={6} elapsed_s={7:.3f} '
            'note=""'.format(
                file_id, idempotent, name, src, dest,
                ','.join(parents_before), ','.join(parents_after), elapsed))
        return 0

    except Exception as exc:  # noqa: BLE001 -- ANY failure here must produce
        # a parseable ok=0 RESULT line, never a bare traceback: the caller is
        # required to treat a move failure as non-fatal to its own run and
        # needs a clear signal to warn on (brief, 2026-08-10).
        elapsed = time.monotonic() - started
        detail = str(exc).replace('"', "'").replace('\n', ' ')
        emit_err('ERROR: {0}'.format(exc))
        emit(
            'RESULT move_drive_file file_id={0} ok=0 idempotent=0 '
            'name="{1}" src_folder_id={2} dest_folder_id={3} '
            'parents_before={4} parents_after={5} elapsed_s={6:.3f} '
            'note="{7}"'.format(
                file_id, name, src, dest,
                ','.join(parents_before), ','.join(parents_after), elapsed,
                detail))
        return 1


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Move one Google Drive file from one folder to another '
                    '(addParents/removeParents; never deletes).')
    parser.add_argument('--file-id', required=True,
                        help='Drive file id to move')
    parser.add_argument('--dest-folder-id', required=True,
                        help='folder id to ADD as a parent (the destination)')
    parser.add_argument('--src-folder-id', required=True,
                        help='folder id to REMOVE as a parent (the source)')
    parser.add_argument('--sa-json', required=True,
                        help='service-account JSON path')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    return run(parse_args(argv))


if __name__ == '__main__':
    sys.exit(main())
