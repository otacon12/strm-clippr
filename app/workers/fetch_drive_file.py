#!/usr/bin/env python3
"""fetch_drive_file: stream ONE Google Drive file to local disk in CONSTANT
memory, from inside the n8n container, using nothing but the Python standard
library plus a shell-out to `openssl` (D-057/D-058, n8n lane).

WHY THIS EXISTS (measured, twice)
---------------------------------
n8n's Google Drive node fetches with `encoding: 'arraybuffer'` — it materializes
the ENTIRE file in the JS heap before it ever reaches binary-data storage — so a
2.70 GB video OOM-kills the container at its 4 GB cap regardless of
N8N_DEFAULT_BINARY_DATA_MODE. The HTTP Request node buffers identically. NO n8n
NODE MAY EVER TOUCH THAT VIDEO; the instance it kills also runs the operator's
client work. Execute Command is exempt because the payload never enters n8n's
runtime — which is what this worker is for.

Operator ruling (2026-08-07, verbatim): "B, server side fetch - this version
needs to stay true to it's portability." So the fetch happens ON THE SERVER and
depends on nothing from the operator's Mac.

THE ONE PROPERTY THAT MATTERS
-----------------------------
**Peak memory is CONSTANT regardless of file size.** The response body is NEVER
read into memory. It is copied to disk in fixed-size chunks (CHUNK_BYTES) by an
explicit read/write loop — the same shape as shutil.copyfileobj(..., length=N),
written out longhand only so a progress hook can sample RSS during the transfer.
Anything that would buffer a body (`resp.read()` with no size, json of a media
response, an in-memory hash of the whole file) is forbidden in this file. The
only unbounded-looking reads are JSON/error bodies, and every one of them is
capped at MAX_JSON_BYTES.

RUNTIME CONSTRAINTS THIS FILE IS BUILT AGAINST (verified read-only 2026-08-07 on
the live container, and on the operator's Mac)
- Debian 12 in-container; python3 3.11.2; openssl 3.0.20; ffmpeg present.
- **pip3 is ABSENT.** Adding any package would force a ~5 GB image rebuild and a
  10+ minute outage, which is forbidden. Hence: stdlib only. No google-auth, no
  cryptography, no requests.
- The Mac runs python3 3.9.6, and this file must import and run there too (that
  is where it is tested), so: `from __future__ import annotations` for typing and
  no 3.10+ runtime syntax anywhere.

AUTH, WITHOUT A CRYPTO PACKAGE
------------------------------
A Google service-account JWT is RS256 = RSASSA-PKCS1-v1_5 over SHA-256, which is
exactly what `openssl dgst -sha256 -sign key.pem` emits. So the JWT is assembled
here and SIGNED BY OPENSSL. The private key is written to a mkstemp() file
(0600, unpredictable name, never a fixed path), overwritten and unlinked in a
`finally` that runs on every path.

SECRETS ARE NEVER PRINTED — not the service-account JSON, not the private key,
not the signed JWT, not the access token, not even truncated. Every value that
must never appear in output is registered with register_secret() and every line
this worker emits (stdout AND stderr, including the top-level exception handler)
passes through scrub(). Google's own API error text IS printed verbatim: that
text is safe and it is the only way to diagnose a 403 honestly.

RESOLUTION IS EXACTLY-ONE OR NOTHING
------------------------------------
`--query` requires EXACTLY ONE match. Zero fails loud; two or more fails loud and
NAMES them; a present `nextPageToken` also fails loud, because more results exist
beyond the page and `len(files) == 1` would then be a lie. Silently taking the
first match is how an unscoped query grabbed the wrong video once already.

A FAILED RUN PERSISTS NOTHING (charter gate 9)
----------------------------------------------
Bytes land in `<name>.part`. The size is asserted against Drive's own metadata
size on the `.part` BEFORE os.replace, so the final name only ever exists after
verification. Any failure unlinks the `.part` in a `finally`. There is no path
that leaves a truncated file under the real name.

RETRY POLICY (stated reading of the brief, so it is reviewable)
---------------------------------------------------------------
The brief says "on a transport failure retry the whole file at most twice". This
implements that PLUS HTTP 429/500/502/503/504, on the reading that a 5xx from
Google is a transient of the same class as a dropped socket. Every other status
(and every token-exchange failure) fails loud on the first occurrence with the
API's own error text. Each retry deletes the `.part` first and re-fetches the
whole file — there is no range-resume, because a resumed byte range that silently
straddles two different file versions is a worse failure than a re-download.

USAGE
    python3 fetch_drive_file.py --file-id 1AbC... [--out-dir DIR]
    python3 fetch_drive_file.py --query "name = 'rec19.mp4' and trashed = false"
    python3 fetch_drive_file.py --file-id 1AbC... --metadata-only

ENV
    CLPR_GDRIVE_SA_JSON   service-account JSON path
                          (default /home/node/.n8n/clpr/.gdrive_sa.json)

Prints RESULT last; ERROR to stderr; exits non-zero on ANY failure.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_SA_PATH = '/home/node/.n8n/clpr/.gdrive_sa.json'
DEFAULT_OUT_DIR = '/home/node/.n8n/clpr/media'

SCOPE = 'https://www.googleapis.com/auth/drive.readonly'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
JWT_AUDIENCE = TOKEN_URL
JWT_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:jwt-bearer'
DRIVE_FILES_URL = 'https://www.googleapis.com/drive/v3/files'

# JWT lifetime. Google caps assertions at 3600 s; stay at the cap, not over it.
JWT_LIFETIME_S = 3600

# The load-bearing number: bytes held in memory at once during a transfer.
CHUNK_BYTES = 1024 * 1024

# Every JSON / error body read is capped. This is the only place an unbounded
# read could sneak back in, so it is bounded by construction.
MAX_JSON_BYTES = 1024 * 1024

JSON_TIMEOUT_S = 60
# urlopen's timeout is per socket operation, not for the whole transfer, so a
# multi-GB download is fine under a per-read timeout.
READ_TIMEOUT_S = 120

FETCH_ATTEMPTS = 3            # 1 initial + at most 2 retries
RETRY_BACKOFF_S = (2.0, 5.0)  # sleep before attempt 2, then before attempt 3
RETRYABLE_STATUS = (429, 500, 502, 503, 504)

SEARCH_PAGE_SIZE = 10
SEARCH_FIELDS = 'nextPageToken,files(id,name,size,mimeType)'
METADATA_FIELDS = 'id,name,size,mimeType'

TRANSPORT_EXC = (
    urllib.error.URLError,
    http.client.HTTPException,
    socket.timeout,
    ConnectionError,
    TimeoutError,
    # ssl.SSLError subclasses OSError but NOT ConnectionError/TimeoutError/
    # URLError/HTTPException. Without it, an SSLEOFError raised by
    # resp.read(chunk) mid-transfer would skip the retry branch, fall into
    # stream_to_file's `except OSError`, and be reported as a non-retryable
    # "write failed" — blaming the disk for a network fault on exactly the
    # long HTTPS transfer FETCH_ATTEMPTS exists to survive.
    ssl.SSLError,
)


# ---------------------------------------------------------------------------
# Secret hygiene — this box's repo is public and the operator streams.
# ---------------------------------------------------------------------------

_SECRETS = []  # type: list[str]


def register_secret(value) -> None:
    """Register a value that must never appear in this worker's output.

    Short values are ignored: scrubbing a 3-character string would redact
    unrelated text and make the logs useless.
    """
    if not value:
        return
    text = value if isinstance(value, str) else str(value)
    for chunk in text.splitlines():
        chunk = chunk.strip()
        if len(chunk) >= 16 and chunk not in _SECRETS:
            _SECRETS.append(chunk)


def scrub(text: str) -> str:
    """Replace every registered secret with a marker. Defence in depth: no code
    path here is supposed to hand a secret to output in the first place."""
    out = text
    for secret in _SECRETS:
        if secret and secret in out:
            out = out.replace(secret, '[REDACTED]')
    return out


def emit(line: str) -> None:
    print(scrub(line))


def emit_err(line: str) -> None:
    print(scrub(line), file=sys.stderr)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class FetchError(RuntimeError):
    """Any loud, non-retryable failure."""


class HttpError(FetchError):
    def __init__(self, status: int, url: str, body: str):
        self.status = status
        self.url = url
        self.body = body
        super().__init__('HTTP {0} from {1}: {2}'.format(status, url, body))


class TransportError(FetchError):
    """A dropped/refused/timed-out connection — the retryable class."""


# ---------------------------------------------------------------------------
# JWT assembly + openssl signing
# ---------------------------------------------------------------------------

def b64url(data: bytes) -> str:
    """base64url WITHOUT padding, per RFC 7515."""
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def load_service_account(path: str) -> dict:
    """Read + validate the service-account JSON. Fails loud and NAMES the path.

    Never echoes any part of the file's contents: a JSON syntax error is reported
    by position, which is what json's own exception gives us.
    """
    if not os.path.exists(path):
        raise FetchError(
            'service-account JSON not found: {0} '
            '(set CLPR_GDRIVE_SA_JSON, or install the key at that path, '
            'node-owned 0600 like .pg_env)'.format(path)
        )
    try:
        with open(path, 'rb') as fh:
            raw = fh.read(MAX_JSON_BYTES)
    except OSError as exc:
        raise FetchError('cannot read service-account JSON {0}: {1}'.format(path, exc))

    register_secret(raw.decode('utf-8', 'replace'))

    try:
        sa = json.loads(raw.decode('utf-8'))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FetchError(
            'service-account JSON is malformed: {0} ({1}) '
            '— contents NOT echoed on purpose'.format(path, exc)
        )
    if not isinstance(sa, dict):
        raise FetchError(
            'service-account JSON is malformed: {0} (top level is {1}, expected an '
            'object)'.format(path, type(sa).__name__)
        )

    for key in ('client_email', 'private_key'):
        value = sa.get(key)
        if not isinstance(value, str) or not value.strip():
            raise FetchError(
                'service-account JSON is malformed: {0} (missing or empty '
                '"{1}")'.format(path, key)
            )
    register_secret(sa['private_key'])

    if 'BEGIN' not in sa['private_key'] or 'PRIVATE KEY' not in sa['private_key']:
        raise FetchError(
            'service-account JSON is malformed: {0} ("private_key" is not a PEM '
            'block)'.format(path)
        )
    if sa.get('type') and sa.get('type') != 'service_account':
        raise FetchError(
            'service-account JSON has type="{0}", expected "service_account": '
            '{1}'.format(sa.get('type'), path)
        )

    token_uri = sa.get('token_uri')
    if token_uri and token_uri != TOKEN_URL:
        # The brief fixes aud + the exchange endpoint to TOKEN_URL. Surfacing the
        # difference beats silently disagreeing with the key file.
        emit_err(
            'NOTE service-account token_uri is "{0}" but this worker uses the '
            'briefed constant "{1}"'.format(token_uri, TOKEN_URL)
        )
    return sa


def sign_rs256(signing_input: bytes, private_key_pem: str) -> bytes:
    """RS256-sign via `openssl dgst -sha256 -sign` (no crypto package exists here).

    The PEM goes to a mkstemp() file: 0600 by construction, O_EXCL, unpredictable
    name — never a fixed path another process could pre-create or read. It is
    overwritten with zeros and unlinked in a finally that runs on EVERY path,
    including an openssl crash. Neither the key nor the path is ever printed.
    """
    fd, pem_path = tempfile.mkstemp(prefix='clpr-gdrive-', suffix='.pem')
    key_bytes = private_key_pem.encode('utf-8')
    try:
        with os.fdopen(fd, 'wb') as fh:
            fh.write(key_bytes)
        cmd = ['openssl', 'dgst', '-sha256', '-sign', pem_path]
        try:
            proc = subprocess.run(cmd, input=signing_input, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        except OSError as exc:
            raise FetchError('cannot execute openssl: {0}'.format(exc))
        if proc.returncode != 0:
            raise FetchError(
                'openssl signing failed exit={0} stderr={1}'.format(
                    proc.returncode,
                    proc.stderr.decode('utf-8', 'replace').strip() or '(empty)',
                )
            )
        if not proc.stdout:
            raise FetchError('openssl signing produced an EMPTY signature')
        return proc.stdout
    finally:
        try:
            with open(pem_path, 'r+b') as fh:
                fh.write(b'\x00' * len(key_bytes))
                fh.flush()
                os.fsync(fh.fileno())
        except OSError:
            pass
        try:
            os.unlink(pem_path)
        except OSError:
            pass


def build_assertion(sa: dict, now: int = None) -> str:
    """Assemble + sign the service-account JWT. The return value is a SECRET."""
    issued = int(time.time()) if now is None else int(now)
    header = {'alg': 'RS256', 'typ': 'JWT'}
    claims = {
        'iss': sa['client_email'],
        'scope': SCOPE,
        'aud': JWT_AUDIENCE,
        'iat': issued,
        'exp': issued + JWT_LIFETIME_S,
    }
    segments = [
        b64url(json.dumps(header, separators=(',', ':'), sort_keys=True).encode('utf-8')),
        b64url(json.dumps(claims, separators=(',', ':'), sort_keys=True).encode('utf-8')),
    ]
    signing_input = '.'.join(segments).encode('ascii')
    signature = sign_rs256(signing_input, sa['private_key'])
    assertion = '.'.join(segments + [b64url(signature)])
    register_secret(assertion)
    return assertion


# ---------------------------------------------------------------------------
# HTTP boundary (the ONLY place this worker talks to the network)
# ---------------------------------------------------------------------------

def _read_capped(stream) -> str:
    try:
        return stream.read(MAX_JSON_BYTES).decode('utf-8', 'replace').strip()
    except Exception:  # a body we cannot read must not mask the real error
        return '(body unreadable)'


def http_json(url: str, method: str = 'GET', headers: dict = None,
              data: bytes = None, timeout: int = JSON_TIMEOUT_S) -> dict:
    """One JSON request/response. Body reads are capped at MAX_JSON_BYTES.

    This is the network SEAM: the negative-control tests replace this function
    (the boundary) and never the resolution/verification logic it feeds.
    """
    req = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = _read_capped(resp)
    except urllib.error.HTTPError as exc:
        raise HttpError(exc.code, url, _read_capped(exc) or '(empty body)')
    except TRANSPORT_EXC as exc:
        raise TransportError('transport failure for {0}: {1}'.format(url, exc))
    try:
        parsed = json.loads(body) if body else None
    except ValueError as exc:
        raise FetchError('non-JSON response from {0}: {1}'.format(url, exc))
    if not isinstance(parsed, dict):
        raise FetchError('unexpected JSON shape from {0}: {1}'.format(
            url, type(parsed).__name__))
    return parsed


def get_access_token(sa: dict) -> str:
    """Exchange the signed assertion for an access token. NEVER retried and
    NEVER printed."""
    assertion = build_assertion(sa)
    body = urllib.parse.urlencode({
        'grant_type': JWT_GRANT_TYPE,
        'assertion': assertion,
    }).encode('ascii')
    register_secret(body.decode('ascii'))
    payload = http_json(
        TOKEN_URL, method='POST',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data=body,
    )
    token = payload.get('access_token')
    if not isinstance(token, str) or not token:
        # payload may carry {"error": "...", "error_description": "..."} — that
        # text is safe and is the only useful diagnostic for a bad key/scope.
        raise FetchError(
            'token endpoint returned no access_token (error="{0}" '
            'description="{1}")'.format(
                payload.get('error', ''), payload.get('error_description', '')))
    register_secret(token)
    return token


def auth_headers(token: str) -> dict:
    return {'Authorization': 'Bearer {0}'.format(token)}


def fetch_metadata(token: str, file_id: str) -> dict:
    url = '{0}/{1}?{2}'.format(
        DRIVE_FILES_URL, urllib.parse.quote(file_id, safe=''),
        urllib.parse.urlencode({
            'fields': METADATA_FIELDS,
            'supportsAllDrives': 'true',
        }))
    return http_json(url, headers=auth_headers(token))


def search_files(token: str, query: str) -> dict:
    url = '{0}?{1}'.format(DRIVE_FILES_URL, urllib.parse.urlencode({
        'q': query,
        'fields': SEARCH_FIELDS,
        'pageSize': str(SEARCH_PAGE_SIZE),
        'supportsAllDrives': 'true',
        'includeItemsFromAllDrives': 'true',
    }))
    return http_json(url, headers=auth_headers(token))


def require_exactly_one(payload: dict, query: str) -> dict:
    """EXACTLY ONE match, or fail loud NAMING what was found.

    Never takes the first of several: an unscoped query silently grabbing the
    wrong video is a prior real finding on this project.

    A present nextPageToken is ambiguity too — more matches exist beyond this
    page, so len(files) == 1 would be a lie about the match SET.
    """
    files = payload.get('files')
    if files is None:
        raise FetchError(
            'Drive search response has no "files" key for query: {0}'.format(query))
    if not isinstance(files, list):
        raise FetchError('Drive search "files" is {0}, expected a list'.format(
            type(files).__name__))
    if len(files) == 0:
        raise FetchError(
            'Drive query matched ZERO files, refusing: {0}'.format(query))
    if payload.get('nextPageToken'):
        raise FetchError(
            'Drive query matched MORE than one page (nextPageToken present) — '
            'refusing rather than guessing. Query: {0}. First {1} match(es): '
            '{2}'.format(query, len(files), describe_matches(files)))
    if len(files) > 1:
        raise FetchError(
            'Drive query matched {0} files, refusing (it must match exactly '
            'one). Query: {1}. Matches: {2}'.format(
                len(files), query, describe_matches(files)))
    match = files[0]
    if not isinstance(match, dict) or not match.get('id'):
        raise FetchError('Drive search returned a match with no id: {0!r}'.format(match))
    return match


def describe_matches(files) -> str:
    parts = []
    for item in files:
        if isinstance(item, dict):
            parts.append('id={0} name="{1}" size={2} mime={3}'.format(
                item.get('id', '?'), item.get('name', '?'),
                item.get('size', '?'), item.get('mimeType', '?')))
        else:
            parts.append(repr(item))
    return '; '.join(parts)


# ---------------------------------------------------------------------------
# Metadata interpretation
# ---------------------------------------------------------------------------

def metadata_size(meta: dict) -> int:
    """Drive v3 reports size as a STRING (int64-as-string). Absent size means the
    file has no binary content — a Google-native Doc/Sheet/Slide — which cannot
    be fetched with alt=media at all. Fail loud rather than download a surprise."""
    raw = meta.get('size')
    if raw is None or raw == '':
        raise FetchError(
            'Drive metadata has no "size" for id={0} name="{1}" mimeType={2} — '
            'a Google-native file has no downloadable bytes; refusing'.format(
                meta.get('id', '?'), meta.get('name', '?'),
                meta.get('mimeType', '?')))
    try:
        size = int(raw)
    except (TypeError, ValueError):
        raise FetchError('Drive metadata "size" is not an integer: {0!r}'.format(raw))
    if size < 0:
        raise FetchError('Drive metadata "size" is negative: {0}'.format(size))
    return size


def safe_filename(name) -> str:
    """A Drive name is attacker-influenced text, not a path. Reduce it to a bare
    basename and refuse anything that could escape --out-dir."""
    if not isinstance(name, str) or not name.strip():
        raise FetchError('Drive file has an empty name; refusing to guess one')
    candidate = os.path.basename(name.strip().replace('\\', '/').rstrip('/'))
    if candidate in ('', '.', '..'):
        raise FetchError('Drive file name is not usable as a filename: {0!r}'.format(name))
    if '/' in candidate or '\x00' in candidate:
        raise FetchError('Drive file name contains a path separator or NUL: {0!r}'.format(name))
    if any(ord(ch) < 0x20 for ch in candidate):
        raise FetchError('Drive file name contains control characters: {0!r}'.format(name))
    return candidate


# ---------------------------------------------------------------------------
# THE CONSTANT-MEMORY TRANSFER
# ---------------------------------------------------------------------------

def stream_to_file(url: str, headers: dict, dest_part: str,
                   chunk_bytes: int = CHUNK_BYTES,
                   timeout: int = READ_TIMEOUT_S,
                   progress=None) -> int:
    """Copy an HTTP response body to disk WITHOUT ever holding it in memory.

    At most `chunk_bytes` of the payload exists in the process at any instant, so
    peak RSS is flat in file size — a 4 GB file costs the same memory as a 4 MB
    one. This is the whole reason this worker exists; the n8n Google Drive and
    HTTP Request nodes both fail exactly here.

    NOTHING in this function may call resp.read() without a size argument, and
    nothing may accumulate the payload (no b''.join, no hashing the whole body).

    urllib follows 3xx itself and its HTTPRedirectHandler carries every header
    except content-length/content-type across the hop, so a redirected media URL
    keeps the Bearer token. 4xx/5xx raise HttpError carrying the API's own text.
    """
    req = urllib.request.Request(url, method='GET')
    for key, value in (headers or {}).items():
        req.add_header(key, value)

    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise HttpError(exc.code, url, _read_capped(exc) or '(empty body)')
    except TRANSPORT_EXC as exc:
        raise TransportError('transport failure opening {0}: {1}'.format(url, exc))

    written = 0
    try:
        with resp:
            with open(dest_part, 'wb') as fh:
                while True:
                    try:
                        buf = resp.read(chunk_bytes)
                    except TRANSPORT_EXC as exc:
                        raise TransportError(
                            'transport failure after {0} bytes: {1}'.format(written, exc))
                    if not buf:
                        break
                    fh.write(buf)          # write-through; nothing accumulates
                    written += len(buf)
                    if progress is not None:
                        progress(written)
    except TransportError:
        raise
    except OSError as exc:
        # A local write failure (ENOSPC, EACCES) is NOT a transport problem and
        # must not be retried into the same wall.
        raise FetchError('write failed at {0} bytes to {1}: {2}'.format(
            written, dest_part, exc))
    return written


def verify_size(path: str, expected_bytes: int) -> int:
    """Assert the bytes on disk EQUAL Drive's own metadata size, exactly.

    A short read that ends cleanly (a truncated proxy response, a silent EOF) is
    indistinguishable from success without this check.
    """
    actual = os.path.getsize(path)
    if actual != expected_bytes:
        raise FetchError(
            'SIZE MISMATCH: expected {0} bytes from Drive metadata, got {1} on '
            'disk (delta {2})'.format(expected_bytes, actual, actual - expected_bytes))
    return actual


def download(token: str, meta: dict, out_dir: str, progress=None) -> tuple:
    """Fetch one file to out_dir. Returns (abs_path, bytes).

    A failed run persists NOTHING: bytes only ever land in `<name>.part`, the
    size is verified on the .part, and only a verified .part is os.replace()d
    onto the final name. Every exit path unlinks a leftover .part.
    """
    file_id = meta.get('id')
    if not file_id:
        raise FetchError('metadata has no id; cannot fetch')
    expected = metadata_size(meta)
    name = safe_filename(meta.get('name'))

    try:
        os.makedirs(out_dir, exist_ok=True)
    except OSError as exc:
        raise FetchError('cannot create --out-dir {0}: {1}'.format(out_dir, exc))

    final_path = os.path.abspath(os.path.join(out_dir, name))
    part_path = final_path + '.part'

    url = '{0}/{1}?{2}'.format(
        DRIVE_FILES_URL, urllib.parse.quote(str(file_id), safe=''),
        urllib.parse.urlencode({'alt': 'media', 'supportsAllDrives': 'true'}))

    try:
        for attempt in range(1, FETCH_ATTEMPTS + 1):
            # Always start from nothing: a leftover .part from a failed attempt
            # would otherwise be silently appended to or mistaken for progress.
            if os.path.exists(part_path):
                os.unlink(part_path)
            try:
                written = stream_to_file(url, auth_headers(token), part_path,
                                         progress=progress)
            except TransportError as exc:
                if attempt < FETCH_ATTEMPTS:
                    backoff = RETRY_BACKOFF_S[attempt - 1]
                    emit('WARN transport failure on attempt {0}/{1} ({2}) — '
                         'deleting .part and retrying in {3}s'.format(
                             attempt, FETCH_ATTEMPTS, exc, backoff))
                    time.sleep(backoff)
                    continue
                raise
            except HttpError as exc:
                if exc.status in RETRYABLE_STATUS and attempt < FETCH_ATTEMPTS:
                    backoff = RETRY_BACKOFF_S[attempt - 1]
                    emit('WARN HTTP {0} on attempt {1}/{2} — deleting .part and '
                         'retrying in {3}s. API said: {4}'.format(
                             exc.status, attempt, FETCH_ATTEMPTS, backoff, exc.body))
                    time.sleep(backoff)
                    continue
                raise
            break

        emit('FETCHED bytes={0} expected={1}'.format(written, expected))
        verify_size(part_path, expected)
        os.replace(part_path, final_path)
    finally:
        if os.path.exists(part_path):
            try:
                os.unlink(part_path)
            except OSError:
                pass

    return (final_path, expected)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def resolve_target(token: str, file_id: str, query: str) -> dict:
    # argparse guarantees one of the two flags is PRESENT, not that it carries a
    # value: `--file-id ""` would otherwise fall through to an empty query and
    # let Drive decide what "everything" means.
    if not file_id and not query:
        raise FetchError('--file-id / --query was given an EMPTY value; refusing')
    if file_id:
        meta = fetch_metadata(token, file_id)
        if not meta.get('id'):
            raise FetchError(
                'Drive returned no id for --file-id {0}: {1!r}'.format(file_id, meta))
        return meta
    return require_exactly_one(search_files(token, query), query)


def run(args: argparse.Namespace) -> int:
    started = time.monotonic()

    sa_path = args.sa_json or os.environ.get(
        'CLPR_GDRIVE_SA_JSON', '').strip() or DEFAULT_SA_PATH
    sa = load_service_account(sa_path)
    token = get_access_token(sa)

    meta = resolve_target(token, args.file_id, args.query)
    file_id = meta.get('id')

    if args.metadata_only:
        size = metadata_size(meta)
        elapsed = time.monotonic() - started
        emit('RESULT fetch_drive_file file_id={0} ok=1 metadata_only=1 '
             'name="{1}" bytes={2} mime="{3}" elapsed_s={4:.3f}'.format(
                 file_id, meta.get('name', ''), size,
                 meta.get('mimeType', ''), elapsed))
        return 0

    if args.max_bytes:
        # metadata_size() does NOT return a falsy value when Drive omits "size" —
        # it raises FetchError directly (see its own docstring/body). So the
        # missing-size case is checked HERE, before calling it, using the exact
        # same condition it uses internally: that is the only way to emit the
        # distinct INGEST_SIZE_UNKNOWN marker this gate requires instead of
        # metadata_size's own unmarked "no size" refusal.
        raw_size = meta.get('size')
        if raw_size is None or raw_size == '':
            raise FetchError(
                'INGEST_SIZE_UNKNOWN file_id={0} name="{1}" — Drive reported no '
                'size; refusing rather than pass an unmeasured file through the '
                '--max-bytes ceiling'.format(file_id, meta.get('name', '')))
        size = metadata_size(meta)
        if size > args.max_bytes:
            raise FetchError(
                'INGEST_CEILING_REFUSED file_id={0} name="{1}" bytes={2} '
                'max_bytes={3}'.format(
                    file_id, meta.get('name', ''), size, args.max_bytes))

    out_dir = args.out_dir or DEFAULT_OUT_DIR
    path, size = download(token, meta, out_dir)
    elapsed = time.monotonic() - started
    emit('RESULT fetch_drive_file file_id={0} ok=1 path="{1}" bytes={2} '
         'elapsed_s={3:.3f}'.format(file_id, path, size, elapsed))
    return 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Stream one Google Drive file to disk in constant memory '
                    '(stdlib + openssl only).')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file-id', default='',
                       help='Drive file id to fetch directly')
    group.add_argument('--query', default='',
                       help='Drive v3 query; must match EXACTLY ONE file')
    parser.add_argument('--out-dir', default='',
                        help='destination directory (default {0})'.format(DEFAULT_OUT_DIR))
    parser.add_argument('--sa-json', default='',
                        help='service-account JSON path (default $CLPR_GDRIVE_SA_JSON '
                             'or {0})'.format(DEFAULT_SA_PATH))
    parser.add_argument('--metadata-only', action='store_true',
                        help='resolve and report id/name/size only; download nothing')
    parser.add_argument('--max-bytes', type=int, default=0,
                        help='refuse BEFORE downloading if Drive-reported size exceeds '
                             'this many bytes; 0 = no ceiling (default)')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    return run(parse_args(argv))


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:  # every failure is loud, scrubbed, and non-zero
        emit_err('ERROR: {0}'.format(exc))
        sys.exit(1)
