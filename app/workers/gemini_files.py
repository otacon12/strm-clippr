#!/usr/bin/env python3
"""gemini_files: transport module for the Gemini Files API (D-066).

Uploads a video to Google's Files API at FULL QUALITY, waits for it to
process, verifies the upload actually reached Google before anything
downstream trusts it, runs a generateContent call against the uploaded file,
and deletes the file afterward (with its own follow-up check that it is
actually gone). This exists because the OTHER vision path in
generate_post_kit.py inlines the clip as base64 through OpenRouter, which has
a request-body size ceiling; large clips get downscaled before that ceiling,
and the downscale was MEASURED FABRICATING scene descriptions -- full quality
reported "the feed is intact for the whole clip" while a downscaled copy of
the same 15 seconds reported black-frame cuts that do not exist in the file.
Uploading at full quality through the Files API removes the downscale
entirely, because this API has no comparable body-size ceiling.

EVERY CONSTANT AND EVERY SHAPE BELOW IS A MEASURED FACT, NOT A DOCUMENTED ONE.
CC ran the whole mechanism against the live API on 2026-08-07 ~21:30 PT, with
a real 15s / 16,947,420-byte clip and the operator's real key. THREE of
Google's own documented/exemplified behaviours turned out to be wrong: the
sha256Hash encoding (§ verify_upload), the upload session URL living in a
response HEADER rather than the body (§ upload_video), and the finalize call
needing no auth header at all (§ upload_video). Where this module disagrees
with Google's docs, THIS MODULE IS RIGHT. Do not "correct" any of it back
toward the documentation.

Python 3 standard library ONLY: no pip, no requests. The n8n container this
runs in has no package manager available to it (see app/AGENTS.md rule 2 --
the operator's live environment is the version oracle), so this constraint is
absolute.

NEVER LOGS: the API key, a request body, or base64/raw bytes. Every error
body is truncated to ERROR_BODY_MAX_CHARS before it enters an exception
message, matching the existing discipline in generate_post_kit.py:519 -- these
strings land in post_kit_requests.error and render in the review UI on a live
stream.

Brief 2 (not this one) wires this module into generate_post_kit.py and removes
the base64 inline path. This brief only has to be correct in isolation: this
module is not imported or called from anywhere yet.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants. Every value in THIS block was measured against the live API on
# 2026-08-07, not read from Google's documentation. See the module docstring.
# ---------------------------------------------------------------------------
GEMINI_HOST = 'https://generativelanguage.googleapis.com'
DEFAULT_GEMINI_MODEL = 'gemini-2.5-pro'          # NOT the OpenRouter slug 'google/gemini-2.5-pro'
DEFAULT_GEMINI_KEY_ENV_PATH = '/home/node/.n8n/clpr/.gemini_env'
GEMINI_KEY_NAME = 'GEMINI_API_KEY'
USER_AGENT = 'clpr-postkit/1.0'                  # Cloudflare 403s Python-urllib/*; keep a real UA
POLL_INTERVAL_S = 5.0                            # Google's own cadence in every sample
POLL_MAX_S = 600.0                               # MEASURED: 5.5s for 17MB. 600 is a loud backstop.
UPLOAD_TIMEOUT_S = 900
GENERATE_TIMEOUT_S = 900
ERROR_BODY_MAX_CHARS = 800

# --- Supplementary constants. NOT part of the brief's measured block above --
# implementation details this module needs, not themselves Gemini-API facts.

# ffprobe's own runtime, not Gemini's. Same value as the sibling constant
# generate_post_kit.py:FFPROBE_TIMEOUT_S, for the same tool.
FFPROBE_TIMEOUT_S = 120

# verify_upload's createTime window (charter gate 21: one instant, one clock,
# or the comparison is a coin flip). Not itself a Google-measured number;
# sized generously against THIS module's own worst case
# (UPLOAD_TIMEOUT_S + POLL_MAX_S = 1500s) so a slow-but-honest run is never
# mistaken for stale/replayed data, plus a small allowance either side for
# clock skew between this host and Google's.
VERIFY_CREATE_TIME_MAX_AGE_S = 1800.0
VERIFY_CREATE_TIME_FUTURE_SKEW_S = 60.0


# ---------------------------------------------------------------------------
# Credential
# ---------------------------------------------------------------------------

def read_gemini_key(path: str | None = None) -> str:
    """Read GEMINI_API_KEY out of a KEY=VALUE env file. Never logged.

    Mirrors generate_post_kit.py's read_api_key exactly in shape (lines
    accept an optional `export ` prefix, match GEMINI_API_KEY=, strip
    surrounding quotes). Path resolution: the `path` argument when given,
    else CLPR_GEMINI_ENV, else DEFAULT_GEMINI_KEY_ENV_PATH -- mirroring the
    .openrouter_env / .pg_env pattern already on the n8n volume. There is
    deliberately NO fallback to another credential and this never returns ''
    on any path: a silent fallback is how the wrong credential gets used.
    """
    if path is None:
        path = os.environ.get('CLPR_GEMINI_ENV', '').strip() or DEFAULT_GEMINI_KEY_ENV_PATH

    p = Path(path)
    if not p.is_file():
        raise RuntimeError(
            f'MISSING_GEMINI_ENV: no Gemini credential file at {p}. This module reads '
            f'{GEMINI_KEY_NAME} from that file (path overridable with CLPR_GEMINI_ENV or the '
            'path argument), mirroring the .openrouter_env / .pg_env pattern already on the '
            'n8n volume. Create it, owned node:node, mode 0600, containing a single '
            f'{GEMINI_KEY_NAME}=<key> line. There is no environment-variable fallback on '
            'purpose.'
        )
    try:
        raw = p.read_text(encoding='utf-8')
    except OSError as exc:
        raise RuntimeError(f'MISSING_GEMINI_ENV: cannot read {p} ({exc!r})') from exc

    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if stripped.startswith('export '):
            stripped = stripped[len('export '):].strip()
        if '=' not in stripped:
            continue
        name, _, value = stripped.partition('=')
        if name.strip() != GEMINI_KEY_NAME:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        if value:
            return value

    raise RuntimeError(
        f'MISSING_GEMINI_ENV: {p} exists but contains no non-empty {GEMINI_KEY_NAME}= line.'
    )


# ---------------------------------------------------------------------------
# Typed failures. A caller that needs to tell an empty completion apart from a
# transport failure (Brief 2 does, to decide whether to retry) needs a TYPE,
# not a string it has to pattern-match (charter gate 10: specify the
# invariant, never a proxy).
# ---------------------------------------------------------------------------

class GeminiError(RuntimeError):
    """Anything that went wrong talking to the Gemini Files / generateContent API."""


class GeminiHTTPError(GeminiError):
    """A real HTTP status came back. `status` is the fact; the message is prose."""

    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = int(status)


class GeminiUnreachable(GeminiError):
    """No status at all: DNS, connection, reset, or a timeout."""


class GeminiUploadURLMissingError(GeminiError):
    """Call 1 (start the resumable session) returned 200 with no upload-url header.

    The silent-default shape: a 200 with no session URL must never be treated
    as success.
    """


class GeminiFileFailedError(GeminiError):
    """Google reported the file's processing state as FAILED."""


class GeminiPollTimeoutError(GeminiError):
    """The file never left PROCESSING within POLL_MAX_S."""


class GeminiVerificationError(GeminiError):
    """verify_upload found evidence the real bytes did NOT genuinely reach Google.

    One type covering all four witnesses (sha256, size, state, createTime);
    each raise site names which one failed in its message, per the brief's
    "each as its own named failure" (matching this codebase's existing idiom
    of distinct message prefixes over distinct exception classes, see
    generate_post_kit.py's validate_kit).
    """


class GeminiEmptyCompletionError(GeminiError):
    """generate_content got 200 but no text in any candidate part.

    Its own type, deliberately distinct from GeminiHTTPError/GeminiUnreachable,
    because Brief 2 needs to tell an empty completion (content-shaped, maybe
    worth a retry) apart from a transport failure without parsing a message.
    """


class GeminiMotionMeasurementError(GeminiError):
    """ffprobe failed, or returned no numeric frames, for measure_motion.

    Never silently returns 0.0: 0.0 means "static" and would pick the wrong
    fps forever.
    """


# ---------------------------------------------------------------------------
# HTTP seam. Deliberately low-level (status, headers, raw body) rather than
# JSON-decoding here: Call 1's payload rides in a response HEADER, not the
# body, and Call 2's body can be legitimately empty on some codepaths. Callers
# decode what they need. Never logs headers, the body, or api_key.
# ---------------------------------------------------------------------------

def _http_call(method: str, url: str, headers: dict, data: bytes | None,
               timeout: float) -> tuple[int, object, bytes]:
    """One raw HTTP call. Returns (status, response_headers, body_bytes).

    Raises GeminiHTTPError (status + a body truncated to ERROR_BODY_MAX_CHARS,
    matching generate_post_kit.py:519's discipline) on any non-2xx, and
    GeminiUnreachable on DNS/connection/reset/timeout faults.
    """
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, resp.headers, body
    # ORDER MATTERS: HTTPError is a subclass of URLError, which is a subclass
    # of OSError, and a read timeout can surface as TimeoutError/OSError
    # directly rather than as a URLError. A broader clause first would
    # swallow the narrower, more informative one.
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')[:ERROR_BODY_MAX_CHARS]
        raise GeminiHTTPError(
            f'GEMINI_HTTP_ERROR: {method} {url} returned status {exc.code}. '
            f'Response body (first {ERROR_BODY_MAX_CHARS} chars, verbatim): {detail!r}',
            status=exc.code,
        ) from exc
    except urllib.error.URLError as exc:
        raise GeminiUnreachable(f'GEMINI_UNREACHABLE: {method} {url} failed: {exc!r}') from exc
    except (TimeoutError, OSError) as exc:
        raise GeminiUnreachable(f'GEMINI_UNREACHABLE: {method} {url} failed: {exc!r}') from exc


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_video(path: str, api_key: str) -> dict:
    """Upload one video file to the Gemini Files API. Returns the finalized File dict.

    Two HTTP calls. Call 1 starts a resumable session and returns the session
    URL in a RESPONSE HEADER (x-goog-upload-url), not the JSON body -- MEASURED
    present, read case-insensitively (http.client's response headers are an
    email.message.Message, whose .get() is already case-insensitive by the
    HTTP spec). A 200 with no such header is the silent-default shape and is
    never treated as success.

    Call 2 transfers the raw bytes and finalizes in one request. MEASURED:
    this call needs NO x-goog-api-key header and returns 200 without one --
    Google's own sample omits it and never explains why. Built without the
    header first; on 401/403 it is retried EXACTLY ONCE with the header added,
    and a distinct marker is printed so a future change in Google's behaviour
    is recorded rather than silently absorbed.

    The finalize response body wraps the File as {"file": {...}}. (The poll in
    wait_active returns the File BARE, unwrapped -- that asymmetry is real, is
    in Google's own samples, and is exactly the kind of thing that produces a
    None three functions later.) Handled here by reading body.get('file') or
    body.

    MEASURED fields present on the returned File: createTime, displayName,
    expirationTime, mimeType, name, sha256Hash, sizeBytes, source, state,
    updateTime, uri.
    """
    start = time.monotonic()
    p = Path(path)
    size = p.stat().st_size
    display_name = p.stem

    print(f'GEMINI_UPLOAD_STARTED bytes={size}')

    start_headers = {
        'x-goog-api-key': api_key,
        'X-Goog-Upload-Protocol': 'resumable',
        'X-Goog-Upload-Command': 'start',
        'X-Goog-Upload-Header-Content-Length': str(size),
        'X-Goog-Upload-Header-Content-Type': 'video/mp4',
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT,
    }
    start_body = json.dumps({'file': {'display_name': display_name}}).encode('utf-8')
    _status, resp_headers, _body = _http_call(
        'POST', f'{GEMINI_HOST}/upload/v1beta/files', start_headers, start_body,
        UPLOAD_TIMEOUT_S)

    upload_url = resp_headers.get('x-goog-upload-url')
    if not upload_url:
        raise GeminiUploadURLMissingError(
            'GEMINI_UPLOAD_URL_MISSING: Call 1 (start the resumable session) returned 200 '
            'with no x-goog-upload-url response header. A 200 with no upload URL is the '
            f'silent-default shape and must never proceed. Headers actually received: '
            f'{sorted(resp_headers.keys())!r}'
        )

    # MEASURED: Call 2 needs no explicit Content-Type; the exact three headers
    # below are what was measured working. urllib.request will insert its own
    # default 'application/x-www-form-urlencoded' when none is set (it always
    # does this for any POST carrying data) -- harmless here because Google's
    # resumable-upload protocol is driven entirely by the X-Goog-Upload-*
    # headers, not by content negotiation, and no example (Google's or this
    # measurement) shows an override being necessary.
    transfer_headers = {
        'Content-Length': str(size),
        'X-Goog-Upload-Offset': '0',
        'X-Goog-Upload-Command': 'upload, finalize',
        'User-Agent': USER_AGENT,
    }
    raw = p.read_bytes()

    def _finalize(headers: dict) -> bytes:
        _s, _h, body = _http_call('POST', upload_url, headers, raw, UPLOAD_TIMEOUT_S)
        return body

    try:
        finalize_body = _finalize(transfer_headers)
    except GeminiHTTPError as exc:
        if exc.status in (401, 403):
            print(
                f'GEMINI_UPLOAD_FINALIZE_AUTH_RETRY status={exc.status} '
                '(Call 2 without x-goog-api-key was rejected; retrying ONCE with the header '
                'added -- Google no longer matches the 2026-08-07 measurement, record this)'
            )
            retry_headers = dict(transfer_headers)
            retry_headers['x-goog-api-key'] = api_key
            finalize_body = _finalize(retry_headers)
        else:
            raise

    finalize_json = json.loads(finalize_body.decode('utf-8'))
    file_obj = finalize_json.get('file') or finalize_json

    elapsed = time.monotonic() - start
    print(
        f'GEMINI_UPLOAD_FINALIZED name={file_obj.get("name")} '
        f'size_bytes={file_obj.get("sizeBytes")} upload_s={elapsed:.1f}'
    )
    return file_obj


# ---------------------------------------------------------------------------
# Poll
# ---------------------------------------------------------------------------

def wait_active(name: str, api_key: str) -> dict:
    """Poll GET {GEMINI_HOST}/v1beta/{name} until state leaves PROCESSING.

    `name` ALREADY contains the "files/" prefix; do not add one. This
    response is the File object BARE (no {"file": ...} wrapper) -- the
    opposite of upload_video's Call 2, which DOES wrap. Loops on PROCESSING,
    returns on ACTIVE, raises on FAILED (reporting the `error` field), raises
    on exceeding POLL_MAX_S, and raises on any state that is none of the
    three measured ones rather than silently treating it as still-processing
    (charter: never guess a status vocabulary).

    MEASURED: PROCESSING -> ACTIVE in 5.5s for a 16.9 MB / 15s clip;
    videoMetadata.videoDuration came back as the string "15s", exact.
    """
    url = f'{GEMINI_HOST}/v1beta/{name}'
    headers = {'x-goog-api-key': api_key, 'User-Agent': USER_AGENT}
    states: list = []
    start = time.monotonic()

    while True:
        _status, _headers, body = _http_call('GET', url, headers, None, GENERATE_TIMEOUT_S)
        file_obj = json.loads(body.decode('utf-8'))
        state = file_obj.get('state')
        states.append(state)

        if state == 'ACTIVE':
            poll_s = time.monotonic() - start
            metadata = file_obj.get('videoMetadata')
            video_duration = metadata.get('videoDuration') if isinstance(metadata, dict) else None
            print(
                f'GEMINI_FILE_ACTIVE name={name} states={"->".join(str(s) for s in states)} '
                f'poll_s={poll_s:.1f} videoDuration={video_duration}'
            )
            return file_obj

        if state == 'FAILED':
            raise GeminiFileFailedError(
                f'GEMINI_FILE_FAILED: name={name} state=FAILED error={file_obj.get("error")!r}'
            )

        if state != 'PROCESSING':
            raise GeminiError(
                f'GEMINI_UNEXPECTED_STATE: name={name} returned state={state!r}, which is none '
                f'of the three measured states (PROCESSING, ACTIVE, FAILED). States observed so '
                f'far: {states!r}. Never guessed at; raised loudly instead.'
            )

        if time.monotonic() - start > POLL_MAX_S:
            raise GeminiPollTimeoutError(
                f'GEMINI_POLL_TIMEOUT: name={name} did not leave PROCESSING within '
                f'POLL_MAX_S={POLL_MAX_S}s. States observed: {states!r}'
            )
        time.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Verification -- THE POSITIVE CONTROL
# ---------------------------------------------------------------------------

def _sha256_file(path: str):
    """Stream the local file through sha256 rather than loading it whole again
    (upload_video already holds one full copy in memory during the transfer)."""
    hasher = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            hasher.update(chunk)
    return hasher


def _parse_gemini_timestamp(value) -> dt.datetime | None:
    """Parse a Gemini RFC3339 timestamp, e.g. '2026-08-07T21:30:05.123456789Z'.

    Google can return sub-microsecond precision (more than 6 fractional
    digits), which datetime.fromisoformat() rejects on Python < 3.11 (this
    project runs 3.9). Fractional seconds are truncated to microsecond
    precision, which is all the window check in verify_upload needs. Returns
    None (never raises) on anything that does not match, so the caller can
    report it as its own named failure.
    """
    if not isinstance(value, str):
        return None
    match = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?Z$', value.strip())
    if not match:
        return None
    base, frac = match.groups()
    try:
        parsed = dt.datetime.strptime(base, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return None
    if frac:
        micros = int((frac[1:] + '000000')[:6])
        parsed = parsed.replace(microsecond=micros)
    return parsed.replace(tzinfo=dt.timezone.utc)


def verify_upload(local_path: str, file_obj: dict) -> dict:
    """THE POSITIVE CONTROL: the witness that the real bytes reached Google.

    It must be impossible to pass without a genuine upload.

    THE TRAP, AND THE MOST IMPORTANT LINE IN THIS MODULE: Google documents
    sha256Hash as "SHA-256 hash of the uploaded bytes. A base64-encoded
    string," which reads as base64(digest_bytes). IT IS NOT. MEASURED: it is
    base64 of the lowercase HEX STRING -- base64.b64encode(hexdigest.encode
    ('ascii')). For the measured 15s file, local hex began
    'b30caad39d556980...' and the returned value decoded to that hex text.

    Tries, in order, and reports which one matched: (1) base64(hexdigest) --
    the measured-correct form, (2) base64(digest_bytes) -- the documented
    reading, (3) the bare hexdigest. If none match, raises with all three
    computed forms and the returned value printed verbatim. This must never
    fall through to a pass: a control that hard-fails a byte-correct upload
    over an encoding convention destroys trust in every other check.

    Also asserts, each as its own named failure: sizeBytes matches the local
    file size, state == 'ACTIVE', and createTime parses and sits within this
    run's wall-clock window (an instantly-ACTIVE file with an old createTime
    is the stale/replayed-data tell).
    """
    hasher = _sha256_file(local_path)
    local_hexdigest = hasher.hexdigest()
    digest_bytes = hasher.digest()

    candidates = {
        'b64_of_hex': base64.b64encode(local_hexdigest.encode('ascii')).decode('ascii'),
        'b64_of_digest': base64.b64encode(digest_bytes).decode('ascii'),
        'hex': local_hexdigest,
    }

    returned = file_obj.get('sha256Hash')
    sha256_match = None
    for label in ('b64_of_hex', 'b64_of_digest', 'hex'):
        if candidates[label] == returned:
            sha256_match = label
            break

    if sha256_match is None:
        raise GeminiVerificationError(
            'GEMINI_SHA256_MISMATCH: none of the three computed encodings match the returned '
            f'sha256Hash. Computed b64(hex)={candidates["b64_of_hex"]!r}, '
            f'b64(digest)={candidates["b64_of_digest"]!r}, hex={candidates["hex"]!r}. Returned '
            f'value, verbatim: {returned!r}. This must never fall through to a pass.'
        )

    local_size = os.path.getsize(local_path)
    try:
        remote_size = int(file_obj['sizeBytes'])
    except (KeyError, TypeError, ValueError) as exc:
        raise GeminiVerificationError(
            f'GEMINI_SIZE_UNREADABLE: sizeBytes={file_obj.get("sizeBytes")!r} could not be '
            f'read as an int ({exc!r}).'
        ) from exc
    if remote_size != local_size:
        raise GeminiVerificationError(
            f'GEMINI_SIZE_MISMATCH: local file is {local_size} bytes, Google reports '
            f'sizeBytes={remote_size}.'
        )

    state = file_obj.get('state')
    if state != 'ACTIVE':
        raise GeminiVerificationError(
            f'GEMINI_STATE_NOT_ACTIVE: verify_upload requires state=ACTIVE, got {state!r}.'
        )

    create_time_raw = file_obj.get('createTime')
    create_time = _parse_gemini_timestamp(create_time_raw)
    if create_time is None:
        raise GeminiVerificationError(
            f'GEMINI_CREATE_TIME_UNPARSEABLE: createTime={create_time_raw!r} did not parse as '
            'an RFC3339 UTC timestamp.'
        )
    now = dt.datetime.now(dt.timezone.utc)
    delta_s = (now - create_time).total_seconds()
    if delta_s < -VERIFY_CREATE_TIME_FUTURE_SKEW_S or delta_s > VERIFY_CREATE_TIME_MAX_AGE_S:
        raise GeminiVerificationError(
            f'GEMINI_CREATE_TIME_OUT_OF_WINDOW: createTime={create_time_raw!r} is {delta_s:.0f}s '
            f'from now ({now.isoformat()}). An instantly-ACTIVE file with an old createTime is '
            'the stale/replayed-data tell.'
        )

    print(f'GEMINI_UPLOAD_VERIFIED sha256_match={sha256_match} size_ok=1 state={state}')
    return {
        'sha256_match': sha256_match,
        'sha256_candidates': candidates,
        'size_ok': True,
        'local_size': local_size,
        'remote_size': remote_size,
        'state': state,
        'create_time': create_time_raw,
    }


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate_content(file_uri: str, mime_type: str, prompt: str, api_key: str,
                     fps: float | None = None, media_resolution: str | None = None,
                     model: str | None = None) -> dict:
    """POST .../models/{model}:generateContent against an uploaded Files API file.

    `video_metadata` is a SIBLING of `file_data` INSIDE THE SAME PART -- not
    in generation_config, not its own part. Omitted entirely when fps is
    None. `media_resolution` is request-level, under generation_config, and
    omitted when None. `file_data` is put FIRST and `text` second -- the only
    order Google exemplifies.

    MEASURED: fps genuinely works on a Files API file_uri. Same uploaded
    file, fps=1 -> promptTokenCount 4432; fps=5 -> 20212. This combination
    appears in no Google example and zero times in their REST reference, and
    a still-open bug report claims it is broken. It works. Do not substitute
    an alternative spelling.

    Raises loudly (with a body truncated to ERROR_BODY_MAX_CHARS, via
    GeminiHTTPError) on any non-200, and raises GeminiEmptyCompletionError, a
    DISTINCT type, when the response is 200 but carries no text -- the
    empty-completion case, which Brief 2 needs to tell apart from a transport
    failure in order to retry it.

    Returns the whole parsed response dict plus the extracted text, so the
    caller keeps the raw shape. `responseId` (the successor to OpenRouter's
    generation id) is also surfaced at the top level for convenience.
    """
    model = model or DEFAULT_GEMINI_MODEL
    url = f'{GEMINI_HOST}/v1beta/models/{model}:generateContent'
    headers = {
        'x-goog-api-key': api_key,
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT,
    }

    file_part: dict = {'file_data': {'mime_type': mime_type, 'file_uri': file_uri}}
    if fps is not None:
        file_part['video_metadata'] = {'fps': fps}

    body: dict = {'contents': [{'parts': [file_part, {'text': prompt}]}]}
    if media_resolution is not None:
        body['generation_config'] = {'media_resolution': media_resolution}

    payload = json.dumps(body).encode('utf-8')
    _status, _headers, resp_body = _http_call(
        'POST', url, headers, payload, GENERATE_TIMEOUT_S)
    parsed = json.loads(resp_body.decode('utf-8'))

    text_parts: list = []
    candidates = parsed.get('candidates')
    if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict):
        content = candidates[0].get('content')
        parts = content.get('parts') if isinstance(content, dict) else None
        if isinstance(parts, list):
            # A part may carry other keys (e.g. thought parts); skip those
            # rather than raising, per the brief.
            for part in parts:
                if isinstance(part, dict) and isinstance(part.get('text'), str):
                    text_parts.append(part['text'])

    text = ''.join(text_parts)
    if not text:
        raise GeminiEmptyCompletionError(
            f'GEMINI_EMPTY_COMPLETION: model={model} returned 200 with no text in any '
            f'candidate part. usageMetadata={parsed.get("usageMetadata")!r}. Distinct from a '
            'transport failure so this can be retried on its own terms.'
        )

    return {'response': parsed, 'text': text, 'response_id': parsed.get('responseId')}


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def delete_file(name: str, api_key: str) -> bool:
    """DELETE {GEMINI_HOST}/v1beta/{name}, then GET the same name and require it gone.

    MEASURED: the follow-up GET returns 403 (not 404). Treat 403 OR 404 as
    proof of deletion; anything else (including a 200) means the file
    survived. A leak must not abort a run that has already produced its
    answer -- it never raises on a leak -- but it must be loud and greppable:
    this is the successor to the local-temp cleanup check the old base64 path
    had, and a file left on Google's store lives 48 hours.
    """
    headers = {'x-goog-api-key': api_key, 'User-Agent': USER_AGENT}
    url = f'{GEMINI_HOST}/v1beta/{name}'

    _http_call('DELETE', url, headers, None, GENERATE_TIMEOUT_S)

    try:
        _http_call('GET', url, headers, None, GENERATE_TIMEOUT_S)
    except GeminiHTTPError as exc:
        if exc.status in (403, 404):
            print(f'GEMINI_FILE_DELETED name={name} verified=1')
            return True
        print(f'GEMINI_FILE_LEAKED name={name} http={exc.status}')
        return False

    # The follow-up GET succeeded (200): the file is still there.
    print(f'GEMINI_FILE_LEAKED name={name} http=200')
    return False


# ---------------------------------------------------------------------------
# Motion measurement (drives choose_fps)
# ---------------------------------------------------------------------------

def _escape_lavfi_path(path: str) -> str:
    """Escape a path for embedding inside a lavfi 'movie=' filter argument.

    ffmpeg's filtergraph parser treats ':' and other characters specially, so
    the whole path is wrapped in single quotes; the only thing that then
    needs escaping is a literal single quote, escaped by closing the quote,
    emitting an escaped quote, and reopening the quote -- ffmpeg's own
    documented escaping idiom for filter option values.
    """
    return path.replace("'", "'\\''")


def measure_motion(path: str) -> float:
    """Mean inter-frame luma difference over the WHOLE clip, used to pick fps.

    Measures the WHOLE clip, never samples: a full pass is 20s on the longest
    clip ever delivered (113.8s), and sampling would miss a clip that starts
    static and turns frantic. If ffprobe fails or returns no frames, RAISES --
    never returns 0.0, because 0.0 silently means "static" and would pick the
    wrong fps forever.

    MEASURED separation on 1080x1920 clips: coding streams 0.287-1.665, IRL
    concert 10.600.
    """
    filter_arg = f"movie='{_escape_lavfi_path(str(path))}',tblend=all_mode=difference,signalstats"
    cmd = [
        'ffprobe', '-v', 'error', '-f', 'lavfi', '-i', filter_arg,
        '-show_entries', 'frame_tags=lavfi.signalstats.YAVG', '-of', 'csv=p=0',
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT_S)
    except subprocess.TimeoutExpired as exc:
        raise GeminiMotionMeasurementError(
            f'GEMINI_MOTION_MEASUREMENT_FAILED: ffprobe timed out after {FFPROBE_TIMEOUT_S}s '
            f'on {path}: {exc!r}'
        ) from exc

    if result.returncode != 0:
        raise GeminiMotionMeasurementError(
            f'GEMINI_MOTION_MEASUREMENT_FAILED: ffprobe exited {result.returncode} on {path}. '
            f'stderr (verbatim): {result.stderr!r}'
        )

    values: list = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            values.append(float(line))
        except ValueError:
            continue

    if not values:
        raise GeminiMotionMeasurementError(
            f'GEMINI_MOTION_MEASUREMENT_FAILED: ffprobe returned no numeric frames for {path}. '
            'Never returning 0.0: that silently means "static" and would pick the wrong fps '
            f'forever. stdout (verbatim): {result.stdout!r}'
        )

    yavg = sum(values) / len(values)
    print(f'MOTION_MEASURED path={Path(path).name} interframe_yavg={yavg:.3f} frames={len(values)}')
    return yavg


def choose_fps(yavg: float, threshold: float = 3.0, low: float = 1.0,
               high: float = 5.0) -> tuple[float, str]:
    """(fps, reason) from measure_motion's yavg. MEASURED separation on
    1080x1920 clips: coding streams 0.287-1.665, IRL concert 10.600.

    Trivial, but its own function so Brief 2 can record both the chosen value
    and the reason on the kit row, and so the threshold is tunable in one
    place.
    """
    if yavg >= threshold:
        return high, 'motion'
    return low, 'static'
