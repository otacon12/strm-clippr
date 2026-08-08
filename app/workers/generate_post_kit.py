#!/usr/bin/env python3
"""generate_post_kit: the POST KIT for one delivered clip (D-061, D-062).

THE SYSTEM SUGGESTS COPY. IT NEVER BURNS TEXT INTO VIDEO AND IT NEVER POSTS.
The operator is the publish gate (D-002). Everything this worker writes is a
draft he reads, edits and pastes himself.

Per clip it produces THREE distinct things, in the operator's own vocabulary
(D-062 ruling 5, which corrected CC's loose use of the word "caption"):

  1. ON-VIDEO TEXT — the hook he types into the platform's own text tool.
     THREE variants, one short line each, deliberately SPREAD ALONG THE
     CONCRETENESS AXIS (withholding / domain-named / payoff-named) rather than
     reworded three ways.
  2. CAPTIONS — the real speech transcription as an SRT, built by build_srt.py
     from the whisper segments already stored for this window.
  3. VIDEO CAPTION — the post descriptor, plus at most three hashtags.

TWO MODEL CALLS, BOTH THROUGH OPENROUTER (D-062 ruling 3). OpenRouter was the
operator's preference AND is the only path that can honour his quality ruling:
n8n's native Gemini node exposes no fps and no media_resolution control, so it
would silently run at provider defaults, which is exactly the lever D-061
ruled on.

  CALL 1 — Gemini WATCHES the clip. The clip travels as a base64 data URL in a
  video_url content part, together with the VERBATIM transcript, because the
  model hears the audio natively but mis-hears this project's vocabulary
  (n8n, psycopg2, Coolify) and whisper already got it right.

  CALL 2 — Haiku WRITES the kit from Gemini's description PLUS the same
  verbatim transcript. Haiku holding the real transcript is a CORRECTNESS
  requirement, not a quality preference: the strongest hooks are lines the
  speaker actually said, and a model reconstructing a quote from a prose
  summary would be inventing one. An invented quote on a public post is
  exactly the over-claim the charter forbids.

QUALITY PASSTHROUGH, DEGRADED LOUDLY, NEVER SILENTLY. OpenRouter forwards
provider-specific keys under provider.options keyed by provider slug, and each
endpoint publishes allowed_passthrough_parameters. This worker READS that list
at runtime and sends only what the endpoint accepts, then records what was
requested, what was accepted and what was dropped, both on stdout and in the
kit row (passthrough_degraded). A kit that ran at provider defaults is
queryable afterwards instead of being a log line nobody reads.

THE ANTI-INVENTION GATES (any failure writes ZERO rows and fails loudly, the
same shape as transcript_signal_ingest.py's response gating):
  - the writer's response must parse as a JSON object
  - all three hooks present, non-empty, distinct, within the research limits
  - hashtags a list of at most three, each a single '#' token
  - no em dash anywhere in anything that could reach a platform
  - quoted_line, when present, must appear VERBATIM in the transcript of the
    shipped clip, or the whole run fails
  - and so must EVERY double-quoted span inside the hooks and the video
    caption, declared or not: a gate that reads only the field the model
    volunteered is a self-report, and a model that simply omits the
    declaration walks straight past it

A FABRICATED QUOTE COSTS A RETRY, NOT THE KIT. The quote is OPTIONAL: kits
ship without one and read fine. So when validation fails SPECIFICALLY because a
quotation is not in the transcript, the WRITER alone is re-asked once, for copy
with no quoted line at all, and the already-paid vision result is reused. The
kit row records that fallback (post_kits.quote_fallback + quote_fallback_reason,
migration 007) because a kit that silently lost its quote to a fabrication
otherwise looks identical to a clip that never had a quotable line. Every OTHER
validation failure still fails loudly on the first try. Measured 2026-08-07:
candidate 45 failed FOUR attempts on INVENTED_QUOTE, inventing a different
plausible sentence each time, and ended the batch with no kit at all.

THE PAYLOAD CEILING, CHECKED BEFORE SENDING (D-064). OpenRouter sits behind
Cloudflare, whose standard maximum request body is 100 MB, and base64 inflates a
file by 4/3. Measured 2026-08-07 against the live API: a 4.7 MB clip made a
6.2 MB payload and worked, a 35.4 MB clip made a 47.2 MB payload and worked, and
an 89.4 MB clip made a 119.2 MB payload that returned 502 five times out of five.
That 502 called ITSELF retryable, so the retry policy below would have re-uploaded
the whole video until the attempt cap, every run, forever. So the exact body size
is computed from the file's size BEFORE the request is built, and when it would
breach the ceiling (CLPR_POST_KIT_MAX_PAYLOAD_BYTES, default 50,000,000) a
THROWAWAY downscaled copy is transcoded for the model alone and deleted the
moment it is encoded. Operator ruling 2026-08-07, verbatim: "for now downscale".
THE DELIVERED CLIP IS NEVER MODIFIED, a clip under the ceiling produces a
byte-identical request to the one it produced before this existed, and a kit
written from a degraded input records that fact with the real numbers
(post_kits.analysis_downscaled and friends, migration 008). When even a
downscaled copy cannot fit, the run fails loudly with the numbers and names the
real fix, which is the Gemini Files API.

TRANSIENT TRANSPORT FAULTS RETRY THEMSELVES. Of 27 clips in that same batch, 6
failed and 5 of those succeeded on an IDENTICAL later retry with no code
change: five OPENROUTER_EMPTY_CONTENTs and a 502 whose own body said
"retryable":true,"retry_after":60. So 429, 5xx, timeouts and empty completions
are retried (honouring retry_after when supplied, else exponential backoff, cap
and delays env-tunable), while any other 4xx is not, because a bad request does
not fix itself. Every retry is logged with its attempt number and reason: a run
that only worked on the third try must never read as a clean first try.

THE WORKER OWNS ITS OWN FAILURE RECORD. All 6 of those failures left
post_kit_requests completely EMPTY, because a request row was only ever created
by the review server's endpoint and the batch invoked this CLI. In the review
UI the failed clips looked like nothing had ever been attempted. Now a run with
no outstanding request opens one itself, so there is ALWAYS a row to close:
satisfied on success, failed with the reason verbatim on any failure, written
on a separate connection that COMMITS while the kit transaction rolls back.

WHAT IT DELIVERS. With CLPR_POSTKIT_OUT (or --kit-out-dir) set, the run writes
two files named after the DELIVERED clip so they sort beside its mp4 in Drive:
`<clip-stem>.vN.txt` (the copy, always) and `<clip-stem>.vN.srt` (the captions,
only when the window really holds transcript segments). Their absolute paths
are published on the RESULT line as kit_file="..." and srt_file="...", the same
shape render_from_slice.py has been emitting on the live verdict lane, because
the n8n file nodes parse exactly that. The version is IN the name: Drive holds
two files of the same name in one folder without complaint, so a regenerate
that reused the stem would leave nothing on his phone to say which is current.

RE-DELIVERY IS FREE AND CRASH-PROOF. If an active kit already exists and no
regenerate was asked for, the run does NOT crash and does NOT pay again: it
rewrites that kit's files and publishes their paths, so a lane that died after
the row committed (a failed upload, a wiped staging directory) recovers by
being run again instead of being permanently undeliverable.

THE REGENERATE LEDGER (004) HAS A CONSUMER, AND IT IS THIS WORKER. The review
UI never spends money from a browser button: it records an INTENT in
post_kit_requests. This worker reads the outstanding rows for the candidate,
treats them as "regenerate", honours force_over_operator_edit from the NEWEST
one only, marks every outstanding row 'satisfied' in the SAME transaction as
the kit insert, and on any failure marks them 'failed' with the reason
VERBATIM. Without that last part a failed generation is invisible: the UI shows
"regenerate requested" forever and the operator never learns it died.

NEVER SILENTLY OVERWRITE THE OPERATOR. Kits are versioned. An operator edit is
its own row (origin='operator_edit') and is authoritative from then on: a
regenerate over an active operator edit REFUSES unless --force is passed, and
even then the edit row survives as history.

  *** n8n MUST NEVER PASS --force. *** The automatic post-delivery trigger
  regenerating over an operator edit would be precisely the silent overwrite
  the versioning exists to prevent. --force is a human's deliberate act.

SECRETS: the OpenRouter key is read from a file whose path comes from
CLPR_OPENROUTER_ENV (default /home/node/.n8n/clpr/.openrouter_env), mirroring
the .pg_env pattern already on the volume. Missing file = loud failure naming
the path. There is deliberately NO fallback to an environment variable: a
silent fallback is how the wrong credential gets used. The key, the base64 and
the request body are NEVER logged.

Connects via the shared adapter app/workers/db.py (CLPR_DB_URL). Tables per
app/docs/naming-map.md plus migrations_pg/003_post_kits.sql. RESULT line last.
Exits non-zero on any failure, failure verdict to stderr (D-047).
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import build_srt
import db
import gemini_files
import transcript_signal as ts

PROMPT_VERSION = 'post_kit_v2'

OPENROUTER_BASE = 'https://openrouter.ai/api/v1'

# Cloudflare fronts openrouter.ai and 403s the default `Python-urllib/<ver>`
# User-Agent as a bot signature. Measured on this project 2026-08-07 (commit
# 5fceb4f): EVERY verdict webhook silently failed until a real User-Agent was
# set. Do not remove this header.
USER_AGENT = 'clpr-post-kit/1.0'

# The writer model. Cited from the repo's own constant rather than from memory:
# app/workers/transcript_signal.py's OPENROUTER_MODEL default is
# 'anthropic/claude-haiku-4.5'.
DEFAULT_WRITER_MODEL = 'anthropic/claude-haiku-4.5'

# The vision model slug. UNVERIFIED against OpenRouter's live catalogue (no key
# exists on the build machine), which is why it is env-overridable AND why the
# endpoints probe below runs first: an unknown slug fails there, loudly, before
# a single byte of video is uploaded.
DEFAULT_VISION_MODEL = 'google/gemini-2.5-pro'

DEFAULT_KEY_ENV_PATH = '/home/node/.n8n/clpr/.openrouter_env'
KEY_NAME = 'OPENROUTER_API_KEY'

# D-061 ruling 3, verbatim: "let's keep the fps and video quality high for max
# quality reccomendations." The tuning insight from the same ruling: for screen
# capture, fps and resolution move in OPPOSITE directions, so low fps plus high
# media resolution is both cheaper and sharper on a mostly-static coding screen.
DEFAULT_FPS = 1.0
DEFAULT_MEDIA_RESOLUTION = 'MEDIA_RESOLUTION_HIGH'

VISION_TIMEOUT_S = 900
WRITER_TIMEOUT_S = 180
PROBE_TIMEOUT_S = 60

# ONE TRUTH for the writer's token ceiling: it is sent in the request AND
# recorded in post_kits.writer_params, and two copies of it would drift.
WRITER_MAX_TOKENS = 1400

# TRANSIENT FAILURES RETRY THEMSELVES (measured on the 27-clip batch of
# 2026-08-07). Six clips failed on the first pass and FIVE of them succeeded on
# an IDENTICAL later retry with no code change at all: candidates 5, 7, 15, 44
# and 54 each returned one OPENROUTER_EMPTY_CONTENT and then worked, and a 502
# from OpenRouter's edge carried a body that itself said
# `"retryable":true,"retry_after":60`. A failure the provider DECLARES
# retryable, that this project then measured recovering unchanged, is a
# transport fault, not a result. It is retried here instead of costing an
# operator round trip.
#
# The cap and the delays are env-tunable because the right numbers depend on
# the batch: a 27-clip run wants patience, a single hand run wants to fail
# fast. Defaults are deliberately modest.
DEFAULT_MAX_ATTEMPTS = 4          # 1 first try + 3 retries
DEFAULT_RETRY_BASE_DELAY_S = 2.0  # doubled per attempt
DEFAULT_RETRY_MAX_DELAY_S = 60.0  # also the ceiling on a provider's retry_after

# A STRUCTURALLY INCOMPLETE WRITER RESPONSE IS A SAMPLING WOBBLE, NOT A DEFECT
# (measured 2026-08-08, see MalformedWriterResponseError's own docstring):
# candidate 1 failed with `on_video_text.payoff is missing or empty` and an
# IDENTICAL re-run succeeded, same clip, same prompt, same model, no code
# change. So unlike the other validation failures, the SAME writer ask is
# worth repeating. 1 first try + 2 structural re-asks by default, env-tunable
# for the same reason DEFAULT_MAX_ATTEMPTS above is.
WRITER_STRUCTURAL_MAX_ATTEMPTS = 3

# ---------------------------------------------------------------------------
# THE PAYLOAD CEILING (D-064). MEASURED, not assumed.
#
# OpenRouter sits behind Cloudflare, whose standard maximum request body is
# 100 MB, and base64 inflates a file by 4/3. Measured against the live API on
# 2026-08-07:
#
#     clip   raw       base64 payload   vision call
#     c43    4.7 MB    6.2 MB           OK
#     c45    35.4 MB   47.2 MB          OK
#     c111   89.4 MB   119.2 MB         502, five times out of five
#
# THE BOUNDARY IS THEREFORE KNOWN TO SIT SOMEWHERE IN (47.2 MB, 119.2 MB]. It
# is not known more precisely than that, and this worker does not pretend it is.
#
# THE ARITHMETIC FOR THE DEFAULT. Cloudflare's documented limit is 100 MB, which
# is ambiguous between 100 * 1000 * 1000 and 100 * 1024 * 1024. The conservative
# reading is the decimal one, 100,000,000 bytes, so that is what is assumed.
# The first default, 80,000,000, was chosen from that arithmetic alone: 20%
# under the decimal reading, enough to absorb the JSON envelope, the prompt and
# the transcript. **IT WAS REFUTED BY MEASUREMENT** (see the constant below):
# c111 failed twice beneath it. Arithmetic headroom against a documented limit
# is not evidence that a request will be accepted, because the limit that
# actually bites may not be the one that is documented. The default is now set
# from the measured boundary instead, and the paragraph above is kept because
# the 100 MB reading it establishes is still what the hard clamp uses.
#
# WHY IT IS ENV TUNABLE, AND THIS IS NOT DECORATION. If the true limit turns out
# to be BELOW the current default, the 502 guard in openrouter_call converts
# what would otherwise be an endless retry into one line telling the operator to
# lower this number. A one-variable fix, with no code change and no rebuild.
# That is exactly the path that produced the 50 MB default: the guard fired, the
# operator re-ran as it instructed, and the second failure settled it.
#
# AND IT TUNES IN ONE DIRECTION ONLY: DOWN. CLPR_POST_KIT_MAX_PAYLOAD_BYTES can
# lower the ceiling and can never raise it past
# CLOUDFLARE_ASSUMED_MAX_BODY_BYTES, because raising it past the documented
# limit does not move the limit, it only blinds the worker to it. That knob is
# the first one an operator reaches for when a clip gets downscaled and he would
# rather it did not, and without the clamp the consequences were exact and bad:
# at 150,000,000 the c111 payload of 119,200,000 bytes reads as fitting, no
# downscale happens, the request is sent, Cloudflare 502s it, and the 502 guard
# STAYS SILENT because its threshold is a FRACTION OF THE CEILING and had been
# raised along with it. That is the original D-064 failure, restored in full by
# one environment variable. So payload_ceiling_bytes() clamps, and it says so.
# ---------------------------------------------------------------------------
CLOUDFLARE_ASSUMED_MAX_BODY_BYTES = 100_000_000
# 80,000,000 was an ESTIMATE derived from Cloudflare's documented 100 MB with
# headroom. Two live runs of c111 on 2026-08-07 refuted it, and the second one
# is the informative one because the SYMPTOM CHANGED while the cause did not:
#   attempt 1  -> 502 origin_bad_gateway   (claims "retryable": true)
#   attempt 2  -> 400 "Invalid JSON payload received. Closing quote expected in
#                 string."  <- Google's parser choking on a TRUNCATED body
# A truncation is what an over-large request looks like once something upstream
# cuts it instead of refusing it, so this size limit has now worn three masks in
# one day: a container OOM, a "retryable" 502, and a JSON parse error. NONE of
# them says "too big". The only honest handle on the real boundary is measured
# evidence, and it brackets it: 47,200,000 bytes SUCCEEDED (c45, live), and
# c111 failed twice below 80,000,000. So the default drops to just above the
# largest payload actually proven to work, rather than being guessed high a
# second time. Operator-approved 2026-08-07: "yes lower it to 50MB".
DEFAULT_PAYLOAD_CEILING_BYTES = 50_000_000

# A 502 on a request whose payload sits this close to the ceiling is treated as
# a SIZE rejection and is never retried. See openrouter_call.
SIZE_GUARD_FRACTION = 0.8

# THE ANALYSIS COPY. Only ever built when a clip would otherwise breach the
# ceiling, and deleted the moment it has been encoded.
#
#   - 1280 on the LONG side. Gemini tokenises video frames at a fixed low
#     resolution regardless of what is uploaded, so 1280 is already more pixels
#     than the model consumes, and the scale expression is written so it can
#     never UPSCALE a clip that is already smaller.
#   - 15 fps cap. This worker asks the provider to sample at DEFAULT_FPS = 1.0,
#     so a 15 fps transport copy still carries fifteen times the frames the
#     analysis will look at. If the fps passthrough is dropped, Gemini's own
#     default sampling is 1 fps, so the cap is safe in both worlds.
#   - Audio KEPT, mono at 64 kbit/s. Gemini listens as well as watches, and one
#     minute of it costs about 0.5 MB, which is nothing against the budget.
#   - The bitrate target is a PREDICTION, never a guarantee, so the real output
#     size is measured and the encode is repeated at a corrected bitrate rather
#     than sending something over the limit and hoping.
ANALYSIS_TARGET_FRACTION = 0.9    # aim at 90% of the ceiling, not at its edge
ANALYSIS_MAX_DIMENSION = 1280
ANALYSIS_MAX_FPS = 15
ANALYSIS_AUDIO_BITRATE_BPS = 64_000
ANALYSIS_CONTAINER_OVERHEAD_FACTOR = 0.97  # mp4 muxing overhead, measured small
ANALYSIS_MAX_TRANSCODE_ATTEMPTS = 3

# Below this the picture stops carrying information a vision model can read, so
# a "successful" downscale to it would be a lie dressed as a saving. Under it,
# the run fails loudly and names the real fix instead.
ANALYSIS_MIN_VIDEO_BITRATE_BPS = 150_000

FFPROBE_TIMEOUT_S = 120
FFMPEG_TIMEOUT_S = 1800

# Research limits, enforced here AND in the 003 CHECK constraints. TikTok's own
# creative guidance is 5-10 words per second of reading, so a hook read in the
# first two seconds is 10-20 words maximum, and shorter is safer.
MAX_HOOK_WORDS = 20
MAX_HOOK_CHARS = 120
MAX_CAPTION_CHARS = 600
MAX_HASHTAGS = 3

# Em dash and horizontal bar. Banned in anything that can reach a platform.
BANNED_DASHES = ('—', '―')

MIN_SCENE_DESCRIPTION_CHARS = 40

# QUOTATION SPANS INSIDE THE COPY ITSELF, not just the declared quoted_line.
# The declared field is a SELF-REPORT: a model that simply does not declare a
# quote sails past a gate that only reads that field. Measured on this worker
# before the gate existed: with quoted_line=null, the hook
# 'He said "this build is cursed"' was ACCEPTED against a transcript that never
# contained the phrase. The property is "no fabricated quotation reaches the
# operator as real", so every double-quoted span in the four platform-bound
# fields is checked against the transcript, not only the field the model
# volunteered. Single quotes are deliberately NOT scanned: apostrophes make
# them ambiguous, and a false rejection here aborts a run that has already paid
# for two model calls.
QUOTE_SPAN_RES = (
    re.compile(r'"([^"\n]{1,400})"'),
    re.compile(r'“([^”\n]{1,400})”'),
)

# Spans shorter than this are emphasis or a proper noun ("post kit", "n8n"),
# not a claim that somebody said a sentence. Three words is where a span starts
# reading as reported speech.
MIN_QUOTED_SPAN_WORDS = 3


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


# ---------------------------------------------------------------------------
# Credential
# ---------------------------------------------------------------------------

def read_api_key(path: str) -> str:
    """Read OPENROUTER_API_KEY out of a KEY=VALUE env file. Never logged."""
    p = Path(path)
    if not p.is_file():
        raise RuntimeError(
            f'MISSING_OPENROUTER_ENV: no OpenRouter credential file at {p}. This worker '
            f'reads {KEY_NAME} from that file (path overridable with CLPR_OPENROUTER_ENV), '
            'mirroring the .pg_env pattern already on the n8n volume. Create it, owned '
            'node:node, mode 0600, containing a single '
            f'{KEY_NAME}=<key> line. There is no environment-variable fallback on purpose.'
        )
    try:
        raw = p.read_text(encoding='utf-8')
    except OSError as exc:
        raise RuntimeError(f'MISSING_OPENROUTER_ENV: cannot read {p} ({exc!r})') from exc

    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if stripped.startswith('export '):
            stripped = stripped[len('export '):].strip()
        if '=' not in stripped:
            continue
        name, _, value = stripped.partition('=')
        if name.strip() != KEY_NAME:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        if value:
            return value

    raise RuntimeError(
        f'MISSING_OPENROUTER_ENV: {p} exists but contains no non-empty {KEY_NAME}= line.'
    )


# ---------------------------------------------------------------------------
# Typed failures. A retry policy that classifies by matching substrings of an
# error MESSAGE is a proxy for the property (charter 1.5): the property is
# "what kind of fault was this", and only the raise site knows. So the raise
# site says so, in the type, and carries the status code and the provider's own
# retry_after with it.
# ---------------------------------------------------------------------------

class OpenRouterError(RuntimeError):
    """Anything that went wrong talking to OpenRouter."""


class OpenRouterHTTPError(OpenRouterError):
    """A real HTTP status came back. `status` decides retryability, never prose."""

    def __init__(self, message: str, status: int, retry_after: float | None = None):
        super().__init__(message)
        self.status = int(status)
        self.retry_after = retry_after


class OpenRouterUnreachable(OpenRouterError):
    """No status at all: DNS, connection, reset, or a timeout. Always transient."""


class OpenRouterEmptyContent(OpenRouterError):
    """A 200 whose completion carried no text. MEASURED transient on this project."""


class PayloadTooLargeError(OpenRouterError):
    """The request body is too big for the endpoint. NEVER retryable.

    Its own type because the failure it names arrives WEARING A DISGUISE.
    Measured 2026-08-07: a 119.2 MB payload came back as `502
    origin_bad_gateway` whose own body said `"retryable": true,
    "retry_after": 60`, five times out of five. Cloudflare is describing its
    ORIGIN there, not the request, and the request is a permanent deterministic
    size rejection. Believing the body would re-upload 89 MB of video until the
    attempt cap, every run, forever.
    """


class InventedQuoteError(RuntimeError):
    """The copy contains a quotation that is NOT in the transcript.

    Its own type because it is the ONE validation failure with a cheap, honest
    remedy: the quote is optional, so the writer can be asked again for copy
    with no quoted line at all. Every other validation failure still fails
    loudly on the first try, and keeping them apart is a TYPE decision rather
    than a string match so a reworded message can never silently widen the
    retry (charter gate 10: specify the invariant, never a proxy).
    """


class MalformedWriterResponseError(RuntimeError):
    """The writer returned a STRUCTURALLY incomplete response: unparseable JSON,
    or a required field missing or empty.

    Its own type, added 2026-08-08, because this class of failure was MEASURED
    to be a sampling wobble rather than a defect. Candidate 1 failed with
    `on_video_text.payoff is missing or empty` and then SUCCEEDED on an
    identical re-run -- same clip, same prompt, same model, no code change --
    producing kit 32. The writer runs at temperature 0.7, so "did the model
    emit every required key this time" is a draw from a distribution, not a
    property of the input.

    That falsifies the reasoning this file carried until today, which grouped a
    missing field with a banned dash and an over-length hook on the grounds
    that "none of those is fixed by ... retrying them would only pay twice for
    the same defect." True for a defect. A missing key is not a defect, it is a
    bad roll, and the cost of treating it as permanent is the entire kit.

    DELIBERATELY NARROW. Only structural incompleteness lives here. A banned
    dash, an over-length hook, a hashtag over the cap and an invented quote all
    keep their existing behaviour: those describe copy the model DID produce,
    where a retry really would pay twice to be told the same thing. Split by
    TYPE, never by message text, for the same reason as InventedQuoteError.
    """


def _find_retry_after(node, depth: int = 0) -> float | None:
    """OpenRouter's own `retry_after`, wherever in the error body it sits.

    Measured verbatim on this project 2026-08-07: a 502 from the edge returned a
    body stating `"retryable":true,"retry_after":60`. The exact nesting is not
    contracted anywhere, so the value is searched for rather than assumed at a
    path that may move.
    """
    if depth > 6:
        return None
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ('retry_after', 'retryAfter', 'retry_after_seconds') and isinstance(
                    value, (int, float)) and not isinstance(value, bool):
                return float(value)
        for value in node.values():
            found = _find_retry_after(value, depth + 1)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _find_retry_after(value, depth + 1)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# HTTP seam — ONE function, so a test can stand in front of the network
# ---------------------------------------------------------------------------

def _http_json(method: str, url: str, api_key: str, payload: dict | None, timeout: int) -> dict:
    """The single HTTP seam. Never logs the key, the body, or the base64.

    Deliberately carries NO retry logic. The retry lives one level up in
    openrouter_call(), because this function is the seam a test stubs: a policy
    implemented below the seam would be deleted by its own test (charter gate
    14, a stub silently deletes the thing under test).
    """
    data = None
    headers = {
        'Authorization': f'Bearer {api_key}',
        'User-Agent': USER_AGENT,
        'Accept': 'application/json',
    }
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode('utf-8', errors='replace')
    # ORDER MATTERS AND IS NOT COSMETIC: HTTPError is a subclass of URLError,
    # which is a subclass of OSError, and TimeoutError IS socket.timeout and is
    # also an OSError. A broader clause first would swallow the narrower case
    # and every 4xx would look like a transient transport fault.
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')[:800]
        retry_after: float | None = None
        header_value = None
        try:
            header_value = exc.headers.get('Retry-After') if exc.headers else None
        except Exception:  # noqa: BLE001 - a malformed header must never mask the status
            header_value = None
        if header_value:
            try:
                retry_after = float(str(header_value).strip())
            except ValueError:
                retry_after = None
        if retry_after is None:
            try:
                retry_after = _find_retry_after(json.loads(detail))
            except (ValueError, TypeError):
                retry_after = None
        raise OpenRouterHTTPError(
            f'OPENROUTER_HTTP_ERROR: {method} {url} returned status {exc.code}. '
            f'Response body (first 800 chars, verbatim): {detail!r}',
            status=exc.code,
            retry_after=retry_after,
        ) from exc
    except urllib.error.URLError as exc:
        raise OpenRouterUnreachable(
            f'OPENROUTER_UNREACHABLE: {method} {url} failed: {exc!r}') from exc
    except (TimeoutError, OSError) as exc:
        # A read that times out mid-body raises socket.timeout/OSError directly,
        # NOT a URLError, so without this clause the single most obviously
        # transient failure there is would have escaped the retry policy.
        raise OpenRouterUnreachable(
            f'OPENROUTER_UNREACHABLE: {method} {url} failed: {exc!r}') from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OpenRouterError(
            f'OPENROUTER_BAD_JSON: {method} {url} did not return JSON ({exc}). '
            f'First 400 chars, verbatim: {body[:400]!r}'
        ) from exc
    if not isinstance(parsed, dict):
        raise OpenRouterError(f'OPENROUTER_BAD_JSON: {method} {url} returned a non-object payload')
    if 'error' in parsed and parsed['error']:
        # AN ERROR CAN ARRIVE INSIDE A 200. When that error object carries a
        # NUMERIC code, that code is the same fact an HTTP status is, so it is
        # classified the same way and a rate limit does not stop being a rate
        # limit for having travelled in the body. Only a genuinely numeric code
        # is trusted: a string code like 'invalid_request' is left as a plain,
        # NON-retryable OpenRouterError, which is the conservative direction
        # (fail loudly rather than pay twice for the same rejection).
        code = None
        if isinstance(parsed['error'], dict):
            raw_code = parsed['error'].get('code')
            if isinstance(raw_code, int) and not isinstance(raw_code, bool):
                code = raw_code
            elif isinstance(raw_code, str) and raw_code.strip().isdigit():
                code = int(raw_code.strip())
        message = f'OPENROUTER_API_ERROR: {parsed["error"]!r}'
        if code is not None and (code == 429 or code >= 500):
            raise OpenRouterHTTPError(message, status=code,
                                      retry_after=_find_retry_after(parsed))
        raise OpenRouterError(message)
    return parsed


# ---------------------------------------------------------------------------
# The retry policy, ABOVE the seam
# ---------------------------------------------------------------------------

def _env_number(name: str, default: float) -> float:
    """A numeric env override, or the default. NON-FINITE IS NOT A NUMBER HERE.

    float() happily accepts 'nan', 'inf' and '1e400', and every one of them is
    poison downstream: int(nan) raises ValueError, int(inf) raises
    OverflowError, and time.sleep(inf) parks the run forever. Each of those
    would escape this function's own WARN path and either abort the run from
    inside a config read or hang it silently, so finiteness is checked HERE,
    once, rather than at each of the three call sites that could be bitten.
    """
    raw = os.environ.get(name, '').strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        print(
            f'WARN {name}={raw!r} is not a number, using the default {default}. '
            'The value is ignored, never guessed at.',
            file=sys.stderr,
        )
        return default
    if not math.isfinite(value):
        print(
            f'WARN {name}={raw!r} is not a finite number, using the default {default}. '
            'The value is ignored, never guessed at.',
            file=sys.stderr,
        )
        return default
    return value


def retry_policy() -> dict:
    """Attempts and delays, env-tunable, floored so a bad value cannot disable
    the first attempt itself."""
    return {
        'max_attempts': max(1, int(_env_number('CLPR_POST_KIT_MAX_ATTEMPTS', DEFAULT_MAX_ATTEMPTS))),
        'base_delay_s': max(0.0, _env_number('CLPR_POST_KIT_RETRY_BASE_DELAY_S',
                                             DEFAULT_RETRY_BASE_DELAY_S)),
        'max_delay_s': max(0.0, _env_number('CLPR_POST_KIT_RETRY_MAX_DELAY_S',
                                            DEFAULT_RETRY_MAX_DELAY_S)),
    }


def classify_failure(exc: BaseException) -> tuple[bool, str]:
    """(retryable, reason). Retryable means the SAME request may work unchanged.

    Retryable: 429, any 5xx, any transport fault or timeout, and an empty
    completion. NOT retryable: any other 4xx (a bad request does not fix
    itself), a non-JSON body, an error object in a 200, and every validation
    failure, which is about the CONTENT and would come back identical.
    """
    if isinstance(exc, OpenRouterEmptyContent):
        return True, 'empty_completion'
    if isinstance(exc, OpenRouterUnreachable):
        return True, 'transport_or_timeout'
    if isinstance(exc, OpenRouterHTTPError):
        if exc.status == 429:
            return True, 'http_429_rate_limited'
        if exc.status >= 500:
            return True, f'http_{exc.status}_server'
        return False, f'http_{exc.status}_client'
    return False, type(exc).__name__


def retry_delay_s(attempt: int, policy: dict, retry_after: float | None) -> float:
    """Honour the provider's own retry_after when it supplies one, else back off
    exponentially. Both are capped: a rogue retry_after of 3600 must not park a
    27-clip batch for an hour."""
    if retry_after is not None and retry_after > 0:
        return min(float(retry_after), policy['max_delay_s'])
    return min(policy['base_delay_s'] * (2 ** (attempt - 1)), policy['max_delay_s'])


def openrouter_call(which: str, method: str, url: str, api_key: str, payload: dict | None,
                    timeout: int, expect_message: bool,
                    size_guard: dict | None = None) -> tuple[dict, str | None]:
    """One OpenRouter call, retried on transient faults only.

    EVERY retry is logged with its attempt number and its reason, so a run that
    only succeeded on the third try can never read as a clean first-try success.

    `expect_message` pulls the completion text INSIDE the loop on purpose: an
    empty completion is a 200, so a policy that only saw the HTTP layer could
    not retry the single most common transient failure this project measured.

    A retry re-sends the payload it was given and nothing else. The vision
    payload is the only one that carries video, and the writer payload is text
    only, so a writer retry can never re-upload a clip.

    `size_guard`, when supplied, is {'predicted_bytes': int, 'ceiling_bytes':
    int} for THIS request. It exists because A 5xx FROM THIS ENDPOINT CANNOT BE
    TRUSTED TO MEAN "TRANSIENT". Cloudflare fronts openrouter.ai and rejects an
    oversized body at its edge with `502 origin_bad_gateway`, whose payload
    self-describes as `"retryable": true, "retry_after": 60`. That flag is about
    Cloudflare's origin, not about the request, and the request is a permanent
    deterministic size failure: it was measured failing five times out of five,
    unchanged. Retrying it re-uploads the whole video for nothing. So a 502 on a
    request already near the ceiling is reported AS a size rejection and is not
    retried. The pre-send gate in _generate should make this unreachable, which
    is exactly why it is here: it is the check that fires if the real limit
    turns out to sit lower than the ceiling this worker assumed. A 413 needs no
    guard, because classify_failure already refuses to retry any non-429 4xx.
    """
    policy = retry_policy()
    attempts = policy['max_attempts']
    for attempt in range(1, attempts + 1):
        try:
            response = _http_json(method, url, api_key, payload, timeout)
            text = message_text(response, which) if expect_message else None
        except Exception as exc:  # noqa: BLE001 - classified immediately below
            retryable, reason = classify_failure(exc)
            if (retryable and size_guard and isinstance(exc, OpenRouterHTTPError)
                    and exc.status == 502
                    and size_guard['predicted_bytes']
                    >= SIZE_GUARD_FRACTION * size_guard['ceiling_bytes']):
                raise PayloadTooLargeError(
                    f'OPENROUTER_PAYLOAD_TOO_LARGE: call={which} got status 502 on a request '
                    f'whose body is {size_guard["predicted_bytes"]} bytes, which is at least '
                    f'{SIZE_GUARD_FRACTION:.0%} of the configured ceiling of '
                    f'{size_guard["ceiling_bytes"]} bytes. At this size a 502 is treated as a '
                    'SIZE rejection at the Cloudflare edge and is NOT retried, because retrying '
                    'a size rejection re-uploads the entire video and fails identically '
                    '(measured five times out of five on 2026-08-07) even though its own body '
                    'says "retryable": true. '
                    'SAY THE HONEST PART: THIS CALL CANNOT TELL THE TWO APART FROM ONE 502. A '
                    'genuine transient outage returns the same status, and one was measured in '
                    'the 27-clip batch that recovered on an unchanged retry, so this run may '
                    'have been refused a retry it would have survived. THE TEST THAT SEPARATES '
                    'THEM IS A RE-RUN, and it is cheap: a size rejection is deterministic and '
                    'fails the same way every time, while an outage clears. RE-RUN THIS CLIP '
                    'FIRST. If it succeeds, this was an outage and nothing needs changing. If it '
                    'fails at the same size again, it is the limit, and THEN the fix is to lower '
                    'CLPR_POST_KIT_MAX_PAYLOAD_BYTES so the analysis copy is downscaled further, '
                    'or to move this call to the Gemini Files API, which has no such body limit. '
                    'Do not lower the ceiling on a single 502: that degrades every later clip to '
                    'fix something that may not be broken. Zero rows written.'
                ) from exc
            if not retryable:
                if attempt > 1:
                    print(f'OPENROUTER_RETRY_ABANDONED call={which} attempt={attempt}/{attempts} '
                          f'reason={reason} (not retryable, failing loudly)')
                raise
            if attempt >= attempts:
                print(f'OPENROUTER_RETRIES_EXHAUSTED call={which} attempts={attempt}/{attempts} '
                      f'reason={reason} (still failing, failing loudly)')
                raise
            delay = retry_delay_s(attempt, policy, getattr(exc, 'retry_after', None))
            print(
                f'OPENROUTER_RETRY call={which} attempt={attempt}/{attempts} reason={reason} '
                f'delay_s={delay:.1f} '
                f'provider_retry_after={getattr(exc, "retry_after", None)} error={exc}'
            )
            time.sleep(delay)
            continue
        if attempt > 1:
            print(f'OPENROUTER_RETRY_SUCCEEDED call={which} attempt={attempt}/{attempts} '
                  '(this run was NOT a clean first try)')
        return response, text
    # Unreachable: the loop either returns or raises. Present so a future edit
    # to the loop bounds cannot fall through to an implicit None.
    raise OpenRouterError(f'OPENROUTER_RETRY_LOOP_FELL_THROUGH: call={which}')


# ---------------------------------------------------------------------------
# Provider passthrough — consult, then degrade LOUDLY
# ---------------------------------------------------------------------------

def _endpoint_slug(endpoint: dict) -> str | None:
    for key in ('tag', 'provider_slug', 'slug', 'provider_name', 'name'):
        value = endpoint.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower().replace(' ', '-')
    return None


def fetch_endpoints(model: str, api_key: str) -> list[dict]:
    """The model's endpoints, each publishing allowed_passthrough_parameters.

    Doubles as the model-slug validator: an unknown slug fails HERE, before any
    video is encoded or uploaded.
    """
    url = f'{OPENROUTER_BASE}/models/{model}/endpoints'
    # Retried like every other OpenRouter call: this probe is cheap, carries no
    # video, and a 502 on it would otherwise abort a run before the paid work
    # even started. An unknown model slug raises OPENROUTER_NO_ENDPOINTS below,
    # which is NOT retryable and still fails on the first try.
    payload, _ = openrouter_call(
        'endpoints', 'GET', url, api_key, None, PROBE_TIMEOUT_S, expect_message=False)
    data = payload.get('data')
    if isinstance(data, dict):
        endpoints = data.get('endpoints')
    else:
        endpoints = payload.get('endpoints')
    if not isinstance(endpoints, list) or not endpoints:
        raise RuntimeError(
            f'OPENROUTER_NO_ENDPOINTS: {url} returned no endpoints for model {model!r}. '
            'Either the model slug is wrong (override with CLPR_POST_KIT_VISION_MODEL) '
            'or the model is not currently served.'
        )
    return [e for e in endpoints if isinstance(e, dict)]


def build_provider_options(endpoints: list[dict], desired: dict) -> dict:
    """provider.options carrying only keys an endpoint actually accepts.

    `desired` is keyed by the LOGICAL parameter (what the operator ruled for),
    each mapping to the alias spellings that mean it:

        {'media_resolution': {'media_resolution': 'HIGH', 'mediaResolution': 'HIGH'},
         'fps':              {'video_metadata': {...}, 'videoMetadata': {...}}}

    That distinction is the whole point of the shape. Both spellings are
    offered because nobody knows which one a given endpoint publishes, and only
    ONE of them will ever be accepted, so counting the other as "dropped" would
    raise the degraded flag on every single healthy run. A flag that is always
    on is not a warning, it is wallpaper. Degradation is measured against the
    logical parameter: it is dropped only when NO spelling of it got through.

    Nothing is guessed: a key absent from allowed_passthrough_parameters is
    never sent.
    """
    options: dict = {}
    accepted: dict = {}
    dropped: dict = {}

    for endpoint in endpoints:
        slug = _endpoint_slug(endpoint)
        allowed_raw = endpoint.get('allowed_passthrough_parameters')
        allowed = {str(a) for a in allowed_raw} if isinstance(allowed_raw, list) else set()
        if not slug:
            continue
        taken: dict = {}
        satisfied: list[str] = []
        for logical, aliases in desired.items():
            hit = {k: v for k, v in aliases.items() if k in allowed}
            if hit:
                taken.update(hit)
                satisfied.append(logical)
        missed = sorted(k for k in desired if k not in satisfied)
        if taken:
            options[slug] = taken
            accepted[slug] = sorted(satisfied)
        if missed:
            dropped[slug] = missed

    # DEGRADED IS PER-REQUEST, AND WE DO NOT CHOOSE THE ENDPOINT. OpenRouter
    # routes each request to ONE endpoint at request time, so a union-wide
    # "some endpoint accepted something" can be true while the endpoint that
    # actually served this call dropped every quality parameter and ran at
    # provider defaults. That is exactly the silent default D-061 ruling 3
    # exists to prevent, so the flag is raised whenever ANY reachable endpoint
    # would drop a logical parameter. Over-flagging is the safe direction: the
    # kit is still written, the operator can still query which endpoints
    # accepted what in vision_params, and nothing is thrown away.
    degraded = (not options) or bool(dropped)
    return {
        'options': options,
        'requested': sorted(desired),
        'accepted': accepted,
        'dropped': dropped,
        'degraded': degraded,
    }


def log_passthrough(report: dict) -> None:
    """Say exactly what was and was not accepted. This is the loud part of
    'degrade loudly': a silent default is the failure mode being prevented."""
    print(f'PASSTHROUGH_REQUESTED {json.dumps(report["requested"], sort_keys=True)}')
    print(f'PASSTHROUGH_ACCEPTED {json.dumps(report["accepted"], sort_keys=True)}')
    print(f'PASSTHROUGH_DROPPED {json.dumps(report["dropped"], sort_keys=True)}')
    if report['degraded']:
        if not report['options']:
            why = 'NO endpoint accepted ANY of the quality parameters'
        else:
            why = (
                'at least one endpoint this request could be routed to would drop a quality '
                f'parameter ({json.dumps(report["dropped"], sort_keys=True)}), and OpenRouter '
                'picks the endpoint, not this worker'
            )
        print(
            f'PASSTHROUGH_DEGRADED 1 - {why}, so this analysis may run at the provider '
            'defaults, NOT at the high fps/resolution the operator ruled for (D-061 ruling '
            '3). The kit row records this.'
        )
    else:
        print('PASSTHROUGH_DEGRADED 0')


# ---------------------------------------------------------------------------
# Prompts. The research is quoted INSIDE the prompt so the model obeys the
# evidence rather than folklore.
# ---------------------------------------------------------------------------

def subject_block(subject_kind: str, subject_text: str | None, context_notes: str | None) -> str:
    if subject_kind == 'me':
        who = (
            'WHO IS ON SCREEN: the channel owner himself (the person described in the '
            'CHANNEL PROFILE below). Use that profile as his identity and voice.'
        )
    elif subject_kind == 'other':
        who = (
            'WHO IS ON SCREEN: NOT the channel owner. This clip is of someone else: '
            f'{subject_text}\n'
            'Never write as if the channel owner is the person speaking or performing. '
            'The channel profile below is the CHANNEL you are posting to, not the person '
            'in this clip.'
        )
    else:
        who = (
            'WHO IS ON SCREEN: NOT IDENTIFIED. Do not assume it is the channel owner. '
            'Do not name, gender or characterise the person. Refer to what happens, not '
            'to who it is. The channel profile below is the CHANNEL you are posting to.'
        )
    notes = f'\nWHAT THIS RECORDING IS: {context_notes}' if context_notes else ''
    return who + notes


def profile_block(profile: dict | None) -> str:
    if not profile:
        return (
            'CHANNEL PROFILE: none on record. Write neutrally and do not invent a channel '
            'name, a handle, a persona or a catchphrase.'
        )
    lines = ['CHANNEL PROFILE (the channel this will be posted to):']
    for label, key in (
        ('channel', 'channel_name'),
        ('handle', 'handle'),
        ('platforms', 'platforms'),
        ('style', 'style_notes'),
        ('do not', 'do_nots'),
        ('other context', 'extra_context'),
    ):
        value = profile.get(key)
        if value:
            lines.append(f'- {label}: {value}')
    return '\n'.join(lines)


def build_vision_prompt(subject: str, profile: str, transcript_lines: str, duration_s: float) -> str:
    return (
        'You are analysing one short vertical video clip so that a copywriter can describe '
        'it accurately. You are NOT writing marketing copy.\n\n'
        f'{subject}\n\n'
        f'{profile}\n\n'
        'THE VERBATIM TRANSCRIPT OF THIS EXACT CLIP is below, produced by whisper and '
        'timestamped in CLIP-RELATIVE seconds. It is authoritative for what was SAID. '
        'It is correct about this project\'s vocabulary (for example n8n, psycopg2, '
        'Coolify, ffmpeg, OBS), which speech recognition usually gets wrong, so prefer it '
        'over what you think you hear.\n'
        f'--- transcript ({duration_s:.1f}s clip) ---\n'
        f'{transcript_lines or "(no speech in this window)"}\n'
        '--- end transcript ---\n\n'
        'Describe, in plain prose and no more than 250 words:\n'
        '1. What is visibly happening, in order. If the screen shows code, a terminal, a '
        'UI or an error, say what it actually says.\n'
        '2. The single most striking or surprising moment, and roughly when it happens in '
        'clip-relative seconds.\n'
        '3. The visible reaction or outcome, if any.\n\n'
        'HARD RULES:\n'
        '- Report only what you can actually see or what the transcript says. Never guess '
        'a name, a number, a product or a fact.\n'
        '- If something is unreadable or unclear, say so plainly. Unknown stays unknown.\n'
        '- No markdown, no headings, no bullet characters. Plain prose only.\n'
        '- Do not use the em dash character (U+2014). Use commas, colons or periods.'
    )


def build_writer_prompt(subject: str, profile: str, scene: str, transcript_plain: str,
                        duration_s: float, forbid_quotes: bool = False) -> str:
    """The writer prompt. With forbid_quotes, the ONLY difference is that no
    quotation of any kind may appear.

    That variant is the no-quote fallback (see the header): the quote is
    optional, so a fabricated one costs one cheap writer call rather than the
    whole kit. Everything else about the prompt is byte-identical, so the
    retry is a rewrite of the same brief and not a different job.
    """
    if forbid_quotes:
        quote_rule = (
            '- DO NOT QUOTE ANYTHING AT ALL. Your previous attempt returned a quotation that '
            'does NOT appear in the transcript, which means it was never said. Write this kit '
            'with NO quoted line: return null for quoted_line, and use no double quotation '
            'marks anywhere in the hooks or the video caption. Describe what was said in your '
            'own words instead. A kit with no quote is completely normal and is what is wanted '
            'here.\n'
        )
    else:
        quote_rule = (
            '- If you use a direct quote, it must appear WORD FOR WORD in the transcript, and '
            'you must also return it in the quoted_line field so it can be checked. If you '
            'quote nothing, return null for quoted_line.\n'
        )
    return (
        'You write short-form social copy for one vertical video clip. The copy is a '
        'SUGGESTION: a human reviews and edits every word before anything is posted.\n\n'
        f'{subject}\n\n'
        f'{profile}\n\n'
        f'WHAT A VIDEO MODEL SAW IN THE CLIP ({duration_s:.1f}s):\n{scene}\n\n'
        'THE VERBATIM TRANSCRIPT OF THE CLIP (authoritative for anything quoted):\n'
        f'{transcript_plain or "(no speech in this clip)"}\n\n'
        'RESEARCH YOU MUST OBEY, because it is measured evidence and not folklore:\n'
        '- Instagram officially demotes reels that are majority text '
        '(https://about.instagram.com/blog/announcements/instagram-ranking-explained). '
        'So each on-video line is ONE SHORT LINE. Never a paragraph, never two sentences.\n'
        '- TikTok\'s own creative guidance puts reading speed at 5 to 10 words per second '
        '(https://ads.tiktok.com/help/article/creative-best-practices), so a hook read in '
        f'the first two seconds is {MAX_HOOK_WORDS} words MAXIMUM and shorter is safer.\n'
        '- Hashtags are near-dead as reach. Instagram caps them at 5, and a study of 24.3 '
        'million posts measured 31% FEWER views on posts carrying them. Produce at most '
        f'{MAX_HASHTAGS}, and produce none at all rather than padding.\n'
        '- Concreteness follows an INVERTED U. A meta-analysis of 8,977 headline '
        'experiments (https://pmc.ncbi.nlm.nih.gov/articles/PMC11704130/) found 50.9% of '
        'headlines got WORSE when made more concrete and only 8.7% better. So the three '
        'on-video variants must be SPREAD ALONG THE CONCRETENESS AXIS, not three '
        'rewordings of one idea:\n'
        '    withheld: names the tension or stakes without naming the subject matter\n'
        '    domain:   names what the clip is about, without giving away the outcome\n'
        '    payoff:   names the specific concrete result or number\n'
        '- There is NO controlled evidence that questions beat statements or the reverse. '
        'Do not prefer either form. Pick whichever fits the individual line.\n\n'
        'HARD RULES:\n'
        '- NEVER invent a fact, a number, a name, a product or a quote. Everything you '
        'write must be supported by the scene description or the transcript above. If you '
        'do not know something, leave it out.\n'
        + quote_rule +
        '- Do not use the em dash character (U+2014) anywhere. Use commas, colons, '
        'parentheses or periods.\n'
        '- No emoji in the on-video lines.\n'
        '- Do not write "in this video" or "watch till the end".\n\n'
        'Return ONE JSON object and nothing else, in exactly this shape:\n'
        '{\n'
        '  "on_video_text": {"withheld": "...", "domain": "...", "payoff": "..."},\n'
        '  "video_caption": "the descriptor that accompanies the post, a few sentences at '
        'most",\n'
        f'  "hashtags": ["#one", "#two"],  // at most {MAX_HASHTAGS}, may be empty\n'
        '  "quoted_line": "verbatim line from the transcript, or null"\n'
        '}'
    )


# ---------------------------------------------------------------------------
# Response validation — any failure writes ZERO rows
# ---------------------------------------------------------------------------

def _normalise_quote(text: str) -> str:
    keep = []
    for ch in text.lower():
        if ch.isalnum():
            keep.append(ch)
        elif ch.isspace():
            keep.append(' ')
    return ' '.join(''.join(keep).split())


def _quoted_spans(value: str) -> list[str]:
    """Every double-quoted span in a copy field, straight and curly."""
    spans: list[str] = []
    for pattern in QUOTE_SPAN_RES:
        spans.extend(pattern.findall(value))
    return spans


def _reject_invented_quotation(field: str, value: str, transcript_plain: str) -> None:
    """Any quotation INSIDE the copy must be real, whether or not the model
    declared it in quoted_line. A self-declared field cannot police itself."""
    haystack = _normalise_quote(transcript_plain)
    for span in _quoted_spans(value):
        normalised = _normalise_quote(span)
        if len(normalised.split()) < MIN_QUOTED_SPAN_WORDS:
            continue
        if not normalised or normalised not in haystack:
            raise InventedQuoteError(
                f'INVENTED_QUOTE: {field} contains the quotation {span!r}, which does not '
                'appear in the transcript of the shipped clip. The model put words in '
                "somebody's mouth. It is rejected whether or not it was declared in "
                'quoted_line, because a self-declared field is not a gate. Zero rows written.'
            )


def _reject_banned_dash(field: str, value: str) -> None:
    for dash in BANNED_DASHES:
        if dash in value:
            raise RuntimeError(
                f'COPY_REJECTED: {field} contains a banned dash character (U+{ord(dash):04X}). '
                'Nothing that can reach a social platform may carry one. Zero rows written.'
            )


def validate_kit(raw_text: str, transcript_plain: str) -> dict:
    """Parse and gate the writer's response. Raises on ANY defect."""
    try:
        data = json.loads(ts.extract_json_payload(raw_text))
    except (ValueError, TypeError) as exc:
        raise MalformedWriterResponseError(
            f'MALFORMED_WRITER_RESPONSE: the writer model did not return parseable JSON '
            f'({exc}). First 400 chars, verbatim: {raw_text[:400]!r}. Zero rows written.'
        ) from exc
    if not isinstance(data, dict):
        raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: response is not a JSON object. Zero rows written.')

    hooks_raw = data.get('on_video_text')
    if not isinstance(hooks_raw, dict):
        raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: missing on_video_text object. Zero rows written.')

    hooks: dict = {}
    for key in ('withheld', 'domain', 'payoff'):
        value = hooks_raw.get(key)
        if not isinstance(value, str) or not value.strip():
            raise MalformedWriterResponseError(
                f'MALFORMED_WRITER_RESPONSE: on_video_text.{key} is missing or empty. '
                'All three concreteness variants are required. Zero rows written.'
            )
        value = ' '.join(value.split())
        words = len(value.split())
        if words > MAX_HOOK_WORDS:
            raise RuntimeError(
                f'COPY_REJECTED: on_video_text.{key} is {words} words, over the '
                f'{MAX_HOOK_WORDS}-word ceiling that TikTok\'s own 5-10 words-per-second '
                'guidance puts on a two-second hook. NOT truncated: a clipped line is a '
                'line the operator never read. Zero rows written.'
            )
        if len(value) > MAX_HOOK_CHARS:
            raise RuntimeError(
                f'COPY_REJECTED: on_video_text.{key} is {len(value)} characters, over the '
                f'{MAX_HOOK_CHARS} ceiling. Zero rows written.'
            )
        _reject_banned_dash(f'on_video_text.{key}', value)
        _reject_invented_quotation(f'on_video_text.{key}', value, transcript_plain)
        hooks[key] = value

    if len({v.lower() for v in hooks.values()}) != 3:
        raise RuntimeError(
            'COPY_REJECTED: the three on-video variants are not distinct. They must be '
            'spread along the concreteness axis, not reworded. Zero rows written.'
        )

    caption = data.get('video_caption')
    if not isinstance(caption, str) or not caption.strip():
        raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: video_caption is missing or empty. Zero rows written.')
    caption = caption.strip()
    if len(caption) > MAX_CAPTION_CHARS:
        raise RuntimeError(
            f'COPY_REJECTED: video_caption is {len(caption)} characters, over the '
            f'{MAX_CAPTION_CHARS} ceiling. Zero rows written.'
        )
    _reject_banned_dash('video_caption', caption)
    _reject_invented_quotation('video_caption', caption, transcript_plain)

    hashtags_raw = data.get('hashtags')
    if hashtags_raw is None:
        hashtags: list[str] = []
    elif isinstance(hashtags_raw, list):
        hashtags = []
        for item in hashtags_raw:
            if not isinstance(item, str) or not item.strip():
                raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: a hashtag is not a non-empty string. Zero rows written.')
            tag = item.strip()
            if not tag.startswith('#') or len(tag.split()) != 1 or len(tag) < 2:
                raise RuntimeError(
                    f'COPY_REJECTED: hashtag {tag!r} is not a single #token. Zero rows written.'
                )
            _reject_banned_dash('hashtags', tag)
            hashtags.append(tag)
    else:
        raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: hashtags is not a list. Zero rows written.')

    if len(hashtags) > MAX_HASHTAGS:
        raise RuntimeError(
            f'COPY_REJECTED: {len(hashtags)} hashtags returned, over the {MAX_HASHTAGS} '
            'ceiling (a 24.3M-post study measured 31% FEWER views on posts with them). '
            'NOT trimmed: the operator decides what to drop. Zero rows written.'
        )

    quoted = data.get('quoted_line')
    if quoted is not None:
        if not isinstance(quoted, str):
            raise MalformedWriterResponseError('MALFORMED_WRITER_RESPONSE: quoted_line is not a string or null. Zero rows written.')
        quoted = quoted.strip()
        if not quoted:
            quoted = None
        else:
            _reject_banned_dash('quoted_line', quoted)
            if _normalise_quote(quoted) not in _normalise_quote(transcript_plain):
                raise InventedQuoteError(
                    f'INVENTED_QUOTE: quoted_line {quoted!r} does not appear in the '
                    'transcript of the shipped clip. The model fabricated a line that was '
                    'never said. Zero rows written.'
                )

    return {
        'hook_withheld': hooks['withheld'],
        'hook_domain': hooks['domain'],
        'hook_payoff': hooks['payoff'],
        'video_caption': caption,
        'hashtags': hashtags,
        'quoted_line': quoted,
    }


def message_text(response: dict, which: str) -> str:
    choices = response.get('choices') or []
    if not choices:
        # Same family as an empty completion: a 200 that carried no result. It
        # is retryable for the same measured reason.
        raise OpenRouterEmptyContent(
            f'OPENROUTER_NO_CHOICES: the {which} response carried no choices')
    message = (choices[0] or {}).get('message') or {}
    content = message.get('content')
    if isinstance(content, list):
        # Some providers return content as parts.
        content = ''.join(
            part.get('text', '') for part in content if isinstance(part, dict)
        )
    text = str(content or '').strip()
    if not text:
        # An empty completion is LOUD but was not DIAGNOSTIC: on a 27-clip batch
        # five candidates failed here and the message could not distinguish a
        # safety refusal from a truncated reasoning budget from a provider that
        # simply returned nothing. finish_reason and usage are already in the
        # response and are the only things that tell those apart, so they are
        # reported. `refusal` and `reasoning` are surfaced as PRESENCE and
        # LENGTH only: a refusal string can quote the input, and this text is
        # written to logs the operator reads on stream.
        choice = choices[0] or {}
        usage = response.get('usage') or {}
        reasoning = message.get('reasoning')
        raise OpenRouterEmptyContent(
            f'OPENROUTER_EMPTY_CONTENT: the {which} response carried no text content '
            f'(finish_reason={choice.get("finish_reason")!r} '
            f'native_finish_reason={choice.get("native_finish_reason")!r} '
            f'refusal_present={message.get("refusal") is not None} '
            f'reasoning_chars={len(reasoning) if isinstance(reasoning, str) else 0} '
            f'prompt_tokens={usage.get("prompt_tokens")} '
            f'completion_tokens={usage.get("completion_tokens")} '
            f'provider={response.get("provider")!r})'
        )
    return text


# ---------------------------------------------------------------------------
# Context layers
# ---------------------------------------------------------------------------

def fetch_profile(cur) -> dict | None:
    cur.execute(
        'SELECT version, channel_name, handle, platforms, style_notes, do_nots, extra_context '
        'FROM creator_profile WHERE is_active = 1'
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        'version': int(row[0]),
        'channel_name': row[1],
        'handle': row[2],
        'platforms': row[3],
        'style_notes': row[4],
        'do_nots': row[5],
        'extra_context': row[6],
    }


def fetch_context(cur, recording_id: int) -> dict | None:
    cur.execute(
        'SELECT version, subject_kind, subject_text, context_notes '
        'FROM recording_context WHERE recording_id = %s AND is_active = 1',
        (recording_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        'version': int(row[0]),
        'subject_kind': str(row[1]),
        'subject_text': row[2],
        'context_notes': row[3],
    }


def fetch_active_kit(cur, candidate_id: int) -> dict | None:
    cur.execute(
        'SELECT id, version, origin FROM post_kits WHERE candidate_id = %s AND is_active = 1',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {'id': int(row[0]), 'version': int(row[1]), 'origin': str(row[2])}


def fetch_active_kit_full(cur, candidate_id: int) -> dict | None:
    """The whole active kit, for RE-DELIVERING it without paying again."""
    cur.execute(
        'SELECT id, version, origin, hook_withheld, hook_domain, hook_payoff, '
        'video_caption, hashtags, quoted_line, srt_text, srt_segment_count, '
        'srt_basis, created_at, quote_fallback, quote_fallback_reason, '
        'analysis_downscaled, analysis_source_bytes, analysis_sent_bytes '
        'FROM post_kits WHERE candidate_id = %s AND is_active = 1',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        'id': int(row[0]),
        'version': int(row[1]),
        'origin': str(row[2]),
        'hook_withheld': row[3],
        'hook_domain': row[4],
        'hook_payoff': row[5],
        'video_caption': row[6],
        'hashtags': list(row[7] or []),
        'quoted_line': row[8],
        'srt_text': row[9],
        'srt_segment_count': int(row[10] or 0),
        'srt_basis': row[11],
        'created_at': row[12],
        # 007. Carried so a RE-DELIVERED .txt says the same thing about its
        # quote as the one written on the day the kit was generated.
        'quote_fallback': int(row[13] or 0),
        'quote_fallback_reason': row[14],
        # 008. Carried for the same reason: a RE-DELIVERED .txt must say the
        # same thing about its analysis input as the one written on the day the
        # kit was generated.
        'analysis_downscaled': int(row[15] or 0),
        'analysis_source_bytes': row[16],
        'analysis_sent_bytes': row[17],
    }


def next_version(cur, candidate_id: int) -> int:
    cur.execute('SELECT COALESCE(MAX(version), 0) FROM post_kits WHERE candidate_id = %s', (candidate_id,))
    return int(cur.fetchone()[0]) + 1


# ---------------------------------------------------------------------------
# The regenerate-request ledger (004). The review UI records an INTENT and
# never spends money from a browser button, so this worker is the consumer.
# Without a consumer the request table is a queue nothing drains: the UI shows
# "regenerate requested" forever, and a run that FAILS is invisible.
# ---------------------------------------------------------------------------

def fetch_outstanding_requests(cur, candidate_id: int) -> list[dict]:
    """Requests still owed a kit, newest first.

    004's contract, verbatim: a request is outstanding while
    COALESCE(MAX(post_kits.version), 0) is still less than or equal to the
    version that was active when the operator asked. It is an INTEGER
    comparison and never a clock, so there is no format, timezone or
    ISO-versus-space ordering trap (charter gate 21).
    """
    cur.execute(
        '''
        SELECT id, active_version_at_request, force_over_operator_edit, requested_by
        FROM post_kit_requests
        WHERE candidate_id = %s
          AND state = 'requested'
          AND active_version_at_request >=
              (SELECT COALESCE(MAX(version), 0) FROM post_kits WHERE candidate_id = %s)
        ORDER BY id DESC
        ''',
        (candidate_id, candidate_id),
    )
    return [
        {
            'id': int(r[0]),
            'active_version_at_request': int(r[1]),
            'force_over_operator_edit': int(r[2]),
            'requested_by': str(r[3]),
        }
        for r in cur.fetchall()
    ]


SELF_REQUEST_ACTOR = 'worker:generate_post_kit'


def open_self_request(candidate_id: int) -> int | None:
    """Open this worker's OWN request row when nothing else asked for this kit.

    WHY THIS EXISTS, MEASURED. On the 27-clip batch of 2026-08-07 six clips
    failed and post_kit_requests was left completely EMPTY, because a request
    row is only ever created by the review server's endpoint and the batch
    invoked this CLI directly. In the review UI those six clips looked like
    nothing had ever been attempted, which is exactly the dishonest-failure
    pattern this project keeps paying for: a failure that leaves no trace is
    indistinguishable from a clip nobody has got to yet.

    So the worker owns its own record. There is now ALWAYS a row to close:
    'satisfied' with the version on success, 'failed' with the reason verbatim
    on any failure.

    THREE PROPERTIES THIS MUST HAVE, and each one is a real trap:

    1. ITS OWN CONNECTION, COMMITTED IMMEDIATELY. The read phase ends in a
       rollback and the write phase rolls back on any failure, so a row written
       on either connection would vanish precisely when it is needed.
    2. IT NEVER CHANGES WHAT THE RUN DOES. An operator's request row means
       "regenerate". This row means only "a run happened", so the caller never
       reads it back as an intent: it is created AFTER the skip gates and the
       regenerate/force decision, and it is deliberately not part of them.
    3. IT NEVER FAILS THE RUN. It is provenance. If the ledger cannot be
       written the real work still stands, and the inability to record is
       itself reported loudly to stderr.

    active_version_at_request is the honest current MAX(version), the same
    value the review server records, so a row orphaned by a hard kill (SIGKILL,
    a lost container) reads as an unanswered request and the next run completes
    the dead run's intent. That is the correct queue semantics, not a leak.
    """
    try:
        conn = db.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                'SELECT COALESCE(MAX(version), 0) FROM post_kits WHERE candidate_id = %s',
                (candidate_id,),
            )
            active_version = int(cur.fetchone()[0])
            cur.execute(
                'INSERT INTO post_kit_requests('
                '  candidate_id, active_version_at_request, force_over_operator_edit, '
                '  state, requested_by, requested_at'
                ") VALUES (%s, %s, 0, 'requested', %s, %s) RETURNING id",
                (candidate_id, active_version, SELF_REQUEST_ACTOR, utc_now_iso()),
            )
            new_id = int(cur.fetchone()[0])
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - provenance must never fail the run
        print(
            f'REQUEST_SELF_OPEN_FAILED candidate={candidate_id}: {exc!r}. This run will produce '
            'no post_kit_requests row, so a failure below would be invisible in the review UI. '
            'The run itself continues.',
            file=sys.stderr,
        )
        return None
    print(
        f'REQUEST_SELF_OPENED id={new_id} candidate={candidate_id} by={SELF_REQUEST_ACTOR} '
        f'active_version_at_request={active_version} (nobody asked for this kit through the '
        'review UI, so the worker owns the record: there is now something to mark satisfied or '
        'failed, and a failure cannot look like a clip nobody attempted)'
    )
    return new_id


def mark_requests_satisfied(cur, request_ids: list[int], version: int) -> int:
    """Close EVERY outstanding request, not just the newest one.

    Leaving an older sibling open would fire a surprise regenerate on the next
    run: the lane would spend money again for an intent that was already
    answered by the kit sitting in front of him.
    """
    if not request_ids:
        return 0
    cur.execute(
        "UPDATE post_kit_requests SET state = 'satisfied', satisfied_kit_version = %s "
        'WHERE id = ANY(%s)',
        (version, request_ids),
    )
    return cur.rowcount


def mark_requests_failed(request_ids: list[int], reason: str) -> None:
    """Record a refusal or failure against the requests it answers.

    THE TRANSACTION BOUNDARY IS THE SUBTLE PART, so it is stated rather than
    implied. Its own connection, because the caller's transaction is being
    rolled back: a failed run persists NO kit (charter gate 9), and this is the
    loud report, not a partial result. The kit rolls back and the FAILURE
    RECORD commits, and those two facts must be on two different connections
    for both to be true at once. The reason is stored VERBATIM. This function
    never raises: a marker that cannot be written must not replace the real
    error with its own, so it prints and returns.

    ONLY STILL-OPEN REQUESTS ARE MARKED. `state = 'requested'` in the WHERE
    clause is what stops a failure AFTER the kit committed (a file write, an
    SRT write) from rewriting an already-satisfied row into a failure and
    contradicting the kit sitting in the database. The ledger answers "is a kit
    owed", and once the kit exists the answer is no. The run still exits
    non-zero, and re-running is free because an existing active kit is
    re-delivered rather than regenerated.
    """
    if not request_ids:
        return
    text = (reason or '').strip() or 'unknown failure (the worker raised with no message)'
    try:
        conn = db.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE post_kit_requests SET state = 'failed', error = %s "
                "WHERE id = ANY(%s) AND state = 'requested'",
                (text, request_ids),
            )
            marked = cur.rowcount
            conn.commit()
        finally:
            conn.close()
        print(
            f'REQUESTS_FAILED marked={marked} ids={request_ids} (the review UI now shows this '
            'failure with the reason, instead of a request that waits forever)',
            file=sys.stderr,
        )
    except Exception as exc:  # noqa: BLE001 - never mask the real failure
        print(
            f'REQUEST_MARK_FAILED could not record the failure against post_kit_requests '
            f'{request_ids}: {exc!r}. The original failure follows and is the one that matters.',
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# The two delivered FILES. D-062 ruling 1, verbatim: "copy goes to a txt file
# in google drive, with a file name corresponding to the clip and the UI review
# too is great." The review UI is bound to 127.0.0.1, so Drive is the only one
# of the two surfaces that reaches the phone he actually posts from.
# ---------------------------------------------------------------------------

def fetch_clip_names(cur, candidate_id: int) -> dict:
    cur.execute(
        'SELECT file_path, drive_sync_path FROM clips WHERE candidate_id = %s',
        (candidate_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f'CLIP_NOT_RENDERED: candidate_id={candidate_id} has no clips row, so there is no '
            'clip for a post kit to be named after or to describe.'
        )
    return {
        'file_path': str(row[0]) if row[0] is not None else None,
        'drive_sync_path': str(row[1]) if row[1] is not None else None,
    }


def clip_stem(names: dict, candidate_id: int) -> str:
    """The delivered clip's stem, so the kit sorts next to its mp4 in Drive.

    The Drive name wins over the local path: the Drive name is what the
    operator actually sees in the folder on his phone, and it is the thing the
    kit has to sort beside.
    """
    for value in (names.get('drive_sync_path'), names.get('file_path')):
        if not value:
            continue
        # basename only: the stem becomes a FILE NAME, so a stored path must
        # never be able to steer where this worker writes.
        stem = Path(str(value)).name
        if '.' in stem:
            stem = stem.rsplit('.', 1)[0]
        stem = ''.join(ch for ch in stem if ch.isalnum() or ch in ('-', '_', '.')).strip('. ')
        if stem:
            return stem
    return f'candidate_{candidate_id}'


def kit_file_paths(out_dir: str, stem: str, version: int) -> tuple[Path, Path]:
    """Both output paths for one kit VERSION.

    The version is in the name on purpose. Google Drive happily holds two files
    with the same name in one folder, so a regenerate that reused the stem
    would drop a second `<stem>.txt` beside the first with nothing on his phone
    to say which is current. `<stem>.v2.txt` still corresponds to the clip and
    is unambiguous.
    """
    base = f'{stem}.v{version}'
    return Path(out_dir) / f'{base}.txt', Path(out_dir) / f'{base}.srt'


def render_kit_text(kit: dict, candidate_id: int, version: int, origin: str,
                    clip_name: str | None) -> str:
    """The .txt the operator reads on his phone. Plain text, no markdown.

    It is a DRAFT, and it says so at the top: the operator is the publish gate
    (D-002) and nothing here has been posted by anything.
    """
    lines: list[str] = []
    lines.append(f'POST KIT for {clip_name or f"candidate {candidate_id}"}')
    lines.append(f'candidate {candidate_id}, kit version {version}, written by: {origin}')
    lines.append('')
    lines.append('This is a SUGGESTION. Nothing has been posted. Edit anything you like.')
    lines.append('')
    lines.append('ON VIDEO TEXT (type ONE of these into the platform text tool, one line)')
    lines.append(f'  1. withheld: {kit["hook_withheld"]}')
    lines.append(f'  2. domain:   {kit["hook_domain"]}')
    lines.append(f'  3. payoff:   {kit["hook_payoff"]}')
    lines.append('')
    lines.append('VIDEO CAPTION (the post descriptor)')
    lines.append(f'  {kit["video_caption"]}')
    lines.append('')
    hashtags = list(kit.get('hashtags') or [])
    lines.append('HASHTAGS')
    lines.append(f'  {" ".join(hashtags) if hashtags else "(none, deliberately)"}')
    lines.append('')
    lines.append('CAPTIONS')
    if kit.get('srt_text'):
        lines.append(
            f'  the .srt beside this file, {int(kit.get("srt_segment_count") or 0)} cues, '
            'built from the stored whisper transcript. Upload it instead of letting the '
            'platform auto caption.'
        )
    else:
        lines.append(
            '  none. This window holds no transcript segments, so no subtitle file was '
            'written. An empty SRT would be a claim that nothing was said.'
        )
    if kit.get('quoted_line'):
        lines.append('')
        lines.append('QUOTED LINE (checked word for word against the transcript)')
        lines.append(f'  {kit["quoted_line"]}')
    if int(kit.get('quote_fallback') or 0) == 1:
        # A kit that silently lost its quote to a fabrication looks exactly
        # like a clip that never had a quotable line. It is not the same thing,
        # and the operator reads this file, so it says so here as well as in
        # the kit row.
        #
        # TWO WORDINGS, BECAUSE THE REWRITE DOES NOT ALWAYS DROP THE QUOTE. The
        # no-quote prompt ASKS for quoted_line null, and a model is free to
        # return a real one anyway, which validate_kit then checks word for
        # word against the transcript and accepts. A single wording saying the
        # copy "was rewritten with no quoted line" would then sit directly
        # underneath a QUOTED LINE block in the same file and contradict it.
        # The quote that survives is real, so the note says what is true of
        # THIS file rather than what was asked for.
        lines.append('')
        lines.append('NOTE ON THE QUOTE')
        if kit.get('quoted_line'):
            lines.append(
                '  The first draft of this kit quoted a line that does not appear in the '
                'transcript of this clip. That draft was rejected in full and the copy was '
                'rewritten. The quoted line above is from the rewrite and was checked word '
                'for word against the transcript. Nothing invented reached this file.'
            )
        else:
            lines.append(
                '  The first draft of this kit quoted a line that does not appear in the '
                'transcript of this clip. That draft was rejected in full and the copy was '
                'rewritten with no quoted line. Nothing invented reached this file.'
            )
    if int(kit.get('analysis_downscaled') or 0) == 1:
        # SAY WHAT WAS DEGRADED AND WHAT WAS NOT, IN THAT ORDER. This file sorts
        # beside the delivered mp4 on his phone, so a bare word like
        # "downscaled" reads as "your video is worse now", which is false and is
        # the opposite of what happened. The clip he posts is untouched.
        src = kit.get('analysis_source_bytes')
        sent = kit.get('analysis_sent_bytes')
        lines.append('')
        lines.append('NOTE ON THE ANALYSIS COPY')
        lines.append(
            '  YOUR CLIP IS UNTOUCHED. The file beside this one is the full quality render, '
            'exactly as it was delivered, and nothing has been re-encoded for posting.'
        )
        sizes = ''
        if src and sent:
            sizes = f' ({_mb(float(src))} down to {_mb(float(sent))} for the model only)'
        lines.append(
            '  What changed is only the throwaway copy that was sent to the vision model'
            f'{sizes}: the full size clip is larger than the upload limit on the analysis '
            'endpoint, so a smaller copy was made for it and deleted straight afterwards. The '
            'model therefore saw fewer pixels than you will, which is worth knowing if a hook '
            'above misreads something small on screen.'
        )
    lines.append('')
    return '\n'.join(lines) + '\n'


def write_kit_files(out_dir: str | None, kit: dict, candidate_id: int, version: int,
                    origin: str, clip_names: dict) -> dict:
    """Write the .txt and, when there is one, the .srt. Returns the paths.

    With no output directory configured nothing is written and both paths are
    None, which is the normal shape of a hand run at a terminal.
    """
    if not out_dir:
        return {'kit_file': None, 'srt_file': None}
    stem = clip_stem(clip_names, candidate_id)
    kit_path, srt_path = kit_file_paths(out_dir, stem, version)
    kit_path.parent.mkdir(parents=True, exist_ok=True)
    # The clip's NAME, never the stored path. drive_sync_path can be a full
    # local Google Drive mount path, and a wall of directories at the top of a
    # file he reads on his phone is noise.
    delivered = clip_names.get('drive_sync_path') or clip_names.get('file_path')
    clip_name = Path(str(delivered)).name if delivered else None
    kit_path.write_text(
        render_kit_text(kit, candidate_id, version, origin, clip_name),
        encoding='utf-8',
    )
    written_srt: Path | None = None
    if kit.get('srt_text'):
        srt_path.write_text(str(kit['srt_text']), encoding='utf-8')
        written_srt = srt_path
    return {
        'kit_file': str(kit_path),
        'srt_file': str(written_srt) if written_srt is not None else None,
    }


def result_file_fields(files: dict) -> str:
    """The `kit_file="..."` / `srt_file="..."` tail of the RESULT line.

    The n8n lane's file nodes parse exactly this shape, the same one
    render_from_slice.py has been emitting on the live verdict lane. Absent
    fields are absent, never empty strings: the workflow routes on presence, so
    an empty path must never look like a file.
    """
    parts = []
    if files.get('kit_file'):
        parts.append(f'kit_file="{files["kit_file"]}"')
    if files.get('srt_file'):
        parts.append(f'srt_file="{files["srt_file"]}"')
    return (' ' + ' '.join(parts)) if parts else ''


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def clip_data_url_prefix(path: Path) -> str:
    """The `data:<mime>;base64,` head of the data URL, WITHOUT the payload.

    ONE TRUTH with encode_clip, which calls this to build the real URL. The
    pre-send size prediction needs this string's LENGTH before a single byte of
    the file has been read, and a second copy of the mime rule would drift.
    """
    suffix = path.suffix.lower().lstrip('.') or 'mp4'
    mime = 'video/mp4' if suffix in ('mp4', 'm4v') else f'video/{suffix}'
    return f'data:{mime};base64,'


def encode_clip(path: Path) -> tuple[str, int, int]:
    """base64 data URL for the clip. The bytes are NEVER logged, only sized.

    Transient cost is roughly 3 to 4 times the file size across the raw bytes,
    the base64 string and the serialised request body. A rendered clip here is
    tens of megabytes, so a few hundred megabytes transient inside a Python
    process, well within the container's 4 GB. This is NOT the D-057 class: the
    2.7 GB VOD that blew n8n's heap is a different order of magnitude and never
    touches this path.
    """
    raw = path.read_bytes()
    b64 = base64.b64encode(raw).decode('ascii')
    return f'{clip_data_url_prefix(path)}{b64}', len(raw), len(b64)


# ---------------------------------------------------------------------------
# THE PAYLOAD CEILING: check BEFORE sending, never discover by 502 (D-064)
# ---------------------------------------------------------------------------

def payload_ceiling_bytes() -> int:
    """The maximum serialised request body this worker will send, in bytes.

    CLAMPED TO CLOUDFLARE_ASSUMED_MAX_BODY_BYTES, and the clamp is the point.
    The env var exists to LOWER this number when the true edge limit turns out
    to sit under the default. Raising it above the documented limit cannot make
    a bigger body acceptable to Cloudflare, it can only stop this worker seeing
    that the body is too big, and it disarms the 502 guard at the same time
    because that guard's threshold is a fraction of THIS number. So the
    documented limit is a hard stop that no environment can lift, and an attempt
    to lift it is reported rather than silently honoured.
    """
    requested = max(1, int(_env_number('CLPR_POST_KIT_MAX_PAYLOAD_BYTES',
                                       DEFAULT_PAYLOAD_CEILING_BYTES)))
    ceiling = min(requested, CLOUDFLARE_ASSUMED_MAX_BODY_BYTES)
    if ceiling != requested:
        print(
            f'PAYLOAD_CEILING_CLAMPED requested_bytes={requested} using_bytes={ceiling} '
            f'({_mb(ceiling)}) CLPR_POST_KIT_MAX_PAYLOAD_BYTES asked for a ceiling above the '
            f'documented Cloudflare request body limit of {CLOUDFLARE_ASSUMED_MAX_BODY_BYTES} '
            'bytes. That variable can only LOWER the ceiling. Raising it would not make a '
            'larger body acceptable at the edge, it would only hide the overflow from this '
            'worker and disarm the 502 size guard, which is exactly the D-064 failure.'
        )
    return ceiling


def b64_length(raw_bytes: int) -> int:
    """The EXACT base64 length of `raw_bytes` bytes: four characters per group
    of three, the last group padded. Not an estimate and not a ratio."""
    return 4 * ((raw_bytes + 2) // 3)


def body_bytes(body: dict) -> int:
    """The exact serialised size of a request body, in bytes.

    len() of the string IS the byte count here, and that is a property of
    json.dumps rather than an assumption: its default ensure_ascii=True escapes
    every non-ASCII character, so the output is pure ASCII and one character is
    one byte. Called with the SAME default settings _http_json uses (a bare
    json.dumps), because a prediction measured with different separators or
    sort_keys would silently stop describing the body that actually goes out.
    """
    return len(json.dumps(body))


def predicted_payload_bytes(envelope: int, prefix_len: int, raw_bytes: int) -> int:
    """The exact size of the request body that `raw_bytes` of video will make.

    EXACT, not approximate, and the verification proves it to the byte: the
    base64 alphabet (A-Za-z0-9+/=) and the data URL prefix contain no character
    json.dumps escapes, and json.dumps does not escape the forward slash, so the
    URL contributes exactly its own length to the body.
    """
    return envelope + prefix_len + b64_length(raw_bytes)


def _mb(n: float) -> str:
    return f'{n / 1_000_000:.1f} MB'


def ffprobe_video(path: Path) -> dict:
    """duration, fps, width and height of a real file, from ffprobe.

    The file's OWN measurements, never the geometry witness or a stored figure:
    the bitrate arithmetic below turns on the duration of the exact bytes being
    transcoded. A probe failure raises rather than falling back to a guess.
    """
    args = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,r_frame_rate',
        '-show_entries', 'format=duration',
        '-of', 'json', str(path),
    ]
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f'ANALYSIS_PROBE_FAILED: could not run ffprobe on {path}: {exc!r}') from exc
    if proc.returncode != 0:
        raise RuntimeError(
            f'ANALYSIS_PROBE_FAILED: ffprobe exited {proc.returncode} on {path}. '
            f'stderr verbatim: {proc.stderr.strip()[:600]!r}'
        )
    try:
        parsed = json.loads(proc.stdout)
        stream = (parsed.get('streams') or [{}])[0]
        duration = float(parsed['format']['duration'])
        width = int(stream['width'])
        height = int(stream['height'])
        num, _, den = str(stream.get('r_frame_rate') or '0/1').partition('/')
        fps = (float(num) / float(den)) if float(den or 0) else 0.0
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        raise RuntimeError(
            f'ANALYSIS_PROBE_FAILED: ffprobe output on {path} was not usable ({exc!r}). '
            f'stdout verbatim: {proc.stdout.strip()[:600]!r}'
        ) from exc
    if duration <= 0:
        raise RuntimeError(
            f'ANALYSIS_PROBE_FAILED: ffprobe reported duration {duration} for {path}, '
            'so no bitrate can be computed from it.'
        )
    return {'duration_s': duration, 'fps': fps, 'width': width, 'height': height}


def transcode_for_analysis(src: Path, dest: Path, envelope: int, ceiling: int) -> dict:
    """Build a THROWAWAY analysis copy of `src` that fits the ceiling.

    THE SOURCE IS NEVER TOUCHED. ffmpeg reads it and writes somewhere else, and
    the delivered clip, the Drive copy and everything the operator sees are the
    same bytes they were before this function ran.

    A BITRATE TARGET IS A PREDICTION, NOT A GUARANTEE. x264's average bitrate
    mode lands near the target, not on it, so the ACTUAL output size is measured
    against the budget after every attempt and the bitrate is corrected from the
    real overshoot rather than from hope. If it still will not fit after
    ANALYSIS_MAX_TRANSCODE_ATTEMPTS, or if the arithmetic demands a bitrate below
    ANALYSIS_MIN_VIDEO_BITRATE_BPS, this raises: sending an over-limit body, or
    sending a smear the model cannot read, are both worse than a loud failure
    that names the real fix.

    Returns the detail dict recorded on the kit row.
    """
    prefix_len = len(clip_data_url_prefix(dest))
    target_payload = int(ceiling * ANALYSIS_TARGET_FRACTION)
    # The largest FILE that can ride inside the target payload, inverting
    # predicted_payload_bytes: (payload - envelope - prefix) * 3/4.
    budget_bytes = ((target_payload - envelope - prefix_len) * 3) // 4
    source_bytes = src.stat().st_size

    if budget_bytes <= 0:
        raise RuntimeError(
            f'ANALYSIS_UNSAVEABLE: the request envelope alone (prompt plus transcript, '
            f'{envelope} bytes) leaves no room for any video under the ceiling of '
            f'{ceiling} bytes. No downscale can fix this. THE REAL FIX: send the clip through '
            'the Gemini Files API, which uploads the file separately and has no request body '
            'limit, instead of inlining it as a base64 data URL. Zero rows written.'
        )

    probe = ffprobe_video(src)
    duration_s = probe['duration_s']
    audio_bps = ANALYSIS_AUDIO_BITRATE_BPS
    total_bps = (budget_bytes * 8.0 / duration_s) * ANALYSIS_CONTAINER_OVERHEAD_FACTOR
    video_bps = int(total_bps - audio_bps)

    if video_bps < ANALYSIS_MIN_VIDEO_BITRATE_BPS:
        raise RuntimeError(
            f'ANALYSIS_UNSAVEABLE: fitting this clip under the ceiling would need a video '
            f'bitrate of {video_bps} bit/s, below the {ANALYSIS_MIN_VIDEO_BITRATE_BPS} bit/s '
            'floor, so the downscaled copy would be a smear the vision model cannot read and '
            'any description of it would be invented. The numbers: source '
            f'{source_bytes} bytes ({_mb(source_bytes)}), duration {duration_s:.1f}s, ceiling '
            f'{ceiling} bytes ({_mb(ceiling)}), video budget {budget_bytes} bytes '
            f'({_mb(budget_bytes)}) after a {envelope} byte envelope. THE REAL FIX: send this '
            'clip through the Gemini Files API, which uploads the file separately and has no '
            'request body limit, instead of inlining it as a base64 data URL. A shorter clip '
            'would also fit. Zero rows written. NOTHING about the delivered clip has changed.'
        )

    attempts: list[dict] = []
    for attempt in range(1, ANALYSIS_MAX_TRANSCODE_ATTEMPTS + 1):
        vf = (
            f"scale='min({ANALYSIS_MAX_DIMENSION},iw)':'min({ANALYSIS_MAX_DIMENSION},ih)'"
            ':force_original_aspect_ratio=decrease:force_divisible_by=2'
        )
        args = [
            'ffmpeg', '-hide_banner', '-nostdin', '-y',
            '-i', str(src),
            '-vf', vf,
        ]
        # Only cap the frame rate when the source is actually faster. Naming -r
        # unconditionally would RESAMPLE a slower clip upwards, inventing frames
        # for no benefit and spending budget on them.
        if probe['fps'] > ANALYSIS_MAX_FPS:
            args += ['-r', str(ANALYSIS_MAX_FPS)]
        args += [
            '-c:v', 'libx264', '-preset', 'veryfast',
            '-b:v', str(video_bps),
            '-maxrate', str(int(video_bps * 1.5)),
            '-bufsize', str(int(video_bps * 3)),
            '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-ac', '1', '-b:a', str(audio_bps),
            '-movflags', '+faststart',
            str(dest),
        ]
        print(
            f'ANALYSIS_TRANSCODE attempt={attempt}/{ANALYSIS_MAX_TRANSCODE_ATTEMPTS} '
            f'video_bps={video_bps} audio_bps={audio_bps} budget_bytes={budget_bytes} '
            f'scale_max={ANALYSIS_MAX_DIMENSION} fps_cap='
            f'{ANALYSIS_MAX_FPS if probe["fps"] > ANALYSIS_MAX_FPS else "source"}'
        )
        try:
            proc = subprocess.run(args, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT_S)
        except (OSError, subprocess.SubprocessError) as exc:
            raise RuntimeError(
                f'ANALYSIS_TRANSCODE_FAILED: could not run ffmpeg on {src}: {exc!r}') from exc
        if proc.returncode != 0:
            tail = '\n'.join((proc.stderr or '').strip().splitlines()[-12:])
            raise RuntimeError(
                f'ANALYSIS_TRANSCODE_FAILED: ffmpeg exited {proc.returncode}. Last lines of '
                f'stderr, verbatim: {tail!r}'
            )
        if not dest.is_file():
            raise RuntimeError(
                f'ANALYSIS_TRANSCODE_FAILED: ffmpeg exited 0 but wrote no file at {dest}.')

        actual = dest.stat().st_size
        predicted = predicted_payload_bytes(envelope, prefix_len, actual)
        attempts.append({'attempt': attempt, 'video_bps': video_bps,
                         'output_bytes': actual, 'predicted_payload_bytes': predicted})
        print(
            f'ANALYSIS_TRANSCODE_RESULT attempt={attempt} output_bytes={actual} '
            f'budget_bytes={budget_bytes} predicted_payload_bytes={predicted} '
            f'ceiling_bytes={ceiling} fits={1 if actual <= budget_bytes else 0}'
        )
        if actual <= budget_bytes:
            out_probe = ffprobe_video(dest)
            return {
                'source_bytes': source_bytes,
                'sent_bytes': actual,
                'ceiling_bytes': ceiling,
                'target_payload_bytes': target_payload,
                'video_budget_bytes': budget_bytes,
                'envelope_bytes': envelope,
                'predicted_payload_bytes': predicted,
                'duration_s': round(duration_s, 3),
                'source_resolution': f'{probe["width"]}x{probe["height"]}',
                'analysis_resolution': f'{out_probe["width"]}x{out_probe["height"]}',
                'source_fps': round(probe['fps'], 3),
                'analysis_fps': round(out_probe['fps'], 3),
                'video_bitrate_bps': video_bps,
                'audio_bitrate_bps': audio_bps,
                'audio_kept': True,
                'attempts': attempts,
                'codec': 'libx264 + aac, mp4',
            }

        # Correct from the MEASURED overshoot, with 10% taken off so the next
        # attempt aims inside the budget rather than at its edge.
        corrected = int(video_bps * (budget_bytes / actual) * 0.9)
        if corrected < ANALYSIS_MIN_VIDEO_BITRATE_BPS:
            raise RuntimeError(
                f'ANALYSIS_UNSAVEABLE: attempt {attempt} produced {actual} bytes '
                f'({_mb(actual)}) against a budget of {budget_bytes} bytes '
                f'({_mb(budget_bytes)}), and the corrected bitrate {corrected} bit/s is below '
                f'the {ANALYSIS_MIN_VIDEO_BITRATE_BPS} bit/s floor. Source {source_bytes} bytes '
                f'({_mb(source_bytes)}), duration {duration_s:.1f}s, ceiling {ceiling} bytes. '
                'THE REAL FIX: send this clip through the Gemini Files API, which uploads the '
                'file separately and has no request body limit. Zero rows written. NOTHING '
                'about the delivered clip has changed.'
            )
        video_bps = corrected

    last = attempts[-1] if attempts else {}
    raise RuntimeError(
        f'ANALYSIS_UNSAVEABLE: {ANALYSIS_MAX_TRANSCODE_ATTEMPTS} transcode attempts all '
        f'overshot the budget of {budget_bytes} bytes ({_mb(budget_bytes)}). Attempts, '
        f'verbatim: {attempts!r}. The last output was {last.get("output_bytes")} bytes. '
        'Refusing to send an over-limit request. THE REAL FIX: send this clip through the '
        'Gemini Files API, which uploads the file separately and has no request body limit. '
        'Zero rows written. NOTHING about the delivered clip has changed.'
    )


def generate(candidate_id: int, regenerate: bool, force: bool, slices_dir: str | None,
             srt_out: str | None, out_dir: str | None = None) -> int:
    """Wrapper so a failure is RECORDED against the requests it answers.

    The requests it consumed are collected as it goes. If anything below
    raises, those rows are marked 'failed' with the reason verbatim and the
    ORIGINAL exception is re-raised unchanged: the review UI gets a failure it
    can render, the exit code stays honest, and no kit row is written.

    BaseException, NOT Exception, AND THE DIFFERENCE IS A REAL ROW. Ctrl-C
    during the vision call is a KeyboardInterrupt, which `except Exception`
    does not catch, so the self-request row this run opened would be left
    'requested' forever and the NEXT run would read that orphan back as an
    operator's regenerate intent it never expressed. open_self_request's
    docstring rules that acceptable for a HARD kill (SIGKILL, a lost
    container), and it is: nothing can run then. An interrupt is different
    precisely because it IS catchable, so it is caught, the row is closed with
    the reason verbatim, and the interrupt is re-raised unchanged so the exit
    code and the traceback are exactly what they were. mark_requests_failed
    never raises, so this clause cannot swallow the original.
    """
    consumed: list[int] = []
    try:
        return _generate(candidate_id, regenerate, force, slices_dir, srt_out, out_dir, consumed)
    except BaseException as exc:
        mark_requests_failed(consumed, f'{type(exc).__name__}: {exc}')
        raise


def _generate(candidate_id: int, regenerate: bool, force: bool, slices_dir: str | None,
              srt_out: str | None, out_dir: str | None, consumed: list[int]) -> int:
    run_id = f'generate_post_kit_{dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'

    # ---- Phase 1: read-only. Cheapest gates first, so a disabled or already
    # ---- kitted clip costs zero tokens and zero base64.
    conn = db.connect()
    try:
        cur = conn.cursor()

        # Cheapest gate first: a clip the operator switched OFF must cost zero
        # tokens, zero base64 and zero geometry work.
        cur.execute('SELECT post_kit_enabled FROM clip_candidates WHERE id = %s', (candidate_id,))
        toggle_row = cur.fetchone()
        if not toggle_row:
            raise RuntimeError(f'candidate_id not found: {candidate_id}')

        # The regenerate ledger is read BEFORE the toggle decides anything, so
        # a request that can never be answered is closed rather than left
        # waiting forever in the review UI.
        requests = fetch_outstanding_requests(cur, candidate_id)
        consumed.extend(r['id'] for r in requests)
        if requests:
            newest = requests[0]
            regenerate = True
            # The NEWEST request's flag only, never an OR across the queue.
            # Over-claiming force is the dangerous direction: it is the one
            # thing that can bury the operator's own words.
            force = force or bool(newest['force_over_operator_edit'])
            print(
                f'REQUESTS_OUTSTANDING candidate={candidate_id} ids={consumed} '
                f'newest={newest["id"]} by={newest["requested_by"]} '
                f'force_over_operator_edit={newest["force_over_operator_edit"]}'
            )

        if int(toggle_row[0]) != 1:
            conn.rollback()
            if consumed:
                # NAME WHO ASKED. fetch_outstanding_requests does not filter on
                # requested_by, so an outstanding row here is not necessarily
                # the operator's: a previous run of THIS worker that was killed
                # after opening its own row leaves an orphan that reads back as
                # an unanswered request (open_self_request's docstring rules
                # that the correct queue semantics). Telling the operator "a
                # regenerate was requested" when only the worker asked would be
                # a claim about his intent that he never made, so the requesters
                # are quoted from the rows instead. The behaviour is unchanged
                # and deliberately so: raising is what closes the row (the
                # wrapper marks it failed), and a request that can never be
                # answered must not wait in the review UI forever.
                by = sorted({r['requested_by'] for r in requests})
                raise RuntimeError(
                    f'POST_KIT_DISABLED: candidate_id={candidate_id} has clip_candidates'
                    '.post_kit_enabled = 0, so no kit may be generated for it, but request(s) '
                    f'{consumed} are outstanding, opened by {by}. If that is you, turn the '
                    'per-clip generate switch back on in the review UI and ask again. If it is '
                    f'{SELF_REQUEST_ACTOR}, it is an earlier run of this worker that was killed '
                    'before it could close its own row, and this failure closes it. Zero rows '
                    'written.'
                )
            print(
                f'RESULT generate_post_kit candidate={candidate_id} ok=1 skipped=1 '
                'reason=post_kit_disabled wrote_rows=0'
            )
            return 0

        existing = fetch_active_kit(cur, candidate_id)
        if existing is not None and not regenerate:
            # RE-DELIVER rather than crash. The kit already exists and is
            # paid for, so the honest answer to "generate a kit for this
            # clip" is to hand the lane the files for the kit it already has.
            # Without this, a lane that failed AFTER the kit committed (an
            # upload error, a wiped staging directory) was permanently
            # undeliverable: every retry printed skipped=1 with no file paths
            # and the file nodes downstream died on nothing to read.
            full = fetch_active_kit_full(cur, candidate_id)
            clip_names = fetch_clip_names(cur, candidate_id)
            conn.rollback()
            files = write_kit_files(
                out_dir, full, candidate_id, full['version'], full['origin'], clip_names)
            print(
                f'RESULT generate_post_kit candidate={candidate_id} ok=1 skipped=1 '
                f'reason=active_kit_exists kit_id={existing["id"]} '
                f'version={existing["version"]} origin={existing["origin"]} wrote_rows=0'
                f'{result_file_fields(files)}'
            )
            return 0

        # PAST EVERY SKIP GATE: this run is now committed to doing the work (or
        # to refusing it loudly just below), so from here on a failure must be
        # visible. When nothing in the review UI asked for this kit, the worker
        # opens its own request row so there is always something to close.
        # Placed HERE, not earlier, on purpose: a disabled clip and an
        # already-kitted clip are no-ops that must not leave ledger rows, so a
        # run that skips at either gate leaves the ledger exactly as it found
        # it and never manufactures a request nobody made.
        #
        # BE PRECISE ABOUT WHAT POST_KIT_DISABLED FIRES ON, because it is NOT
        # "a real operator request". fetch_outstanding_requests has no
        # requested_by filter, so an orphaned row from a killed earlier run of
        # this worker also satisfies it. That is correct (the row is owed an
        # answer either way) and it is why the raise names its requesters
        # rather than asserting the operator asked.
        if not consumed:
            self_request_id = open_self_request(candidate_id)
            if self_request_id is not None:
                consumed.append(self_request_id)

        if existing is not None and existing['origin'] == 'operator_edit' and not force:
            raise RuntimeError(
                f'OPERATOR_EDIT_ACTIVE: candidate_id={candidate_id} has an active kit the '
                f'OPERATOR wrote (version {existing["version"]}). Regenerating would demote '
                'his own words to history and put model output in front of him instead. '
                'Refusing. Pass --force only as a deliberate human act, or confirm the '
                'overwrite in the review UI so the request itself carries it. n8n must never '
                'pass it.'
            )

        # Geometry + transcript. This is where CLIP_NOT_RENDERED surfaces.
        product = build_srt.build_for_candidate(cur, candidate_id, slices_dir)
        info = product['info']

        clip_path = Path(info['clip_file_path'])
        if not clip_path.is_file():
            raise RuntimeError(
                f'CLIP_FILE_MISSING: clips.file_path {clip_path} does not exist on this '
                f'machine for candidate_id={candidate_id}. The rendered clip is what the '
                'vision model watches, so nothing can be generated without it. The delivered '
                f'copy is at drive_sync_path={info["drive_sync_path"]!r}. Note that the '
                'analyzer pipeline wipes the n8n staging directory at the start of every run, '
                'so a retroactive regenerate can legitimately find the local file gone.'
            )

        profile = fetch_profile(cur)
        context = fetch_context(cur, info['recording_id'])
        conn.rollback()  # read-only phase, nothing to keep
    finally:
        conn.close()

    geom = product['geometry']
    duration_s = geom['measured_duration_s']
    subject_kind = context['subject_kind'] if context else 'unknown'

    if context is None:
        print(
            f'WARN no recording_context row for recording_id={info["recording_id"]}: the '
            'subject of this clip is UNKNOWN. The prompts will say so explicitly and will '
            'NOT assume the channel owner is the person on screen (D-062 ruling 4).'
        )
    if profile is None:
        print('WARN no active creator_profile row: the copy will be written without channel context.')
    if product['srt_text'] is None:
        print(
            f'WARN no transcript segments inside the shipped clip for candidate={candidate_id}: '
            'captions will be NULL and any quote is impossible by construction.'
        )

    print(
        f'GEOMETRY candidate={candidate_id} basis={geom["basis"]} '
        f'clip_t0_abs_s={geom["clip_t0_abs_s"]:.3f} measured_duration_s={duration_s:.3f} '
        f'delta_s={geom["duration_delta_s"]:.3f} cues={product["cue_count"]}'
    )

    # ---- Phase 2: the paid work. No DB transaction is held open across it.
    key_path = os.environ.get('CLPR_OPENROUTER_ENV', '').strip() or DEFAULT_KEY_ENV_PATH
    api_key = read_api_key(key_path)
    print(f'OPENROUTER_KEY_SOURCE {key_path} (value never printed)')

    # D-066: the run now loads TWO credentials, and a log naming only one is a
    # half-truth. gemini_files.read_gemini_key resolves its own path (the
    # CLPR_GEMINI_ENV override, else its own DEFAULT_GEMINI_KEY_ENV_PATH), the
    # same shape read_api_key/DEFAULT_KEY_ENV_PATH use for OpenRouter above.
    gemini_key_path = (
        os.environ.get('CLPR_GEMINI_ENV', '').strip() or gemini_files.DEFAULT_GEMINI_KEY_ENV_PATH
    )
    gemini_key = gemini_files.read_gemini_key(gemini_key_path)
    print(f'GEMINI_KEY_SOURCE {gemini_key_path} (value never printed)')

    writer_model = os.environ.get('CLPR_POST_KIT_WRITER_MODEL', '').strip() or DEFAULT_WRITER_MODEL
    writer_temperature = float(os.environ.get('CLPR_POST_KIT_TEMPERATURE', '').strip() or 0.7)

    subject = subject_block(
        subject_kind,
        context['subject_text'] if context else None,
        context['context_notes'] if context else None,
    )
    profile_text = profile_block(profile)

    # ---- D-066: THE VISION CALL MOVES TO THE GEMINI FILES API.
    #
    # THE OLD CEILING/DOWNSCALE MACHINERY STAYS DEFINED, JUST UNCALLED.
    # payload_ceiling_bytes, transcode_for_analysis, encode_clip,
    # clip_data_url_prefix, predicted_payload_bytes, the size_guard in
    # openrouter_call, and their constants are untouched below -- Brief 4
    # removes them. This path uploads the DELIVERED clip at full quality
    # through gemini_files, which has no comparable request-body ceiling, so
    # no downscale copy is ever built for analysis: THE DELIVERED CLIP IS
    # NEVER MODIFIED, and there is no transcode on this path at all. That
    # removes the exact failure mode that motivated this brief: on the same
    # 15 seconds, full quality reported "the feed is intact for the whole
    # clip" while a downscaled copy of identical footage reported black-frame
    # cuts that do not exist in the file.
    #
    # THE OPENROUTER PROVIDER-PASSTHROUGH NEGOTIATION IS ALSO GONE FROM THIS
    # PATH. fetch_endpoints/build_provider_options/log_passthrough (still
    # defined below, untouched) existed only to get fps and media_resolution
    # onto an OpenRouter vision request via provider.options, because
    # n8n's native Gemini node exposes neither. Gemini's own API takes both
    # natively -- MEASURED 2026-08-07: fps genuinely works on a Files API
    # file_uri, a combination that appears in none of Google's own examples --
    # so there is nothing left to negotiate here. post_kits.passthrough_degraded
    # is written NULL below: the negotiation does not exist for this row, which
    # is a different fact from "nothing was refused".
    vision_prompt = build_vision_prompt(
        subject, profile_text, product['transcript_lines'], duration_s)

    yavg = gemini_files.measure_motion(clip_path)
    fps, fps_reason = gemini_files.choose_fps(yavg)
    print(
        f'VISION_CALL model={gemini_files.DEFAULT_GEMINI_MODEL} transport=gemini_files '
        f'motion_yavg={yavg:.3f} fps={fps} fps_reason={fps_reason} '
        '(request body never logged)'
    )

    def gemini_generate_call(file_uri: str) -> dict:
        """generate_content, retried on transient faults only -- SAME SHAPE as
        openrouter_call's retry loop (see below, unchanged): the same
        retry_policy() (DEFAULT_MAX_ATTEMPTS and friends), no second policy
        invented. Leaving openrouter_call behind on the vision path also
        leaves behind its whole retry apparatus, and one part of that is
        load-bearing: five of the six failures in the 27-clip batch were
        empty completions that recovered on an unchanged retry. A Gemini path
        without an equivalent retry would be weaker than what it replaces.

        Retries GeminiEmptyCompletionError (the direct analogue of that
        empty-completion case) and transient transport faults --
        GeminiUnreachable unconditionally, and a GeminiHTTPError only on 429
        or a 5xx, mirroring classify_failure's OpenRouter classification
        above. Any other 4xx is not retried: a bad request does not fix
        itself.

        DELIBERATELY RETRIES ONLY THIS CALL, NOT THE UPLOAD. `file_uri` is the
        ALREADY-UPLOADED file's URI, passed in once from the caller; nothing
        in this closure re-uploads, so a retry here costs one more
        generateContent call, never another upload of the whole clip.
        """
        policy = retry_policy()
        attempts = policy['max_attempts']
        for attempt in range(1, attempts + 1):
            try:
                return gemini_files.generate_content(
                    file_uri, 'video/mp4', vision_prompt, gemini_key,
                    fps=fps, media_resolution=DEFAULT_MEDIA_RESOLUTION)
            except Exception as exc:  # noqa: BLE001 - classified immediately below
                if isinstance(exc, gemini_files.GeminiEmptyCompletionError):
                    retryable, reason = True, 'empty_completion'
                elif isinstance(exc, gemini_files.GeminiUnreachable):
                    retryable, reason = True, 'transport_or_timeout'
                elif isinstance(exc, gemini_files.GeminiHTTPError):
                    if exc.status == 429:
                        retryable, reason = True, 'http_429_rate_limited'
                    elif exc.status >= 500:
                        retryable, reason = True, f'http_{exc.status}_server'
                    else:
                        retryable, reason = False, f'http_{exc.status}_client'
                else:
                    retryable, reason = False, type(exc).__name__
                if not retryable:
                    if attempt > 1:
                        print(f'GEMINI_RETRY_ABANDONED attempt={attempt}/{attempts} '
                              f'reason={reason} (not retryable, failing loudly)')
                    raise
                if attempt >= attempts:
                    print(f'GEMINI_RETRIES_EXHAUSTED attempts={attempt}/{attempts} '
                          f'reason={reason} (still failing, failing loudly)')
                    raise
                delay = retry_delay_s(attempt, policy, None)
                print(
                    f'GEMINI_RETRY attempt={attempt}/{attempts} reason={reason} '
                    f'delay_s={delay:.1f} error={exc}'
                )
                time.sleep(delay)
                continue
        # Unreachable: the loop either returns or raises. Present so a future
        # edit to the loop bounds cannot fall through to an implicit None.
        raise gemini_files.GeminiError('GEMINI_RETRY_LOOP_FELL_THROUGH')

    # 008's own docstring: "the delivered clip's size in bytes... recorded on
    # every generated kit, downscaled or not... costs nothing to keep." No
    # downscale ever happens on this path (that is the whole point of
    # D-066), so analysis_source_bytes and analysis_sent_bytes stay equal --
    # both the DELIVERED clip's real size, matching what migration 008's own
    # undownscaled ("fits") case always wrote -- and analysis_downscale_detail
    # stays NULL, the same as that case.
    source_bytes = clip_path.stat().st_size

    file_obj = gemini_files.upload_video(clip_path, gemini_key)
    try:
        active = gemini_files.wait_active(file_obj['name'], gemini_key)
        # THE POSITIVE CONTROL: the witness that the real bytes reached
        # Google. post_kits.gemini_sha256_match is the receipt that this ran.
        witnesses = gemini_files.verify_upload(clip_path, active)
        out = gemini_generate_call(active['uri'])
    finally:
        # In a finally so the remote file is removed on failure too. A leak
        # must be loud but must NOT abort a run that already has its answer --
        # delete_file itself never raises on a leak (see its own docstring),
        # so this cannot mask a real result or a real failure raised above it.
        gemini_files.delete_file(file_obj['name'], gemini_key)

    scene = out['text']
    if len(scene) < MIN_SCENE_DESCRIPTION_CHARS:
        raise RuntimeError(
            f'VISION_RESPONSE_TOO_SHORT: the vision model returned {len(scene)} characters, '
            f'under the {MIN_SCENE_DESCRIPTION_CHARS} minimum. Verbatim: {scene!r}. '
            'Zero rows written.'
        )
    # Deliberately NOT dash-gated. scene_description is an INTERNAL audit field:
    # it is never posted and never shown to a client, only fed to the writer and
    # kept for provenance. Rejecting it here would abort the run AFTER the
    # expensive video call over a stylistic tic these models produce constantly,
    # and the retry could fail identically. The ban is scoped to its real blast
    # radius: the four platform-bound copy fields, which validate_kit gates.
    vision_generation_id = str(out['response_id'] or '')
    print(
        f'VISION_OK chars={len(scene)} generation_id={vision_generation_id} '
        f'sha256_match={witnesses["sha256_match"]}'
    )

    chat_url = f'{OPENROUTER_BASE}/chat/completions'

    transcript_plain = product['transcript_plain']

    def writer_payload(forbid_quotes: bool) -> dict:
        """The writer request. TEXT ONLY: no video part appears anywhere in it,
        which is what makes a writer retry cheap and keeps the vision upload at
        exactly one per run."""
        return {
            'model': writer_model,
            'max_tokens': WRITER_MAX_TOKENS,
            'temperature': writer_temperature,
            'messages': [{
                'role': 'user',
                'content': build_writer_prompt(
                    subject, profile_text, scene, transcript_plain, duration_s,
                    forbid_quotes=forbid_quotes),
            }],
        }

    # THE STRUCTURAL RETRY. MalformedWriterResponseError was MEASURED
    # (2026-08-08, see its own docstring) to be a sampling wobble, not a
    # defect: candidate 1 failed with `on_video_text.payoff is missing or
    # empty` and an IDENTICAL re-run succeeded, same clip, same prompt, same
    # model, no code change. So unlike every other validation failure, the
    # SAME ask is worth repeating. Bounded by WRITER_STRUCTURAL_MAX_ATTEMPTS
    # (env CLPR_WRITER_STRUCTURAL_MAX_ATTEMPTS) so a persistently malformed
    # writer still fails loudly rather than looping forever. Caught BY TYPE,
    # never by matching message text (charter gate 10), so a reworded
    # MALFORMED_WRITER_RESPONSE message can never silently widen the retry to
    # cover a real defect. The InventedQuoteError fallback nested below is
    # UNCHANGED: it still fires on its own type, independently of this loop.
    structural_max_attempts = max(1, int(_env_number(
        'CLPR_WRITER_STRUCTURAL_MAX_ATTEMPTS', WRITER_STRUCTURAL_MAX_ATTEMPTS)))

    quote_fallback = 0
    quote_fallback_reason: str | None = None
    for structural_attempt in range(1, structural_max_attempts + 1):
        label = 'writer' if structural_attempt == 1 else 'writer_structural_retry'
        print(
            f'WRITER_CALL model={writer_model} temperature={writer_temperature} '
            f'attempt={structural_attempt}/{structural_max_attempts}'
        )
        writer_response, writer_text = openrouter_call(
            label, 'POST', chat_url, api_key, writer_payload(False), WRITER_TIMEOUT_S,
            expect_message=True)
        writer_generation_id = str(writer_response.get('id') or '')

        # THE NO-QUOTE FALLBACK. A fabricated quote costs ONE cheap writer call, not
        # the whole kit. Measured 2026-08-07: candidate 45 failed FOUR separate
        # attempts on INVENTED_QUOTE, fabricating a DIFFERENT plausible sentence
        # every time, so that clip ended the batch with no kit at all. The quote is
        # OPTIONAL and several kits that day shipped without one and read fine, so
        # the honest remedy is to ask for copy with no quoted line rather than to
        # throw the paid vision call away.
        #
        # SCOPED BY TYPE, NEVER BY MESSAGE. This except clause catches only
        # InventedQuoteError; the structural retry below catches only
        # MalformedWriterResponseError. A banned dash, an over-length hook and
        # a hashtag over the cap are caught by NEITHER and still fail loudly on
        # the first try, because none of those is fixed by dropping a quote or
        # by re-asking, and retrying them would only pay twice for the same
        # defect. A missing or empty required field USED to sit in that list
        # and no longer does -- it was measured on 2026-08-08 to be a sampling
        # wobble that clears on an identical re-ask, so it moved to the
        # structural retry. See MalformedWriterResponseError's docstring.
        try:
            kit = validate_kit(writer_text, transcript_plain)
            break
        except InventedQuoteError as first_exc:
            quote_fallback_reason = str(first_exc)
            print(
                f'QUOTE_FALLBACK candidate={candidate_id} the writer fabricated a quotation, so it '
                'is being re-asked ONCE for copy with no quoted line. The vision call is NOT '
                f'repeated. First failure (verbatim): {first_exc}'
            )
            # THE RETRY CALL IS INSIDE THIS try, NOT ABOVE IT, AND THAT PLACEMENT IS
            # THE WHOLE POINT. The failure record written by generate() is built
            # from whatever escapes here, so a retry that dies in TRANSPORT (a
            # timeout, a 429 whose retries ran out) escaping bare would put a plain
            # network error in post_kit_requests.error and the operator would never
            # learn that a quotation had been fabricated at all. The fabrication is
            # the fact worth keeping. Both failures travel together or the record is
            # a half-truth.
            try:
                writer_response, writer_text = openrouter_call(
                    'writer_no_quote', 'POST', chat_url, api_key, writer_payload(True),
                    WRITER_TIMEOUT_S, expect_message=True)
                writer_generation_id = str(writer_response.get('id') or '')
                kit = validate_kit(writer_text, transcript_plain)
            except Exception as second_exc:
                # Deliberately NOT "also failed validation": the second failure may
                # now be a transport fault, and naming it as a validation failure
                # would be a paraphrase of a machine outcome. The verbatim SECOND
                # says which kind it actually was.
                raise RuntimeError(
                    'QUOTE_FALLBACK_FAILED: the writer fabricated a quotation, and the no-quote '
                    'retry ALSO failed. Both failures, verbatim. FIRST: '
                    f'{first_exc} SECOND: {type(second_exc).__name__}: {second_exc} '
                    'Zero rows written.'
                ) from second_exc
            quote_fallback = 1
            print(
                f'QUOTE_FALLBACK_OK candidate={candidate_id} the rewritten copy validated. This '
                'kit LOST ITS QUOTE to a fabrication, and post_kits.quote_fallback records that so '
                'it cannot be mistaken for a clip that simply had no quotable line.'
            )
            break
        except MalformedWriterResponseError as structural_exc:
            if structural_attempt >= structural_max_attempts:
                raise
            # This file's own rule: "a run that only worked on the third try
            # must never read as a clean first try." Every retry is printed,
            # never swallowed.
            print(
                f'WRITER_STRUCTURAL_RETRY candidate={candidate_id} '
                f'attempt={structural_attempt}/{structural_max_attempts} re-asking the SAME '
                'payload -- candidate 1 (2026-08-08) failed this way and an identical re-run '
                f'succeeded. Verbatim reason: {structural_exc}'
            )
            continue

    print(
        f'WRITER_OK generation_id={writer_generation_id} hashtags={len(kit["hashtags"])} '
        f'quoted={"1" if kit["quoted_line"] else "0"} quote_fallback={quote_fallback}'
    )

    # D-066: fps and media_resolution are recorded as genuinely SENT (Gemini's
    # API takes both natively, no passthrough negotiation involved), plus
    # yavg and fps_reason so the fps choice is reconstructable after the fact.
    # No passthrough_requested/accepted/dropped: that negotiation does not
    # exist on this path (see passthrough_degraded below).
    vision_params = json.dumps({
        'model': gemini_files.DEFAULT_GEMINI_MODEL,
        'fps': fps,
        'fps_reason': fps_reason,
        'media_resolution': DEFAULT_MEDIA_RESOLUTION,
        'motion_yavg': yavg,
    }, sort_keys=True)
    writer_params = json.dumps({
        'model': writer_model,
        'temperature': writer_temperature,
        'max_tokens': WRITER_MAX_TOKENS,
    }, sort_keys=True)

    # ---- Phase 3: ONE transaction. Deactivate-old and insert-new commit
    # ---- together or not at all, so a failure anywhere above leaves the
    # ---- previous active kit exactly where it was.
    conn = db.connect()
    try:
        cur = conn.cursor()
        # Re-check: the operator may have edited the kit while the models ran.
        current = fetch_active_kit(cur, candidate_id)
        before_id = existing['id'] if existing else None
        current_id = current['id'] if current else None
        if current_id != before_id:
            if not force:
                raise RuntimeError(
                    f'KIT_CHANGED_DURING_GENERATION: the active kit for candidate_id='
                    f'{candidate_id} changed while the models were running (was '
                    f'{before_id!r}, now {current_id!r}). Most likely the operator edited it. '
                    'Refusing to overwrite. Nothing written.'
                )
        if current is not None and current['origin'] == 'operator_edit' and not force:
            raise RuntimeError(
                f'OPERATOR_EDIT_ACTIVE: candidate_id={candidate_id} now has an active operator '
                'edit. Refusing to overwrite. Nothing written.'
            )

        version = next_version(cur, candidate_id)
        cur.execute(
            'UPDATE post_kits SET is_active = 0 WHERE candidate_id = %s AND is_active = 1',
            (candidate_id,),
        )
        deactivated = cur.rowcount

        cur.execute(
            '''
            INSERT INTO post_kits(
                candidate_id, version, origin, is_active,
                hook_withheld, hook_domain, hook_payoff,
                video_caption, hashtags, quoted_line,
                srt_text, srt_segment_count, srt_basis,
                srt_clip_t0_abs_s, srt_clip_duration_s,
                scene_description,
                subject_kind, recording_context_version, profile_version,
                vision_model, writer_model, prompt_version,
                vision_generation_id, writer_generation_id,
                vision_params, writer_params, passthrough_degraded,
                quote_fallback, quote_fallback_reason,
                analysis_downscaled, analysis_source_bytes, analysis_sent_bytes,
                analysis_downscale_detail,
                analysis_transport, motion_yavg, fps_used, fps_reason,
                gemini_file_name, gemini_sha256_match,
                created_by_run, created_at
            ) VALUES (
                %s, %s, 'generated', 1,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s
            ) RETURNING id
            ''',
            (
                candidate_id, version,
                kit['hook_withheld'], kit['hook_domain'], kit['hook_payoff'],
                kit['video_caption'], kit['hashtags'], kit['quoted_line'],
                product['srt_text'], product['cue_count'], geom['basis'],
                geom['clip_t0_abs_s'], duration_s,
                scene,
                subject_kind,
                context['version'] if context else None,
                profile['version'] if profile else None,
                gemini_files.DEFAULT_GEMINI_MODEL, writer_model, PROMPT_VERSION,
                vision_generation_id, writer_generation_id,
                # D-066: the OpenRouter passthrough negotiation does not exist
                # on this path, so NULL (the question does not apply) rather
                # than 0 (nothing was refused) is the honest value here.
                vision_params, writer_params, None,
                quote_fallback, quote_fallback_reason,
                # 008. No downscale ever happens on the gemini_files path --
                # that is the whole point of D-066 -- so analysis_downscaled
                # is always 0, source and sent bytes are equal (both the
                # DELIVERED clip's real size), and detail is NULL, exactly
                # matching what migration 008's own undownscaled case wrote.
                0,
                source_bytes,
                source_bytes,
                None,
                # D-066: the new Gemini-transport provenance columns
                # (migration 009). gemini_sha256_match is the receipt that
                # verify_upload's positive control actually ran; it is never
                # hardcoded, only the real witnesses result.
                'gemini_files', yavg, fps, fps_reason,
                file_obj['name'], witnesses['sha256_match'],
                run_id, utc_now_iso(),
            ),
        )
        kit_id = int(cur.fetchone()[0])
        # The requests this run answers close in the SAME transaction as the
        # kit they were asking for. Either both facts are true or neither is.
        satisfied = mark_requests_satisfied(cur, consumed, version)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    if consumed:
        print(f'REQUESTS_SATISFIED marked={satisfied} ids={consumed} version={version}')

    if srt_out and product['srt_text'] is not None:
        Path(srt_out).parent.mkdir(parents=True, exist_ok=True)
        Path(srt_out).write_text(product['srt_text'], encoding='utf-8')
        print(f'SRT_WRITTEN {srt_out}')

    # The two DELIVERED files, written only after the row is committed, so a
    # file on disk can never describe a kit that does not exist.
    files = write_kit_files(
        out_dir,
        {
            'hook_withheld': kit['hook_withheld'],
            'hook_domain': kit['hook_domain'],
            'hook_payoff': kit['hook_payoff'],
            'video_caption': kit['video_caption'],
            'hashtags': kit['hashtags'],
            'quoted_line': kit['quoted_line'],
            'srt_text': product['srt_text'],
            'srt_segment_count': product['cue_count'],
            'quote_fallback': quote_fallback,
            'analysis_downscaled': 0,
            'analysis_source_bytes': source_bytes,
            'analysis_sent_bytes': source_bytes,
        },
        candidate_id, version, 'generated',
        {'file_path': info['clip_file_path'], 'drive_sync_path': info['drive_sync_path']},
    )

    print(
        f'RESULT generate_post_kit candidate={candidate_id} ok=1 skipped=0 kit_id={kit_id} '
        f'version={version} deactivated={deactivated} wrote_rows=1 '
        f'srt_cues={product["cue_count"]} srt_basis={geom["basis"]} '
        f'hashtags={len(kit["hashtags"])} subject={subject_kind} '
        f'analysis_transport=gemini_files gemini_sha256_match={witnesses["sha256_match"]} '
        f'quote_fallback={quote_fallback} '
        f'analysis_downscaled=0'
        f'{result_file_fields(files)}'
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Generate the post kit (on-video hooks, SRT captions, video caption) for one clip'
    )
    parser.add_argument('--candidate-id', type=int, required=True)
    parser.add_argument(
        '--regenerate', action='store_true',
        help='write a NEW version over an existing active GENERATED kit (never destroys history)',
    )
    parser.add_argument(
        '--force', action='store_true',
        help='also allowed to supersede an active OPERATOR EDIT. A deliberate human act. n8n must never pass this.',
    )
    parser.add_argument('--slices-dir', type=str, default=None)
    parser.add_argument('--srt-out', type=str, default=None, help='also write the SRT to this path')
    parser.add_argument(
        '--kit-out-dir', type=str, default=None,
        help='write the delivered <clip-stem>.vN.txt and .vN.srt here (env CLPR_POSTKIT_OUT). '
             'The n8n lane sets this to the one directory its file nodes may read.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    slices_dir = args.slices_dir or os.environ.get('CLPR_SLICES_DIR', '').strip() or None
    out_dir = args.kit_out_dir or os.environ.get('CLPR_POSTKIT_OUT', '').strip() or None
    # --force is the stronger statement of the same intent, so it implies
    # --regenerate: otherwise --force alone would silently do nothing.
    regenerate = args.regenerate or args.force
    return generate(
        args.candidate_id, regenerate, args.force, slices_dir, args.srt_out, out_dir)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
