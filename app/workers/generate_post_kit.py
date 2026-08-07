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
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

import build_srt
import db
import transcript_signal as ts

PROMPT_VERSION = 'post_kit_v1'

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
# HTTP seam — ONE function, so a test can stand in front of the network
# ---------------------------------------------------------------------------

def _http_json(method: str, url: str, api_key: str, payload: dict | None, timeout: int) -> dict:
    """The single HTTP seam. Never logs the key, the body, or the base64."""
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
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')[:800]
        raise RuntimeError(
            f'OPENROUTER_HTTP_ERROR: {method} {url} returned status {exc.code}. '
            f'Response body (first 800 chars, verbatim): {detail!r}'
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f'OPENROUTER_UNREACHABLE: {method} {url} failed: {exc!r}') from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f'OPENROUTER_BAD_JSON: {method} {url} did not return JSON ({exc}). '
            f'First 400 chars, verbatim: {body[:400]!r}'
        ) from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f'OPENROUTER_BAD_JSON: {method} {url} returned a non-object payload')
    if 'error' in parsed and parsed['error']:
        raise RuntimeError(f'OPENROUTER_API_ERROR: {parsed["error"]!r}')
    return parsed


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
    payload = _http_json('GET', url, api_key, None, PROBE_TIMEOUT_S)
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
                        duration_s: float) -> str:
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
        '- If you use a direct quote, it must appear WORD FOR WORD in the transcript, and '
        'you must also return it in the quoted_line field so it can be checked. If you '
        'quote nothing, return null for quoted_line.\n'
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
            raise RuntimeError(
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
        raise RuntimeError(
            f'MALFORMED_WRITER_RESPONSE: the writer model did not return parseable JSON '
            f'({exc}). First 400 chars, verbatim: {raw_text[:400]!r}. Zero rows written.'
        ) from exc
    if not isinstance(data, dict):
        raise RuntimeError('MALFORMED_WRITER_RESPONSE: response is not a JSON object. Zero rows written.')

    hooks_raw = data.get('on_video_text')
    if not isinstance(hooks_raw, dict):
        raise RuntimeError('MALFORMED_WRITER_RESPONSE: missing on_video_text object. Zero rows written.')

    hooks: dict = {}
    for key in ('withheld', 'domain', 'payoff'):
        value = hooks_raw.get(key)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(
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
        raise RuntimeError('MALFORMED_WRITER_RESPONSE: video_caption is missing or empty. Zero rows written.')
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
                raise RuntimeError('MALFORMED_WRITER_RESPONSE: a hashtag is not a non-empty string. Zero rows written.')
            tag = item.strip()
            if not tag.startswith('#') or len(tag.split()) != 1 or len(tag) < 2:
                raise RuntimeError(
                    f'COPY_REJECTED: hashtag {tag!r} is not a single #token. Zero rows written.'
                )
            _reject_banned_dash('hashtags', tag)
            hashtags.append(tag)
    else:
        raise RuntimeError('MALFORMED_WRITER_RESPONSE: hashtags is not a list. Zero rows written.')

    if len(hashtags) > MAX_HASHTAGS:
        raise RuntimeError(
            f'COPY_REJECTED: {len(hashtags)} hashtags returned, over the {MAX_HASHTAGS} '
            'ceiling (a 24.3M-post study measured 31% FEWER views on posts with them). '
            'NOT trimmed: the operator decides what to drop. Zero rows written.'
        )

    quoted = data.get('quoted_line')
    if quoted is not None:
        if not isinstance(quoted, str):
            raise RuntimeError('MALFORMED_WRITER_RESPONSE: quoted_line is not a string or null. Zero rows written.')
        quoted = quoted.strip()
        if not quoted:
            quoted = None
        else:
            _reject_banned_dash('quoted_line', quoted)
            if _normalise_quote(quoted) not in _normalise_quote(transcript_plain):
                raise RuntimeError(
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
        raise RuntimeError(f'OPENROUTER_NO_CHOICES: the {which} response carried no choices')
    message = (choices[0] or {}).get('message') or {}
    content = message.get('content')
    if isinstance(content, list):
        # Some providers return content as parts.
        content = ''.join(
            part.get('text', '') for part in content if isinstance(part, dict)
        )
    text = str(content or '').strip()
    if not text:
        raise RuntimeError(f'OPENROUTER_EMPTY_CONTENT: the {which} response carried no text content')
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
        'srt_basis, created_at '
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

    Its own connection, because the caller's transaction is being rolled back:
    a failed run persists NO kit (charter gate 9), and this is the loud report,
    not a partial result. The reason is stored VERBATIM. This function never
    raises: a marker that cannot be written must not replace the real error
    with its own, so it prints and returns.
    """
    if not request_ids:
        return
    text = (reason or '').strip() or 'unknown failure (the worker raised with no message)'
    try:
        conn = db.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE post_kit_requests SET state = 'failed', error = %s WHERE id = ANY(%s)",
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
    suffix = path.suffix.lower().lstrip('.') or 'mp4'
    mime = 'video/mp4' if suffix in ('mp4', 'm4v') else f'video/{suffix}'
    return f'data:{mime};base64,{b64}', len(raw), len(b64)


def generate(candidate_id: int, regenerate: bool, force: bool, slices_dir: str | None,
             srt_out: str | None, out_dir: str | None = None) -> int:
    """Wrapper so a failure is RECORDED against the requests it answers.

    The requests it consumed are collected as it goes. If anything below
    raises, those rows are marked 'failed' with the reason verbatim and the
    ORIGINAL exception is re-raised unchanged: the review UI gets a failure it
    can render, the exit code stays honest, and no kit row is written.
    """
    consumed: list[int] = []
    try:
        return _generate(candidate_id, regenerate, force, slices_dir, srt_out, out_dir, consumed)
    except Exception as exc:
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
                raise RuntimeError(
                    f'POST_KIT_DISABLED: candidate_id={candidate_id} has clip_candidates'
                    '.post_kit_enabled = 0, so no kit may be generated for it, but a '
                    'regenerate was requested. Turn the per-clip generate switch back on in '
                    'the review UI and ask again. Zero rows written.'
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

    vision_model = os.environ.get('CLPR_POST_KIT_VISION_MODEL', '').strip() or DEFAULT_VISION_MODEL
    writer_model = os.environ.get('CLPR_POST_KIT_WRITER_MODEL', '').strip() or DEFAULT_WRITER_MODEL
    fps = float(os.environ.get('CLPR_POST_KIT_FPS', '').strip() or DEFAULT_FPS)
    media_resolution = (
        os.environ.get('CLPR_POST_KIT_MEDIA_RESOLUTION', '').strip() or DEFAULT_MEDIA_RESOLUTION
    )
    writer_temperature = float(os.environ.get('CLPR_POST_KIT_TEMPERATURE', '').strip() or 0.7)

    # Keyed by the LOGICAL parameter, with both snake_case and camelCase
    # offered underneath it. Only what an endpoint publishes as allowed is ever
    # sent, and a logical parameter counts as delivered when EITHER spelling
    # lands (see build_provider_options).
    desired = {
        'media_resolution': {
            'media_resolution': media_resolution,
            'mediaResolution': media_resolution,
        },
        'fps': {
            'video_metadata': {'fps': fps},
            'videoMetadata': {'fps': fps},
        },
    }
    endpoints = fetch_endpoints(vision_model, api_key)
    print(f'VISION_ENDPOINTS model={vision_model} count={len(endpoints)}')
    passthrough = build_provider_options(endpoints, desired)
    log_passthrough(passthrough)

    subject = subject_block(
        subject_kind,
        context['subject_text'] if context else None,
        context['context_notes'] if context else None,
    )
    profile_text = profile_block(profile)

    data_url, raw_bytes, b64_chars = encode_clip(clip_path)
    print(f'CLIP_ENCODED candidate={candidate_id} bytes={raw_bytes} base64_chars={b64_chars}')

    vision_body: dict = {
        'model': vision_model,
        'messages': [{
            'role': 'user',
            'content': [
                {'type': 'text', 'text': build_vision_prompt(
                    subject, profile_text, product['transcript_lines'], duration_s)},
                {'type': 'video_url', 'video_url': {'url': data_url}},
            ],
        }],
    }
    if passthrough['options']:
        vision_body['provider'] = {'options': passthrough['options']}

    print(f'VISION_CALL model={vision_model} (request body never logged)')
    vision_response = _http_json(
        'POST', f'{OPENROUTER_BASE}/chat/completions', api_key, vision_body, VISION_TIMEOUT_S)
    scene = message_text(vision_response, 'vision')
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
    vision_generation_id = str(vision_response.get('id') or '')
    print(f'VISION_OK chars={len(scene)} generation_id={vision_generation_id}')

    writer_body = {
        'model': writer_model,
        'max_tokens': 1400,
        'temperature': writer_temperature,
        'messages': [{
            'role': 'user',
            'content': build_writer_prompt(
                subject, profile_text, scene, product['transcript_plain'], duration_s),
        }],
    }
    print(f'WRITER_CALL model={writer_model} temperature={writer_temperature}')
    writer_response = _http_json(
        'POST', f'{OPENROUTER_BASE}/chat/completions', api_key, writer_body, WRITER_TIMEOUT_S)
    writer_text = message_text(writer_response, 'writer')
    writer_generation_id = str(writer_response.get('id') or '')

    kit = validate_kit(writer_text, product['transcript_plain'])
    print(
        f'WRITER_OK generation_id={writer_generation_id} hashtags={len(kit["hashtags"])} '
        f'quoted={"1" if kit["quoted_line"] else "0"}'
    )

    vision_params = json.dumps({
        'model': vision_model,
        'fps': fps,
        'media_resolution': media_resolution,
        'passthrough_requested': passthrough['requested'],
        'passthrough_accepted': passthrough['accepted'],
        'passthrough_dropped': passthrough['dropped'],
    }, sort_keys=True)
    writer_params = json.dumps({
        'model': writer_model,
        'temperature': writer_temperature,
        'max_tokens': writer_body['max_tokens'],
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
                vision_model, writer_model, PROMPT_VERSION,
                vision_generation_id, writer_generation_id,
                vision_params, writer_params, 1 if passthrough['degraded'] else 0,
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
        },
        candidate_id, version, 'generated',
        {'file_path': info['clip_file_path'], 'drive_sync_path': info['drive_sync_path']},
    )

    print(
        f'RESULT generate_post_kit candidate={candidate_id} ok=1 skipped=0 kit_id={kit_id} '
        f'version={version} deactivated={deactivated} wrote_rows=1 '
        f'srt_cues={product["cue_count"]} srt_basis={geom["basis"]} '
        f'hashtags={len(kit["hashtags"])} subject={subject_kind} '
        f'passthrough_degraded={1 if passthrough["degraded"] else 0}'
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
