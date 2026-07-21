# AGENTS.md — builder build-rule contract for The Clipper (clpr)

When this and `design-spec.md` disagree, the spec wins; flag the conflict.

## Ground rules (charter §9/§11, instantiated for this project)

1. **Additive-only edits; touch nothing you were not asked to touch.** Verified modules are never put at risk by a neighboring change.
2. **The operator's live machine is the version oracle:** Python 3.9 (system), ffmpeg as installed, whisper.cpp as built at M0. Never assume a newer interpreter or a flag the installed ffmpeg lacks; when in doubt the brief states the verified version.
3. **Verify before reporting; report back per the brief footer.** Every brief ends with the standard footer (branch, commit-don't-deploy, per-item mapping, verification output, artifact via `git show origin/<branch>:<path>`).
4. **Secrets never in the repo.** `clpr/.env` is gitignored; code reads env by NAME. You never receive or hold the Twitch/YouTube/Claude keys themselves.
5. **SQLite schema changes ONLY via numbered migration files** (`app/migrations/NNN_description.sql`). No ORM, no framework.
6. **A failed run writes nothing** (charter §11 gate 9): on any stage failure, report loudly, persist no partial rows. A garbage candidate row makes a failure look like a finding.
7. **Idempotent processing:** re-running any stage on the same VOD must not duplicate rows (find-or-create on the natural key stated in the brief).
8. **Poison regions are sacred (D-005):** detection code refuses to run without a poison sidecar for the VOD (absence = loud failure, never "assume clean"), and no candidate may overlap a poison region. Never "fix" this check to be lenient; escalate instead.
9. **LLM calls are contained:** structured input, structured output, judged only from real data given to them. An LLM never invents a timestamp, metric, or fact about a clip. Unknown stays null.
10. **Whisper/ffmpeg output is quoted verbatim** in reports (charter §11 gate 7) — never paraphrase an exit code, a duration, or a count.

## Canonical patterns (inline so you never open a big artifact to recall one)

**Worker skeleton (every pipeline stage):**
```python
#!/usr/bin/env python3
"""<stage>: <one line>. Reads .env by name; exits non-zero on ANY failure; prints
machine-parseable result line last: RESULT <stage> vod=<id> <key>=<value>..."""
import os, sys, sqlite3

def main() -> int:
    # ... do the one job; raise on anything unexpected — never swallow ...
    print(f"RESULT <stage> vod={vod_id} ok=1 ...")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

**ffmpeg invocation:** always `subprocess.run([...], check=True, capture_output=True)` with an explicit arg list (never a shell string), log the full command, quote stderr verbatim on failure.

**DB access:** `sqlite3` stdlib, parameterized queries only, one connection per worker run, explicit transactions — commit only after the stage fully succeeds (rule 6).

**Chat/API fetches:** every external fetch writes a provenance record (source, when, what); rate-limit to human-scale; on HTTP failure report the raw status + body excerpt verbatim, write nothing.

## Report-back footer (standard, every brief)
```
Branch: <feature/...>   Base: main or the continuing branch
On done: commit (do NOT deploy/apply), then REPORT BACK AS:
- commit hash + message
- changed file(s)
- per-change summary mapped to the brief's numbered items
- the required verification output, quoted VERBATIM
- diff summary
- artifact via `git show origin/<branch>:<path>`
```
