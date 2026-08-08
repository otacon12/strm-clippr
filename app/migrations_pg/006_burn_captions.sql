-- 006_burn_captions.sql — D-063: per-clip BURNED-IN SPEECH CAPTIONS.
--
-- FORMATTING CONSTRAINT inherited from 003, 004 and 005: the live applicator
-- app/workers/migrations.py splits this file on the semicolon character, so no
-- semicolon may appear inside any comment or string literal here. Every
-- semicolon below terminates a real statement.
--
-- WHAT THE OPERATOR RULED (2026-08-07). Clips may carry burned-in speech
-- captions, chosen "C" (on demand, not always) as a "UI option while
-- approving". So the intent is per candidate, it is OFF by default, ticking it
-- is the opt-in, and the burn happens in the SAME render pass as the clip.
--
-- WHY THIS DOES NOT CONTRADICT D-061's NO-BURN-IN RULING. That ruling governs
-- the HOOK, the attention line the operator types into each platform's own
-- text tool, and it stays a suggestion. Captions are a different product: a
-- line of subtitles along the bottom is not a "majority text" frame, neither
-- Reels nor TikTok accepts a caption sidecar file at all, and platform
-- auto-captions mangle this project's vocabulary (n8n, psycopg2, Coolify) that
-- whisper already transcribed correctly. Burning is the only way to ship
-- accurate captions there.
--
-- THREE COLUMNS, NOT ONE, AND THAT IS THE WHOLE POINT (charter gate 20: when
-- one column carries two facts, every predicate over it is right about at most
-- one of them). Three genuinely different facts live here:
--
--   1. clip_candidates.burn_captions — what the operator WANTS, right now. He
--      may flip it after a render, so it can never be read as a statement
--      about any file that already exists.
--   2. clips.captions_requested — what was asked of the render that actually
--      produced the file on disk, frozen at render time.
--   3. clips.captions_burned — whether the encode really carried the subtitles
--      filter. This is the only column that may ever be read as "this file has
--      captions in it".
--
-- Facts 2 and 3 differ in a real, expected case: captions were requested and
-- the clip's window holds NO speech, so there was nothing to burn. That must
-- read as "asked, nothing to say", never as a failure and never as a delivered
-- caption. captions_cue_count is the witness that distinguishes them, and it
-- is NOT derivable from the other two.
--
-- A CLIP DESCRIBED AS CAPTIONED THAT HAS NO CAPTIONS IS THE FAILURE THIS
-- MIGRATION EXISTS TO MAKE IMPOSSIBLE, so the constraints below are checked by
-- the database rather than by care: nothing can be marked burned unless it was
-- also requested, and nothing can be marked burned with zero cues.
--
-- BACKFILL HONESTY. NOT NULL DEFAULT 0 stamps every existing row with 0, and 0
-- is TRUE of every one of them: no clip in this database was rendered by a
-- build that could burn anything. captions_cue_count stays NULL on those rows,
-- which reads as "never asked", not as "asked and found nothing".

-- ---------------------------------------------------------------------------
-- 1. The INTENT, on the candidate — the review UI's captions toggle.
--    DEFAULT 0 because the ruling was "on demand, not always": ticking the box
--    is the opt-in, and a clip nobody thought about ships exactly as it does
--    today.
-- ---------------------------------------------------------------------------
ALTER TABLE clip_candidates
    ADD COLUMN burn_captions integer NOT NULL DEFAULT 0
        CHECK (burn_captions IN (0, 1));

-- ---------------------------------------------------------------------------
-- 2. The FACTS, on the clip row that describes the actual file.
-- ---------------------------------------------------------------------------

-- What the render that produced this file was asked to do. Frozen at render
-- time so a later flip of the candidate toggle cannot rewrite history.
ALTER TABLE clips
    ADD COLUMN captions_requested integer NOT NULL DEFAULT 0
        CHECK (captions_requested IN (0, 1));

-- Whether the subtitles filter was really in the encode that made this file.
-- The ONLY column any reader may treat as "this video has captions in it".
ALTER TABLE clips
    ADD COLUMN captions_burned integer NOT NULL DEFAULT 0
        CHECK (captions_burned IN (0, 1));

-- How many caption cues went in. NULL means captions were never requested for
-- this render. 0 means they were requested and the clip's window holds no
-- speech, so there was nothing to burn — an honest, expected outcome.
ALTER TABLE clips
    ADD COLUMN captions_cue_count integer
        CHECK (captions_cue_count IS NULL OR captions_cue_count >= 0);

-- A burn that nobody asked for cannot exist.
ALTER TABLE clips
    ADD CONSTRAINT clips_captions_burned_requires_request
        CHECK (captions_burned = 0 OR captions_requested = 1);

-- A burn with no cues cannot exist: that is the no-speech case, which is
-- captions_burned = 0 with captions_cue_count = 0.
ALTER TABLE clips
    ADD CONSTRAINT clips_captions_burned_has_cues
        CHECK (captions_burned = 0 OR (captions_cue_count IS NOT NULL AND captions_cue_count > 0));

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw. Columns added later are covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('006_burn_captions.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
