-- 009_gemini_transport.sql — D-066: provenance for the Gemini transport era.
--
-- FORMATTING CONSTRAINT inherited from 003 through 008: the live applicator
-- app/workers/migrations.py splits this file on the semicolon character, so
-- no semicolon may appear inside any comment or string literal here. Every
-- semicolon below terminates a real statement.
--
-- WHAT HAPPENED, MEASURED 2026-08-07. The post-kit vision call is moving off
-- OpenRouter's inline base64 video path onto the Gemini Files API, for two
-- reasons.
--
-- THE OLD PATH'S DOWNSCALE FABRICATES. Against the same 15 second clip, the
-- full quality copy reported "The feed is intact for the whole clip", and the
-- downscaled copy of that identical footage reported black-frame cuts at
-- 00:05, 00:06, 00:08, 00:11 and 00:14 that do not exist in the file
-- (blackdetect finds zero black frames, and three extracted frames show an
-- intact concert).
--
-- FPS IS REAL ON THE NEW PATH. Same uploaded file, fps=1 gave 4432 prompt
-- tokens and fps=5 gave 20212. The operator ruled fps becomes content
-- dependent.
--
-- WHY COLUMNS AND NOT A LOG LINE. A kit row must record which transport
-- produced it and what analysis settings actually applied, because a kit
-- produced from a degraded input must never be indistinguishable from one
-- produced at full quality. That principle is already stated in D-064 and
-- this migration extends it to the new era. Queryable rather than buried in a
-- worker's stdout:
--
--     SELECT candidate_id, version, analysis_transport, fps_used, fps_reason
--     FROM post_kits WHERE analysis_transport = 'gemini_files'
--
-- ALL NULLABLE, NO DEFAULTS, NO BACKFILL. Existing rows are from the
-- OpenRouter era and their NULLs are the honest answer, not missing data.
-- Nothing here touches analysis_downscaled, analysis_source_bytes,
-- analysis_sent_bytes or analysis_downscale_detail: they stay exactly as they
-- are, the true history of the OpenRouter era, and their future is a later
-- brief's decision, not this one.
--
-- NO CHECK ON gemini_sha256_match. Its value set is Google's, not ours, and a
-- constraint we would have to migrate the day Google changes an encoding is a
-- liability, not a guard. Recorded as free text and the worker is the
-- authority on it.
--
-- NO COMMENT ON COLUMN. 003 through 008 document columns with comments in
-- this file, not with the SQL COMMENT ON COLUMN statement, so this migration
-- matches that house style rather than introducing a new one.

-- Which path produced this kit. 'openrouter_inline' or 'gemini_files'. The
-- era marker. NULL on every existing row, because no row written before this
-- migration was produced by either named transport as a recorded fact.
ALTER TABLE post_kits
    ADD COLUMN analysis_transport text
        CHECK (analysis_transport IS NULL OR analysis_transport IN ('openrouter_inline', 'gemini_files'));

-- The measured mean inter-frame luma difference of the delivered clip. The
-- classifier input that decides whether the clip reads as motion or static.
ALTER TABLE post_kits ADD COLUMN motion_yavg real;

-- The fps actually SENT to the model. Not a requested value, not a default:
-- what was really encoded into the request, so a kit's token cost and
-- coverage can be reconstructed after the fact.
ALTER TABLE post_kits ADD COLUMN fps_used real;

-- Why that fps. 'motion' means motion_yavg crossed the threshold that calls
-- for denser sampling. 'static' means it did not. NULL when no classification
-- ran, which is every OpenRouter era row.
ALTER TABLE post_kits
    ADD COLUMN fps_reason text
        CHECK (fps_reason IS NULL OR fps_reason IN ('motion', 'static'));

-- The server minted file id from the Gemini Files API upload, form
-- files/abc123. The provenance handle for the specific upload this kit's
-- analysis was run against.
ALTER TABLE post_kits ADD COLUMN gemini_file_name text;

-- Which encoding matched the hash Gemini returned for the uploaded file:
-- 'b64_of_hex', 'b64_of_digest', or 'hex'. This is the receipt that the
-- positive control actually ran. A NULL here on a gemini_files row means the
-- witness was skipped, not that it passed.
ALTER TABLE post_kits ADD COLUMN gemini_sha256_match text;

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw, with 003 restating it for post_kits. Columns added later are
-- covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('009_gemini_transport.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
