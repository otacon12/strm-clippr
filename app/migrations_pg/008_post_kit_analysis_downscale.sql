-- 008_post_kit_analysis_downscale.sql — D-064: THE PAYLOAD CEILING's provenance.
--
-- FORMATTING CONSTRAINT inherited from 003, 004, 005, 006 and 007: the live
-- applicator app/workers/migrations.py splits this file on the semicolon
-- character, so no semicolon may appear inside any comment or string literal
-- here. Every semicolon below terminates a real statement.
--
-- WHAT HAPPENED, MEASURED 2026-08-07 against the live OpenRouter API. The post
-- kit worker sends the clip to the vision model as a base64 data URL inside the
-- request body. OpenRouter sits behind Cloudflare, whose standard maximum
-- request body is 100 MB, and base64 inflates a file by 4/3:
--
--     clip   raw       base64 payload   vision call
--     c43    4.7 MB    6.2 MB           OK
--     c45    35.4 MB   47.2 MB          OK
--     c111   89.4 MB   119.2 MB         502, five times out of five
--
-- THE DANGEROUS PART WAS THE DISGUISE. The over limit request came back as
-- 502 origin_bad_gateway with a body stating "retryable" true and "retry_after"
-- 60. That is Cloudflare describing its own origin, not the truth about the
-- request: a PERMANENT, DETERMINISTIC size failure wearing a transient
-- outage's costume. The retry policy landed in 5b9c78f would have re-uploaded
-- that clip until the attempt cap, every run, forever.
--
-- THE OPERATOR'S RULING, verbatim, 2026-08-07: "for now downscale". So when a
-- clip would otherwise exceed the ceiling, and ONLY then, the worker transcodes
-- a THROWAWAY analysis copy for the model. The DELIVERED clip is never touched
-- and never changes, and 26 of the 27 clips in that batch are far under the
-- ceiling and take a byte-identical request to the one they take today.
--
-- WHY COLUMNS AND NOT A LOG LINE. A kit written from a downscaled analysis copy
-- looks EXACTLY like a kit written from the full quality clip: same shape, same
-- fields, same confident copy. It is not the same thing. The vision model saw
-- fewer pixels, so the scene description it produced, and every hook derived
-- from it, rest on a degraded input. That is provenance, not decoration, and it
-- has to be queryable rather than buried in a stream log:
--
--     SELECT candidate_id, version, analysis_source_bytes, analysis_sent_bytes
--     FROM post_kits WHERE analysis_downscaled = 1
--
-- BACKFILL HONESTY. NOT NULL DEFAULT 0 stamps every existing row with 0, and 0
-- is TRUE of every one of them: no kit in this database was written by a build
-- that could downscale anything at all. The three detail columns stay NULL on
-- those rows, which is also true of them.
--
-- OPERATOR EDITS ARE UNAFFECTED, AND CORRECTLY SO. The review server's edit
-- INSERT lists its columns explicitly and does not mention these, so an
-- operator edit takes the default 0 with NULL detail. That is the honest
-- reading: these columns describe how ONE generated kit's analysis input was
-- prepared, and an operator edit is the operator's own words rather than a
-- model's reading of any video.

-- 1 when the video sent to the vision model was a downscaled throwaway copy
-- because the full size clip would have exceeded the request body ceiling.
-- 0 means the model watched the delivered clip itself, byte for byte, which is
-- the ordinary case.
ALTER TABLE post_kits
    ADD COLUMN analysis_downscaled integer NOT NULL DEFAULT 0
        CHECK (analysis_downscaled IN (0, 1));

-- The DELIVERED clip's size in bytes, as measured on disk at generation time.
-- Recorded on every generated kit, downscaled or not, because "how big was the
-- thing the model was asked to watch" is the number the whole ceiling turns on
-- and it costs nothing to keep.
ALTER TABLE post_kits ADD COLUMN analysis_source_bytes bigint;

-- The size in bytes of the video that was ACTUALLY encoded into the request.
-- Equal to analysis_source_bytes on a normal run. Smaller, and by exactly how
-- much, when the transport copy was downscaled.
ALTER TABLE post_kits ADD COLUMN analysis_sent_bytes bigint;

-- JSON text: the recipe and the real numbers. Predicted and measured payload
-- sizes, the ceiling in force, the scale, the frame rate cap, the video and
-- audio bitrates, and how many encode attempts it took to land under the
-- ceiling. Stored because "it was downscaled" is not actionable and "89.4 MB
-- became 61.2 MB at 1280 on the long side and 15 fps" is.
ALTER TABLE post_kits ADD COLUMN analysis_downscale_detail text;

-- A degradation flag that cannot say WHAT was degraded is a flag nobody can act
-- on. The database enforces the pairing rather than trusting the worker to
-- always pass all of it. Falsifiable in both directions: an INSERT claiming a
-- downscale without the numbers is refused, and an ordinary kit needs none of
-- them.
ALTER TABLE post_kits
    ADD CONSTRAINT post_kits_analysis_downscale_has_detail CHECK (
        analysis_downscaled = 0 OR (
            analysis_source_bytes IS NOT NULL
            AND analysis_sent_bytes IS NOT NULL
            AND analysis_downscale_detail IS NOT NULL
            AND btrim(analysis_downscale_detail) <> ''
        )
    );

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw, with 003 restating it for post_kits. Columns added later are covered
-- by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('008_post_kit_analysis_downscale.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
