-- 007_post_kit_quote_fallback.sql — the NO-QUOTE FALLBACK's provenance.
--
-- FORMATTING CONSTRAINT inherited from 003, 004, 005 and 006: the live
-- applicator app/workers/migrations.py splits this file on the semicolon
-- character, so no semicolon may appear inside any comment or string literal
-- here. Every semicolon below terminates a real statement.
--
-- WHAT HAPPENED, MEASURED. On the 27-clip batch of 2026-08-07 candidate 45
-- failed FOUR separate attempts on the INVENTED_QUOTE gate, fabricating a
-- DIFFERENT plausible sentence every time. The gate was right and wrote zero
-- rows every time, so the clip ended the batch with no post kit at all.
--
-- WHAT CHANGED. The quoted line is OPTIONAL, and several kits generated that
-- same day carry no quote and read fine. So generate_post_kit.py now re-asks
-- the WRITER once, for copy with NO quoted line, and reuses the vision result
-- it has already paid for. Only the writer is re-called, and only for a QUOTE
-- failure: a banned dash, an over-length hook or a hashtag over the cap still
-- fails loudly on the first try.
--
-- WHY A COLUMN AND NOT A LOG LINE. A kit that silently dropped its quote to a
-- fabrication looks EXACTLY like a clip that never had a quotable line: both
-- have quoted_line NULL. That is a real difference the operator has to be able
-- to see, and one of them means a model tried to put words in somebody's
-- mouth. This is provenance, not decoration, so it is queryable:
--
--     SELECT candidate_id, version, quote_fallback_reason
--     FROM post_kits WHERE quote_fallback = 1
--
-- BACKFILL HONESTY. NOT NULL DEFAULT 0 stamps every existing row with 0, and 0
-- is TRUE of every one of them: no kit in this database was written by a build
-- that could fall back at all. quote_fallback_reason stays NULL on those rows.
--
-- OPERATOR EDITS ARE UNAFFECTED, AND CORRECTLY SO. The review server's edit
-- INSERT lists its columns explicitly and does not mention these, so an
-- operator edit takes the default 0. That is the truth: the operator wrote
-- those words himself, and no model fabricated anything in them.

-- 1 when this kit's FIRST draft quoted something that is not in the transcript
-- and the copy was rewritten with no quoted line. 0 means the first draft was
-- accepted as written, which is the ordinary case.
ALTER TABLE post_kits
    ADD COLUMN quote_fallback integer NOT NULL DEFAULT 0
        CHECK (quote_fallback IN (0, 1));

-- The rejected draft's failure, VERBATIM, including the fabricated quotation
-- itself. Stored because "a quote was invented" is not actionable and "the
-- model claimed he said <this>, and he did not" is.
ALTER TABLE post_kits
    ADD COLUMN quote_fallback_reason text;

-- A fallback that cannot say what it fell back FROM is a flag nobody can act
-- on. The database enforces the pairing rather than trusting the worker to
-- always pass both.
ALTER TABLE post_kits
    ADD CONSTRAINT post_kits_quote_fallback_has_reason CHECK (
        quote_fallback = 0 OR (quote_fallback_reason IS NOT NULL AND btrim(quote_fallback_reason) <> '')
    );

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw. Columns added later are covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('007_post_kit_quote_fallback.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
