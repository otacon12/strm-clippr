-- 010_hook_burn_colors.sql — D-061/D-063 amendment (2026-08-08): an OPTION to
-- burn the on-video hook text into the render, plus color choices for
-- captions and the hook, with auto-contrast backing computed by the renderer.
--
-- FORMATTING CONSTRAINT inherited from 003 through 009: the live applicator
-- app/workers/migrations.py splits this file on the semicolon character, so
-- no semicolon may appear inside any comment or string literal here. Every
-- semicolon below terminates a real statement.
--
-- WHAT THE OPERATOR RULED (2026-08-08). D-061 said the hook is a suggestion
-- the operator types into the platform's own text tool, never burned into the
-- delivered file. This amends that: burning the hook is now an OPTION, off by
-- default, so D-061's no-burn behavior is exactly what every existing and
-- unedited candidate keeps getting. The operator also wants to choose the
-- color of the burned captions and the burned hook, rather than the single
-- hardcoded white-on-black-outline style render_from_slice.py has shipped
-- since D-063.
--
-- THREE COLUMNS, ALL ADDITIVE, ALL NULLABLE-OR-DEFAULTED, so this migration
-- changes the behavior of NO existing candidate:
--
--   1. clip_candidates.burn_hook — the intent, same shape as D-063's
--      burn_captions column (006): what the operator wants for the NEXT
--      render, an integer 0/1, DEFAULT 0. Ticking it is the opt-in, and a
--      candidate nobody touched renders exactly as it does today (D-061's
--      overlay-only hook, never burned).
--   2. clip_candidates.caption_color — the operator's chosen hex color for
--      burned captions, or NULL to mean "renderer default" (the existing
--      white CAPTION_FORCE_STYLE). NULLABLE WITH NO DEFAULT, deliberately:
--      unlike burn_hook and burn_captions this is not a boolean opt-in, it is
--      a value that is either stated or absent, and absent must stay absent
--      rather than defaulting to some color that looks like a choice nobody
--      made.
--   3. clip_candidates.hook_color — the same shape as caption_color, for the
--      burned hook text. Meaningless unless burn_hook = 1, but not
--      constrained on that: the operator may set a hook color before ever
--      ticking burn_hook, same as he may set burn_captions before a kit
--      exists.
--
-- COLOR FORMAT. A CSS-style hex string, '#' optional, exactly 6 hex digits
-- (no 3-digit shorthand, no alpha channel) — RGB, because the renderer alone
-- decides alpha/backing opacity via the auto-contrast rule, and the operator
-- is picking a foreground ink color, not a translucency. Validated by a CHECK
-- using a POSIX regexp so a malformed value can never reach the renderer,
-- which would otherwise have to decide whether to fail loudly mid-encode or
-- silently fall back — the database refusing it up front is the honest
-- failure point (charter gate 2: a check that cannot fail loudly is not a
-- check).
--
-- BACKFILL HONESTY. burn_hook DEFAULT 0 stamps every existing row with 0, and
-- 0 is true of every one of them: no clip in this database was rendered by a
-- build that could burn a hook. caption_color and hook_color stay NULL on
-- every existing row, which reads as "never chosen", not as "chose the
-- default" — the same distinction 007's quote_fallback_reason draws between
-- absent and empty.
--
-- WHY NOT A CHECK PAIRING burn_hook TO hook_color THE WAY 006 PAIRS
-- captions_burned TO captions_cue_count. That 006 pairing polices a FACT
-- about a file that already exists (a burn that happened must have cues).
-- burn_hook is an INTENT column on the candidate, read at render time
-- exactly like burn_captions — the render is free to burn the hook with NO
-- hook_color set (the renderer's own default ink applies), so requiring one
-- would forbid a legitimate, common case rather than catch a lie.
--
-- OPERATOR APPLICATION REQUIRED (2026-08-08, D-074 ruling 15). app_rw holds
-- only SELECT/INSERT/UPDATE/DELETE (001), never ALTER, so it cannot run this
-- file. The operator applies it as the postgres superuser. Until then, the
-- app's own runtime migration path refuses new writes that need these
-- columns with MIGRATION_PENDING rather than attempting an ALTER app_rw
-- cannot perform and failing on a confusing permissions error instead. That
-- refusal is designed, not a bug to route around.

-- 1. The intent: burn the hook into the SAME render pass as the clip, same
--    shape as D-063's burn_captions (006) — OFF by default, an opt-in tick.
ALTER TABLE clip_candidates
    ADD COLUMN burn_hook integer NOT NULL DEFAULT 0
        CHECK (burn_hook IN (0, 1));

-- 2. The operator's chosen color for burned captions. NULL means "renderer
--    default" (today's white). 6 hex digits, '#' optional, case-insensitive.
ALTER TABLE clip_candidates
    ADD COLUMN caption_color text
        CHECK (caption_color IS NULL OR caption_color ~ '^#?[0-9A-Fa-f]{6}$');

-- 3. The operator's chosen color for the burned hook. Same shape and same
--    NULL-means-default reading as caption_color.
ALTER TABLE clip_candidates
    ADD COLUMN hook_color text
        CHECK (hook_color IS NULL OR hook_color ~ '^#?[0-9A-Fa-f]{6}$');

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw. Columns added later are covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('010_hook_burn_colors.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
