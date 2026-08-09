-- 011_render_seq.sql — versioned re-renders (operator ask, 2026-08-09:
-- "regen should save a new video with descriptive txt, ie reg1 or something
-- like that").
--
-- FORMATTING CONSTRAINT inherited from 003 through 010: the live applicator
-- app/workers/migrations.py splits this file on the semicolon character, so
-- no semicolon may appear inside any comment or string literal here. Every
-- semicolon below terminates a real statement.
--
-- THE PROBLEM. A re-render of the same candidate overwrote the SAME
-- deterministic Drive filename (deliver_approved.delivered_name(), D-068),
-- so two renders of one candidate were indistinguishable in Drive -- the
-- operator could not tell which upload was current, and nothing in the
-- filename itself gave a stale local cache (review_server.py's
-- fetch_clip_from_server) a reason to fetch a fresh copy.
--
-- THE FIX. ONE new column, clips.render_seq: how many times this
-- candidate's clip row has been written by a render worker (cut_clip.py,
-- deliver_approved.render_adjusted_clip, render_from_slice.py). A fresh
-- INSERT takes the DEFAULT, 1. Every subsequent render of the SAME
-- candidate -- the ON CONFLICT (candidate_id) DO UPDATE path every render
-- worker already uses -- bumps it. delivered_name() (D-068, in
-- app/workers/deliver_approved.py) now takes render_seq as a parameter:
-- seq 1 renders the exact name it has always produced, byte-identical;
-- seq > 1 appends _r<seq> before the extension, so a re-render never lands
-- on an indistinguishable filename again.
--
-- BACKFILL HONESTY. NOT NULL DEFAULT 1 stamps every existing clips row with
-- 1, and 1 is true of every one of them: no clip in this database has ever
-- been rendered more than once under this column's watch, so every
-- existing delivered filename is already exactly what seq=1 produces.
--
-- IF NOT EXISTS on the column itself (belt and suspenders alongside
-- migrations.py's own per-clause duplicate-column savepoint, D-055's
-- fixer): either mechanism alone already makes this migration idempotent,
-- and the two do not conflict.
--
-- OPERATOR APPLICATION REQUIRED, same as 010 (2026-08-08, D-074 ruling 15).
-- app_rw holds only SELECT/INSERT/UPDATE/DELETE (001), never ALTER, so it
-- cannot run this file. The operator applies it as the postgres superuser.
-- Until then, any worker read or write that names render_seq fails loudly
-- with Postgres's own undefined-column error rather than silently guessing
-- -- the same honest refusal 010 documents for its three columns.

ALTER TABLE clips ADD COLUMN IF NOT EXISTS render_seq integer NOT NULL DEFAULT 1;

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 001 already
-- granted SELECT, INSERT, UPDATE, DELETE on every table in the schema to
-- app_rw. Columns added later are covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('011_render_seq.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
