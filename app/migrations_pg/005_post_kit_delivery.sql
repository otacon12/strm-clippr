-- 005_post_kit_delivery.sql — the POST KIT's own Drive delivery witness.
--
-- FORMATTING CONSTRAINT inherited from 003 and 004: the live applicator
-- app/workers/migrations.py splits this file on the semicolon character, so no
-- semicolon may appear inside any comment or string literal here. Every
-- semicolon below terminates a real statement.
--
-- WHY THIS EXISTS, AND WHY IT IS A SEPARATE MIGRATION. The n8n post-kit lane
-- uploads two files to the clips Drive folder and then records that it did.
-- Those three columns were written into the workflow and into
-- clpr/n8n/D062-canvas-steps.md before any migration created them, so the
-- final node of the lane issued an UPDATE against columns that did not exist:
-- the kit row committed, the money was spent, and the delivery step then died
-- on a missing column with no way to recover except a second paid generation.
-- An adversarial review measured it against a real applied schema (post_kits
-- had 31 columns and none of the three) before a single live run.
--
-- THE WITNESS IS THE KIT'S OWN, NOT THE CLIP'S. clips.drive_synced_at (D-056)
-- is the witness the review queue gates on, and it answers "is the VIDEO in
-- Drive". post_kits.drive_synced_at answers "is the COPY in Drive". The two
-- are deliberately separate columns on separate tables: a post kit landing in
-- Drive says nothing about whether the video did, and conflating them would
-- let a delivered kit make an undelivered clip look shipped.
--
-- These are additive nullable columns with no default, so every existing
-- post_kits row keeps its exact current bytes and every existing reader keeps
-- working unchanged. NULL means one honest thing: this kit has not been
-- delivered to Drive.

-- The ISO8601 UTC instant the lane finished BOTH uploads. Text, for port
-- fidelity with 001 and with clips.drive_synced_at.
ALTER TABLE post_kits ADD COLUMN drive_synced_at text;

-- The Drive file NAME the kit text landed under, exactly as the Drive node
-- reported it back. Never a local path.
ALTER TABLE post_kits ADD COLUMN drive_sync_path text;

-- The Drive file NAME the captions SRT landed under. NULL is a real and
-- expected state: a clip whose window holds no transcript segments ships no
-- SRT at all, because an empty SRT would be a claim that nothing was said.
ALTER TABLE post_kits ADD COLUMN srt_drive_sync_path text;

-- ---------------------------------------------------------------------------
-- No new GRANT is needed: privileges are held on the TABLE, and 003 already
-- granted SELECT, INSERT, UPDATE, DELETE on post_kits to app_rw. A column
-- added later is covered by that table-level grant.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('005_post_kit_delivery.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
