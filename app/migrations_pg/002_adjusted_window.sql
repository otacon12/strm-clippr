-- 002_adjusted_window.sql — D-055: operator clip-window editing (review surface).
-- Adds nullable adjusted window columns to clip_candidates. The ORIGINAL
-- start_s/end_s are IMMUTABLE and never overwritten; every cut uses the
-- EFFECTIVE window COALESCE(adjusted_start_s, start_s)..COALESCE(adjusted_end_s, end_s).
-- Types are double precision, matching 001's start_s/end_s.

ALTER TABLE clip_candidates ADD COLUMN adjusted_start_s double precision, ADD COLUMN adjusted_end_s double precision;

-- ---------------------------------------------------------------------------
-- Record this migration in the ledger
-- ---------------------------------------------------------------------------
INSERT INTO schema_migrations (filename, applied_at)
VALUES ('002_adjusted_window.sql', to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'));
