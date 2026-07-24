-- 004_beats_offset.sql
-- offset_s: vod-relative seconds where the beat/trigger point falls. Nullable.
ALTER TABLE beats ADD COLUMN offset_s REAL;
-- source vocabulary enforced in code (SQLite ALTER TABLE cannot add CHECK).
ALTER TABLE beats ADD COLUMN source TEXT NOT NULL DEFAULT 'manual';
