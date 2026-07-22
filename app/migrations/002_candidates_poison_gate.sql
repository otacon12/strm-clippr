-- 002_candidates_poison_gate.sql
-- poison_reviewed: a VOD's poison sidecar has NOT been generated just because
-- poison_regions has zero rows for it. This column is the explicit
-- "someone reviewed this VOD for poison regions" marker.
-- Absence (0) means detection MUST refuse to run.
ALTER TABLE vods ADD COLUMN poison_reviewed INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS candidates (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER NOT NULL REFERENCES vods(id),
 start_s REAL NOT NULL,
 end_s REAL NOT NULL,
 score REAL,
 signal_audio REAL,
 signal_transcript REAL,
 signal_chat REAL,
 signal_beat_boost REAL,
 state TEXT NOT NULL DEFAULT 'candidate'
 CHECK (state IN ('candidate','approved','rejected','poisoned')),
 created_by_run TEXT NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(vod_id, start_s, end_s)
);

CREATE TABLE IF NOT EXISTS audio_energy (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER NOT NULL REFERENCES vods(id),
 start_s REAL NOT NULL,
 end_s REAL NOT NULL,
 loudness_lufs REAL NOT NULL,
 UNIQUE(vod_id, start_s, end_s)
);
