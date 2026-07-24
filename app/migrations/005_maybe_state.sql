-- 005_maybe_state.sql
CREATE TABLE IF NOT EXISTS candidates_v2 (
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
 CHECK (state IN ('candidate','approved','rejected','poisoned','maybe')),
 created_by_run TEXT NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(vod_id, start_s, end_s)
);
INSERT OR IGNORE INTO candidates_v2 (id, vod_id, start_s, end_s, score, signal_audio, signal_transcript, signal_chat, signal_beat_boost, state, created_by_run, created_at)
SELECT id, vod_id, start_s, end_s, score, signal_audio, signal_transcript, signal_chat, signal_beat_boost, state, created_by_run, created_at FROM candidates;
DROP TABLE IF EXISTS candidates;
ALTER TABLE candidates_v2 RENAME TO candidates;
