-- 001_init.sql — M0 schema: vods, poison_regions, beats, transcript_segments
CREATE TABLE IF NOT EXISTS vods (
 id INTEGER PRIMARY KEY,
 path TEXT NOT NULL UNIQUE,
 session_label TEXT NOT NULL, -- e.g. '2026-07-20' — groups multi-segment days
 duration_s REAL,
 ingested_at TEXT NOT NULL, -- ISO8601, set by the worker at insert time
 state TEXT NOT NULL DEFAULT 'ingested'
 CHECK (state IN ('ingested','transcribed','detected','done'))
);

CREATE TABLE IF NOT EXISTS poison_regions (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER NOT NULL REFERENCES vods(id),
 start_s REAL NOT NULL,
 end_s REAL NOT NULL,
 source TEXT NOT NULL, -- 'safe_mode' | 'manual' | 'incident'
 reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS beats (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER REFERENCES vods(id),
 ts_utc TEXT NOT NULL, -- when the beat was marked, ISO8601
 note TEXT
);

CREATE TABLE IF NOT EXISTS transcript_segments (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER NOT NULL REFERENCES vods(id),
 start_s REAL NOT NULL,
 end_s REAL NOT NULL,
 text TEXT NOT NULL
);
