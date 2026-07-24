-- 006_clips.sql
CREATE TABLE IF NOT EXISTS clips (
 id INTEGER PRIMARY KEY,
 candidate_id INTEGER NOT NULL REFERENCES candidates(id),
 file_path TEXT NOT NULL,
 duration_s REAL NOT NULL,
 state TEXT NOT NULL DEFAULT 'rendered'
 CHECK (state IN ('rendered','published')),
 created_by_run TEXT NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(candidate_id)
);
