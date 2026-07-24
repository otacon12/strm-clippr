-- 003_chat_capture.sql
CREATE TABLE IF NOT EXISTS chat_raw (
 id INTEGER PRIMARY KEY,
 session_label TEXT NOT NULL,
 ts_utc TEXT NOT NULL,
 author TEXT NOT NULL,
 text TEXT NOT NULL,
 captured_by_run TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
 id INTEGER PRIMARY KEY,
 vod_id INTEGER NOT NULL REFERENCES vods(id),
 offset_s REAL NOT NULL,
 author TEXT NOT NULL,
 text TEXT NOT NULL,
 UNIQUE(vod_id, offset_s, author, text)
);
