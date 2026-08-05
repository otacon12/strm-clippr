-- 007_clips_drive_sync.sql
ALTER TABLE clips ADD COLUMN drive_synced_at TEXT;
ALTER TABLE clips ADD COLUMN drive_sync_path TEXT;
