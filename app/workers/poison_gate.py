"""poison_gate: poison-review and overlap checks for detector workers.
Pure helper functions only.

PostgreSQL port (D-052 P3): helpers take a psycopg2 cursor (checklist D1);
tables per app/docs/naming-map.md (vods -> recordings, vod_id -> recording_id).
Callers own commit/rollback — nothing here commits.
"""

from __future__ import annotations


def is_poison_reviewed(cur, recording_id: int) -> bool:
    """True only when recordings.poison_reviewed=1 for this recording.
    Missing recording row is treated as not reviewed (fail closed).
    """
    cur.execute(
        'SELECT poison_reviewed FROM recordings WHERE id = %s',
        (recording_id,),
    )
    row = cur.fetchone()
    if row is None:
        return False
    return int(row[0] or 0) == 1


def mark_poison_reviewed(cur, recording_id: int) -> None:
    """Explicitly mark a recording as poison reviewed.
    This is never inferred from zero poison_regions rows.
    """
    cur.execute(
        'UPDATE recordings SET poison_reviewed = 1 WHERE id = %s',
        (recording_id,),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f'recording_id not found: {recording_id}')


def is_poisoned(cur, recording_id: int, start_s: float, end_s: float) -> bool:
    """True when [start_s, end_s] intersects any poison region for the recording.
    Overlap: NOT (end_s <= region.start_s OR start_s >= region.end_s)
    """
    cur.execute(
        '''
        SELECT 1
        FROM poison_regions
        WHERE recording_id = %s
          AND NOT (%s <= start_s OR %s >= end_s)
        LIMIT 1
        ''',
        (recording_id, end_s, start_s),
    )
    return cur.fetchone() is not None


def require_poison_reviewed_or_raise(cur, recording_id: int) -> None:
    """Raise when a recording has not been explicitly poison-reviewed."""
    if not is_poison_reviewed(cur, recording_id):
        raise RuntimeError(
            f'poison review required before detection: recording_id={recording_id} poison_reviewed=0'
        )
