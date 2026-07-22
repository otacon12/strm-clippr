"""poison_gate: poison-review and overlap checks for detector workers.
Pure helper functions only.
"""

from __future__ import annotations

import sqlite3


def is_poison_reviewed(conn: sqlite3.Connection, vod_id: int) -> bool:
    """True only when vods.poison_reviewed=1 for this VOD.
    Missing vod row is treated as not reviewed (fail closed).
    """
    row = conn.execute(
        'SELECT poison_reviewed FROM vods WHERE id = ?',
        (vod_id,),
    ).fetchone()
    if row is None:
        return False
    return int(row[0] or 0) == 1


def mark_poison_reviewed(conn: sqlite3.Connection, vod_id: int) -> None:
    """Explicitly mark a VOD as poison reviewed.
    This is never inferred from zero poison_regions rows.
    """
    cur = conn.execute(
        'UPDATE vods SET poison_reviewed = 1 WHERE id = ?',
        (vod_id,),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f'vod_id not found: {vod_id}')


def is_poisoned(conn: sqlite3.Connection, vod_id: int, start_s: float, end_s: float) -> bool:
    """True when [start_s, end_s] intersects any poison region for the VOD.
    Overlap: NOT (end_s <= region.start_s OR start_s >= region.end_s)
    """
    row = conn.execute(
        '''
        SELECT 1
        FROM poison_regions
        WHERE vod_id = ?
          AND NOT (? <= start_s OR ? >= end_s)
        LIMIT 1
        ''',
        (vod_id, end_s, start_s),
    ).fetchone()
    return row is not None


def require_poison_reviewed_or_raise(conn: sqlite3.Connection, vod_id: int) -> None:
    """Raise when a VOD has not been explicitly poison-reviewed."""
    if not is_poison_reviewed(conn, vod_id):
        raise RuntimeError(
            f'poison review required before detection: vod_id={vod_id} poison_reviewed=0'
        )
