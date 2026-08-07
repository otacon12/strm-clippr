#!/usr/bin/env python3
"""db: shared PostgreSQL connection adapter for clpr workers (D-052 P3).

Every worker connects through this module and nothing else. The database is
the consolidated PostgreSQL `clpr` (schema: app/migrations_pg/001_consolidated_schema.sql,
naming contract: app/docs/naming-map.md). There is NO sqlite fallback and NO
default URL: a missing CLPR_DB_URL fails loudly (ERROR to stderr, exit 1)
instead of silently connecting to the wrong database — the sqlite-era
'./clpr.db' default is deliberately gone.
"""

from __future__ import annotations

import os
import sys

import psycopg2


def get_db_url() -> str:
    """Return CLPR_DB_URL or fail loudly (ERROR to stderr, exit 1) when unset."""
    url = os.environ.get('CLPR_DB_URL', '').strip()
    if not url:
        print(
            'ERROR: CLPR_DB_URL is not set '
            '(expected postgresql://... for the consolidated clpr database)',
            file=sys.stderr,
        )
        sys.exit(1)
    return url


def connect():
    """Open a psycopg2 connection to CLPR_DB_URL with autocommit OFF.

    autocommit OFF means the first statement opens a transaction implicitly;
    callers own commit()/rollback()/close() explicitly (no sqlite-style
    context-manager reliance — see app/docs/PORTING_CHECKLIST.md).
    """
    conn = psycopg2.connect(get_db_url())
    conn.autocommit = False
    return conn
