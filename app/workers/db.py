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

# AMP MODE (CLPR-AMP-01, ruling clpr-amp-mode-eight-changes-approved-amp-project-only-2026-10-07):
# the clipper run for the amp project only. OFF unless CLPR_AMP_MODE is exactly
# '1'; unset, empty or '0' is OFF and every code path below is the pre-amp one.
# Any other value is refused rather than read as OFF, because a mistyped switch
# read as OFF would send an amp run to CLPR_DB_URL.
AMP_MODE_ENV = 'CLPR_AMP_MODE'
# The amp project's own database on this Mac (app/scripts/amp_db.sh). Amp mode
# reads ONLY this name and never falls back to CLPR_DB_URL.
AMP_DB_URL_ENV = 'CLPR_AMP_DB_URL'


def amp_mode() -> bool:
    """True when CLPR_AMP_MODE=1; False when unset, empty or '0'; otherwise
    fail loudly (ERROR to stderr, exit 1)."""
    raw = os.environ.get(AMP_MODE_ENV)
    if raw is None or raw.strip() in ('', '0'):
        return False
    if raw.strip() == '1':
        return True
    print(
        f'ERROR: {AMP_MODE_ENV}={raw!r} is not a recognised value '
        '(1 = amp mode, 0 or unset = off); refusing to guess which database to use',
        file=sys.stderr,
    )
    sys.exit(1)


def get_db_url() -> str:
    """Return CLPR_DB_URL or fail loudly (ERROR to stderr, exit 1) when unset.

    In amp mode, return CLPR_AMP_DB_URL instead, or fail loudly when it is
    unset; CLPR_DB_URL is never read in amp mode.
    """
    if amp_mode():
        amp_url = os.environ.get(AMP_DB_URL_ENV, '').strip()
        if not amp_url:
            print(
                f'ERROR: {AMP_DB_URL_ENV} is not set ({AMP_MODE_ENV}=1: the amp project\'s '
                'own clipper database on this Mac; amp mode never falls back to CLPR_DB_URL)',
                file=sys.stderr,
            )
            sys.exit(1)
        return amp_url
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
