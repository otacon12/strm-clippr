#!/usr/bin/env bash
# amp_db.sh — the amp project's OWN persistent clipper database on this Mac
# (CLPR-AMP-01 item 1; ruling clpr-amp-mode-eight-changes-approved-amp-project-only-2026-10-07).
#
# A PostgreSQL cluster in $CLPR_AMP_PGDATA (no default: unset fails loudly).
# Unix-socket only (listen_addresses='', socket inside the data dir, written
# into postgresql.conf so every start keeps it), so it can never collide with
# or be mistaken for the live database's tunnel. Unlike pg_test_harness.sh it
# is NOT torn down: the data directory persists across runs and reboots.
#
#   CLPR_AMP_PGDATA=/abs/dir app/scripts/amp_db.sh init    # create + migrate; refuses an existing dir
#   CLPR_AMP_PGDATA=/abs/dir app/scripts/amp_db.sh start   # start it (after a reboot or stop)
#   CLPR_AMP_PGDATA=/abs/dir app/scripts/amp_db.sh stop
#   CLPR_AMP_PGDATA=/abs/dir app/scripts/amp_db.sh url     # print the CLPR_AMP_DB_URL value
#
# init creates role app_rw (the app role the schema GRANTs to, as live and as
# pg_test_harness.sh), database clpr_amp, and applies every
# app/migrations_pg/*.sql in sorted order through workers/migrations.py's
# apply_migrations (the clipper's own runner, so schema_migrations records
# every file and no worker re-runs one). Schema only: no data is loaded.
# Every step is checked; a failure prints the server log and exits non-zero.

set -u

die() { echo "ERROR: $*" >&2; exit 2; }

# Same PGBIN resolution as pg_test_harness.sh.
resolve_pgbin() {
    local found
    if found="$(command -v initdb 2>/dev/null)" && [ -n "$found" ]; then
        dirname "$found"
        return 0
    fi
    local candidate
    for candidate in /opt/homebrew/bin /usr/local/bin; do
        if [ -x "$candidate/initdb" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

CMD="${1:-}"
case "$CMD" in
    init|start|stop|url) ;;
    *) die "usage: CLPR_AMP_PGDATA=/abs/dir $0 init|start|stop|url" ;;
esac

PGDATA="${CLPR_AMP_PGDATA:-}"
[ -n "$PGDATA" ] || die "CLPR_AMP_PGDATA is not set (the amp clipper database's data directory; there is no default)"
case "$PGDATA" in
    /*) ;;
    *) die "CLPR_AMP_PGDATA must be an absolute path: $PGDATA" ;;
esac
# Unix socket paths are limited to ~103 bytes; the socket lives in PGDATA.
SOCKET="$PGDATA/.s.PGSQL.5432"
[ "${#SOCKET}" -le 103 ] || die "CLPR_AMP_PGDATA is too long for a unix socket (${#SOCKET} > 103 bytes): $SOCKET"

DBNAME=clpr_amp
URL="postgresql://app_rw@/$DBNAME?host=$PGDATA"
LOG="$PGDATA/amp_db.log"

if [ "$CMD" = url ]; then
    printf '%s\n' "$URL"
    exit 0
fi

PGBIN="$(resolve_pgbin)" || die "could not locate initdb (PATH, /opt/homebrew/bin, /usr/local/bin)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKERS_DIR="$SCRIPT_DIR/../workers"
MIGRATIONS_DIR="$SCRIPT_DIR/../migrations_pg"

step() { # step <name> <cmd...>: run; on failure show the log and exit
    local name="$1"; shift
    if ! "$@"; then
        echo "ERROR: amp_db step failed: $name" >&2
        [ -f "$LOG" ] && tail -n 40 "$LOG" >&2
        exit 2
    fi
}

running() { "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; }

case "$CMD" in
start)
    [ -f "$PGDATA/PG_VERSION" ] || die "no cluster at $PGDATA (run init first)"
    if running; then echo "amp_db: already running ($PGDATA)"; exit 0; fi
    step pg_ctl_start "$PGBIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w start
    echo "amp_db: started ($PGDATA)"
    ;;
stop)
    [ -f "$PGDATA/PG_VERSION" ] || die "no cluster at $PGDATA"
    if ! running; then echo "amp_db: not running ($PGDATA)"; exit 0; fi
    step pg_ctl_stop "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
    echo "amp_db: stopped ($PGDATA)"
    ;;
init)
    [ ! -e "$PGDATA" ] || die "$PGDATA already exists; init refuses to touch an existing directory"
    shopt -s nullglob
    MIGRATIONS=("$MIGRATIONS_DIR"/*.sql)
    shopt -u nullglob
    [ "${#MIGRATIONS[@]}" -gt 0 ] || die "no migrations found: $MIGRATIONS_DIR/*.sql"

    step initdb "$PGBIN/initdb" -D "$PGDATA" --auth=trust -U "$(id -un)" >/dev/null
    {
        echo ""
        echo "# amp_db.sh: unix socket only, socket inside the data directory"
        echo "listen_addresses = ''"
        echo "unix_socket_directories = '$PGDATA'"
    } >>"$PGDATA/postgresql.conf" || die "could not write $PGDATA/postgresql.conf"
    step pg_ctl_start "$PGBIN/pg_ctl" -D "$PGDATA" -l "$LOG" -w start
    step createdb "$PGBIN/createdb" -h "$PGDATA" "$DBNAME"
    step create_role_app_rw "$PGBIN/psql" -h "$PGDATA" -d "$DBNAME" -v ON_ERROR_STOP=1 -q \
        -c "CREATE ROLE app_rw LOGIN"
    step apply_migrations env AMP_DB_WORKERS="$WORKERS_DIR" AMP_DB_MIGRATIONS="$MIGRATIONS_DIR" \
        AMP_DB_ADMIN_URL="postgresql:///$DBNAME?host=$PGDATA" python3 -c '
import os, sys
from pathlib import Path
sys.path.insert(0, os.environ["AMP_DB_WORKERS"])
import psycopg2
import migrations
conn = psycopg2.connect(os.environ["AMP_DB_ADMIN_URL"])
conn.autocommit = False
try:
    migrations.apply_migrations(conn, Path(os.environ["AMP_DB_MIGRATIONS"]))
    conn.commit()
finally:
    conn.close()
'
    LEDGER="$("$PGBIN/psql" -h "$PGDATA" -d "$DBNAME" -At -v ON_ERROR_STOP=1 \
        -c "SELECT count(*) FROM schema_migrations")" || die "could not read schema_migrations"
    [ "$LEDGER" = "${#MIGRATIONS[@]}" ] \
        || die "schema_migrations has $LEDGER rows, expected ${#MIGRATIONS[@]} (one per migration file)"
    echo "amp_db: initialised $PGDATA ($DBNAME, ${#MIGRATIONS[@]} migrations applied)"
    echo "CLPR_AMP_DB_URL=$URL"
    ;;
esac
