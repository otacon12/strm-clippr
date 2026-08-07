#!/usr/bin/env bash
# pg_test_harness.sh — throwaway PostgreSQL cluster for clpr PG-port verification (D-052 P3).
#
# Creates a unix-socket-only cluster in a mktemp dir (no TCP ports, so parallel
# agents never clash), creates role app_rw (LOGIN — 001 GRANTs to it, so it must
# exist BEFORE the schema applies), applies app/migrations_pg/001_consolidated_schema.sql,
# loads a data file (default: ~/Downloads/consolidated_data.sql), exports
# CLPR_DB_URL (connecting AS app_rw, the live app role, so the grants are part
# of what every proof exercises), runs the caller's command, and ALWAYS tears
# down via an EXIT trap. The command's exit code is propagated.
#
# Usage:
#   app/scripts/pg_test_harness.sh [--data FILE] command [args...]
#
# Harness chatter goes to stderr; the command's stdout is untouched.
# Every internal step is checked — a half-applied schema never reaches the
# caller's command as a confusing downstream error (charter 1.5 gate 2).

set -u

PGBIN=/usr/local/bin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA="$SCRIPT_DIR/../migrations_pg/001_consolidated_schema.sql"
DATA="$HOME/Downloads/consolidated_data.sql"

if [ "${1:-}" = "--data" ]; then
    DATA="${2:?--data requires a file argument}"
    shift 2
fi

if [ $# -lt 1 ]; then
    echo "ERROR: no command given. Usage: pg_test_harness.sh [--data FILE] command [args...]" >&2
    exit 2
fi
if [ ! -f "$SCHEMA" ]; then
    echo "ERROR: schema not found: $SCHEMA" >&2
    exit 2
fi
if [ ! -f "$DATA" ]; then
    echo "ERROR: data file not found: $DATA" >&2
    exit 2
fi

# Short path under /tmp: unix socket paths have a ~103-char limit; the default
# macOS mktemp dir under /var/folders/... risks blowing it.
TMP="$(mktemp -d /tmp/clpr_pgh.XXXXXXXX)" || { echo "ERROR: mktemp failed" >&2; exit 2; }
PGDATA="$TMP/pgdata"
PGLOG="$TMP/pg.log"
STARTED=0

teardown() {
    if [ "$STARTED" = 1 ]; then
        "$PGBIN/pg_ctl" -D "$PGDATA" stop -m immediate >/dev/null 2>&1 || true
    fi
    rm -rf "$TMP"
}
trap teardown EXIT

step() { # step <name> <cmd...> — run, and on failure dump the server log and exit
    local name="$1"; shift
    if ! "$@" >>"$PGLOG" 2>&1; then
        echo "ERROR: harness step failed: $name" >&2
        echo "--- $PGLOG tail ---" >&2
        tail -n 40 "$PGLOG" >&2 || true
        exit 2
    fi
}

step initdb "$PGBIN/initdb" -D "$PGDATA" --auth=trust --no-sync

if ! "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGLOG" -w \
        -o "-c listen_addresses='' -c unix_socket_directories='$TMP'" start >/dev/null 2>&1; then
    echo "ERROR: harness step failed: pg_ctl start" >&2
    echo "--- $PGLOG tail ---" >&2
    tail -n 40 "$PGLOG" >&2 || true
    exit 2
fi
STARTED=1

step createdb "$PGBIN/createdb" -h "$TMP" clpr
# 001 GRANTs to app_rw, so the role must exist first (LOGIN, as in the live DB).
step create_role_app_rw "$PGBIN/psql" -h "$TMP" -d clpr -v ON_ERROR_STOP=1 \
    -c "CREATE ROLE app_rw LOGIN"
step apply_schema "$PGBIN/psql" -h "$TMP" -d clpr -v ON_ERROR_STOP=1 -q -f "$SCHEMA"
step load_data "$PGBIN/psql" -h "$TMP" -d clpr -v ON_ERROR_STOP=1 -q -f "$DATA"

# Connect AS the app role over the socket (host = socket dir).
export CLPR_DB_URL="postgresql://app_rw@/clpr?host=$TMP"

"$@"
rc=$?
exit $rc
