#!/usr/bin/env bash
# pg_test_harness.sh — throwaway PostgreSQL cluster for clpr PG-port verification (D-052 P3).
#
# Creates a unix-socket-only cluster in a mktemp dir (no TCP ports, so parallel
# agents never clash), creates role app_rw (LOGIN — 001 GRANTs to it, so it must
# exist BEFORE the schema applies), applies EVERY app/migrations_pg/*.sql in
# sorted order (D-055 fixer: a hardcoded 001 silently skipped later migrations),
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

# PGBIN resolution (D-052 P3 / golden-review F18 item 4): a hardcoded
# /usr/local/bin is Intel-Homebrew-pinned and silently wrong on Apple
# Silicon (Homebrew's default prefix there is /opt/homebrew) or any host
# where postgresql isn't installed via Homebrew at all. Resolve from
# `command -v initdb` first (honors PATH, including a non-Homebrew install),
# then fall back through the two known Homebrew prefixes, and refuse by name
# -- listing everywhere it looked -- when none of them has it, rather than
# silently trying a wrong path and failing later with a confusing error.
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

if ! PGBIN="$(resolve_pgbin)"; then
    echo "ERROR: could not locate initdb. Looked: (1) PATH via 'command -v initdb', (2)" >&2
    echo "/opt/homebrew/bin/initdb, (3) /usr/local/bin/initdb. Install PostgreSQL (e.g." >&2
    echo "'brew install postgresql@16') or add its bin directory to PATH." >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/../migrations_pg"
DATA="$HOME/Downloads/consolidated_data.sql"

if [ "${1:-}" = "--data" ]; then
    DATA="${2:?--data requires a file argument}"
    shift 2
fi

if [ $# -lt 1 ]; then
    echo "ERROR: no command given. Usage: pg_test_harness.sh [--data FILE] command [args...]" >&2
    exit 2
fi
# Fail loudly if the migrations glob matches nothing (charter 1.5 gate 2: an
# empty match must never fall through to a "clean" run with no schema).
shopt -s nullglob
MIGRATIONS=("$MIGRATIONS_DIR"/*.sql)
shopt -u nullglob
if [ "${#MIGRATIONS[@]}" -eq 0 ]; then
    echo "ERROR: no migrations found: $MIGRATIONS_DIR/*.sql" >&2
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
# Bash glob expansion is already lexicographically sorted => numbered order.
for mig in "${MIGRATIONS[@]}"; do
    step "apply_$(basename "$mig")" "$PGBIN/psql" -h "$TMP" -d clpr -v ON_ERROR_STOP=1 -q -f "$mig"
done
step load_data "$PGBIN/psql" -h "$TMP" -d clpr -v ON_ERROR_STOP=1 -q -f "$DATA"

# Connect AS the app role over the socket (host = socket dir).
export CLPR_DB_URL="postgresql://app_rw@/clpr?host=$TMP"

"$@"
rc=$?
exit $rc
