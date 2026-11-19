#!/usr/bin/env bash
# Ensure a local PostgreSQL server is running on :5432 with the CI credentials
# and the `aaiclick` database. Reuses one already up; otherwise inits a cluster
# under .cache/postgres/data (once) and starts it. `stop` stops it.
# Uses the system postgres binaries; they refuse root, so they run as `postgres`.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

pg_bin="$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"
dir="$here/../.cache/postgres"
export PGPASSWORD=secret

if [ "${1:-}" = "stop" ]; then
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$dir/data" -m fast stop >/dev/null 2>&1 || true
  echo "[postgres] stopped"
  exit 0
fi

if "$pg_bin/pg_isready" -h localhost -q; then
  echo "[postgres] already up on :5432"
else
  if [ ! -f "$dir/data/PG_VERSION" ]; then
    mkdir -p "$dir"
    chown postgres: "$dir"
    echo secret > "$dir/pwfile"
    chmod 644 "$dir/pwfile"
    runuser -u postgres -- "$pg_bin/initdb" -D "$dir/data" -U aaiclick --pwfile="$dir/pwfile" -A scram-sha-256 >"$dir/initdb.log"
    rm "$dir/pwfile"
    echo "[postgres] cluster initialized at $dir/data"
  fi
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$dir/data" -l "$dir/postgres.log" -o "-k $dir" start >/dev/null \
    || { echo "[postgres] error: failed to start; see $dir/postgres.log" >&2; exit 1; }
  echo "[postgres] up on :5432"
fi
psql -h localhost -U aaiclick -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'aaiclick'" | grep -q 1 \
  || "$pg_bin/createdb" -h localhost -U aaiclick aaiclick
