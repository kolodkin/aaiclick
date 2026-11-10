#!/usr/bin/env bash
# Start (or `stop`) the local distributed-backend services the CI test jobs
# use: ClickHouse on :8123, PostgreSQL on :5432 and MinIO (S3) on :9100, then
# apply migrations.
# Binaries/clusters come from claudeai/setup.sh (run here if missing);
# processes do not survive a container resume, so run this each session.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"
. "$here/env.sh"

cache="$here/../.cache"
ch_bin="$cache/clickhouse/clickhouse"
pg_bin="$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)"
pg_dir="$cache/postgres"
minio_bin="$cache/minio/minio"
export PGPASSWORD=secret

stop() {
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$pg_dir/data" -m fast stop 2>/dev/null || true
  [ -f "$cache/clickhouse/clickhouse.pid" ] && kill "$(cat "$cache/clickhouse/clickhouse.pid")" 2>/dev/null || true
  [ -f "$cache/minio/minio.pid" ] && kill "$(cat "$cache/minio/minio.pid")" 2>/dev/null || true
  echo "[run] all services stopped"
}

case "${1:-start}" in
  stop) stop; exit 0 ;;
  start) ;;
  *) echo "usage: $0 [start|stop]" >&2; exit 2 ;;
esac

[ -x "$ch_bin" ] && [ -f "$pg_dir/data/PG_VERSION" ] && [ -x "$minio_bin" ] || "$here/setup.sh"

if curl -fs localhost:8123/ping >/dev/null; then
  echo "[clickhouse] already up"
else
  # Minimal server config with the CI credentials, laid out like the stock
  # image: users.xml plus users.d/, which gets the same S3 profile file the CI
  # blob job mounts (docker/clickhouse/users.d/s3.xml).
  mkdir -p "$cache/clickhouse/users.d"
  cp "$here/../docker/clickhouse/users.d/s3.xml" "$cache/clickhouse/users.d/s3.xml"
  cat >"$cache/clickhouse/config.xml" <<XML
<clickhouse>
  <logger><level>warning</level><console>1</console></logger>
  <listen_host>127.0.0.1</listen_host>
  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>
  <path>$cache/clickhouse/data/</path>
  <user_directories><users_xml><path>users.xml</path></users_xml></user_directories>
</clickhouse>
XML
  cat >"$cache/clickhouse/users.xml" <<XML
<clickhouse>
  <profiles><default/></profiles>
  <quotas><default/></quotas>
  <users>
    <default>
      <password>click123</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </default>
  </users>
</clickhouse>
XML
  # The AWS pair is MinIO's (see env.sh): the server's own S3 identity, lent to
  # queries by users.d/s3.xml.
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    nohup "$ch_bin" server --config-file="$cache/clickhouse/config.xml" --pid-file="$cache/clickhouse/clickhouse.pid" \
    >"$cache/clickhouse.log" 2>&1 &
  disown
  for _ in $(seq 1 60); do curl -fs localhost:8123/ping >/dev/null && break; sleep 1; done
  curl -fs localhost:8123/ping >/dev/null || { echo "[clickhouse] error: not ready; see $cache/clickhouse.log" >&2; exit 1; }
fi
echo "[clickhouse] up on :8123"

if "$pg_bin/pg_isready" -h localhost -q; then
  echo "[postgres] already up"
else
  runuser -u postgres -- "$pg_bin/pg_ctl" -D "$pg_dir/data" -l "$pg_dir/postgres.log" -o "-k $pg_dir" start >/dev/null \
    || { echo "[postgres] error: failed to start; see $pg_dir/postgres.log" >&2; exit 1; }
fi
psql -h localhost -U aaiclick -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'aaiclick'" | grep -q 1 \
  || "$pg_bin/createdb" -h localhost -U aaiclick aaiclick
echo "[postgres] up on :5432"

# MinIO on :9100 (ClickHouse's TCP port holds :9000). Credentials match the
# env.sh exports; the bucket is created with curl's SigV4 since mc is gone.
if curl -fs localhost:9100/minio/health/live >/dev/null; then
  echo "[minio] already up"
else
  mkdir -p "$cache/minio/data"
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    nohup "$minio_bin" server "$cache/minio/data" --address :9100 --console-address :9101 \
    >"$cache/minio.log" 2>&1 &
  echo $! >"$cache/minio/minio.pid"
  disown
  for _ in $(seq 1 30); do curl -fs localhost:9100/minio/health/live >/dev/null && break; sleep 1; done
  curl -fs localhost:9100/minio/health/live >/dev/null || { echo "[minio] error: not ready; see $cache/minio.log" >&2; exit 1; }
fi
curl -fs -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin localhost:9100/aaiclick-test >/dev/null \
  || curl -fs --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin localhost:9100/aaiclick-test >/dev/null \
  || { echo "[minio] error: cannot create bucket aaiclick-test" >&2; exit 1; }
echo "[minio] up on :9100"

(cd "$here/.." && uv run --frozen --extra distributed python -m aaiclick migrate upgrade head >"$cache/migrate.log" 2>&1) \
  || { echo "[migrate] error: see $cache/migrate.log" >&2; exit 1; }
echo "[migrate] schema at head"
echo "[run] all services up — source claudeai/env.sh to point tests at them"
