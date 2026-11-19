#!/usr/bin/env bash
# Ensure a local MinIO (S3) server is running on :9100 with the bucket
# aaiclick-test. Reuses one already up; otherwise builds MinIO from source into
# .cache/minio/ (once, ~2 min — MinIO stopped publishing binaries, Docker
# images and mc in 2025) and starts it. `stop` stops it. Safe to rerun.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

cache="$here/../.cache"
dir="$cache/minio"
pidfile="$dir/minio.pid"
up() { curl -fs localhost:9100/minio/health/live >/dev/null; }

if [ "${1:-}" = "stop" ]; then
  if [ -f "$pidfile" ] && pid="$(cat "$pidfile")" && kill "$pid" 2>/dev/null; then
    # Wait for the exit, so a start right after does not see the dying server as up.
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  rm -f "$pidfile"
  echo "[minio] stopped"
  exit 0
fi

# :9100 because ClickHouse's TCP port holds :9000. Credentials match env.sh.
if up; then
  echo "[minio] already up on :9100"
else
  mkdir -p "$dir/data"
  if [ ! -x "$dir/minio" ]; then
    # Last tag before MinIO went source-only; bump deliberately, like the CI
    # clickhouse-server / postgres images in .github/workflows/_test-reusable.yaml.
    version="${MINIO_VERSION:-RELEASE.2025-10-15T17-29-55Z}"
    echo "[minio] building github.com/minio/minio@$version (a few minutes)…"
    GOBIN="$(cd "$dir" && pwd)" GOFLAGS=-mod=mod go install "github.com/minio/minio@$version"
    echo "[minio] installed $("$dir/minio" --version | head -1)"
  fi
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    nohup "$dir/minio" server "$dir/data" --address :9100 --console-address :9101 \
    >"$cache/minio.log" 2>&1 &
  echo $! >"$pidfile"
  disown
  for _ in $(seq 1 30); do up && break; sleep 1; done
  up || { echo "[minio] error: not ready; see $cache/minio.log" >&2; exit 1; }
  echo "[minio] up on :9100"
fi
# The bucket is created with curl's SigV4 since mc is gone.
curl -fs -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin localhost:9100/aaiclick-test >/dev/null \
  || curl -fs --aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin localhost:9100/aaiclick-test >/dev/null \
  || { echo "[minio] error: cannot create bucket aaiclick-test" >&2; exit 1; }
