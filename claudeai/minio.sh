#!/usr/bin/env bash
# Build (from source — MinIO stopped publishing binaries in 2025) and start a
# throwaway MinIO server under .cache/minio for blob-storage tests and spikes.
# Idempotent: skips the build when the binary exists, restarts the server.
#
#   claudeai/minio.sh            # start; prints the env vars to export
#   claudeai/minio.sh stop
set -euo pipefail

# shellcheck source=claudeai/_guard.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guard.sh"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="$ROOT/.cache/minio"
BIN="$DIR/minio"
PIDFILE="$DIR/minio.pid"
MINIO_VERSION="${MINIO_VERSION:-latest}"
PORT="${MINIO_PORT:-9000}"
BUCKET="${MINIO_BUCKET:-aaiclick-test}"
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

stop() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    kill "$(cat "$PIDFILE")"
  fi
  rm -f "$PIDFILE"
}

if [ "${1:-}" = "stop" ]; then
  stop
  exit 0
fi

mkdir -p "$DIR/data"
if [ ! -x "$BIN" ]; then
  echo "[minio] building github.com/minio/minio@$MINIO_VERSION (a few minutes)…" >&2
  GOBIN="$DIR" GOFLAGS=-mod=mod go install "github.com/minio/minio@$MINIO_VERSION"
fi

stop
nohup "$BIN" server "$DIR/data" --address ":$PORT" --console-address ":$((PORT + 1))" \
  > "$DIR/minio.log" 2>&1 &
echo $! > "$PIDFILE"

for _ in $(seq 1 30); do
  curl -sf "http://localhost:$PORT/minio/health/live" >/dev/null && break
  sleep 1
done
curl -sf "http://localhost:$PORT/minio/health/live" >/dev/null || {
  echo "[minio] did not start; see $DIR/minio.log" >&2
  exit 1
}

# Create the bucket with SigV4 — mc is no longer distributed either.
curl -sf -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD" "http://localhost:$PORT/$BUCKET" \
  -o /dev/null -w "" || true

cat <<EOF
[minio] running on http://localhost:$PORT (pid $(cat "$PIDFILE")), log: $DIR/minio.log
export AAICLICK_BLOB_TEST_URL=http://localhost:$PORT/$BUCKET
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD
EOF
