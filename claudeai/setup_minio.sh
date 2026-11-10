#!/usr/bin/env bash
# Build MinIO from source into .cache/minio/ (idempotent). MinIO stopped
# publishing binaries, Docker images and mc in 2025; `go install` takes ~2 min.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/_guard.sh"

# Last tag before MinIO went source-only; bump deliberately, like the CI
# clickhouse-server / postgres images in .github/workflows/_test-reusable.yaml.
version="${MINIO_VERSION:-RELEASE.2025-10-15T17-29-55Z}"
dir="$here/../.cache/minio"
if [ -x "$dir/minio" ]; then
  echo "[minio] binary present: $dir/minio"
  exit 0
fi
mkdir -p "$dir"
echo "[minio] building github.com/minio/minio@$version (a few minutes)…"
GOBIN="$(cd "$dir" && pwd)" GOFLAGS=-mod=mod go install "github.com/minio/minio@$version"
echo "[minio] installed $("$dir/minio" --version | head -1)"
