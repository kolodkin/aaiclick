#!/usr/bin/env bash
# Build the `aaiclick` meta wheel + sdist pinned to aaiclick-core==VERSION.
# Usage: meta/build.sh VERSION OUT_DIR
set -euo pipefail
version="$1"
out="$(realpath -m "$2")"
here="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cp "$here/pyproject.toml" "$work/"
cp "$here/../README.md" "$work/"
sed -i "s/0\.0\.0/${version}/g" "$work/pyproject.toml"
uv build "$work" --out-dir "$out"

wheel="$(ls "$out"/aaiclick-"${version}"-*.whl)"
unzip -p "$wheel" "aaiclick-${version}.dist-info/METADATA" \
  | grep -qxF "Requires-Dist: aaiclick-core[all]==${version}" \
  || { echo "meta wheel $wheel does not pin aaiclick-core[all]==${version}" >&2; exit 1; }
