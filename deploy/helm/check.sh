#!/usr/bin/env bash
# Render the chart several ways and assert on the output: lint, generated
# credentials, existingSecret, external database URLs, image tags.
set -euo pipefail
CHART="$(cd "$(dirname "$0")" && pwd)/aaiclick"

render() { helm template t "$CHART" "$@"; }
has() { grep -qF -- "$2" <<<"$1" || { echo "FAIL [$3]: missing: $2" >&2; exit 1; }; }
lacks() { if grep -qF -- "$2" <<<"$1"; then echo "FAIL [$3]: unexpected: $2" >&2; exit 1; fi; }

helm lint "$CHART" --set devDependencies.enabled=true

out=$(render --set devDependencies.enabled=true)
for s in "name: t-credentials" "jwt-secret:" "admin-password:" "pg-password:" "ch-password:" "sql-url:" "ch-url:" \
         "helm.sh/resource-policy: keep" "secretKeyRef:" "checksum/credentials:" "image: \"ghcr.io/kolodkin/aaiclick:0.0.0\""; do
  has "$out" "$s" generated
done
lacks "$out" "change-me" generated
grep -A1 -- "- name: AAICLICK_JWT_SECRET" <<<"$out" | grep -q "value:" && { echo "FAIL [generated]: inline JWT value" >&2; exit 1; }

out=$(render --set devDependencies.enabled=true --set auth.existingSecret=mine)
lacks "$out" "name: t-credentials" existingSecret
has "$out" "name: mine" existingSecret

out=$(render --set env.sqlUrl=postgresql+asyncpg://u:p@db/x --set env.chUrl=clickhouse://u:p@ch/x)
lacks "$out" "pg-password:" externalDb
lacks "$out" "ch-password:" externalDb
has "$out" "sql-url: $(printf %s 'postgresql+asyncpg://u:p@db/x' | base64 | tr -d '\n')" externalDb

out=$(render --set devDependencies.enabled=true --set images.server.tag=rc1)
has "$out" "image: \"ghcr.io/kolodkin/aaiclick:rc1\"" imageTag

# No database configured at all: fail the render, don't ship an empty URL.
if err=$(render 2>&1); then echo "FAIL [noDatabase]: rendered without any database" >&2; exit 1; fi
has "$err" "env.sqlUrl" noDatabase

# The checksum hashes inputs, not random output: two fresh renders agree.
sum() { render --set devDependencies.enabled=true -s templates/server.yaml | grep "checksum/credentials"; }
[ "$(sum)" = "$(sum)" ] || { echo "FAIL [checksum]: differs between identical renders" >&2; exit 1; }

echo "chart checks passed"
