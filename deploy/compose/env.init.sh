#!/bin/sh
# Write .env from .env.template: every KEY=change-me-... line gets a random value.
set -eu
cd "$(dirname "$0")"
[ -e .env ] && { echo ".env already exists; delete it to regenerate" >&2; exit 1; }
umask 077
trap 'rm -f .env.tmp' EXIT
while IFS= read -r line; do
  case $line in
    [A-Z]*=change-me-*)
      # A plain assignment, so set -e aborts if openssl fails instead of writing an empty secret.
      value=$(openssl rand -hex 24)
      printf '%s=%s\n' "${line%%=*}" "$value" ;;
    *) printf '%s\n' "$line" ;;
  esac
done < .env.template > .env.tmp
mv .env.tmp .env
echo "Wrote .env (keep it out of git)"
grep '^AAICLICK_ADMIN_PASSWORD=' .env | sed 's/^AAICLICK_ADMIN_PASSWORD=/Admin login: admin \/ /'
