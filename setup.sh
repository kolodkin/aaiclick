#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=claudeai/_guard.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/claudeai/_guard.sh"

# Install pre-commit and register git hooks.
pip install --upgrade pre-commit
pre-commit install

# Chromium for test_e2e/web/, matching the Playwright locked in uv.lock.
# No-op when already installed.
uv run --frozen --extra dev playwright install chromium

# Distributed-backend services for the web environment; started per session by
# claudeai/run.sh (see .claude/skills/claudeai-run).
"$(dirname "${BASH_SOURCE[0]}")"/claudeai/setup_clickhouse.sh
"$(dirname "${BASH_SOURCE[0]}")"/claudeai/setup_postgres.sh
