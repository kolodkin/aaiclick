#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=claudeai/_guard.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/claudeai/_guard.sh"

# Install pre-commit and register git hooks.
pip install --upgrade pre-commit
pre-commit install

# Chromium for the browser e2e suite (test_e2e/web/). Playwright is locked in
# uv.lock, and `playwright install` fetches the build that version expects, so
# the browser can never drift from the package. No-op when already installed.
uv run --frozen --extra dev playwright install chromium
