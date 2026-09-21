#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=claudeai/_guard.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/claudeai/_guard.sh"

# Install pre-commit and register git hooks.
pip install --upgrade pre-commit
pre-commit install
