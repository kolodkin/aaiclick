#!/usr/bin/env bash
set -euo pipefail

# Install pre-commit and register git hooks.
pip install --upgrade pre-commit
pre-commit install
