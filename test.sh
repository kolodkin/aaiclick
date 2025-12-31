#!/bin/bash
# Wrapper script to run tests with automatic ClickHouse setup

set -e

# Run the setup and test script
uv run python scripts/setup_and_test.py "$@"
