#!/bin/bash
# AI lineage example: build a revenue pipeline, then explain it with an LLM.
#
# Requires: ollama running with a model (default: llama3.2:3b)
#
# Usage: ./basic_lineage.sh
#        AAICLICK_AI_MODEL=ollama/llama3.1:8b ./basic_lineage.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

export AAICLICK_AI_MODEL="${AAICLICK_AI_MODEL:-ollama/llama3.2:3b}"

$PYTHON -m basic_lineage
