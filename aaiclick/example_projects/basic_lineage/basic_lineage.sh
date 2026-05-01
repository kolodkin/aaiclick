#!/bin/bash
# AI lineage example: build a revenue pipeline, then explain it with an LLM.
#
# Uses NVIDIA NIM (via LiteLLM) by default. AAICLICK_AI_API_KEY is read from
# the environment (already provided in GitHub Actions).
#
# Usage: ./basic_lineage.sh
#        AAICLICK_AI_MODEL=nvidia_nim/meta/llama-3.1-70b-instruct ./basic_lineage.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

export AAICLICK_AI_MODEL="${AAICLICK_AI_MODEL:-nvidia_nim/meta/llama-3.1-8b-instruct}"

$PYTHON -m basic_lineage
