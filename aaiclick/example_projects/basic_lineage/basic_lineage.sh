#!/bin/bash
# AI lineage example: build a revenue pipeline, then explain it with an LLM.
#
# Uses a local Ollama model (ollama/llama3.1:8b) by default — `aaiclick setup
# --ai` pulls it when an Ollama server is running. Set AAICLICK_AI_MODEL and
# AAICLICK_AI_API_KEY for a remote model instead (GitHub Actions uses NVIDIA
# NIM this way). Without either backend, the AI step is skipped.
#
# Usage: ./basic_lineage.sh
#        AAICLICK_AI_MODEL=nvidia_nim/meta/llama-3.3-70b-instruct AAICLICK_AI_API_KEY=... ./basic_lineage.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

export AAICLICK_AI_MODEL="${AAICLICK_AI_MODEL:-ollama/llama3.1:8b}"

$PYTHON -m aaiclick setup --ai
$PYTHON -m basic_lineage
