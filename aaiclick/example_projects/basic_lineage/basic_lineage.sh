#!/bin/bash
# AI lineage example: build a revenue pipeline, then explain it with an LLM
# served by NVIDIA NIM (via LiteLLM).
#
# Requires:
#   NVIDIA_NIM_API_KEY  — your NVIDIA API key (https://build.nvidia.com)
#
# Optional overrides:
#   AAICLICK_AI_MODEL   — LiteLLM model string
#                         (default: nvidia_nim/meta/llama-3.1-8b-instruct)
#   NVIDIA_NIM_API_BASE — custom NIM endpoint
#                         (default: https://integrate.api.nvidia.com/v1)
#
# Usage:
#   NVIDIA_NIM_API_KEY=nvapi-... ./basic_lineage.sh
#   AAICLICK_AI_MODEL=nvidia_nim/meta/llama-3.1-70b-instruct \
#       NVIDIA_NIM_API_KEY=nvapi-... ./basic_lineage.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-uv run python}"

if [ -z "${NVIDIA_NIM_API_KEY:-}" ]; then
    echo "error: NVIDIA_NIM_API_KEY is not set." >&2
    echo "       Get a key at https://build.nvidia.com and export it before running." >&2
    exit 1
fi

export AAICLICK_AI_MODEL="${AAICLICK_AI_MODEL:-nvidia_nim/meta/llama-3.1-8b-instruct}"

# LiteLLM reads NVIDIA_NIM_API_KEY directly; mirror it into AAICLICK_AI_API_KEY
# so AIProvider's explicit `api_key=` path also forwards it.
export AAICLICK_AI_API_KEY="${AAICLICK_AI_API_KEY:-$NVIDIA_NIM_API_KEY}"

$PYTHON -m basic_lineage
