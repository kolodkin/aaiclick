#!/usr/bin/env bash
# Movie Plot RAG — local mode: runs the whole job in this process on
# embedded chdb + SQLite under ~/.aaiclick. No servers, no workers.
#
# Generation needs an LLM: by default a local Ollama server with
# llama3.1:8b pulled, or AAICLICK_AI_MODEL + AAICLICK_AI_API_KEY for a
# hosted model.
#
# Usage: ./movie_plot_rag.sh [--params '{"corpus_size": 500, "top_k": 5}']
set -euo pipefail
cd "$(dirname "$0")"
PYTHON="${PYTHON:-uv run python}"

unset AAICLICK_CH_URL AAICLICK_SQL_URL
$PYTHON -m aaiclick setup >/dev/null
$PYTHON -m movie_plot_rag "$@"
