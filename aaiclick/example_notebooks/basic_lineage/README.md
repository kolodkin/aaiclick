Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline, as a Colab-ready notebook. Builds a `prices * quantities + bonus` computation, traces its full backward and forward lineage graphs, then uses an LLM to explain how the result was produced and to answer a debugging question via an agentic tool loop. Set `AAICLICK_AI_MODEL` to any LiteLLM-compatible model string (default: `ollama/llama3.2:3b`).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kolodkin/aaiclick/blob/main/aaiclick/example_notebooks/basic_lineage/basic_lineage.ipynb)
