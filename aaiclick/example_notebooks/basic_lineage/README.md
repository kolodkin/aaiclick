Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline, as a Colab-ready notebook. Builds a `prices * quantities + bonus` computation, traces its full backward and forward lineage graphs, then uses an LLM to explain how the result was produced and to answer a debugging question via an agentic tool loop. Set `AAICLICK_AI_MODEL` to any LiteLLM-compatible model string (default: `ollama/llama3.2:3b`).

Open `basic_lineage.ipynb` in Google Colab or Jupyter and run all cells.
