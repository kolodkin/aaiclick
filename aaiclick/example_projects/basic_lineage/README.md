Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline. Builds a
`prices * quantities + bonus` computation, traces its full backward lineage
graph, then uses an LLM to explain how the result was produced. Requires a
running Ollama server (default model: `llama3.2:3b`).

```bash
./basic_lineage.sh
```
