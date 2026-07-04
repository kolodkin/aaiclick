Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline. Builds a
`prices * quantities + bonus` computation, traces its full backward and forward
lineage graphs, then uses an LLM to explain how the result was produced. Runs a
local Ollama model (`ollama/llama3.1:8b`) by default — the script pulls it via
`aaiclick setup --ai` when an Ollama server is running. Set `AAICLICK_AI_MODEL`
and `AAICLICK_AI_API_KEY` to use a remote model instead; without any AI backend
the example prints the lineage graphs and skips the LLM explanation.

```bash
./basic_lineage.sh
```
