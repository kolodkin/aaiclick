Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline. Builds a
`prices * quantities + bonus` computation, traces its full backward and forward
lineage graphs, then uses an LLM to explain how the result was produced. The AI
step runs only when `AAICLICK_AI_API_KEY` is set; without it, the example prints
the lineage graphs and skips the LLM explanation.

```bash
AAICLICK_AI_API_KEY=... ./basic_lineage.sh
```
