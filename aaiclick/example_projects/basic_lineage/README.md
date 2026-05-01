Basic Lineage
---

AI-powered lineage explanation for a revenue pipeline. Builds a
`prices * quantities + bonus` computation, traces its full backward lineage
graph, then uses an LLM served by NVIDIA NIM (via LiteLLM) to explain how the
result was produced. Requires an NVIDIA API key (`NVIDIA_NIM_API_KEY`); the
default model is `nvidia_nim/meta/llama-3.1-8b-instruct` on
`https://integrate.api.nvidia.com/v1`.

```bash
NVIDIA_NIM_API_KEY=nvapi-... ./basic_lineage.sh
```
