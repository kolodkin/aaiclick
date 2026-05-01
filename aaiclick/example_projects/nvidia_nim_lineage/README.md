NVIDIA NIM Lineage
---

AI-powered lineage explanation for a revenue pipeline, served by an NVIDIA NIM
endpoint via LiteLLM. Builds a `prices * quantities + bonus` computation,
traces its full backward lineage graph, then asks a NIM-hosted Llama model to
explain how the result was produced. Requires an NVIDIA API key
(`NVIDIA_NIM_API_KEY`); the default model is
`nvidia_nim/meta/llama-3.1-8b-instruct` on `https://integrate.api.nvidia.com/v1`.

```bash
NVIDIA_NIM_API_KEY=nvapi-... ./nvidia_nim_lineage.sh
```
