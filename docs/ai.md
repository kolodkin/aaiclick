AI Layer
---

Optional AI-powered lineage querying and debugging for aaiclick. Lives in `aaiclick/ai/` within the core package — installed via `pip install aaiclick[ai]`. The core `aaiclick` package works identically without it.

**Implementation**: `aaiclick/ai/` — see `AIProvider`, `get_ai_provider()`, `explain_lineage()`, `debug_result()`

**Depends on**: `docs/oplog.md` — the oplog module provides the provenance data that AI agents query.

---

# Design Principles

- **Oplog in core, AI outside** — operation logging has zero AI dependencies
- **Single provider via LiteLLM** — one interface for local (Ollama) and remote (Anthropic, OpenAI) models
- **User chooses model** — no hard local/remote split; any model for any query
- **AI agents are `@task` functions** — dogfood the orchestration engine
- **Opt-in** — `pip install aaiclick[ai]` pulls in LiteLLM; without it the package is unaffected

---

# Package Structure

```
aaiclick/
└── ai/
    ├── __init__.py
    ├── provider.py          # AIProvider — thin wrapper around litellm
    ├── config.py            # Configuration from env vars
    └── agents/
        ├── __init__.py
        ├── lineage_agent.py # Backward/forward lineage + explanation
        ├── debug_agent.py   # "Why" queries, anomaly root-cause
        └── tools.py         # Tools exposed to AI agents
```

**Installation**:

```bash
pip install aaiclick[ai]   # installs litellm alongside aaiclick
```

---

# AIProvider

```python
# aaiclick/ai/provider.py

class AIProvider:
    """Unified AI provider via LiteLLM. Works with any model string."""

    def __init__(self, model: str = "ollama/llama3.1:8b", api_key: str | None = None): ...

    async def query(self, prompt: str, context: str = "", system: str = "") -> str:
        """Single-turn query. Returns the model's text response."""

    async def query_with_tools(self, prompt: str, tools: list[dict], context: str = "") -> dict:
        """Single-round query with tool-calling. Returns {content, tool_calls, finish_reason}."""
```

# Configuration

```bash
AAICLICK_AI_MODEL=ollama/llama3.1:8b   # Default model (local-first)
AAICLICK_AI_API_KEY=...                 # Only needed for remote APIs
# Standard LiteLLM env vars also work (ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.)
```

```python
# aaiclick/ai/config.py
def get_ai_provider() -> AIProvider:
    model = os.environ.get("AAICLICK_AI_MODEL", "ollama/llama3.1:8b")
    return AIProvider(model=model)
```

---

# Agents

## Lineage Agent

```python
# aaiclick/ai/agents/lineage_agent.py

async def explain_lineage(target_table: str, question: str | None = None) -> str:
    """Trace and explain how target_table was produced.

    Calls backward_oplog(), samples each node, formats context for LLM.
    Can be called standalone or as a @task in a job.
    """
```

## Debug Agent

```python
# aaiclick/ai/agents/debug_agent.py

async def debug_result(target_table: str, question: str) -> str:
    """Answer 'why' questions about a result by tracing lineage
    and inspecting intermediate data.

    Examples:
        "Why is this value negative?"
        "Why are there only 3 rows instead of 10?"
        "Which input caused the NaN values?"
    """
```

## Agent Tools

Tools callable by the AI via tool-calling protocol — see `aaiclick/ai/agents/tools.py`:

| Tool             | Parameters                          | Returns                          |
|------------------|-------------------------------------|----------------------------------|
| `sample_table`   | `table`, `limit=10`, `where=None`   | Formatted rows as text           |
| `get_schema`     | `table`                             | Column names and types           |
| `get_stats`      | `table`, `column`                   | count, non_null, min, max        |
| `trace_upstream` | `table`, `depth=10`                 | Upstream operation graph as text |

---

# Graceful Degradation

```python
# aaiclick core — surfaced in aaiclick/__init__.py

async def explain(target_table: str, question: str | None = None) -> str:
    try:
        from aaiclick.ai.agents.lineage_agent import explain_lineage
    except ImportError:
        raise ImportError(
            "AI features require the aaiclick[ai] extra. "
            "Install with: pip install aaiclick[ai]"
        )
    return await explain_lineage(target_table, question)
```

---

# Historical Tables

After job cleanup, each table is replaced with a sample of rows referenced by oplog lineage — inputs and outputs of recorded operations (see `docs/oplog.md`). AI agents calling `sample_table()` on historical nodes transparently get the preserved sample.
