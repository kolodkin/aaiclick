AI Layer
---

Optional AI-powered lineage querying and debugging (`pip install aaiclick[ai]`); the core package works identically without it.

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
    ├── provider.py           # AIProvider — thin wrapper around litellm
    ├── config.py             # Configuration from env vars
    └── agents/
        ├── __init__.py
        ├── lineage_agent.py  # Backward/forward lineage + explanation
        ├── debug_agent.py    # Tier 1 "why" loop over lineage_tools
        ├── lineage_tools.py  # Graph-scoped Tier 1 toolbox
        ├── tools.py          # Generic table-inspection tools
        └── prompts.py        # Shared prompt fragments
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

async def explain_lineage(
    target_table: str,
    question: str | None = None,
    graph: OplogGraph | None = None,
) -> str:
    """Trace and explain how target_table was produced.

    Single-shot LLM call. Context is purely structural — the operation
    graph, rendered SQL templates, and table schemas. No row samples:
    partial data invited the model to invent narratives about values
    it couldn't see. For value-level questions use ``debug_result()``
    instead, which hands the agent live-query tools.
    """
```

Use `explain_lineage()` for *"how was this produced?"* (structure).
Use `debug_result()` for *"why does this row look wrong?"* (values).

## Debug Agent

```python
# aaiclick/ai/agents/debug_agent.py

async def debug_result(
    target_table: str,
    question: str,
    graph: OplogGraph | None = None,
    max_iterations: int = 10,
) -> str:
    """Tier 1 lineage debug loop. Answers 'why' questions by reading the
    backward oplog graph of `target_table` and issuing tool calls against
    the tables in that graph.

    Examples:
        "Why is this value negative?"
        "Why are there only 3 rows instead of 10?"
        "Which input caused the NaN values?"
    """
```

**Implementation**: `aaiclick/ai/agents/debug_agent.py` — see `debug_result()`.
The loop stops when the model emits an answer without tool calls, or after
`max_iterations` rounds (default 10), at which point the model is prompted
once for a final explanation.

## Agent Tools

Two toolsets live under `aaiclick/ai/agents/`:

- `tools.py` — generic table-inspection tools (used by legacy callers).
- `lineage_tools.py` — Tier 1 graph-scoped tools used by `debug_result()`.
  `explain_lineage()` does not invoke tools; it passes structural context
  to the LLM in a single shot.

### Lineage Tools (Tier 1)

Instantiate `LineageToolbox(graph)` once per debug session. Every tool is
scoped to the tables in `graph` — a query against a table outside the graph
returns `ToolError("out_of_scope", ...)` rather than leaking across jobs.

| Tool                | Parameters                   | Returns                                                        |
|---------------------|------------------------------|----------------------------------------------------------------|
| `list_graph_nodes`  | —                            | `[GraphNode]` with kind (input/intermediate/target) + liveness |
| `get_op_sql`        | `table`                      | Rendered SQL template for the op that produced `table`         |
| `get_schema`        | `table`                      | `TableSchema` with columns and types                           |
| `query_table`       | `sql`, `row_limit=100`       | `QueryResult` — read-only SELECT, auto-LIMIT, scope-checked    |

Safety rails on `query_table`:

- Rejects anything other than `SELECT` / `WITH ... SELECT`
- Rejects tables outside the current graph's node set
- Wraps in `LIMIT row_limit + 1` when no `LIMIT` is given so truncation is reported
- Pins `max_execution_time` and `max_result_rows` on every query so an accidental scan cannot tie up the cluster

Tool results use typed `NamedTuple`s (`QueryResult`, `TableSchema`,
`GraphNode`) or a `ToolError(kind, message)`. The agent sees formatted
strings; the `ToolError` `kind` discriminator (`not_select`,
`out_of_scope`, `not_found`, `not_live`) tells it whether to retry or
escalate.

### Generic Tools

Used by `explain_lineage()` and legacy callers — see `aaiclick/ai/agents/tools.py`:

| Tool               | Parameters                          | Returns                                                     |
|--------------------|-------------------------------------|-------------------------------------------------------------|
| `sample_table`     | `table`, `limit=10`, `where=None`   | Formatted rows as text                                      |
| `get_schema`       | `table`                             | Column names and types                                      |
| `get_column_stats` | `table`                             | count, non_null, min, max for every column                  |
| `trace_upstream`   | `table`, `depth=10`                 | Upstream operation graph as text                            |

---

# Graceful Degradation

```python
# aaiclick core — surfaced in aaiclick/__init__.py

async def explain(target_table: str, question: str | None = None) -> str:
    try:
        from aaiclick.ai.agents.lineage_agent import explain_lineage
    except ImportError:
        raise ImportError("AI features require aaiclick[ai]: pip install aaiclick[ai]")
    return await explain_lineage(target_table, question)
```
