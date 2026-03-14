# AI Layer Specification: Lineage & Conversational Debugging

## Overview

Add an AI-powered lineage tracking and conversational debugging layer to aaiclick. The system captures data operation provenance in core, and provides an optional AI module for natural language querying of dataflow history.

### Design Principles

- **Lineage in core, AI outside** — operation logging has zero AI dependencies
- **Single provider via LiteLLM** — one interface for local (Ollama) and remote (Anthropic, OpenAI) models
- **User chooses model** — no hard local/remote split; any model for any query
- **AI agents are `@task` functions** — dogfood the orchestration engine
- **Opt-in everywhere** — aaiclick works identically without the AI package installed

---

## Part 1: Lineage Capture (Core — `aaiclick/lineage/`)

### Gap Analysis

**Currently captured** (orchestration layer):
- Task→Task dependencies via `Dependency` table
- Upstream task references in `Task.kwargs` JSON
- Task results with Object table references
- Dynamic task registration with parent→child deps

**Currently missing** (data layer):
- No record of which Objects produced a new Object
- No operation type stored (add, concat, copy, etc.)
- SQL built dynamically and discarded after execution
- View source reference lost on serialization
- No Object→Task mapping

### 1.1 OperationLog Model

Stored in the orchestration SQL database (SQLite/PostgreSQL), same as Task/Job.

```python
# aaiclick/lineage/models.py

class OperationLog(SQLModel, table=True):
    __tablename__ = "operation_log"

    id: int              # Snowflake ID (PK)
    result_table: str    # Table created or modified
    operation: str       # "create", "create_from_value", "add", "sub", "mul",
                         # "div", "concat", "insert", "copy", "min", "max",
                         # "sum", "mean", "std", "eq", "ne", "lt", "gt", ...
    source_tables: str   # JSON list of input table names: '["t_123", "t_456"]'
    sql_template: str | None  # SQL executed (with table names, no data)
    task_id: int | None  # Which task produced this (nullable for interactive use)
    job_id: int | None   # Job scope (nullable for interactive use)
    created_at: datetime
```

Index on `result_table` for backward lineage queries. Index on `source_tables` (or GIN on PostgreSQL) for forward lineage queries.

### 1.2 LineageCollector

A lightweight event sink, injected into DataContext via context variable.

```python
# aaiclick/lineage/collector.py

class LineageCollector:
    """Collects operation events during a data_context session."""

    def __init__(self, task_id: int | None = None, job_id: int | None = None):
        self._buffer: list[OperationLog] = []
        self.task_id = task_id
        self.job_id = job_id

    def record(self, result_table: str, operation: str,
               source_tables: list[str], sql: str | None = None) -> None:
        """Buffer an operation event."""
        ...

    async def flush(self) -> None:
        """Write buffered events to database."""
        ...
```

Activation via `data_context(lineage=True)` — stores collector in a ContextVar. When `lineage=False` (default), no collector exists, no overhead.

### 1.3 Instrumentation Points

Emit `collector.record(...)` calls at these locations:

| Location                          | Operation          | Source Tables                  |
|-----------------------------------|--------------------|--------------------------------|
| `data_context.create_object()`    | `"create"`         | `[]`                           |
| `data_context.create_object_from_value()` | `"create_from_value"` | `[]`                 |
| `operators._apply_operator_db()`  | `"add"`, `"sub"`, etc. | `[left.table, right.table]` |
| `operators._apply_agg_db()`       | `"sum"`, `"mean"`, etc. | `[source.table]`           |
| `ingest.concat_objects_db()`      | `"concat"`         | `[s.table for s in sources]`   |
| `ingest.insert_objects_db()`      | `"insert"`         | `[source.table]`               |
| `ingest.copy_db()`                | `"copy"`           | `[source.table]`               |
| `object.Object.copy()`            | `"copy"`           | `[self.table]`                 |

Each instrumentation is a 2-line addition: get collector from ContextVar, call `record()` if not None.

### 1.4 Lineage Graph Queries (Pure SQL)

```python
# aaiclick/lineage/graph.py

async def backward_lineage(table: str, max_depth: int = 10) -> list[OperationLog]:
    """Trace all upstream operations that produced `table`."""
    # Recursive: find op where result_table=table, then recurse on source_tables
    ...

async def forward_lineage(table: str, max_depth: int = 10) -> list[OperationLog]:
    """Trace all downstream operations that consumed `table`."""
    # Find ops where table appears in source_tables, recurse on result_table
    ...

async def lineage_subgraph(table: str, direction: str = "backward",
                           max_depth: int = 10) -> LineageGraph:
    """Return structured graph (nodes + edges) for visualization or AI context."""
    ...

@dataclass
class LineageGraph:
    nodes: list[LineageNode]  # Tables with metadata
    edges: list[LineageEdge]  # Operations connecting tables

    def to_prompt_context(self) -> str:
        """Format graph as text for LLM consumption."""
        ...
```

### 1.5 Alembic Migration

Add `operation_log` table to the existing orchestration migration chain.

---

## Part 2: AI Module (Separate Package — `aaiclick-ai/`)

### 2.1 Package Structure

```
aaiclick-ai/
├── pyproject.toml           # deps: litellm, aaiclick
├── aaiclick_ai/
│   ├── __init__.py
│   ├── provider.py          # AIProvider — thin wrapper around litellm
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── lineage_agent.py # Backward/forward lineage + explanation
│   │   ├── debug_agent.py   # "Why" queries, anomaly root-cause
│   │   └── tools.py         # Tools exposed to AI agents
│   └── config.py            # Configuration from env vars
```

### 2.2 AIProvider (Single Class)

```python
# aaiclick_ai/provider.py

from litellm import acompletion

class AIProvider:
    """Unified AI provider via LiteLLM. Works with any model string."""

    def __init__(self, model: str = "ollama/llama3.1:8b"):
        self.model = model

    async def query(self, prompt: str, context: str = "",
                    system: str = "") -> str:
        """Single-turn query."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": f"{context}\n\n{prompt}"})
        response = await acompletion(model=self.model, messages=messages)
        return response.choices[0].message.content

    async def query_with_tools(self, prompt: str, tools: list[dict],
                                context: str = "") -> dict:
        """Query with tool-calling support for agents that need
        to fetch additional data (e.g., sample rows, schema info)."""
        ...
```

### 2.3 Configuration

```bash
# Environment variables
AAICLICK_AI_MODEL=ollama/llama3.1:8b       # Default model for all queries
AAICLICK_AI_API_KEY=...                     # Only needed for remote APIs
# Standard LiteLLM env vars also work (ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.)
```

```python
# aaiclick_ai/config.py

def get_ai_provider() -> AIProvider:
    model = os.environ.get("AAICLICK_AI_MODEL", "ollama/llama3.1:8b")
    return AIProvider(model=model)
```

### 2.4 Lineage Agent

```python
# aaiclick_ai/agents/lineage_agent.py

async def explain_lineage(target_table: str, question: str | None = None) -> str:
    """Trace and explain how target_table was produced.

    Can be called standalone or as a @task in a job.
    """
    graph = await backward_lineage(target_table)
    samples = await _sample_nodes(graph, limit=5)  # Fetch sample rows per table
    context = _format_lineage_context(graph, samples)

    provider = get_ai_provider()
    prompt = question or "Explain how this result was produced, step by step."
    return await provider.query(
        prompt=prompt,
        context=context,
        system=LINEAGE_SYSTEM_PROMPT,
    )
```

### 2.5 Debug Agent ("Why" Queries)

```python
# aaiclick_ai/agents/debug_agent.py

async def debug_result(target_table: str, question: str) -> str:
    """Answer 'why' questions about a result by tracing lineage
    and inspecting intermediate data.

    Examples:
        "Why is this value negative?"
        "Why are there only 3 rows instead of 10?"
        "Which input caused the NaN values?"
    """
    graph = await backward_lineage(target_table)
    samples = await _sample_nodes(graph, limit=10)
    schemas = await _get_schemas(graph)
    context = _format_debug_context(graph, samples, schemas)

    provider = get_ai_provider()
    return await provider.query(
        prompt=question,
        context=context,
        system=DEBUG_SYSTEM_PROMPT,
    )
```

### 2.6 Agent Tools

Tools the AI can call for deeper inspection (via tool-calling protocol):

```python
# aaiclick_ai/agents/tools.py

TOOLS = [
    {
        "name": "sample_table",
        "description": "Fetch sample rows from a ClickHouse table",
        "parameters": {"table": "str", "limit": "int", "where": "str | None"},
    },
    {
        "name": "get_schema",
        "description": "Get column names and types for a table",
        "parameters": {"table": "str"},
    },
    {
        "name": "get_stats",
        "description": "Get min/max/count/nulls for a table column",
        "parameters": {"table": "str", "column": "str"},
    },
    {
        "name": "trace_upstream",
        "description": "Get the lineage graph for a table",
        "parameters": {"table": "str", "depth": "int"},
    },
]
```

### 2.7 Graceful Degradation in Core

```python
# aaiclick/__init__.py or wherever AI features are surfaced

def explain(target_table: str, question: str | None = None) -> str:
    try:
        from aaiclick_ai.agents.lineage_agent import explain_lineage
    except ImportError:
        raise ImportError(
            "AI features require the aaiclick-ai package. "
            "Install with: pip install aaiclick-ai"
        )
    return explain_lineage(target_table, question)
```

---

## Part 3: Integration with Orchestration

### 3.1 Auto-Lineage in Task Execution

When a job runs with `lineage=True`, the worker injects a `LineageCollector` into each task's `data_context`:

```python
# In execution.py — execute_task()
async with data_context(lineage=job.lineage_enabled):
    # collector automatically created, task_id/job_id set
    result = await func(**deserialized_kwargs)
    # collector.flush() on context exit
```

This means **all object operations within a task are automatically logged** — no changes to user task code.

### 3.2 AI Agents as @task Functions

```python
from aaiclick.orchestration.decorators import task

@task(name="explain_lineage")
async def explain_lineage_task(target: Object, question: str) -> str:
    from aaiclick_ai.agents.lineage_agent import explain_lineage
    return await explain_lineage(target.table, question)

# Usage in a job:
@job("pipeline_with_debug")
async def my_pipeline():
    data = load_data(url="...")
    result = transform(data=data)
    explanation = explain_lineage_task(target=result, question="Summarize this pipeline")
    return [result, explanation]
```

---

## Implementation Plan

### Phase 1: Lineage Core (`aaiclick/lineage/`)

1. Create `aaiclick/lineage/` module with `__init__.py`
2. Define `OperationLog` model in `models.py`
3. Create Alembic migration for `operation_log` table
4. Implement `LineageCollector` in `collector.py`
5. Add `lineage` ContextVar and opt-in to `data_context()`
6. Instrument operators, ingest, and data_context creation functions
7. Implement `backward_lineage()`, `forward_lineage()`, `LineageGraph` in `graph.py`
8. Write tests: verify operations produce correct log entries, verify graph traversal

### Phase 2: AI Package (`aaiclick-ai/`)

1. Create `aaiclick-ai/` package with `pyproject.toml` (dep: `litellm>=1.0`)
2. Implement `AIProvider` wrapping `litellm.acompletion()`
3. Implement `config.py` with env-based provider construction
4. Implement `lineage_agent.py` — graph formatting + LLM query
5. Implement `debug_agent.py` — "why" queries with data sampling
6. Implement `tools.py` — table sampling, schema, stats tools
7. Write tests with mock provider (no real LLM needed for unit tests)
8. Add example script: interactive lineage exploration with Ollama

### Phase 3: Orchestration Integration

1. Add `lineage_enabled` flag to Job model (default False)
2. Wire LineageCollector into `execute_task()` when lineage enabled
3. Auto-flush collector on data_context exit
4. Create `@task` wrappers for AI agents
5. Write integration test: job with lineage → AI explanation

### Phase 4: Examples & Documentation

1. Example: interactive session with lineage + local Ollama model
2. Example: job pipeline with built-in AI debugging task
3. Update `docs/` with AI layer specification reference
4. Add `ai` optional dependency group to core `pyproject.toml`
