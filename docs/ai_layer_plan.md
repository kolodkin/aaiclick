# AI Layer Implementation Plan

**Specification**: `docs/ai_layer_spec.md`

---

## Phase 1: Lineage Core (`aaiclick/lineage/`) ✅ IMPLEMENTED

**Objective**: Capture data operation provenance in core with zero AI dependencies.

**Implementation**:
- `aaiclick/lineage/models.py` — `OperationLog` SQLModel
- `aaiclick/lineage/collector.py` — `LineageCollector` with ContextVar, buffered flush
- `aaiclick/lineage/graph.py` — `backward_explain()`, `forward_impact()`, `LineageContext`
- `aaiclick/lineage/__init__.py` — public exports
- `aaiclick/data/data_context.py` — `lineage=True` parameter, instruments `create_object`/`create_object_from_value`
- `aaiclick/data/operators.py` — instruments `_apply_operator_db`, `_apply_aggregation`
- `aaiclick/data/ingest.py` — instruments `concat_objects_db`, `insert_objects_db`, `copy_db`
- `aaiclick/lineage/test_collector.py` — 14 tests for collector and instrumentation
- `aaiclick/lineage/test_graph.py` — 6 tests for graph traversal

**Note**: Alembic migration for `operation_log` table is deferred — the table is created inline via `SQLModel.metadata.create_all()` during tests. Migration will be added when the orchestration DB schema stabilizes.

---

## Phase 2: AI Package (`aaiclick-ai/`) ⚠️ NOT YET IMPLEMENTED

**Objective**: Separate package providing LLM-powered lineage queries and debugging.

### Tasks

1. **Create package structure**
   ```
   aaiclick-ai/
   ├── pyproject.toml
   └── aaiclick_ai/
       ├── __init__.py
       ├── provider.py
       ├── config.py
       └── agents/
           ├── __init__.py
           ├── lineage_agent.py
           ├── debug_agent.py
           └── tools.py
   ```

2. **pyproject.toml**
   - Dependencies: `litellm>=1.0`, `aaiclick`
   - No Ollama dependency needed (LiteLLM handles it natively via `ollama/` prefix)

3. **Implement AIProvider** (`provider.py`)
   - Single class wrapping `litellm.acompletion()`
   - `query(prompt, context, system)` → `str`
   - `query_with_tools(prompt, tools, context)` → `dict`
   - Model string from constructor: `"ollama/llama3.1:8b"`, `"anthropic/claude-sonnet-4-6"`, etc.

4. **Implement config** (`config.py`)
   - `get_ai_provider()` reads `AAICLICK_AI_MODEL` env var
   - Default: `ollama/llama3.1:8b` (local-first)

5. **Implement lineage agent** (`agents/lineage_agent.py`)
   - `explain_lineage(target_table, question)` — trace + explain
   - Formats `LineageGraph.to_prompt_context()` as LLM input
   - Samples data from each node for concrete examples
   - System prompt tuned for lineage explanation

6. **Implement debug agent** (`agents/debug_agent.py`)
   - `debug_result(target_table, question)` — "why" queries
   - Fetches lineage graph + samples + schemas
   - System prompt tuned for root-cause analysis
   - Supports tool-calling for deeper inspection (sample more rows, check stats)

7. **Implement agent tools** (`agents/tools.py`)
   - `sample_table(table, limit, where)` — fetch sample rows
   - `get_schema(table)` — column names and types
   - `get_stats(table, column)` — min/max/count/nulls
   - `trace_upstream(table, depth)` — lineage subgraph

8. **Tests**
   - Mock `litellm.acompletion` — test agent logic without real LLM
   - Test context formatting (graph → prompt string)
   - Test tool dispatch

9. **Update CI/CD to release both packages on tag**
   - Extend `.github/workflows/publish.yaml` to build and publish `aaiclick-ai` alongside `aaiclick`
   - Add a `build-ai` job that runs `uv build` inside `aaiclick-ai/`
   - Upload `aaiclick-ai` dist artifacts separately (`dist-ai`)
   - Add a `publish-ai` job (depends on `build-ai` and `test-package`) that publishes `aaiclick-ai` to PyPI via trusted publisher
   - Both packages published on the same `v*` tag — `aaiclick-ai` uses the same version (via `setuptools-scm` or pinned to core version)
   - Ensure `aaiclick-ai` dependency on `aaiclick` uses `>=` constraint so the core package is published first (job ordering handles this)
   - Job dependency chain: `build` + `build-ai` → `test-package` → `publish` → `publish-ai`
     (publish-ai depends on publish so the core package is available on PyPI when aaiclick-ai installs)

### Deliverables
- `aaiclick-ai/` package installable via `pip install aaiclick-ai`
- Works with any LiteLLM-supported model (Ollama, Anthropic, OpenAI, etc.)
- Example: `AAICLICK_AI_MODEL=ollama/qwen2.5-coder:7b python -c "..."`
- Both `aaiclick` and `aaiclick-ai` released automatically on `v*` tag push

---

## Phase 3: Orchestration Integration ⚠️ NOT YET IMPLEMENTED

**Objective**: Automatic lineage capture during job execution + AI agents as tasks.

### Tasks

1. **Add lineage flag to Job model**
   - `Job.lineage_enabled: bool = False`
   - Alembic migration to add column

2. **Wire LineageCollector into execute_task()**
   - When `job.lineage_enabled`: create collector with `task_id` and `job_id`
   - Pass `lineage=True` to `data_context()` inside task execution
   - Collector auto-flushes on context exit

3. **AI agents as @task wrappers**
   ```python
   @task(name="explain_lineage")
   async def explain_lineage_task(target: Object, question: str) -> str:
       from aaiclick_ai.agents.lineage_agent import explain_lineage
       return await explain_lineage(target.table, question)
   ```
   - Lazy import: works only if aaiclick-ai is installed
   - Participates in normal DAG dependencies

4. **Integration tests**
   - Job with lineage=True → verify operation_log populated
   - AI task in DAG → verify explanation returned (mock LLM)

### Deliverables
- Zero-config lineage for jobs (just set `lineage_enabled=True`)
- AI agents composable with regular tasks in job DAGs

---

## Phase 4: Examples & Documentation ⚠️ NOT YET IMPLEMENTED

**Objective**: Show how to use lineage + AI features.

### Tasks

1. **Example: Interactive lineage exploration**
   - Script using `data_context(lineage=True)` + local Ollama
   - Build a small pipeline, then ask "explain this result"
   - Demonstrates fully local setup (no API keys needed)

2. **Example: Job pipeline with AI debugging**
   - Job with data tasks + `explain_lineage_task` at the end
   - Shows AI agents composing with regular tasks

3. **Update docs**
   - Add AI layer section to `docs/aaiclick.md`
   - Reference `docs/ai_layer_spec.md` for full specification

4. **Add `ai` optional dependency group**
   - In core `pyproject.toml`: `ai = ["aaiclick-ai"]`
   - Enables `pip install aaiclick[ai]`

### Deliverables
- Working examples runnable with Ollama locally
- Updated documentation
