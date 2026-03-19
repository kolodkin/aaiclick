# AI Layer Implementation Plan

**Specification**: `docs/ai_layer_spec.md`

---

## Phase 1: Lineage Core (`aaiclick/oplog/`) ✅ IMPLEMENTED

**Objective**: Capture data operation provenance in core with zero AI dependencies.

**Implementation**: `aaiclick/oplog/` — see `init_oplog_tables()`, `OplogCollector`, `backward_oplog()`, `forward_oplog()`, `oplog_subgraph()`

### Tasks

1. **Create module structure** ✅
   - `aaiclick/oplog/__init__.py`
   - `aaiclick/oplog/models.py` — DDL constants and `init_oplog_tables()`
   - `aaiclick/oplog/collector.py` — `OplogCollector` event sink
   - `aaiclick/oplog/graph.py` — lineage graph queries

2. **Define ClickHouse DDL constants** ✅ (`models.py`)
   - Snowflake ID primary key (`DEFAULT generateSnowflakeID()`)
   - Fields: `result_table`, `operation`, `args` (`Array(String)`), `kwargs` (`Map(String, String)`), `sql_template`, `task_id`, `job_id`, `created_at`
   - `table_registry` table for cleanup worker (populated by `OplogCollector`)
   - `init_oplog_tables(ch_client)` — `CREATE TABLE IF NOT EXISTS` on context startup
   - Schema validation on startup to detect stale/mismatched tables

3. **Implement OplogCollector** ✅ (`collector.py`)
   - Buffer-based: collects events in memory, flushes to DB
   - ContextVar-based: `_oplog_collector: ContextVar[OplogCollector | None]`
   - Helper: `get_oplog_collector()` returns current or None
   - `record_table()` populates `table_registry` buffer (one entry per new table)

4. **Opt-in via data_context** ✅ (`data_context.py`)
   - Added `oplog: bool = False`, `task_id`, `job_id` parameters to `data_context()`
   - When True: create `OplogCollector`, store in ContextVar
   - On clean exit only (no exception): call `collector.flush()`
   - On failure: discard buffer to avoid partial oplog

5. **Instrument data operations** ✅
   - `data_context.create_object()` → `record_table()` only (table registry, not operation_log)
   - `data_context.create_object_from_value()` → record `"create_from_value"`
   - `operators._apply_operator_db()` → record operator name, kwargs={"left": ..., "right": ...}
   - `operators._apply_aggregation()` → record aggregation name, kwargs={"source": ...}
   - `ingest.concat_objects_db()` → record `"concat"`, args=[source tables]
   - `ingest.insert_objects_db()` → record `"insert"`, kwargs={"source": ..., "target": ...}
   - `object.Object.copy()` → record `"copy"`, kwargs={"source": self.table}

6. **Implement oplog graph queries** ✅ (`graph.py`)
   - `backward_oplog(table, ch_client, max_depth)` — iterative BFS upstream trace
   - `forward_oplog(table, ch_client, max_depth)` — iterative BFS downstream trace
   - `oplog_subgraph(table, ch_client, direction, max_depth)` → `OplogGraph`
   - `OplogGraph.to_prompt_context()` — text formatter for LLM consumption

7. **Tests** ✅
   - `aaiclick/oplog/test_collector.py` — 13 tests covering buffering, flushing, all instrumentation points
   - `aaiclick/oplog/test_graph.py` — 11 tests covering backward/forward traversal and edge cases

### Notes

- `ORDER BY created_at` (not `job_id, created_at`) — nullable columns cannot be in MergeTree sorting key
- chdb returns `Map(String, String)` as list of `(key, value)` tuples; `_to_dict()` normalises both formats
- `create_object()` only writes to `table_registry` (not `operation_log`) to avoid double-counting with higher-level operations

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
   - Formats `OplogGraph.to_prompt_context()` as LLM input
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

**Objective**: Automatic lineage capture during job execution + AI agents as tasks + table cleanup worker.

### Tasks

1. **Add oplog flag to Job model**
   - `Job.oplog_enabled: bool = False`
   - Alembic migration to add column

2. **Wire OplogCollector into execute_task()**
   - When `job.oplog_enabled`: create collector with `task_id` and `job_id`
   - Pass `oplog=True` to `data_context()` inside task execution
   - Collector auto-flushes on context exit

3. **Background cleanup worker** (deferred from Phase 1 — needs `job_id` from orchestration)
   - On job completion (COMPLETED / FAILED / CANCELLED), replace each ephemeral table with a 10-row sample:
     ```python
     result = await ch_client.query(
         "SELECT table_name FROM table_registry WHERE job_id = {job_id:UInt64}"
     )
     for (table,) in result.result_rows:
         await ch_client.command(f"CREATE TABLE {table}_sample AS {table}")
         await ch_client.command(f"INSERT INTO {table}_sample SELECT * FROM {table} LIMIT 10")
         await ch_client.command(f"DROP TABLE {table}")
         await ch_client.command(f"RENAME TABLE {table}_sample TO {table}")
     await ch_client.command("DELETE FROM table_registry WHERE job_id = {job_id:UInt64}")
     ```
   - `CREATE TABLE new AS source` copies ENGINE, ORDER BY, codecs — confirmed working in chdb
   - Persistent tables (`p_` prefix) excluded (no `job_id` in registry)

4. **AI agents as @task wrappers**
   ```python
   @task(name="explain_lineage")
   async def explain_lineage_task(target: Object, question: str) -> str:
       from aaiclick_ai.agents.lineage_agent import explain_lineage
       return await explain_lineage(target.table, question)
   ```
   - Lazy import: works only if aaiclick-ai is installed
   - Participates in normal DAG dependencies

5. **Integration tests**
   - Job with oplog=True → verify operation_log populated
   - AI task in DAG → verify explanation returned (mock LLM)

### Deliverables
- Zero-config lineage for jobs (just set `oplog_enabled=True`)
- AI agents composable with regular tasks in job DAGs
- Post-job table sampling preserves lineage-accessible data without storage bloat

---

## Phase 4: Examples & Documentation ⚠️ NOT YET IMPLEMENTED

**Objective**: Show how to use lineage + AI features.

### Tasks

1. **Example: Interactive lineage exploration**
   - Script using `data_context(oplog=True)` + local Ollama
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
