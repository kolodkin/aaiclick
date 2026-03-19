AI Layer Implementation Plan
---

**Specs**: `docs/oplog.md` (oplog core), `docs/ai.md` (AI package)

---

## Phase 1: Oplog Core (`aaiclick/oplog/`) ✅ IMPLEMENTED

**Objective**: Capture data operation provenance in core with zero AI dependencies.

**Implementation**: `aaiclick/oplog/` — see `init_oplog_tables()`, `OplogCollector`, `backward_oplog()`, `forward_oplog()`, `oplog_subgraph()`

### Tasks

1. **Create module structure** ✅
   - `aaiclick/oplog/__init__.py`
   - `aaiclick/oplog/models.py` — DDL constants and `init_oplog_tables()`
   - `aaiclick/oplog/collector.py` — `OplogCollector` event sink
   - `aaiclick/oplog/lineage.py` — oplog graph queries

2. **Define ClickHouse DDL** ✅ (`models.py`)
   - `operation_log` — Snowflake ID PK, fields for operation/args/kwargs/sql_template/task_id/job_id/created_at
   - `table_registry` — `table_name → job_id` mapping for cleanup worker
   - `init_oplog_tables()` — `CREATE TABLE IF NOT EXISTS` + schema validation on context startup

3. **Implement OplogCollector** ✅ (`collector.py`)
   - Buffer-based: collects events in memory, batch-flushes to DB
   - ContextVar-based: `_oplog_collector: ContextVar[OplogCollector | None]`
   - `get_oplog_collector()` — returns current collector or None
   - `record_table()` — populates `table_registry` buffer only

4. **Opt-in via data_context** ✅ (`data_context.py`)
   - `oplog: bool | OplogCollector = False`
   - `True` → creates plain `OplogCollector()`
   - `OplogCollector` instance → uses it directly (orchestration passes pre-configured collector with task_id/job_id)
   - Flush on clean exit only; discard buffer on exception

5. **Instrument data operations** ✅
   - `data_context.create_object()` → `record_table()` only (registry, not operation_log — avoids double-counting)
   - `data_context.create_object_from_value()` → record `"create_from_value"`
   - `operators._apply_operator_db()` → record operator name, kwargs={"left": ..., "right": ...}
   - `operators._apply_aggregation()` → record aggregation name, kwargs={"source": ...}
   - `ingest.concat_objects_db()` → record `"concat"`, args=[source tables]
   - `ingest.insert_objects_db()` → record `"insert"`, kwargs={"source": ..., "target": ...}
   - `object.Object.copy()` → record `"copy"`, kwargs={"source": self.table}

6. **Implement oplog graph queries** ✅ (`lineage.py`)
   - `backward_oplog()`, `forward_oplog()` — iterative BFS (no `WITH RECURSIVE`)
   - `oplog_subgraph()` → `OplogGraph`
   - `OplogGraph.to_prompt_context()` — text formatter for LLM consumption

7. **Tests** ✅
   - `aaiclick/oplog/test_collector.py` — 13 tests: buffering, flushing, all instrumentation points
   - `aaiclick/oplog/test_graph.py` — 11 tests: backward/forward traversal, edge cases

### Notes

- `ORDER BY created_at` — nullable `job_id` cannot be in MergeTree sorting key
- chdb returns `Map(String, String)` as list of `(key, value)` tuples; `_to_dict()` normalises both formats
- `create_object()` only writes to `table_registry` to avoid double-counting with higher-level operations

---

## Phase 2: AI Sub-package (`aaiclick/ai/`) ⚠️ NOT YET IMPLEMENTED

**Objective**: Optional sub-package providing LLM-powered lineage queries and debugging.
Installed via `pip install aaiclick[ai]` — lives inside the main `aaiclick` package.

**Spec**: `docs/ai.md`

### Tasks

1. **Create sub-package structure** — `aaiclick/ai/` with `provider.py`, `config.py`, `agents/`

2. **Add `ai` extras group to `pyproject.toml`** — `litellm>=1.0`

3. **Implement AIProvider** (`provider.py`)
   - `query(prompt, context, system)` → `str`
   - `query_with_tools(prompt, tools, context)` → `dict`

4. **Implement config** (`config.py`) — `get_ai_provider()` reads `AAICLICK_AI_MODEL` (default: `ollama/llama3.1:8b`)

5. **Implement lineage agent** (`agents/lineage_agent.py`) — `explain_lineage(target_table, question)`

6. **Implement debug agent** (`agents/debug_agent.py`) — `debug_result(target_table, question)`

7. **Implement agent tools** (`agents/tools.py`) — `sample_table`, `get_schema`, `get_stats`, `trace_upstream`

8. **Add `explain()` to `aaiclick/__init__.py`** — graceful degradation wrapper

9. **Tests** — mock `litellm.acompletion`, test context formatting and tool dispatch

10. **CI/CD** — extend publish workflow to build and release `aaiclick` (with AI extras) on `v*` tag

### Deliverables
- `pip install aaiclick[ai]` works with any LiteLLM-supported model
- Package released automatically on `v*` tag push

---

## Phase 3: Orchestration Integration ⚠️ NOT YET IMPLEMENTED

**Objective**: Automatic oplog capture during job execution + AI agents as tasks + table cleanup worker.

### Tasks

1. **Wire OplogCollector into execute_task()**
   - Orchestration always creates `OplogCollector(task_id=task.id, job_id=job.id)`
   - Passes it as `data_context(oplog=collector)` — no Job-level flag needed
   - Collector auto-flushes on context exit

2. **Background cleanup worker** — on job completion (COMPLETED / FAILED / CANCELLED):
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
   - Persistent tables (`p_` prefix) excluded — no `job_id` in registry

3. **AI agents as @task wrappers** — lazy import, participates in normal DAG dependencies

4. **Integration tests** — job execution → verify `operation_log` populated; cleanup → verify sample tables

### Deliverables
- Zero-config oplog for all jobs (always-on in orchestration context)
- AI agents composable with regular tasks in job DAGs
- Post-job table sampling preserves lineage-accessible data

---

## Phase 4: Examples & Documentation ⚠️ NOT YET IMPLEMENTED

### Tasks

1. **Example**: interactive lineage exploration with `data_context(oplog=True)` + local Ollama
2. **Example**: job pipeline with AI debugging task at the end
3. **Update docs** — reference `docs/oplog.md` and `docs/ai.md` from main docs
4. **Add `ai` optional dependency group** in `pyproject.toml` — enables `pip install aaiclick[ai]`

---

## Phase 5: Pinned Row Sampling ⚠️ FUTURE

Allow user-defined predicates that ensure matching rows always survive cleanup. Phase 1 preserves an arbitrary 10 rows; Phase 5 lets you guarantee semantically important rows are included.

```python
pin_rows("my_table", where="value < 5")
```

Rules are WHERE clause predicates registered during task execution (before job completion triggers cleanup). Cleanup prioritises matching rows, fills remainder up to 10 with arbitrary rows.
