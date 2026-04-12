Lineage: Three-Phase Debugging — Implementation Plan
---

Companion to `docs/lineage_3_phases.md`. Breaks the feature into sequential
phases with concrete tasks, deliverables, and success criteria. Each phase is
independently shippable.

Phase 1 is already in place (`backward_oplog()`, `forward_oplog()`,
`OplogGraph`) so the plan starts at Phase 0 and ends at Phase 3.

---

# Phase 0 — `SamplingStrategy` + Preservation Modes

**Objective**: Replace random oplog sampling with a strategy-driven interface,
and give jobs a three-way run mode that controls cleanup behavior.

## Tasks

1. **Define `SamplingStrategy` type**
   - Add `SamplingStrategy = dict[str, str]` type alias in `aaiclick/oplog/sampling.py`
   - Empty dict is the default — no targeting, `kwargs_aai_ids` / `result_aai_ids` stay empty
   - Document semantics: keys are fully-qualified table names, values are raw WHERE clauses evaluated in ClickHouse against the referenced table

2. **Define `PreservationMode` enum**
   - Add to `aaiclick/orchestration/models.py` alongside `JobStatus`, `TaskStatus`
   - Variants: `NORMAL`, `FULL`, `STRATEGY`
   - Thread through `Job` model as a column (nullable → defaults to `NORMAL`)
   - Alembic migration via `alembic revision --autogenerate -m "add job preservation_mode and strategy"`

3. **Strategy as a job run parameter**
   - Add `strategy: SamplingStrategy` column to `Job` model — JSON-encoded, nullable
   - Accept `strategy` (and `preservation_mode`) in every job submission entry point: `registered_jobs.submit_job()`, `orchestration/cli.py` `run`/`submit` commands, `execution/debug.py` local runner
   - CLI flag: `--strategy '<json>'` or `--strategy-file <path>`
   - When omitted, fall back to the env var `AAICLICK_DEFAULT_SAMPLING_STRATEGY` (JSON string) parsed once at submission time; if that's also unset, default is `{}`
   - Same fallback logic for `preservation_mode` via `AAICLICK_DEFAULT_PRESERVATION_MODE` (values: `normal`/`full`/`strategy`); if strategy is non-empty and mode is unset, auto-select `STRATEGY`
   - Centralize the env-var parsing in `aaiclick/orchestration/config.py` (new module or existing config file) so submission sites call one helper

4. **Strategy-driven oplog sampling**
   - Replace `sample_lineage()` in `aaiclick/oplog/sampling.py` with `apply_strategy()`
   - New signature: `apply_strategy(ch_client, result_table, kwargs, strategy) -> (kwargs_aai_ids, result_aai_ids)`
   - When `strategy` has no entry matching `result_table` or any `kwargs.values()`, return `({}, [])`
   - When matched, translate the WHERE clause into a `SELECT aai_id FROM <table> WHERE <clause>` and align positions via the existing row-number join pattern
   - Delete `_pick_aai_ids()` — random fallback is gone

5. **Wire strategy through the lifecycle queue**
   - `OrchContext` (`aaiclick/orchestration/orch_context.py`) — load `Job.strategy` at task start and stash it on the context (alongside `job_id`/`run_id`)
   - `oplog_record_sample()` — read the context's strategy and attach it to `OplogPayload`
   - `_process_msg` OPLOG_SAMPLE branch — call `apply_strategy()` instead of `sample_lineage()`
   - When preservation mode is `NORMAL` or `FULL`, skip sampling entirely (pass empty dicts)

6. **Cleanup behavior per mode**
   - `BackgroundWorker._cleanup_unreferenced_tables` (`aaiclick/orchestration/background/background_worker.py`)
     - `NORMAL`: existing behavior — drop unpinned non-`p_` tables via `lineage_aware_drop()`
     - `FULL`: skip drop entirely while the job is alive; only `_cleanup_expired_jobs()` collects them at TTL
     - `STRATEGY`: drop but keep strategy-matched rows — `lineage_aware_drop()` already preserves `operation_log`-referenced ids, which is exactly the strategy output
   - `lineage_aware_drop()` — remove the `LIMIT 10` random fallback; when no lineage ids exist, just drop

7. **Remove dead code and config**
   - `backward_oplog_row()` and `RowLineageStep` from `aaiclick/oplog/lineage.py` — Phase 3 will reintroduce a strategy-aware replacement
   - Update `aaiclick/oplog/__init__.py` re-exports
   - `AAICLICK_OPLOG_SAMPLE_SIZE` env var from `sampling.py` and `docs/oplog.md`
   - `DEFAULT_FALLBACK_SAMPLE` from `cleanup.py`

8. **Tests**
   - `aaiclick/oplog/test_sampling.py` — `apply_strategy()` with matching / non-matching tables, multi-kwarg ops, empty strategy
   - `aaiclick/orchestration/test_preservation_mode.py` — submit jobs in each mode, verify which tables survive after completion
   - `aaiclick/orchestration/test_job_parameters.py` — submission honors `strategy` / `preservation_mode` args, falls back to env vars, and env vars parse correctly (valid JSON, invalid JSON rejected, unknown mode rejected)
   - Extend `aaiclick/oplog/test_collector.py` — assert `kwargs_aai_ids` / `result_aai_ids` stay empty under `NORMAL` mode
   - Delete `backward_oplog_row` tests

9. **Docs**
   - `docs/oplog.md` — drop `AAICLICK_OPLOG_SAMPLE_SIZE` row, describe `SamplingStrategy`, document `AAICLICK_DEFAULT_SAMPLING_STRATEGY` and `AAICLICK_DEFAULT_PRESERVATION_MODE`
   - `docs/data_context.md` — new section "Preservation Modes" with the three-mode table from `lineage_3_phases.md`
   - `docs/orchestration.md` — add `strategy` / `preservation_mode` to the job submission parameter reference

## Deliverables

- `SamplingStrategy` type alias exported from `aaiclick.oplog`
- `PreservationMode` enum exported from `aaiclick.orchestration`
- Job submission accepts `strategy` and `preservation_mode` as run parameters, persisted on `Job`
- `AAICLICK_DEFAULT_SAMPLING_STRATEGY` and `AAICLICK_DEFAULT_PRESERVATION_MODE` env-var defaults
- All random-sampling code paths removed
- Alembic migration applied locally and via `/generate-migration`
- Green test suite including new preservation-mode tests

## Success Criteria

- A job submitted with `preservation_mode=NORMAL` behaves exactly as today (persistent tables only survive)
- A job submitted with `preservation_mode=FULL` leaves every intermediate table in place until TTL
- A job submitted with `preservation_mode=STRATEGY` leaves only strategy-matched rows
- `submit_job(..., strategy={"t_foo": "x = 1"})` persists the strategy on the `Job` row and drives sampling at every OPLOG_SAMPLE call
- Setting `AAICLICK_DEFAULT_SAMPLING_STRATEGY='{"t_foo": "x = 1"}'` applies to every job that doesn't pass an explicit strategy
- No references to `AAICLICK_OPLOG_SAMPLE_SIZE` remain
- No `grep` hits for `sample_lineage|_pick_aai_ids|backward_oplog_row|RowLineageStep`

---

# Phase 1 — Graph Traversal ✅ IMPLEMENTED

**Implementation**: `aaiclick/oplog/lineage.py` — see `backward_oplog()`,
`forward_oplog()`, `oplog_subgraph()`, `OplogGraph`

No action required. Phase 2 consumes `OplogGraph` directly.

---

# Phase 2 — Strategy-Producing Agent

**Objective**: Given a natural-language question + an `OplogGraph`, the agent
emits a `SamplingStrategy` that targets the rows relevant to the question.

## Tasks

1. **New agent entry point**
   - Add `aaiclick/ai/agents/strategy_agent.py` with `async def produce_strategy(question, graph) -> SamplingStrategy`
   - Reuses `get_ai_provider()` from `aaiclick/ai/config.py`
   - Reuses `get_schemas_for_nodes()` from `aaiclick/ai/agents/tools.py` to include column types in the prompt (no sample data — the agent only emits WHERE clauses)

2. **Prompt design**
   - Add `STRATEGY_SYSTEM_PROMPT` to `aaiclick/ai/agents/prompts.py`
   - Few-shot examples matching the table in `lineage_3_phases.md` ("Why does CVE-X have no KEV data?" → `{"t_kev_catalog": "cve_id = 'CVE-X'", ...}`)
   - Require the model to emit strict JSON: `{"<table>": "<where-clause>", ...}`
   - Reject clauses referencing columns not present in the node schema

3. **Output validation**
   - Parse model JSON, validate keys exist in `graph.nodes`, reject on malformed output
   - Optional: do a `SELECT count() FROM <table> WHERE <clause> LIMIT 1` dry-run per entry to catch syntax errors before Phase 3 runs — skip on failure and let the agent retry
   - On validation failure, feed the error back to the provider for one retry

4. **Wire into existing agents**
   - `debug_agent.py` and `lineage_agent.py` — when a question is supplied, call `produce_strategy()` first and include the resulting strategy in the explanation context
   - At this phase the strategy is informational only; Phase 3 is what actually re-executes

5. **Tests**
   - `aaiclick/ai/agents/test_strategy_agent.py` — mocked `AIProvider` covering: well-formed JSON, malformed JSON retry, unknown-table rejection, empty-graph edge case
   - Live smoke test gated behind the existing live-test marker used by `test_lineage_agent_live.py`

6. **Docs**
   - `docs/ai.md` — add `produce_strategy` to the agent tools table with input/output schema
   - `docs/lineage_3_phases.md` — update Phase 2 status to ✅ and add implementation reference

## Deliverables

- `produce_strategy()` function callable from user code and from `debug_agent`
- Mocked + live tests
- Updated `ai.md`

## Success Criteria

- Given a real `OplogGraph` and the sample questions in the spec, the agent returns a valid `SamplingStrategy` that passes dry-run validation
- All strategy keys are tables in the graph
- Strategy survives a round-trip through `json.dumps`/`json.loads`

---

# Phase 3 — Strategy-Driven Replay

**Objective**: Re-execute the job that produced a target table with a
`SamplingStrategy` attached, so the oplog accumulates a complete
source-to-output row trace for the targeted rows.

## Tasks

1. **Input task detection**
   - Extend `Task` metadata (or derive at runtime) with `is_input: bool`
   - A task qualifies when all its returned Objects resolve to persistent tables (`p_` prefix) — check via `table_registry` lookup in `aaiclick/orchestration/execution/runner.py` result post-processing
   - No user annotation required — detection is automatic

2. **Replay API — thin wrapper over job submission**
   - Phase 0 already made `strategy` and `preservation_mode` first-class job run parameters, so replay is just a resubmission with those fields populated
   - Add `replay_job(job_id, strategy)` to `aaiclick/orchestration/registered_jobs.py` that:
     1. Loads the original job's task graph
     2. Walks backward from terminal tasks, stopping at input tasks (their outputs are persistent — reuse in place)
     3. Calls the normal `submit_job()` path with `strategy=strategy`, `preservation_mode=STRATEGY`, and a `replay_of` pointer to the original
   - Alembic migration for `jobs.replay_of` nullable foreign key

3. **Row-trace query**
   - Reintroduce `backward_oplog_row(table, aai_id)` in `aaiclick/oplog/lineage.py` — now strategy-aware, walking the non-empty `kwargs_aai_ids` / `result_aai_ids` populated by replay
   - Return a `RowLineageStep` chain ending at input tables

4. **CLI / agent integration**
   - `aaiclick replay <job_id> --strategy <json>` command in `aaiclick/orchestration/cli.py` (thin wrapper over the existing `submit` command's `--strategy` flag from Phase 0)
   - `debug_agent` end-to-end: question → `produce_strategy()` → `replay_job()` → `backward_oplog_row()` → explanation with row-level evidence

5. **Tests**
   - `aaiclick/orchestration/test_replay.py` — submit a 3-task job, replay with a strategy, assert the new job has populated `kwargs_aai_ids` matching the strategy and untouched persistent inputs
   - Input-task detection unit tests — persistent-only task, mixed persistent+ephemeral task, all-ephemeral task
   - End-to-end test in `aaiclick/ai/agents/test_debug_agent.py` using a mocked provider that returns a canned strategy

6. **Docs**
   - `docs/orchestration.md` — new "Replay" section describing `replay_job()` and input-task detection
   - `docs/data_context.md` — document input-task detection via persistent Objects
   - `docs/lineage_3_phases.md` — update Phase 3 + prerequisites to ✅ with implementation references

## Deliverables

- `replay_job(job_id, strategy)` API + CLI command
- Automatic input-task detection
- Strategy-aware `backward_oplog_row()`
- End-to-end debug agent flow: question → strategy → replay → row trace → explanation

## Success Criteria

- Replaying a job with `{"t_scores": "cvss < 0"}` produces an oplog where every relevant row's `kwargs_aai_ids` / `result_aai_ids` point at `cvss < 0` rows all the way back to the persistent input tables
- Input tasks are skipped during replay (no re-fetching of external data)
- `backward_oplog_row()` on a replayed target table returns a complete chain to an input table
- `docs/future.md` entry for "Lineage: Three-Phase Debugging" can be removed

---

# Phase Dependencies

| Phase   | Depends on | Can ship independently? |
|---------|------------|-------------------------|
| Phase 0 | —          | Yes                     |
| Phase 1 | —          | Already shipped         |
| Phase 2 | Phase 0    | Yes (strategy is informational until Phase 3) |
| Phase 3 | Phase 0, 2 | No — needs both         |

Phase 0 is the unblocker: it removes the random-sampling assumption and lets
the other phases slot in cleanly. Ship Phase 0 and Phase 2 before tackling
Phase 3's replay machinery.
