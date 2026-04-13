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

Two distinct concepts, kept separate throughout:

| Concept               | Type                | Scope               | Configured via                                                 |
|-----------------------|---------------------|---------------------|----------------------------------------------------------------|
| **Preservation mode** | Enum                | Global default + per-job override | Env var (default) or job submission parameter   |
| **Sampling strategy** | `dict[str, str]`    | Per-job only        | Job submission parameter — no env default                      |

The preservation mode picks the cleanup policy. The sampling strategy is a
payload consumed by the `STRATEGY` mode to target which rows get tracked.

## Tasks

1. **Define `SamplingStrategy` type**
   - Add `SamplingStrategy = dict[str, str]` type alias in `aaiclick/oplog/sampling.py`
   - Empty dict is the default — no targeting, `kwargs_aai_ids` / `result_aai_ids` stay empty
   - Document semantics: keys are fully-qualified table names, values are raw WHERE clauses evaluated in ClickHouse against the referenced table

2. **Define `PreservationMode` enum**
   - Add to `aaiclick/orchestration/models.py` alongside `JobStatus`, `TaskStatus`
   - Variants: `NONE` (default — persistent tables only survive), `FULL` (everything survives until TTL), `STRATEGY` (persistent + strategy-matched rows survive)
   - Add a `preservation_mode` column to the `Job` model (nullable, defaults to `NONE`)
   - Alembic migration via `alembic revision --autogenerate -m "add job preservation_mode and sampling_strategy"`

3. **Preservation mode — global default via env var**
   - New env var `AAICLICK_DEFAULT_PRESERVATION_MODE` with string values: `NONE` (default), `FULL`, `STRATEGY`
   - Parsed once at submission time in a single helper (`aaiclick/orchestration/config.py::get_default_preservation_mode()`)
   - Invalid value → raise with a clear error listing accepted values
   - No JSON, no dict — just a keyword

4. **Sampling strategy — per-job run parameter only**
   - Add `sampling_strategy` column to `Job` model — JSON-encoded (dict persisted as JSON in the DB), nullable
   - Accept `sampling_strategy: SamplingStrategy | None` in every job submission entry point: `registered_jobs.submit_job()`, `orchestration/cli.py` `run`/`submit` commands, `execution/debug.py` local runner
   - CLI flag: `--sampling-strategy '<json>'` or `--sampling-strategy-file <path>`
   - No env var. A default strategy across unrelated jobs does not make sense — strategies are question-specific
   - Validation at submission: when `preservation_mode=STRATEGY`, require a non-empty `sampling_strategy`; when mode is `NONE`/`FULL`, reject a non-empty strategy with a clear error

5. **Strategy-driven oplog sampling**
   - Replace `sample_lineage()` in `aaiclick/oplog/sampling.py` with `apply_strategy()`
   - New signature: `apply_strategy(ch_client, result_table, kwargs, strategy) -> (kwargs_aai_ids, result_aai_ids)`
   - When `strategy` has no entry matching `result_table` or any `kwargs.values()`, return `({}, [])`
   - When matched, translate the WHERE clause into a `SELECT aai_id FROM <table> WHERE <clause>` and align positions via the existing row-number join pattern
   - Delete `_pick_aai_ids()` — random fallback is gone

6. **Wire strategy through the lifecycle queue**
   - `OrchContext` (`aaiclick/orchestration/orch_context.py`) — load `Job.preservation_mode` and `Job.sampling_strategy` at task start and stash on the context (alongside `job_id`/`run_id`)
   - `oplog_record_sample()` — when mode is `STRATEGY`, read the strategy from context and attach it to `OplogPayload`; otherwise enqueue an OPLOG_RECORD (no sampling)
   - `_process_msg` OPLOG_SAMPLE branch — call `apply_strategy()` instead of `sample_lineage()`

7. **Cleanup behavior per preservation mode**
   - `BackgroundWorker._cleanup_unreferenced_tables` (`aaiclick/orchestration/background/background_worker.py`)
     - `NONE`: existing behavior — drop unpinned non-`p_` tables via `lineage_aware_drop()`
     - `FULL`: skip drop entirely while the job is alive; only `_cleanup_expired_jobs()` collects them at TTL
     - `STRATEGY`: drop but keep strategy-matched rows — `lineage_aware_drop()` already preserves `operation_log`-referenced ids, which is exactly the strategy output
   - `lineage_aware_drop()` — remove the `LIMIT 10` random fallback; when no lineage ids exist, just drop

8. **Remove dead code and config**
   - `backward_oplog_row()` and `RowLineageStep` from `aaiclick/oplog/lineage.py` — Phase 3 will reintroduce a strategy-aware replacement
   - Update `aaiclick/oplog/__init__.py` re-exports
   - `AAICLICK_OPLOG_SAMPLE_SIZE` env var from `sampling.py` and `docs/oplog.md`
   - `DEFAULT_FALLBACK_SAMPLE` from `cleanup.py`

9. **Tests**
   - `aaiclick/oplog/test_sampling.py` — `apply_strategy()` with matching / non-matching tables, multi-kwarg ops, empty strategy
   - `aaiclick/orchestration/test_preservation_mode.py` — submit jobs in each mode, verify which tables survive after completion
   - `aaiclick/orchestration/test_job_parameters.py` — submission honors `preservation_mode` and `sampling_strategy` args, env var fallback for mode, rejection of invalid env values, rejection of non-empty strategy under `NONE`/`FULL`, rejection of empty strategy under `STRATEGY`
   - Extend `aaiclick/oplog/test_collector.py` — assert `kwargs_aai_ids` / `result_aai_ids` stay empty under `NONE` mode
   - Delete `backward_oplog_row` tests

10. **Docs**
    - `docs/oplog.md` — drop `AAICLICK_OPLOG_SAMPLE_SIZE` row, describe `SamplingStrategy`, document `AAICLICK_DEFAULT_PRESERVATION_MODE`
    - `docs/data_context.md` — new section "Preservation Modes" with the three-mode table from `lineage_3_phases.md`
    - `docs/orchestration.md` — add `preservation_mode` and `sampling_strategy` to the job submission parameter reference

## Deliverables

- `SamplingStrategy` type alias exported from `aaiclick.oplog`
- `PreservationMode` enum (`NONE` / `FULL` / `STRATEGY`) exported from `aaiclick.orchestration`
- `Job` model carries both `preservation_mode` and `sampling_strategy` columns
- Job submission accepts both as run parameters
- `AAICLICK_DEFAULT_PRESERVATION_MODE` env-var default (keyword string)
- All random-sampling code paths removed
- Alembic migration applied locally and via `/generate-migration`
- Green test suite including new preservation-mode tests

## Success Criteria

- A job submitted with `preservation_mode=NONE` behaves exactly as today (persistent tables only survive)
- A job submitted with `preservation_mode=FULL` leaves every intermediate table in place until TTL
- A job submitted with `preservation_mode=STRATEGY, sampling_strategy={"t_foo": "x = 1"}` persists the strategy on the `Job` row and leaves only matching rows at cleanup time
- Setting `AAICLICK_DEFAULT_PRESERVATION_MODE=FULL` makes every job that doesn't pass an explicit mode run in `FULL`
- Invalid `AAICLICK_DEFAULT_PRESERVATION_MODE` raises at startup with a clear error
- Non-empty `sampling_strategy` without `preservation_mode=STRATEGY` is rejected at submission
- No references to `AAICLICK_OPLOG_SAMPLE_SIZE` remain
- No `grep` hits for `sample_lineage|_pick_aai_ids|backward_oplog_row|RowLineageStep`

---

# Phase 1 — Graph Traversal ✅ IMPLEMENTED

**Implementation**: `aaiclick/oplog/lineage.py` — see `backward_oplog()`,
`forward_oplog()`, `oplog_subgraph()`, `OplogGraph`

No action required. Phase 2 consumes `OplogGraph` directly.

---

# Phase 2 — Strategy-Producing Agent ✅ IMPLEMENTED

**Implementation**: `aaiclick/ai/agents/strategy_agent.py` — see `produce_strategy()`

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

# Phase 3a — Row Trace Primitives ✅ IMPLEMENTED

**Implementation**:
- `aaiclick/oplog/lineage.py` — `backward_oplog_row()`, `RowLineageStep`
- `aaiclick/orchestration/lineage.py` — `is_input_task()`
- `aaiclick/ai/agents/tools.py` — `trace_row` agent tool

**Objective**: Ship the row-level primitives so STRATEGY-mode jobs have
a usable trace end-to-end today, without waiting on the full replay
machinery.

## Shipped

1. **`is_input_task(task: Task) -> bool`** — pure function over the
   serialized result on the `Task` row. Returns `True` when the result
   is a persistent Object (`p_*` table with `persistent: true`). The
   foundation for Phase 3b's replay-skips-input-tasks logic.

2. **`backward_oplog_row(table, aai_id, max_depth=10)`** — walks one
   hop at a time through `operation_log.kwargs_aai_ids` /
   `result_aai_ids`. Returns `[]` when the job ran in NONE mode (empty
   arrays). Returns ordered `RowLineageStep`s otherwise.

3. **`trace_row` agent tool** — exposes `backward_oplog_row` to the LLM
   so `debug_result` can request row-level provenance for strategy-matched
   rows.

## Tests

- `aaiclick/orchestration/test_lineage.py` — `is_input_task` covering
  unrun task, persistent/ephemeral Object results, upstream refs, native
  values, pydantic results, missing `p_` prefix guards
- `aaiclick/oplog/test_graph.py` — `backward_oplog_row` end-to-end under
  STRATEGY mode and empty-return under NONE mode
- `aaiclick/ai/agents/test_tools.py` — `trace_row` formatter + strategy
  hint message

---

# Phase 3b — Task-Graph Replay

**Not yet implemented.** Complex enough to warrant its own PR.

**Objective**: Re-execute the job that produced a target table with a
`SamplingStrategy` attached, without re-running input tasks (whose
outputs are already persistent).

## Tasks

1. **Replay API** — `replay_job(original_job_id, sampling_strategy)`:
   - Loads the original job's task graph (tasks + dependencies)
   - Uses `is_input_task()` to split the graph into input tasks (skipped)
     and compute tasks (cloned)
   - Allocates fresh snowflake IDs for cloned tasks, maintains an
     `old_id → new_id` map
   - Rewrites cloned tasks' kwargs: `{"ref_type": "upstream", "task_id": X}`
     where `X` is an input task becomes a direct Object ref
     `{"object_type": "object", "table": "p_xxx", "persistent": true}`
   - Clones dependencies with remapped ids, dropping any edge that
     terminated on an input task
   - Submits as a new Job with `preservation_mode=STRATEGY`,
     `sampling_strategy=...`, and `replay_of=original_job_id`

2. **Alembic migration** — `jobs.replay_of` nullable FK to `jobs.id`.

3. **CLI** — `aaiclick replay <job_id> --sampling-strategy <json>` in
   `aaiclick/orchestration/cli.py`. Thin wrapper over `replay_job()`.

4. **`debug_agent` end-to-end flow** — question → `produce_strategy()`
   → `replay_job()` → `backward_oplog_row()` → explanation with row-level
   evidence. Today, debug_result already invokes the first two steps
   inline and surfaces `trace_row` as a tool; replay is the missing
   middle link that makes the trace populated on the first call.

5. **Tests** — `aaiclick/orchestration/test_replay.py`:
   - 3-task job (2 inputs + 1 compute) replayed with a strategy, assert
     new job's `kwargs_aai_ids` matches the strategy
   - Assert persistent input tables are untouched
   - Assert `jobs.replay_of` points at the original
   - End-to-end test in `test_debug_agent.py` with mocked provider

## Workaround until 3b ships

Users who want replay today can resubmit manually:

```bash
python -m aaiclick run-job <original_pipeline_entrypoint> \
  --preservation-mode STRATEGY \
  --sampling-strategy '{"p_kev_catalog": "cve_id = ''CVE-2024-001''"}'
```

This re-runs the entire pipeline (including input fetches) but produces
the full strategy-driven row trace. `backward_oplog_row()` on the new
job's target table returns the complete chain. Phase 3b just automates
this path and adds persistent-input reuse.

5. **Tests**
   - `aaiclick/orchestration/test_replay.py` — submit a 3-task job, replay with a strategy, assert the new job has populated `kwargs_aai_ids` matching the strategy and untouched persistent inputs
   - Input-task detection unit tests — persistent-only task, mixed persistent+ephemeral task, all-ephemeral task
   - End-to-end test in `aaiclick/ai/agents/test_debug_agent.py` using a mocked provider that returns a canned strategy

6. **Docs**
   - `docs/orchestration.md` — new "Replay" section describing `replay_job()` and input-task detection
   - `docs/data_context.md` — document input-task detection via persistent Objects
   - `docs/lineage_3_phases.md` — update Phase 3 + prerequisites to ✅ with implementation references

## Deliverables

- `replay_job(job_id, sampling_strategy)` API + CLI command
- Automatic input-task detection
- Strategy-aware `backward_oplog_row()`
- End-to-end debug agent flow: question → strategy → replay → row trace → explanation

## Success Criteria

- Replaying a job with `{"t_scores": "cvss < 0"}` produces an oplog where every relevant row's `kwargs_aai_ids` / `result_aai_ids` point at `cvss < 0` rows all the way back to the persistent input tables
- Input tasks are skipped during replay (no re-fetching of external data)
- `backward_oplog_row()` on a replayed target table returns a complete chain to an input table
- `docs/future.md` entry for "Lineage: Three-Phase Debugging" can be removed

---

# Phase 4 — Registered-Job Configuration Defaults ✅ IMPLEMENTED

**Implementation**:
- `aaiclick/orchestration/models.py` — `RegisteredJob.preservation_mode` + `RegisteredJob.sampling_strategy`
- `aaiclick/orchestration/factories.py` — see `resolve_job_config()`, `ResolvedJobConfig`
- `aaiclick/orchestration/registered_jobs.py` — see `register_job()`, `upsert_registered_job()`, `run_job()`, `_validate_registered_defaults()`
- `aaiclick/orchestration/cli.py` — see `register_job_cmd()`
- `aaiclick/orchestration/migrations/versions/8ec83b0c3148_add_preservation_defaults_to_registered_.py`

**Objective**: Hoist `preservation_mode` and `sampling_strategy` onto
`RegisteredJob` as job-definition defaults, so each run inherits from
the registered job unless the caller supplies an explicit override.
Mirrors the existing `RegisteredJob.default_kwargs` pattern.

## Tasks

1. **Model changes** — `aaiclick/orchestration/models.py`
   - Add `RegisteredJob.preservation_mode: PreservationMode` — nullable,
     defaults to `None` (meaning "inherit from env/hardcoded default")
   - Add `RegisteredJob.sampling_strategy: dict[str, str] | None` —
     nullable, JSON-encoded
   - Reuses the `preservationmode` ENUM type created by Phase 0's
     Alembic migration — no new enum needed

2. **Alembic migration** — `alembic revision --autogenerate -m "add registered_job preservation defaults"`
   - Adds both columns to `registered_jobs`
   - Idempotent upgrade/downgrade pair

3. **Precedence chain in `run_job()` / `create_job()`**
   - Extract a shared `resolve_job_config(explicit_mode, explicit_strategy, registered_job)` helper
   - Order of precedence:
     1. Explicit argument (non-`None`)
     2. `registered_job.preservation_mode` / `.sampling_strategy` (non-`None`)
     3. `get_default_preservation_mode()` (env var)
     4. `PreservationMode.NONE` + empty strategy
   - Cross-validation (strategy vs. STRATEGY mode) runs on the
     resolved values, not the raw inputs

4. **CLI** — `aaiclick/__main__.py` + `aaiclick/orchestration/cli.py`
   - `register-job` gains `--preservation-mode` and `--sampling-strategy`
     flags that set level-2 defaults at registration time
   - `run-job` flags stay unchanged — they're level-1 overrides

5. **`replay_job()` interaction**
   - `replay_job(original_job_id, sampling_strategy)` always supplies
     both params explicitly, so registered-job defaults never leak into
     a replay. This is the correct behavior — replay should use exactly
     the strategy the caller specified, never the job's baseline.

6. **Tests** — `aaiclick/orchestration/test_registered_jobs.py`
   - Register a job with `preservation_mode=FULL`, run without flags,
     assert resolved mode is `FULL`
   - Register a job with `preservation_mode=FULL`, run with
     `preservation_mode=NONE`, assert override wins
   - Env var set, no registered default, no explicit → env var wins
   - Default chain fallthrough: no registered, no env, no explicit →
     `NONE`
   - Strategy-only variants of the above for `sampling_strategy`
   - Invariant: `preservation_mode=STRATEGY` requires a non-empty
     strategy at whichever level supplies it

7. **Docs**
   - `docs/orchestration.md` — "Registered Jobs" section expanded with
     the precedence chain and the new `register-job` flags
   - `docs/lineage_3_phases.md` — Phase 4 marked ✅ with implementation
     pointer

## Deliverables

- Two new columns on `registered_jobs` with matching Alembic migration
- `resolve_job_config` helper used by both `create_job` and `run_job`
- `register-job` CLI accepts both defaults
- Green tests covering the four-level precedence chain

## Success Criteria

- Scheduled runs inherit both params from the registered job without
  extra wiring
- A registered job with `preservation_mode=FULL` yields `FULL` runs
  on every trigger path (cron, manual, API) unless overridden
- Manual replays via `replay_job()` or CLI `replay` are unaffected by
  the registered job's baseline

## Why it's the final phase

Every prior phase added *capability* (strategy typing, LLM agent, row
trace, replay). Phase 4 is pure configuration polish: where the knobs
live, not what they do. Landing it last means one migration reuses
the `preservationmode` ENUM from Phase 0 and the precedence logic
doesn't have to anticipate new levels.

---

# Phase Dependencies

| Phase    | Depends on     | Can ship independently? |
|----------|----------------|-------------------------|
| Phase 0  | —              | Yes                     |
| Phase 1  | —              | Already shipped         |
| Phase 2  | Phase 0        | Yes (strategy is informational until Phase 3) |
| Phase 3a | Phase 0        | Yes — works on any STRATEGY-mode job |
| Phase 3b | Phase 0, 2, 3a | No — needs the primitives + a replay target |
| Phase 4  | Phase 0        | Yes — pure config layer; orthogonal to 2/3a/3b |

Phase 0 is the unblocker: it removes the random-sampling assumption
and defines the `PreservationMode` + `SamplingStrategy` types every
later phase consumes. Ship order in practice: 0 → 2 → 3a → 3b → 4,
but 4 can slot in anywhere after 0 without blocking others.
