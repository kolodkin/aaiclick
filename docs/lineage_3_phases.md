Lineage: Three-Phase Debugging
---

Every lineage question reduces to this: How did this data get here?
Why is that data missing here?
The answer requires three phases, each building on the previous.

**Implementation plan**: `docs/lineage_3_phases_implementation_plan.md`

---

# Table Preservation Modes

Three run modes controlling which tables survive after a job completes.
Same task execution, different cleanup behavior.

| Mode         | What survives after job              | Use case                     |
|--------------|--------------------------------------|------------------------------|
| **None**     | Persistent tables only               | Production runs (as today)   |
| **Full**     | All tables                           | Development / debugging      |
| **Strategy** | Persistent + strategy-matched rows   | Lineage replay (Phase 2+3)  |

Preservation mode is a global default set via `AAICLICK_DEFAULT_PRESERVATION_MODE`
(keyword: `NONE` / `FULL` / `STRATEGY`), overridable per job at submission time.
The sampling strategy itself (`dict[str, str]`) is a separate per-job parameter —
no env default, since strategies are question-specific.

---

# Phase 0 -- Introduce Sampling Strategy

Replace random sampling with a `dict[str, str]` strategy interface — table
name to WHERE clause. The strategy determines which rows get sampled at each
table in the graph.

```python
# Strategy type: table name → WHERE clause
SamplingStrategy = dict[str, str]

# Default: empty — no targeting, columns stay empty
strategy: SamplingStrategy = {}

# Phase 2 output:
strategy = {
    "t_kev_catalog": "cve_id = 'CVE-2024-001'",
    "t_merged": "vendor IS NULL",
}
```

**Code to remove** (random sampling):

| File                   | What                                                         |
|------------------------|--------------------------------------------------------------|
| `oplog/lineage.py`     | `backward_oplog_row()`, `RowLineageStep`                     |
| `oplog/sampling.py`    | `sample_lineage()`, `_pick_aai_ids()` — random sampling helpers |
| `oplog/collector.py`   | sampling logic in `record()` that populates the two columns  |

**Config to remove**:

- `AAICLICK_OPLOG_SAMPLE_SIZE` env var

**Keep as-is**: `kwargs_aai_ids` / `result_aai_ids` columns (Phase 2 populates
them), `lineage_aware_drop()` (handles empty arrays), and all Phase 1 graph
queries. Cleanup is now job-driven via `_cleanup_expired_jobs()`.

---

# Phase 1 -- Graph (table scope)

Run `backward_oplog()` from the target table. Returns the DAG of tables,
operations, SQL templates, and task/job IDs. No row sampling needed.

**Answers**: which tables, which operations, which tasks, in what order.

**Available today**: `aaiclick/oplog/lineage.py` -- see `backward_oplog()`,
`forward_oplog()`, `OplogGraph`

---

# Phase 2 -- AI Agent Produces the Strategy ✅ IMPLEMENTED

**Implementation**: `aaiclick/ai/agents/strategy_agent.py` — see `produce_strategy()`

The AI agent takes a natural language question + the graph from Phase 1, and
outputs a `SamplingStrategy` (`dict[str, str]`).

| Question                             | Agent output                                                           |
|--------------------------------------|------------------------------------------------------------------------|
| Why does CVE-X have no KEV data?     | `{"t_kev_catalog": "cve_id = 'CVE-X'", "t_merged": "vendor IS NULL"}` |
| How come negative values in table T? | `{"t_scores": "cvss < 0", "t_raw_feed": "cvss < 0"}`                  |

The question *is* the pin — no pre-sampling needed. The agent validates
every emitted clause via a ClickHouse `LIMIT 0` dry run, retries once on
malformed JSON or unknown tables, and is wired into `debug_result()` so
questions automatically produce strategies alongside the debug context.

---

# Phase 3 -- Row Trace (replay scope)

Re-run the entire job with a `SamplingStrategy` applied. The strategy from
Phase 2 tells each operation which rows to track.

Phase 3 ships in two steps: Phase 3a provides the row trace primitives
(`is_input_task`, `backward_oplog_row`, `trace_row` agent tool) and Phase
3b provides the task-graph replay machinery (`replay_job`, `aaiclick
replay` CLI). 3a works end-to-end today for any job resubmitted with
`--preservation-mode STRATEGY`; 3b automates the resubmission with
persistent-input reuse.

## Phase 3a -- Row Trace Primitives ✅ IMPLEMENTED

**Implementation**:
- `aaiclick/oplog/lineage.py` — see `backward_oplog_row()`, `RowLineageStep`
- `aaiclick/orchestration/lineage.py` — see `is_input_task()`
- `aaiclick/ai/agents/tools.py` — see `trace_row` agent tool

Any job that ran under `PreservationMode.STRATEGY` populates
`kwargs_aai_ids` / `result_aai_ids` in `operation_log`. `backward_oplog_row`
walks one step at a time: given a `(table, aai_id)` it finds the operation
that produced that id, reads the positionally-aligned source ids, and
recurses. The debug agent exposes this as a `trace_row` tool so the LLM
can request row-level provenance for any strategy-matched row.

Input-task detection (`is_input_task`) is the foundation for Phase 3b:
given a `Task` row, return `True` if its result is a persistent Object
(`p_*` table). Used by replay to decide which tasks to skip.

## Phase 3b -- Task-Graph Replay

Not yet implemented. Clones a completed job's task graph, skips input
tasks (reusing their persistent outputs in place), rewrites child task
kwargs to point at the persistent tables directly, and submits the
clone as a new job with `preservation_mode=STRATEGY` and a `replay_of`
pointer to the original.

**Design sketch**:

```python
@task
async def fetch_kev_catalog(url: str) -> Object:
    return await create_object_from_value(data, name="kev_catalog")  # → p_kev_catalog

@task
async def merge_sources(kev: Object, scores: Object) -> Object:
    return await kev.concat(scores)  # → ephemeral table
```

Persistent tables survive cleanup, so replay always has its inputs.
The system walks the task graph backward and stops at input tasks.

Replay steps:

1. Re-run the entire job with the `SamplingStrategy` from Phase 2
2. At each operation, sample the targeted rows (not random)

After replay, the oplog contains a complete source-to-output trace for the
strategy-matched rows.

Workaround until Phase 3b lands: resubmit the original pipeline manually
with `run_job(..., preservation_mode=STRATEGY, sampling_strategy=...)` or
the `--preservation-mode STRATEGY --sampling-strategy '<json>'` CLI flags
from Phase 0.

---

# Phase 4 -- Registered-Job Configuration Defaults

Hoist `preservation_mode` and `sampling_strategy` onto `RegisteredJob` as
job-definition defaults, following the same pattern `default_kwargs`
already uses. Each run inherits the job-level config unless explicitly
overridden at submission time.

## Precedence chain

For both `preservation_mode` and `sampling_strategy`:

```
1. Explicit run_job(...) / replay_job() argument   ← wins
2. RegisteredJob.preservation_mode                   ← job-definition baseline
3. AAICLICK_DEFAULT_PRESERVATION_MODE env var        ← global default
4. PreservationMode.NONE                             ← hardcoded fallback
```

Same chain for `sampling_strategy`, with `None` collapsing to the next
level.

## Use cases

- **`preservation_mode` as job-level config**: mark a dev pipeline
  "always FULL" or a production pipeline "always NONE" once at
  registration time. Scheduled runs inherit automatically; manual runs
  can still override via CLI/API.
- **`sampling_strategy` as job-level config**: useful for monitoring /
  QA pipelines that always want to track specific canary rows
  (`{"p_feed": "is_canary = 1"}`). Most jobs leave this null.
- **Replay**: `replay_job()` always supplies both params explicitly, so
  it's a level-1 override and the replayed job is unaffected by
  whatever the registered job's baseline says.

## Why it's the final phase

Every prior phase added *capability* (strategy typing, LLM agent, row
trace, replay). Phase 4 is pure configuration polish: it doesn't
change what the system does, only where its knobs live. Landing it
last means `RegisteredJob` only needs one migration adding both
columns (and the `preservationmode` ENUM type already exists from
Phase 0).

---

# Current State

| Phase    | Status              | Notes                                                          |
|----------|---------------------|----------------------------------------------------------------|
| Phase 0  | ✅ Implemented       | `SamplingStrategy` + `PreservationMode` + strategy-driven oplog |
| Phase 1  | ✅ Implemented       | `backward_oplog()`, `forward_oplog()`, `OplogGraph`            |
| Phase 2  | ✅ Implemented       | `produce_strategy()` — question + graph → `SamplingStrategy`   |
| Phase 3a | ✅ Implemented       | `backward_oplog_row()`, `is_input_task()`, `trace_row` agent tool |
| Phase 3b | Not yet implemented | `replay_job()` + `aaiclick replay` CLI — auto-resubmit with persistent-input reuse |
| Phase 4  | Not yet implemented | `RegisteredJob.preservation_mode` + `RegisteredJob.sampling_strategy` + precedence chain |

## Prerequisites for Phase 3

| Prerequisite               | Status              | Notes                                              |
|----------------------------|---------------------|----------------------------------------------------|
| Scoped replay (row subset) | Not yet implemented | Re-run tasks on targeted `aai_id`s only            |

## Documentation Updates

Each phase should update the relevant docs as it lands:

| Phase    | Docs to update                                                          |
|----------|-------------------------------------------------------------------------|
| Phase 0  | `docs/oplog.md` — remove `AAICLICK_OPLOG_SAMPLE_SIZE`, add strategy interface |
| Phase 0  | `docs/data_context.md` — document preservation modes (none/full/strategy) |
| Phase 1  | Already documented in `docs/oplog.md`                                   |
| Phase 2  | `docs/ai.md` — add strategy agent to agent tools table                  |
| Phase 3a | `docs/oplog.md` — document `backward_oplog_row` + `trace_row` tool       |
| Phase 3b | `docs/orchestration.md` — document replay mechanism                     |
| Phase 3b | `docs/data_context.md` — document input task detection via persistent Objects |
| Phase 4  | `docs/orchestration.md` — document registered-job config precedence     |
