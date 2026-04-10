Lineage: Three-Phase Debugging
---

Every lineage question reduces to this: How did this data get here?
Why is that data missing here?
The answer requires three phases, each building on the previous.

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

# Phase 2 AI agent will produce:
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

**Keep as-is**:

- `kwargs_aai_ids` and `result_aai_ids` columns on `operation_log` — populated when strategy is provided
- `lineage_aware_drop()` in `cleanup.py` — handles empty arrays gracefully (falls back to random rows)
- `_cleanup_expired_samples()` — still needed for table lifecycle
- All Phase 1 graph queries — they only use table-level metadata

---

# Phase 1 -- Graph (table scope)

Run `backward_oplog()` from the target table. Returns the DAG of tables,
operations, SQL templates, and task/job IDs. No row sampling needed.

**Answers**: which tables, which operations, which tasks, in what order.

**Available today**: `aaiclick/oplog/lineage.py` -- see `backward_oplog()`,
`forward_oplog()`, `OplogGraph`

---

# Phase 2 -- AI Agent Produces the Strategy

The AI agent takes a natural language question + the graph from Phase 1, and
outputs a `SamplingStrategy` (`dict[str, str]`).

| Question                                 | Agent output                                                          |
|------------------------------------------|-----------------------------------------------------------------------|
| Why does CVE-X have no KEV data?         | `{"t_kev_catalog": "cve_id = 'CVE-X'", "t_merged": "vendor IS NULL"}` |
| Why no data before 12/04?                | `{"t_raw_feed": "date < '2024-12-04'", "t_output": "date < '2024-12-04'"}` |
| How come negative values in table T?     | `{"t_scores": "cvss < 0", "t_raw_feed": "cvss < 0"}`                  |
| No rows with vendor score < X?           | `{"t_source": "vendor_score < 5", "t_output": "vendor_score < 5"}`    |

The agent has access to the graph structure (tables, operations, schemas) and
translates the user's question into WHERE clauses on the right tables.

**Key insight**: the question *is* the pin. No pre-sampling or pre-pinning
needed. The AI agent maps the question to the strategy, the strategy targets
exactly the rows the user cares about.

---

# Phase 3 -- Row Trace (replay scope)

Re-run part of the job on the targeted subset to produce a full row-level trace.

## Input Tasks via Persistent Tables

No tags or labels needed. A task is an **input task** when all its returned
Objects are persistent (`p_` prefix). The system detects this automatically
from the task's result metadata.

```python
@task
async def fetch_kev_catalog(url: str) -> Object:
    return await create_object_from_value(data, name="kev_catalog")  # → p_kev_catalog

@task
async def merge_sources(kev: Object, scores: Object) -> Object:
    return await kev.concat(scores)  # → ephemeral table
```

`fetch_kev_catalog` is an input task (returns persistent Object).
`merge_sources` is not (returns ephemeral Object).

Persistent tables survive cleanup, so their data is always available for
replay. Ephemeral tables may be gone — the system walks the task graph
backward and stops at input tasks whose output is guaranteed to exist.

## Clear Task

Before replay, the system identifies input tasks in the upstream path and
clears everything downstream. The choice of where to start depends on the
question:

| Question                          | Input task boundary   | Why                                       |
|-----------------------------------|-----------------------|-------------------------------------------|
| Why NULLs in the merged table?    | merge task            | `p_kev_catalog`, `p_scores` exist         |
| Why is CVE-X missing entirely?    | ingest task           | Re-fetch into `p_kev_catalog`             |
| Why wrong scores after transform? | transform task        | `p_raw_scores` exists                     |

**Clear** resets the selected task and all its downstream tasks to PENDING --
same concept as Airflow's "clear task". Input tasks' persistent tables remain
as-is and become the replay inputs.

## Replay

1. Clear the target task + downstream
2. Re-run from that task, scoped to the `aai_id`s found in Phase 2
3. Record lineage at each step -- smart sampling, not random

This is a **replay**, not a query against existing oplog data. The original job
may have processed millions of rows; the replay processes only the rows that
answer the question.

**Smart sampling**: during replay, every operation samples the targeted rows
(not a random subset). The result is a complete trace from source to output for
exactly the rows the user cares about.

**Answers**: for this specific row, what operation produced it, what were its
source values, and where in the pipeline did the data appear or disappear.

---

# Current State

| Phase   | Status              | Notes                                                         |
|---------|---------------------|---------------------------------------------------------------|
| Phase 0 | Not yet implemented | Introduce `SamplingStrategy`; remove random sampling          |
| Phase 1 | Implemented         | `backward_oplog()`, `forward_oplog()`, `OplogGraph`           |
| Phase 2 | Not yet implemented | AI agent: question + graph → `SamplingStrategy`               |
| Phase 3 | Not yet implemented | Needs: clear task + downstream, replay with smart sampling    |

## Prerequisites for Phase 3

| Prerequisite               | Status              | Notes                                              |
|----------------------------|---------------------|----------------------------------------------------|
| Clear task + downstream    | Not yet implemented | Reset selected task and all downstream to PENDING  |
| Scoped replay (row subset) | Not yet implemented | Re-run tasks on targeted `aai_id`s only            |
