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
| **Normal**   | Persistent tables only               | Production runs (as today)   |
| **Full**     | All tables                           | Development / debugging      |
| **Strategy** | Persistent + strategy-matched rows   | Lineage replay (Phase 2+3)  |

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

# Phase 2 -- AI Agent Produces the Strategy

The AI agent takes a natural language question + the graph from Phase 1, and
outputs a `SamplingStrategy` (`dict[str, str]`).

| Question                             | Agent output                                                           |
|--------------------------------------|------------------------------------------------------------------------|
| Why does CVE-X have no KEV data?     | `{"t_kev_catalog": "cve_id = 'CVE-X'", "t_merged": "vendor IS NULL"}` |
| How come negative values in table T? | `{"t_scores": "cvss < 0", "t_raw_feed": "cvss < 0"}`                  |

The question *is* the pin — no pre-sampling needed.

---

# Phase 3 -- Row Trace (replay scope)

Re-run the entire job with a `SamplingStrategy` applied. The strategy from
Phase 2 tells each operation which rows to track.

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

Persistent tables survive cleanup, so replay always has its inputs.
The system walks the task graph backward and stops at input tasks.

## Replay

1. Re-run the entire job with the `SamplingStrategy` from Phase 2
2. At each operation, sample the targeted rows (not random)

After replay, the oplog contains a complete source-to-output trace for the
strategy-matched rows.

---

# Current State

| Phase   | Status              | Notes                                                         |
|---------|---------------------|---------------------------------------------------------------|
| Phase 0 | Not yet implemented | Introduce `SamplingStrategy`; remove random sampling          |
| Phase 1 | Implemented         | `backward_oplog()`, `forward_oplog()`, `OplogGraph`           |
| Phase 2 | Not yet implemented | AI agent: question + graph → `SamplingStrategy`               |
| Phase 3 | Not yet implemented | Re-run entire job with strategy-driven smart sampling         |

## Prerequisites for Phase 3

| Prerequisite               | Status              | Notes                                              |
|----------------------------|---------------------|----------------------------------------------------|
| Scoped replay (row subset) | Not yet implemented | Re-run tasks on targeted `aai_id`s only            |

## Documentation Updates

Each phase should update the relevant docs as it lands:

| Phase   | Docs to update                                                          |
|---------|-------------------------------------------------------------------------|
| Phase 0 | `docs/oplog.md` — remove `AAICLICK_OPLOG_SAMPLE_SIZE`, add strategy interface |
| Phase 0 | `docs/data_context.md` — document preservation modes (normal/full/strategy) |
| Phase 1 | Already documented in `docs/oplog.md`                                   |
| Phase 2 | `docs/ai.md` — add strategy agent to agent tools table                  |
| Phase 3 | `docs/orchestration.md` — document replay mechanism                     |
| Phase 3 | `docs/data_context.md` — document input task detection via persistent Objects |
