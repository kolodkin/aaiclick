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

## Prerequisite: Task Tags and Clear Task

### Task Tags

New `tags` parameter on the `@task` decorator. A list of strings stored on
the Task model. General-purpose mechanism; lineage reserves one tag:

```python
@task(tags=["input"])
async def fetch_kev_catalog(url: str) -> Object: ...

@task
async def merge_sources(kev: Object, scores: Object) -> Object: ...
```

The `"input"` tag marks a task as a data boundary -- its output tables are
treated as given during replay. This is a user declaration, not an automatic
classification. A pipeline may have multiple input tasks, and not all of them
are ingest (e.g., a task that reads from a persistent Object is also an input).

### Clear Task

Before replay, the user (or system) picks an **input task** -- the task where
replay starts. This is not necessarily an `"input"`-tagged task. The choice
depends on the question:

| Question                          | Input task            | Why                                       |
|-----------------------------------|-----------------------|-------------------------------------------|
| Why NULLs in the merged table?    | merge task            | Source data is fine, problem is in merging |
| Why is CVE-X missing entirely?    | ingest task           | Maybe ingest never fetched it              |
| Why wrong scores after transform? | transform task        | Raw scores are correct, transform is wrong |

**Clear** resets the input task and all its downstream tasks to PENDING --
same concept as Airflow's "clear task". Upstream tasks are untouched; their
output tables remain as-is and become the inputs for the replay.

Not all ingest tasks are input boundaries. If the question points to a
processing step, clearing starts there -- no need to re-fetch external data.

## Replay

1. Clear the input task + downstream
2. Re-run from the input task, scoped to the `aai_id`s found in Phase 2
3. Record lineage at each step -- smart sampling, not random

This is a **replay**, not a query against existing oplog data. The original job
may have processed millions of rows; the replay processes only the rows that
answer the question.

**Smart sampling**: during replay, every operation samples the targeted rows
(not a random subset). The result is a complete trace from source to output for
exactly the rows the user cares about.

**Why replay instead of querying existing oplog?** The original job's oplog has
random samples in `kwargs_aai_ids` / `result_aai_ids`. The specific rows from
Phase 2 are unlikely to appear in those random samples. Replay guarantees the
targeted rows are tracked through every operation.

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
| Task tags on `@task`       | Not yet implemented | `tags` field on Task model + decorator             |
| Clear task + downstream    | Not yet implemented | Reset selected task and all downstream to PENDING  |
| Scoped replay (row subset) | Not yet implemented | Re-run tasks on targeted `aai_id`s only            |
