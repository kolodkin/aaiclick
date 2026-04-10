Lineage: Three-Phase Debugging
---

How did this data get here? Every lineage question reduces to this.
The answer requires three phases, each building on the previous.

---

# Phase 0 -- Remove Row-Scope Sampling

The three-phase design replaces random pre-sampling with on-demand,
question-driven sampling. Before implementing Phases 2-3, remove the
existing row-scope lineage machinery:

**operation_log columns to remove**:

- `kwargs_aai_ids` — `Map(String, Array(UInt64))` sampled source row IDs
- `result_aai_ids` — `Array(UInt64)` sampled result row IDs

**Code to remove**:

| File                     | What                                                              |
|--------------------------|-------------------------------------------------------------------|
| `oplog/lineage.py`       | `backward_oplog_row()`, `RowLineageStep`                         |
| `oplog/sampling.py`      | `sample_lineage()`, `_pick_aai_ids()` — sampling helpers         |
| `oplog/cleanup.py`       | `lineage_aware_drop()`, `_get_lineage_aai_ids()` — sample preservation |
| `oplog/collector.py`     | sampling logic in `record()` that populates `kwargs_aai_ids` / `result_aai_ids` |
| `background_worker.py`   | `_cleanup_expired_samples()` — sample table cleanup              |
| `oplog/models.py`        | `kwargs_aai_ids` and `result_aai_ids` in DDL and schema validation |

**Config to remove**:

- `AAICLICK_OPLOG_SAMPLE_SIZE` env var

**Requires**: Alembic migration to drop the two columns from `operation_log`.

The `operation_log` table keeps all other fields (`operation`, `kwargs`,
`sql_template`, `task_id`, `job_id`, `created_at`). Phase 1 graph queries
are unaffected — they only use table-level metadata.

---

# Phase 1 -- Graph (table scope)

Run `backward_oplog()` from the target table. Returns the DAG of tables,
operations, SQL templates, and task/job IDs. No row sampling needed.

**Answers**: which tables, which operations, which tasks, in what order.

**Available today**: `aaiclick/oplog/lineage.py` -- see `backward_oplog()`,
`forward_oplog()`, `OplogGraph`

---

# Phase 2 -- Targeted Sample (question scope)

The user asks a concrete question:

| Question                                 | WHERE clause                      | Applied along                |
|------------------------------------------|-----------------------------------|------------------------------|
| Why does CVE-X have no KEV data?         | `cve_id = 'CVE-X'`               | KEV source table             |
| Why no data before 12/04?                | `date < '2024-12-04'`            | each table in the graph      |
| How come negative values in table T?     | `value < 0`                      | T and its upstream tables    |
| No rows with vendor score < X?           | `vendor_score < X`               | source table, output table   |

The question becomes one or more WHERE clauses. Walk the graph from Phase 1
and apply each WHERE at the relevant tables. This finds the specific `aai_id`s
that matter -- not random samples, but rows targeted by the question.

**Key insight**: the question *is* the pin. No pre-sampling or pre-pinning
needed. The WHERE clause targets exactly the rows the user cares about.

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
| Phase 0 | Not yet implemented | Remove row-scope sampling + migration                         |
| Phase 1 | Implemented         | `backward_oplog()`, `forward_oplog()`, `OplogGraph`           |
| Phase 2 | Not yet implemented | Needs: graph walker + WHERE application at each node          |
| Phase 3 | Not yet implemented | Needs: clear task + downstream, replay with smart sampling    |

## Prerequisites for Phase 3

| Prerequisite               | Status              | Notes                                              |
|----------------------------|---------------------|----------------------------------------------------|
| Task tags on `@task`       | Not yet implemented | `tags` field on Task model + decorator             |
| Clear task + downstream    | Not yet implemented | Reset selected task and all downstream to PENDING  |
| Scoped replay (row subset) | Not yet implemented | Re-run tasks on targeted `aai_id`s only            |
