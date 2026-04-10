Lineage: Three-Phase Debugging
---

How did this data get here? Every lineage question reduces to this.
The answer requires three phases, each building on the previous.

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

Re-run the job on the targeted subset to produce a full row-level trace.

**Steps**:

1. Take the task graph from Phase 1
2. Skip ingest tasks -- source data already exists in the tables
3. Re-run processing tasks only, scoped to the `aai_id`s found in Phase 2
4. Record lineage at each step -- smart sampling, not random

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

# Why Not Pre-Sample?

Random sampling collects `aai_id`s at operation time, but without a concrete
question the samples are just numbers. They don't help debug anything because
there is no question driving the investigation. Targeted sampling (Phase 2)
flips the model: sample on demand, guided by the question.

Pre-sampled `aai_id`s in `kwargs_aai_ids` / `result_aai_ids` remain useful for
structural validation (confirming operations ran and produced output), but
row-level debugging should use the three-phase flow.

---

# Current State

| Phase   | Status              | Notes                                                         |
|---------|---------------------|---------------------------------------------------------------|
| Phase 1 | Implemented         | `backward_oplog()`, `forward_oplog()`, `OplogGraph`           |
| Phase 2 | Not yet implemented | Needs: graph walker + WHERE application at each node          |
| Phase 3 | Not yet implemented | Needs: job replay mechanism (skip ingest, smart sampling)     |
