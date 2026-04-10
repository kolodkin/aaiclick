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

# Phase 3 -- Row Trace (row scope)

Take the `aai_id`s found in Phase 2 and run `backward_oplog_row()` to trace
each one through the operation chain. At every hop, inspect the actual values
in the intermediate table.

**Answers**: for this specific row, what operation produced it, what were its
source values, and where in the pipeline did the data appear or disappear.

**Available today**: `aaiclick/oplog/lineage.py` -- see `backward_oplog_row()`

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

| Phase   | Status                 | Notes                                                    |
|---------|------------------------|----------------------------------------------------------|
| Phase 1 | Implemented            | `backward_oplog()`, `forward_oplog()`, `OplogGraph`      |
| Phase 2 | Not yet implemented    | Needs: graph walker + WHERE application at each node     |
| Phase 3 | Partially implemented  | `backward_oplog_row()` exists; needs Phase 2 as input    |
