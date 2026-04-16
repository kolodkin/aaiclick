Lineage: Interactive Debugging
---

Every lineage question reduces to: *how did this row get here, or why is
that row missing?* The debugger needs to see pipeline state, form
hypotheses, and verify them against live data. Sampling row subsets
against a pre-baked strategy does not give the debugger what it needs —
it compresses exactly the evidence a debugger would want to look at.

Lineage is a two-tier agent loop over the pipeline's own tables. The
agent queries what exists, forms a hypothesis, and escalates to a full
replay only when static state is insufficient.

**Implementation plan**: `docs/lineage_implementation_plan.md`

---

# Two-Tier Model

| Tier | What runs                           | What the agent can query                        | Cost         |
|------|-------------------------------------|------------------------------------------------|--------------|
| 1    | Nothing — use existing state        | Persistent inputs (`p_*`), target table, oplog SQL | Zero replay |
| 2    | Full replay under `PreservationMode.FULL` | Everything above + every intermediate table    | One full pipeline run |

Tier 1 is tried first. The agent escalates to Tier 2 only when it cannot
answer from static state alone. The user can also pre-commit to Tier 2
via a `--deep` flag when they already know a superficial pass will not
suffice.

Tier 3 — step-by-step execution with agent-driven eviction — is out of
scope. Revisit only if Tier 2's peak storage becomes a real constraint.

---

# Tier 1 -- Static Reasoning

Inputs the agent starts with:

- **Operation graph** from `backward_oplog()` — tables, operations,
  rendered SQL templates, task and job ids
- **Persistent input tables** (`p_*`) — always survive cleanup, always
  queryable
- **Target table** — the final-stage table whose data prompted the
  question
- **Natural-language question** from the user

Agent tools (⚠️ Phase 1/2 — not yet implemented; see "Agent Tools" below for
planned signatures):

- `query_table` — arbitrary read-only SQL against any node in the graph
- `get_op_sql` — rendered SQL for a specific operation
- `list_graph_nodes` — all tables in the lineage graph with their node
  kind (input / intermediate / target) and liveness
- `get_schema` — columns and types for a table
- `request_full_replay` — escalate to Tier 2

The loop:

1. Agent reads the question, the graph, and the target table's
   offending rows
2. Forms a hypothesis from the SQL templates alone (*"this LEFT JOIN on
   `vendor_id` — input `p_vendors` has no row for `MS-001` — therefore
   `t_final.vendor` is NULL for CVE-X"*)
3. Verifies by querying persistent inputs and the target table
4. If the hypothesis holds, outputs an explanation and the evidence
   rows
5. If the hypothesis cannot be formed or verified from static state,
   calls `request_full_replay` and continues as Tier 2

Tier 1 handles the common deducible-bug cases: wrong join keys, missing
input rows, type mismatches, off-by-one filters, obvious SQL errors.
Anything whose explanation lives inside an intermediate table that no
longer exists must escalate.

---

# Tier 2 -- Full Replay + Exploration

Tier 2 is not a separate API — it is the existing `run_job()` entry
point invoked with `preservation_mode=FULL`. No cloning, no task-graph
surgery, no special replay function.

Triggered by `request_full_replay` (or by `--deep` on the initial
request). Mechanics:

1. Read the original job's `registered_job_id` and `kwargs` off its row
2. Submit a fresh run:
   `run_job(registered_job_id, kwargs=original.kwargs, preservation_mode=FULL)`
3. Wait for the fresh run to complete — every intermediate table is
   now materialized and alive under the new job's id
4. Build a graph from the new job's oplog
5. Resume the agent loop; `query_table` now works against every
   intermediate table, not just inputs and target
6. Agent forms and verifies hypotheses against real intermediate state

Tier 2 is correct by construction. Every value at every stage is the
value the pipeline computes on the original inputs. No population
trap, no sampling approximation, no pre-classification of operations.

The cost is a full pipeline re-execution. That is the trade: exact
evidence for the price of one run.

!!! warning "Input drift"
    A Tier 2 run re-executes input tasks, which may re-fetch from
    external sources. If the upstream data has changed since the
    original run, the replayed inputs can differ from the historical
    inputs. The agent compares input row counts against the original
    job's persistent `p_*` tables and flags any delta in its
    explanation so the user knows to distinguish "the pipeline broke"
    from "the source data changed".

---

# Preservation Mode

Two values.

| Mode     | What survives after job      | Use case                                      |
|----------|------------------------------|-----------------------------------------------|
| `NONE`   | Persistent tables only       | Production runs, Tier 1 debugging             |
| `FULL`   | All tables until job TTL     | Development, Tier 2 debugging, replay target  |

Precedence:

```
1. Explicit run_job(..., preservation_mode=...) argument
2. RegisteredJob.preservation_mode
3. AAICLICK_DEFAULT_PRESERVATION_MODE env var
4. PreservationMode.NONE
```

---

# Agent Tools

⚠️ NOT YET IMPLEMENTED — Phase 1/2 planned tools.

All tools are scoped to the job being debugged. `query_table` cannot
reach tables outside the lineage graph of the current job, preventing
accidental cross-job queries.

```python
async def query_table(
    sql: str,
    row_limit: int = 100,
) -> QueryResult:
    """
    Execute a read-only SELECT against a table in the current job's
    lineage graph. `sql` must reference only nodes present in the
    graph. Automatically wrapped in `LIMIT row_limit` if not already
    limited. Rejects any statement other than SELECT.
    """

async def get_op_sql(op_id: str) -> str:
    """Rendered SQL for a single operation in the graph."""

async def list_graph_nodes() -> list[GraphNode]:
    """All nodes in the current graph with kind + liveness."""

async def get_schema(table: str) -> TableSchema:
    """Columns and types for a table in the graph."""

async def request_full_replay(reason: str) -> ReplayHandle:
    """
    Escalate to Tier 2. Submits a fresh run of the original job's
    registered_job with the original kwargs and `preservation_mode=FULL`,
    waits for completion, and returns a handle the agent uses to continue
    querying against the new job's graph. The `reason` is logged so the
    user can see why the escalation happened.
    """
```

Safety rails on `query_table`:

- Read-only — parser rejects anything that is not `SELECT`
- Scoped — the FROM clause must reference a table known to the graph
- Bounded — `row_limit` defaulted low, ceiling enforced by ClickHouse
  `max_result_rows`
- Cheap — `max_execution_time` set to keep accidental table scans from
  tying up the cluster

## Tool Result Types

The agent-facing result types are typed NamedTuples / dataclasses so the
loop can reason over them without string parsing.

```python
from typing import Literal, NamedTuple

NodeKind = Literal["input", "intermediate", "target"]

class GraphNode(NamedTuple):
    table: str            # raw table id, e.g. "t_1234567890123456"
    kind: NodeKind        # input = persistent `p_*`, target = terminal node
    operation: str        # oplog operation name
    live: bool            # whether the table currently exists in ClickHouse
    task_id: int | None
    job_id: int | None

class ColumnSchema(NamedTuple):
    name: str
    type: str             # ClickHouse type string

class TableSchema(NamedTuple):
    table: str
    columns: list[ColumnSchema]

class QueryResult(NamedTuple):
    columns: list[str]
    rows: list[tuple]     # at most `row_limit` rows
    truncated: bool       # true iff the underlying query returned > row_limit

class ReplayHandle(NamedTuple):
    original_job_id: int
    replayed_job_id: int
    drift: dict[str, int] # per-input delta: new_rows - original_rows
```

Error surface — tools never raise to the agent. Each tool returns a
discriminated-union shape with either the success payload above or a
typed error the agent can read and retry from:

```python
class ToolError(NamedTuple):
    kind: Literal[
        "not_select",     # query_table: non-SELECT rejected
        "out_of_scope",   # query_table: table outside current graph
        "not_found",      # get_schema / get_op_sql: unknown id
        "not_live",       # query_table: table exists in graph but not in ClickHouse
        "replay_timeout", # request_full_replay: new job did not COMPLETE in time
        "replay_failed",  # request_full_replay: new job ended non-COMPLETE
    ]
    message: str          # agent-readable diagnostic
```

The agent loop surfaces `ToolError` as the tool's return value; the
prompt instructs the agent to inspect `kind` and either retry with a
corrected call (`not_select`, `out_of_scope`) or escalate (`not_live`
triggers `request_full_replay`).

---

# Why Not Sampling

An earlier iteration of this work shipped a `SamplingStrategy` type and
a `PreservationMode.STRATEGY` mode that kept strategy-matched rows
through cleanup. That code has been deleted (Phase 0). The idea was:
an AI agent would emit a strategy from a natural-language question, and
replay would tag those rows across every operation to give a row-level
trace.

The premise was wrong. A debugger does not want "these 10 rows that
match my WHERE clause" — they want to see the shape of the data, ask
ad-hoc questions of intermediate tables, follow threads as they form.
Sampling compresses exactly what a human would look at. It also
introduces correctness traps: any operation whose output depends on
the population of its input (window functions, global aggregates,
percentiles, dedup, top-K) produces a different value on a subset,
silently misleading the debugger.

The interactive approach is strictly more powerful: Tier 1 gives the
zero-cost debugging path, Tier 2 gives the full-fidelity path, and the
agent chooses between them per-question. No strategies to produce, no
populations to classify, no planners to write.

---

# Current State

| Component                                      | Status      | Implementation / Notes                                                    |
|------------------------------------------------|-------------|---------------------------------------------------------------------------|
| `backward_oplog()` / `forward_oplog()`         | Shipped     | `aaiclick/oplog/lineage.py` — recursive graph traversal over `operation_log` |
| `OplogGraph`                                   | Shipped     | `aaiclick/oplog/lineage.py` — `OplogGraph`, `OplogNode`, `OplogEdge`      |
| `run_job()` with `preservation_mode`           | Shipped     | `aaiclick/orchestration/registered_jobs.py` — Tier 2 reuses this entry point |
| `PreservationMode` (narrow to `NONE`/`FULL`)   | ✅ Done     | `aaiclick/orchestration/models.py` — `STRATEGY` variant removed (Phase 0) |
| Sampling / strategy machinery                  | ✅ Done     | Deleted (Phase 0, #223)                                                   |
| `replay_job()` / `is_input_task()`             | ✅ Done     | Deleted (Phase 0, #223)                                                   |
| Tier 1 agent loop + `query_table` tool         | Phase 1     | Replaces `debug_result` single-shot explanation                           |
| Tier 2 auto-escalation + `request_full_replay` | Phase 2     | Wires Tier 1 to `run_job(..., FULL)`                                      |
