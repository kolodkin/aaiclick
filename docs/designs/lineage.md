Lineage: Interactive Debugging
---

Every lineage question reduces to: *how did this row get here, or why is
that row missing?* The debugger needs to see pipeline state, form
hypotheses and verify them against live data — exactly the evidence that
sampling rows against a pre-baked strategy compresses away.

Lineage is a two-tier agent loop over the pipeline's own tables. The agent
is any MCP client (Claude Code, Codex, any tool-using model) driving the
tools in `aaiclick/server/mcp.py`. It queries what exists, forms a
hypothesis, and escalates to a full replay only when static state is
insufficient.

# Two-Tier Model

| Tier | What runs                              | What the agent can query                           | Cost                  |
|------|----------------------------------------|----------------------------------------------------|-----------------------|
| 1    | Nothing — use existing state           | Persistent inputs (`p_*`), target table, oplog SQL | Zero replay           |
| 2    | Full replay with `"FULL"` preservation | Everything above + every intermediate table        | One full pipeline run |

Tier 1 runs first; the agent escalates only when static state cannot
answer, or when the user asks for Tier 2 up front.

Tier 3 — step-by-step execution with agent-driven eviction — is out of
scope. Revisit only if Tier 2's peak storage becomes a real constraint.

# Tier 1 -- Static Reasoning

The agent starts with:

- the **operation graph** from the oplog — tables, operations, rendered SQL
  templates, task and job ids
- **persistent input tables** (`p_*`) — survive cleanup, always queryable
- the **target table** whose data prompted the question
- the user's **natural-language question**

It works through the [Agent Tools](#agent-tools):

1. Read the question, the graph, and the target table's offending rows
2. Form a hypothesis from the SQL templates alone (*"this LEFT JOIN on
   `vendor_id` — input `p_vendors` has no row for `MS-001` — therefore
   `t_final.vendor` is NULL for CVE-X"*)
3. Verify it by querying persistent inputs and the target table
4. If it holds, output the explanation and the evidence rows
5. If it cannot be formed or verified from static state, escalate to Tier 2

Tier 1 handles the common deducible bugs: wrong join keys, missing input
rows, type mismatches, off-by-one filters, obvious SQL errors. Anything
whose explanation lives in an intermediate table that no longer exists
must escalate.

# Tier 2 -- Full Replay + Exploration

Tier 2 is not a separate API: it is `run_job()` with
`preservation_mode="FULL"` — no cloning, no task-graph surgery, no special
replay function. An agent does it by hand today; the one-call
`request_full_replay` is in `docs/designs/future.md`.

1. Read the original job's registered job and `kwargs` off its row
2. Submit a fresh run with those kwargs and `preservation_mode="FULL"`
3. Wait for it to complete — every intermediate table is now alive under
   the new job's id
4. Build a graph from the new job's oplog
5. Resume the loop; `lineage_query` now reaches every intermediate table,
   and the agent verifies hypotheses against real intermediate state

Tier 2 is correct by construction: every value at every stage is what the
pipeline computes on the original inputs. No population trap, no sampling
approximation, no pre-classification of operations. The trade is exact
evidence for the price of one run.

!!! warning "Input drift"
    A Tier 2 run re-executes input tasks, which may re-fetch from external
    sources whose data has changed since the original run. The agent
    compares input row counts against the original job's persistent `p_*`
    tables and flags any delta, so the user can tell "the pipeline broke"
    from "the source data changed".

# Preservation Mode

| Mode   | What survives after job  | Use case                                     |
|--------|--------------------------|----------------------------------------------|
| `NONE` | Persistent tables only   | Production runs, Tier 1 debugging            |
| `FULL` | All tables until job TTL | Development, Tier 2 debugging, replay target |

Precedence, highest first:

1. Explicit `run_job(..., preservation_mode=...)` argument
2. `RegisteredJob.preservation_mode`
3. `AAICLICK_DEFAULT_PRESERVATION_MODE` env var
4. `"NONE"`

# Agent Tools

**Implementation**: `aaiclick/oplog/query_sandbox.py`

Every tool takes `target_table` (plus `direction` / `max_depth`) and
resolves its scope server-side from that table's lineage graph, so a caller
cannot widen it:

```python
async def lineage_graph(target_table: str, ...) -> OplogGraph:
    """The graph; each node's `sql_template` is the operation's rendered SQL."""

async def lineage_nodes(target_table: str, ...) -> list[GraphNode]:
    """All tables in the graph with kind (input / intermediate / target) + liveness."""

async def lineage_schema(table: str, target_table: str, ...) -> TableSchema:
    """Columns and types for a table in the graph."""

async def lineage_query(sql: str, target_table: str, row_limit: int = 100, ...) -> QueryResult:
    """Read-only SELECT over tables of the graph, capped at `row_limit` rows."""

async def request_full_replay(reason: str) -> ReplayHandle:  # planned (future.md)
    """Re-run the original job with its kwargs and `preservation_mode="FULL"`,
    wait, and return a handle onto the new job's graph. `reason` is logged so
    the user can see why the agent escalated."""
```

ClickHouse parses `lineage_query`'s SQL (`EXPLAIN AST`) and the check reads
table position off the parse tree, so scoping is positive: anything in
table position that is not a graph table is rejected. Safety rails:

- **Read-only** — anything but `SELECT` is rejected
- **Scoped** — every table position must name a graph table. Table
  functions (`merge`, `remote`, `url`, `file`, `cluster`) are rejected
  because they name their targets in string literals. A CTE name is not in
  the graph; write it as a subquery in `FROM`
- **Scoped through `IN`** — a bare identifier on the right of any IN-family
  function (`IN`, `GLOBAL NOT IN`, `nullIn`, …) reads a table, so it must be
  a graph table. The parse tree cannot tell it from an array column; use
  `has(column, value)` for arrays
- **Read-only in the engine** — `readonly=2` / `allow_ddl=0` on the query,
  so the keyword regex is a first line, not the only one. `readonly=2`
  still permits settings changes, and a `SETTINGS` clause in the text
  outranks the caps sent beside the query, so one at any depth is rejected
- **Bounded** — `row_limit` defaults low; ClickHouse `max_result_rows`
  enforces the ceiling, with `result_overflow_mode='break'` truncating a
  larger result instead of failing it
- **Cheap** — `max_execution_time` keeps accidental scans from tying up the
  cluster

## Tool Result Types

Pydantic models, so they serialize through MCP unchanged. A refused query
raises `SandboxError` with a `kind` (`not_select`, `out_of_scope`,
`invalid_argument`), mapped to `Invalid`; a target with no lineage, or a
failing `DESCRIBE`, maps to `NotFound`. MCP surfaces both as a tool error
carrying the message.

Tier 2 adds one type, with `replay_timeout` / `replay_failed` as its error
kinds:

```python
class ReplayHandle(NamedTuple):
    original_job_id: int
    replayed_job_id: int
    drift: dict[str, int]  # per-input delta: new_rows - original_rows
```

# Why Not Sampling

An earlier iteration shipped a `SamplingStrategy` type and a
`PreservationMode.STRATEGY` mode that kept strategy-matched rows through
cleanup: an agent would emit a strategy from the question, and replay would
tag those rows across every operation for a row-level trace. It was
deleted.

The premise was wrong. A debugger does not want "these 10 rows that match
my WHERE clause" — they want the shape of the data, ad-hoc questions of
intermediate tables, threads to follow as they form. Sampling compresses
exactly that. It also sets correctness traps: any operation whose output
depends on its input's population (window functions, global aggregates,
percentiles, dedup, top-K) produces a different value on a subset, silently
misleading the debugger.

The interactive approach is strictly more powerful: Tier 1 is the zero-cost
path, Tier 2 the full-fidelity one, and the agent picks per question. No
strategies to produce, no populations to classify, no planners to write.
