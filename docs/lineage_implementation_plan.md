Lineage: Implementation Plan
---

Companion to `docs/lineage.md`. Three phases:

1. **Phase 0** ✅ — remove all sampling and strategy machinery, narrow
   `PreservationMode` to `NONE` / `FULL` (merged in #223)
2. **Phase 1** — Tier 1: agent tool loop over persistent inputs, target
   table, and oplog graph (no replay)
3. **Phase 2** — Tier 2: agent escalates to a `run_job(..., FULL)` fresh
   run and continues querying against the new run's intermediates

Each phase is independently shippable. Phase 0 was pure deletion and
unblocked Phases 1 and 2 cleanly — no dead branches to reason about.

---

# Phase 0 -- Remove Sampling and Strategy ✅ Complete

Shipped in #223. Everything below is preserved for historical context —
the deletions landed as described.

**Objective**: Delete every piece of code that exists because of the
`SamplingStrategy` / `PreservationMode.STRATEGY` direction. Narrow
`PreservationMode` to `NONE` and `FULL`. Leave the codebase smaller
and simpler before Phase 1 adds anything new.

## Tasks

1. **Narrow `PreservationMode` enum**
   - `aaiclick/orchestration/models.py` — drop the `STRATEGY` variant
   - Alembic migration: drop `STRATEGY` from the `preservationmode` ENUM
     type and data-migrate any in-flight rows to `NONE`
   - `get_default_preservation_mode()` — reject `STRATEGY` as an invalid
     env-var value with a clear error

2. **Drop `sampling_strategy` columns**
   - Drop `Job.sampling_strategy` and `RegisteredJob.sampling_strategy`
     via Alembic
   - Remove the param from `register_job()`, `upsert_registered_job()`,
     `run_job()`, `resolve_job_config()`, `submit_job()`, and any other
     entry points
   - Remove `--sampling-strategy` / `--sampling-strategy-file` from
     `register-job` and `run-job` CLI commands

3. **Delete the strategy agent**
   - `aaiclick/ai/agents/strategy_agent.py` — delete file
   - `test_strategy_agent.py` — delete file
   - Remove `STRATEGY_SYSTEM_PROMPT` from `prompts.py`
   - Remove all references to `produce_strategy` in `debug_agent` /
     `lineage_agent` wiring

4. **Delete oplog row-id machinery**
   - Drop `operation_log.kwargs_aai_ids` and `operation_log.result_aai_ids`
     columns via Alembic
   - Delete `apply_strategy()`, `_pick_aai_ids()`, any remaining helpers
     in `aaiclick/oplog/sampling.py` — if the file ends up empty, delete
     the file and update `oplog/__init__.py`
   - Delete `backward_oplog_row()` and `RowLineageStep` from
     `aaiclick/oplog/lineage.py`

5. **Delete `trace_row` agent tool**
   - Remove from `aaiclick/ai/agents/tools.py`
   - Remove tests from `test_tools.py`
   - Drop from the tool lists of `debug_agent` and `lineage_agent`

6. **Delete `replay_job()` and `is_input_task()`**
   - Delete `aaiclick/orchestration/replay.py` and `test_replay.py`
   - Remove `replay_job_cmd` from `aaiclick/orchestration/cli.py`
   - Unregister the `aaiclick replay` CLI command
   - Delete `is_input_task()` from `aaiclick/orchestration/lineage.py`
     and its tests from `test_lineage.py`

7. **Clean up context plumbing**
   - `OrchContext` — remove `sampling_strategy` from the per-task context
   - Lifecycle queue — delete the `OPLOG_SAMPLE` message branch; only
     `OPLOG_RECORD` remains
   - `lineage_aware_drop()` — keep as-is since `NONE` mode already works,
     but audit for any dead strategy-aware branches

8. **Docs updates**
   - `docs/oplog.md` — remove sampling section, `trace_row`,
     `backward_oplog_row`
   - `docs/orchestration.md` — remove Replay section, remove
     `sampling_strategy` from the job parameter reference
   - `docs/data_context.md` — narrow preservation-mode section to two
     values
   - `docs/ai.md` — remove `produce_strategy` and `trace_row` from the
     agent tools table
   - `docs/future.md` — prune any stale lineage references

9. **Tests**
   - Delete every test exercising deleted code — do not rewrite, just
     delete
   - Verify `pytest` is green after deletions — no stale imports
   - Add one smoke test: a job submitted with `preservation_mode=FULL`
     keeps all its intermediates alive through cleanup, so Phase 1's
     agent will have tables to query

## Deliverables

- `PreservationMode` with exactly two members
- One Alembic migration dropping the enum variant and all four sampling
  columns (two on `Job`/`RegisteredJob`, two on `operation_log`)
- Deleted modules: `ai/agents/strategy_agent.py`,
  `orchestration/replay.py`; `oplog/sampling.py` deleted or gutted
- Deleted functions and tests per the task list
- Green test suite

## Success Criteria

- Grep across `aaiclick/` returns zero hits for any of:
  `SamplingStrategy`, `sampling_strategy`, `apply_strategy`,
  `backward_oplog_row`, `RowLineageStep`, `is_input_task`, `trace_row`,
  `replay_job`, `produce_strategy`, `kwargs_aai_ids`, `result_aai_ids`
- `PreservationMode` exposes only `NONE` and `FULL`
- `run_job` and `register-job` CLI still work end-to-end for NONE and
  FULL modes
- Alembic upgrade and downgrade execute cleanly on a DB containing
  pre-existing jobs in all three original modes
- No references to `aaiclick replay` in CLI help output

---

# Phase 1 -- Tier 1: Static Reasoning

**Objective**: Give the debug agent a tool loop over persistent inputs,
the target table, and the oplog graph. No replay happens. The agent
forms hypotheses from SQL templates and verifies them against live
tables.

## Tasks

1. **New agent tools module** — `aaiclick/ai/agents/lineage_tools.py`
   - `query_table(sql, row_limit=100)` — read-only SELECT scoped to
     the current graph
   - `get_op_sql(op_id)` — rendered SQL for one operation
   - `list_graph_nodes()` — all nodes in the current graph with kind
     (input / intermediate / target) and liveness
   - `get_schema(table)` — column names and types
   - Each tool returns a typed result the agent can reason over

2. **Scope enforcement for `query_table`**
   - Parse the incoming SQL with a SELECT-only parser
   - Verify every referenced table is in the current graph's node set
   - Reject non-SELECT statements with a clear error message the agent
     can read and retry from
   - Wrap in `LIMIT row_limit` if the SQL does not already have a
     `LIMIT`
   - Pass ClickHouse settings `max_result_rows` and
     `max_execution_time` on the query so accidental scans cannot tie
     up the cluster

3. **Graph liveness resolution**
   - For each node in the graph, determine whether the table currently
     exists
   - Persistent `p_*` tables are always live
   - Ephemeral tables live iff the job's preservation mode was `FULL`
     and the TTL has not expired
   - Expose liveness via `list_graph_nodes` so the agent knows which
     tables it can query versus which require escalation

4. **Rework `debug_result()` as a tool loop**
   - Replace the current one-shot "explain the context" flow
   - Initialize with: the question, the oplog graph (from
     `backward_oplog`), target table info, the list of live persistent
     inputs
   - Run the agent until it emits a final explanation or hits a max
     iteration count (configurable, default 10)
   - Reuse existing `get_ai_provider()` scaffolding

5. **Prompt design**
   - New prompt in `aaiclick/ai/agents/prompts.py`:
     `LINEAGE_TIER1_SYSTEM_PROMPT`
   - Tells the agent: here is the question, here is the graph, these
     are the tools, read the SQL templates first, form a hypothesis
     before issuing queries, cite evidence rows in the final
     explanation
   - Few-shot examples: missing-row join case, bad-filter case

6. **Tests**
   - `test_lineage_tools.py`
     - Each tool happy path
     - `query_table` scope rejection (table outside graph)
     - `query_table` non-SELECT rejection
     - Row-limit enforcement
     - Schema introspection against a known table
   - `test_debug_agent.py`
     - Mocked AI provider tool loop on a canned scenario
     - Assert the agent calls `query_table` at least once before its
       final explanation
     - Assert the loop terminates on a final explanation
   - Live smoke test behind the existing live-test marker

7. **Docs**
   - `docs/ai.md` — new "Lineage Tools" subsection listing the four
     tools
   - `docs/lineage.md` — already covers the design; link from ai.md

## Deliverables

- `lineage_tools.py` module with four tools and scope enforcement
- Reworked `debug_result()` tool loop
- New system prompt
- Green tests
- Updated `docs/ai.md`

## Success Criteria

- A job in `preservation_mode=NONE` whose question has an answer in a
  persistent input table is answered correctly by Tier 1 using only
  `query_table`
- `query_table` never reaches a table outside the current graph's node
  set — verified by both unit tests and a scope-violation attempt in
  an integration test
- Non-SELECT SQL is rejected with a clear, agent-readable error
- `debug_result()` returns a final explanation that cites evidence rows
  it fetched via `query_table`

---

# Phase 2 -- Tier 2: Full Replay + Exploration

**Objective**: Give the Tier 1 agent an escalation path. The agent can
request a fresh `run_job(..., preservation_mode=FULL)` of the original
registered job and continue querying against the new run's
intermediates.

## Tasks

1. **`request_full_replay` tool**
   - Add to `aaiclick/ai/agents/lineage_tools.py`
   - Signature: `request_full_replay(reason: str) -> ReplayHandle`
   - Implementation:
     - Read `registered_job_id` and `kwargs` off the original `Job` row
     - Call `run_job(registered_job_id, kwargs=original.kwargs,
       preservation_mode=FULL)`
     - Wait for the new job to reach `COMPLETE`
     - Return a handle containing the new job id
   - The `reason` string is logged so the user sees why the escalation
     happened

2. **Agent context rebind**
   - After `request_full_replay` succeeds, the tool loop rebinds
     subsequent `query_table` / `list_graph_nodes` / `get_op_sql` calls
     to the new job's graph
   - Retain both graphs (original and replayed) in the agent's context
     so it can cross-compare — e.g., "input row count delta between
     original and replayed"

3. **Input-drift detection**
   - Before escalation: snapshot persistent input row counts (and
     optionally hashes of a stable column set) from the original job's
     `p_*` tables
   - After the replayed job completes: compare the same snapshots
     against the new job's `p_*` tables
   - Expose any delta in the agent's context under a `drift` field
     so the final explanation can distinguish "pipeline bug" from
     "source data changed since the original run"

4. **`--deep` pre-commit path**
   - CLI: `aaiclick debug <job_id> --deep` skips Tier 1 and escalates
     directly to Tier 2
   - Python: `debug_result(job_id, deep=True)` has the same effect
   - Useful when the user already knows Tier 1 is insufficient

5. **Timeout and cancellation**
   - `request_full_replay` accepts a timeout (default: the registered
     job's `expected_runtime * 2` or a configurable hardcoded ceiling)
   - On timeout, the tool returns a failure result the agent surfaces
     to the user as "replay timed out — try `--deep` with a larger
     timeout or use FULL mode on the original run next time"
   - No hanging, no silent long waits

6. **Tests**
   - `test_lineage_tools.py`
     - `request_full_replay` happy path with mocked `run_job` + wait
     - Timeout path
     - Input-drift detection with synthetic drift
   - `test_debug_agent.py`
     - Two-tier end-to-end with mocked provider: Tier 1 says "can't
       tell, escalating" then Tier 2 returns final explanation
     - `--deep` pre-commit path skips Tier 1 entirely
   - Integration test with a real small pipeline: a question Tier 1
     answers, a second question that escalates to Tier 2

7. **Docs**
   - `docs/ai.md` — add `request_full_replay` to the tools table
   - `docs/lineage.md` — already covers Tier 2; reference from ai.md
   - `docs/orchestration.md` — mention that `debug <id> --deep`
     triggers a FULL-mode fresh run under the hood

## Deliverables

- `request_full_replay` tool with escalation plumbing
- Input-drift detection
- `--deep` CLI flag + Python parameter
- Green tests
- Updated docs

## Success Criteria

- A debug question that requires intermediate state is recognized by
  Tier 1, escalated via `request_full_replay`, and answered by Tier 2
- Input drift is flagged whenever the replayed `p_*` tables differ
  from the originals
- `--deep` bypasses Tier 1 and runs the fresh job directly
- Replay timeout produces a clean failure, not a hang
- The agent never sees a half-initialized replayed graph — the tool
  only returns after the new job is `COMPLETE`

---

# Phase Dependencies

| Phase   | Depends on | Can ship independently? |
|---------|------------|-------------------------|
| Phase 0 | —          | Yes — pure deletion     |
| Phase 1 | Phase 0    | Yes — Tier 1 is useful alone |
| Phase 2 | Phase 1    | No — extends the Tier 1 tool loop |

Ship order: 0 → 1 → 2. Phase 0 is high-confidence deletion; Phase 1 is
the interesting new capability; Phase 2 is the escalation wiring.

---

# Open Questions

1. **Raw SQL vs structured tool surface for `query_table`.** Current
   plan is raw SQL with scope + read-only + row-limit guardrails.
   Alternative is a structured API (`count_where`, `sample_rows`,
   `distinct_values`, `distribution`) that is safer but less
   expressive. Raw SQL matches how a human debugs and is easier for
   the LLM to compose — revisit only if the guardrails prove leaky.

2. **Hypothesis-first prompting.** Phase 1's prompt tells the agent to
   write a hypothesis before issuing queries. This may be too
   restrictive — some questions warrant a few exploratory queries
   first. If this hurts in practice, relax to "prefer hypotheses, but
   exploration is allowed".

3. **Agent max-iteration limit.** Default 10 iterations for the Tier 1
   loop is a guess. Needs tuning against real debug scenarios.

4. **Tier 2 timeout default.** "Expected runtime × 2" requires jobs to
   publish an expected runtime, which they may not today. Fallback:
   hardcoded ceiling (e.g., 30 min) that the user can override.

5. **Tier 3 (step-by-step with agent eviction) — deferred.** Revisit
   only if Tier 2 peak storage becomes a real constraint on large
   pipelines.
