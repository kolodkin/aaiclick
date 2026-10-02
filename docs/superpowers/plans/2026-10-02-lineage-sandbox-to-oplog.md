# Lineage Sandbox → oplog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lineage SQL sandbox and graph classification independent of `aaiclick/ai/`, and expose them over MCP with agent guidance, so an external agent (Claude Code, Codex) can triage lineage without the in-process LLM. This is PR 1 of 3; PR 2 deletes `aaiclick/ai/` and LiteLLM.

**Architecture:** The stateless sandbox (`validate_select_safety`, `validate_scope`, `run_select`, `describe_table`, `liveness`, the `QueryResult` / `TableSchema` / `ToolError` types) moves verbatim from `aaiclick/ai/agents/lineage_tools.py` to a new `aaiclick/oplog/query_sandbox.py`, gaining one composing function `sandboxed_select` that both `internal_api.lineage` and `LineageToolbox` call. Graph classification (`NodeKind`, `GraphNode`, `classify_nodes`) moves next to `OplogGraph` in `aaiclick/oplog/lineage.py`. `internal_api.lineage` and the MCP server gain `list_graph_nodes`; the MCP `instructions` carry the Tier 1 method. `lineage_tools.py` keeps only `LineageToolbox`, `LINEAGE_TOOL_DEFINITIONS` and the LLM text formatting, importing everything else from `oplog`.

**Tech Stack:** Python 3.10+, pydantic, FastMCP, pytest (`orch_ctx` fixture, chdb).

**Spec:** `docs/designs/lineage.md` (Tier 1 tools and scope rule); the three-PR evaluation agreed in session.

## Global Constraints

- CLAUDE.md: all imports at top of file; no `Any` shortcuts; `Literal` over enums; `NamedTuple` over tuples; no history comments; no `__all__`.
- Behaviour of the sandbox is unchanged: same error kinds (`not_select`, `out_of_scope`, `not_found`, `not_live`, `invalid_argument`), same `DEFAULT_ROW_LIMIT = 100`, `ROW_LIMIT_CEILING = 1000`, same pinned ClickHouse settings in `run_select`.
- `aaiclick/ai/` still imports and passes its tests after this PR (it is deleted in PR 2, not here).
- `aaiclick.oplog` must not import from `aaiclick.ai`.
- Test files follow the `python-testing-style` skill; patch targets change to `aaiclick.oplog.query_sandbox.get_ch_client`.

## Review Focus

1. A `query_table` call via MCP naming a table outside the graph is rejected (`Invalid` → MCP ToolError) — `test_mcp.py::test_query_table_rejects_out_of_scope` already pins this; keep it green.
2. `list_graph_nodes` for a target with no lineage raises `NotFound`, not an empty list — Task 3 test.
3. `list_graph_nodes` reports `live: False` for a dropped table instead of raising — Task 3 test (drop the table, then call).
4. `sandboxed_select` with a `row_limit` above the ceiling truncates rather than failing — moved test `test_run_select_truncates_past_the_ceiling_instead_of_failing`.
5. The MCP `tools/list` set changes by exactly one name (`list_graph_nodes`) — `EXPECTED_TOOLS` exact-match test in Task 4.

---

### Task 1: Move the sandbox to `aaiclick/oplog/query_sandbox.py`

**Files:**
- Create: `aaiclick/oplog/query_sandbox.py`
- Create: `aaiclick/oplog/test_query_sandbox.py`
- Modify: `aaiclick/oplog/lineage.py` (append `NodeKind`, `GraphNode`, `classify_nodes`)
- Modify: `aaiclick/oplog/test_graph.py` (append `classify_nodes` test)
- Modify: `aaiclick/ai/agents/lineage_tools.py` (strip moved code, import from oplog)
- Modify: `aaiclick/ai/agents/test_lineage_tools.py` (keep toolbox-only tests)
- Modify: `aaiclick/ai/agents/tools.py:9` (import `describe_table` from oplog)

**Interfaces:**
- Produces, in `aaiclick/oplog/query_sandbox.py`:
  - `ToolErrorKind`, `ToolError(kind, message)`, `TableSchema`, `QueryResult`, `DEFAULT_ROW_LIMIT`, `ROW_LIMIT_CEILING` — unchanged definitions.
  - `validate_select_safety(sql: str, *, scan: str | None = None) -> ToolError | None`
  - `async validate_scope(sql: str, scope_tables: set[str]) -> ToolError | None`
  - `async run_select(sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult`
  - `async describe_table(table: str) -> TableSchema`
  - `async liveness(tables: set[str]) -> dict[str, bool]` (was `_liveness`)
  - **New** `async sandboxed_select(sql: str, scope_tables: set[str], row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult | ToolError` — runs `validate_select_safety`, then `validate_scope`, then `run_select`; returns the first `ToolError` without the toolbox's "Use list_graph_nodes()" suffix.
- Produces, in `aaiclick/oplog/lineage.py`:
  - `NodeKind = Literal["input", "intermediate", "target"]`
  - `class GraphNode(BaseModel)`: `table: str`, `kind: NodeKind`, `operation: str`, `live: bool`, `task_id: int | None = None`, `job_id: int | None = None`
  - `classify_nodes(graph: OplogGraph) -> dict[str, NodeKind]` (was `_classify_nodes`, with `_target_tables` / `_input_tables` moved alongside as private helpers)

- [ ] **Step 1: Create `aaiclick/oplog/test_query_sandbox.py`** by moving from `test_lineage_tools.py` every test that exercises the sandbox through `toolbox.query_table(...)` or `run_select` directly, rewritten to call `sandboxed_select(sql, SCOPE)` where `SCOPE = _sample_graph().tables`. Keep `_sample_graph`, `_mock_query_result`, `_ch_client_with_real_parser`, `OUT_OF_SCOPE_TABLES` helpers. Patch target becomes `aaiclick.oplog.query_sandbox.get_ch_client`. Tests to move: `rejects_out_of_scope_table`, `rejects_table_functions`, `rejects_non_graph_table_identifiers`, `rejects_system_tables`, `never_evaluates_sql_while_validating_scope`, `reports_unparseable_sql`, `happy_path_caps_rows`, `truncation_flag`, `sends_sql_unchanged`, `pins_execution_settings`, `rejects_in_with_out_of_scope_table`, `rejects_settings_clause`, `run_select_truncates_past_the_ceiling_instead_of_failing`, `run_select_is_refused_write_access_by_clickhouse`, `accepts_valid_select`, `rejects_write_statements`, `get_schema_returns_columns` (as `describe_table`), plus a new one:

```python
async def test_liveness_reports_missing_tables():
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))
    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        alive = await liveness({TARGET_TABLE, PERSISTENT_INPUT})
    assert alive == {TARGET_TABLE: True, PERSISTENT_INPUT: False}
```

- [ ] **Step 2: Append to `aaiclick/oplog/test_graph.py`**:

```python
def test_classify_nodes_labels_input_intermediate_target():
    nodes = [
        make_oplog_node("t_1", "filter", {"input": "p_raw"}),
        make_oplog_node("t_2", "aggregate", {"input": "t_1"}),
    ]
    graph = OplogGraph(nodes=nodes, edges=[])
    assert classify_nodes(graph) == {"p_raw": "input", "t_1": "intermediate", "t_2": "target"}
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `uv run pytest aaiclick/oplog/test_query_sandbox.py aaiclick/oplog/test_graph.py -q`
Expected: ImportError on `aaiclick.oplog.query_sandbox` / `classify_nodes`.

- [ ] **Step 4: Create `aaiclick/oplog/query_sandbox.py`** by moving lines `DEFAULT_ROW_LIMIT` … `_liveness` (inclusive of `ToolErrorKind`, `ToolError`, `TableSchema`, `QueryResult`, the `_AST_*` constants, regexes, `validate_select_safety`, `_explain_ast`, `_AstRow`, `_ast_rows`, `_node_name`, `_direct_children`, `_TableReads`, `_table_reads`, `validate_scope`, `run_select`, `describe_table`) from `lineage_tools.py`; rename `_liveness` → `liveness`; add `sandboxed_select`. Module docstring: "Read-only, graph-scoped SQL for lineage triage — the sandbox every agent surface (MCP, CLI, in-process) runs SELECTs through."

- [ ] **Step 5: Append `NodeKind`, `GraphNode`, `_target_tables`, `_input_tables`, `classify_nodes` to `aaiclick/oplog/lineage.py`** (moved from `lineage_tools.py:99-127`, `GraphNode` from `:52-60`).

- [ ] **Step 6: Slim `aaiclick/ai/agents/lineage_tools.py`** to: imports from `aaiclick.oplog.query_sandbox` and `aaiclick.oplog.lineage`; `LineageToolbox` (its `query_table` now: coerce `row_limit`, call `sandboxed_select(sql, self._tables, row_limit)`, append the `list_graph_nodes()` hint on `out_of_scope`; `list_graph_nodes` calls `liveness`; `__init__` uses `classify_nodes`); `_TOOL_HANDLERS`; `_format_tool_result`; `LINEAGE_TOOL_DEFINITIONS`. Update `tools.py:9` to import `describe_table` from `aaiclick.oplog.query_sandbox`.

- [ ] **Step 7: Prune `aaiclick/ai/agents/test_lineage_tools.py`** to the toolbox-only tests: `coerces_string_row_limit`, `rejects_non_numeric_row_limit`, `get_op_sql_unknown_table_not_found`, `list_graph_nodes_classifies_kinds_and_liveness`, `list_graph_nodes_caches_liveness_within_session`, `get_schema_rejects_out_of_scope`, `get_schema_not_live_when_describe_fails`, `lineage_tool_definitions_cover_every_toolbox_method`, the `dispatch_tool_*` tests, and one `out_of_scope` test asserting the "Use list_graph_nodes()" suffix. Re-target patches to `aaiclick.oplog.query_sandbox.get_ch_client`.

- [ ] **Step 8: Run both suites**

Run: `uv run pytest aaiclick/oplog aaiclick/ai/agents/test_lineage_tools.py aaiclick/ai/agents/test_debug_agent.py aaiclick/internal_api/test_lineage.py aaiclick/server/test_mcp.py -q`
Expected: all PASS.

- [ ] **Step 9: Lint and type-check**

Run: `uv run ruff check aaiclick && uv run ruff format --check aaiclick && uv run mypy aaiclick/oplog aaiclick/ai aaiclick/internal_api/lineage.py`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add aaiclick/oplog aaiclick/ai/agents
git commit -m "refactor: move the lineage SQL sandbox and graph classification into oplog"
```

### Task 2: `internal_api.lineage` uses the oplog sandbox and gains `list_graph_nodes`

**Files:**
- Modify: `aaiclick/internal_api/lineage.py`
- Test: `aaiclick/internal_api/test_lineage.py`

**Interfaces:**
- Consumes: Task 1's `sandboxed_select`, `describe_table`, `liveness`, `classify_nodes`, `GraphNode`.
- Produces: `async list_graph_nodes(target_table: str, *, direction: LineageDirection = "backward", max_depth: int = DEFAULT_MAX_DEPTH) -> list[GraphNode]` — sorted by table; raises `NotFound` when the target has no lineage. `query_table` keeps its signature but is implemented as `sandboxed_select` + `Invalid` on `ToolError`.

- [ ] **Step 1: Write failing tests** in `test_lineage.py`:

```python
async def test_list_graph_nodes_reports_kind_and_liveness(revenue_table):
    nodes = await lineage_api.list_graph_nodes(revenue_table, direction="forward", max_depth=3)
    by_table = {n.table: n for n in nodes}
    assert by_table[revenue_table].kind == "target"
    assert by_table[revenue_table].live is True

async def test_list_graph_nodes_marks_dropped_table_not_live(revenue_table):
    await get_ch_client().command(f"DROP TABLE {revenue_table}")
    nodes = await lineage_api.list_graph_nodes(revenue_table, direction="forward", max_depth=3)
    assert {n.table: n.live for n in nodes}[revenue_table] is False

async def test_list_graph_nodes_without_lineage_raises_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await lineage_api.list_graph_nodes("p_never_recorded")
```

(Check how `test_objects.py` drops a CH table and use the same call.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_lineage.py -q -k list_graph_nodes`
Expected: AttributeError `list_graph_nodes`.

- [ ] **Step 3: Implement** — replace the `from aaiclick.ai.agents.lineage_tools import ...` block with imports from `aaiclick.oplog.query_sandbox` / `aaiclick.oplog.lineage`; make `_lineage_scope` return the `OplogGraph` (rename to `_lineage_graph`) so `list_graph_nodes` can classify it and `query_table` / `get_table_schema` read `.tables` from it; add `list_graph_nodes` mirroring `LineageToolbox.list_graph_nodes` without the cache. Update the module docstring: these are the primitives any MCP client composes.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/internal_api/test_lineage.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api/lineage.py aaiclick/internal_api/test_lineage.py
git commit -m "feat(internal_api): list_graph_nodes with kind and liveness for lineage triage"
```

### Task 3: MCP `list_graph_nodes` tool and Tier 1 instructions

**Files:**
- Modify: `aaiclick/server/mcp.py:37-38` (imports), `:104-112` (instructions), after `get_table_schema` (new tool)
- Test: `aaiclick/server/test_mcp.py`

**Interfaces:**
- Consumes: Task 2's `lineage_api.list_graph_nodes`; `GraphNode` from `aaiclick.oplog.lineage`.
- Produces: MCP tool `list_graph_nodes(target_table: str, direction: LineageDirection = "backward", max_depth: int = DEFAULT_MAX_DEPTH) -> list[GraphNode]`, tag `TAG_READ`.

- [ ] **Step 1: Write failing tests** — add `"list_graph_nodes"` to `EXPECTED_TOOLS`; add:

```python
async def test_list_graph_nodes_returns_kind_and_liveness(orch_ctx, mcp_client, revenue_lineage):
    result = await mcp_client.call_tool("list_graph_nodes", {"target_table": "p_mcp_revenue"})
    nodes = [GraphNode.model_validate(n) for n in result.structured_content["result"]]
    assert {n.table for n in nodes} >= {"p_mcp_revenue"}
    assert all(n.live for n in nodes)

async def test_instructions_carry_the_triage_method(mcp_client):
    init = mcp_client.initialize_result
    assert "get_table_schema" in init.instructions and "live" in init.instructions
```

(Check the FastMCP `Client` attribute name for the initialize result; if it is not exposed, assert on `mcp.instructions` directly.)

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest aaiclick/server/test_mcp.py -q -k "registered_tools or list_graph_nodes or instructions"`
Expected: FAIL (tool missing / instructions text missing).

- [ ] **Step 3: Implement** — import `DEFAULT_ROW_LIMIT, QueryResult, TableSchema` from `aaiclick.oplog.query_sandbox` and `GraphNode` from `aaiclick.oplog.lineage`; add the tool under the lineage section; replace the "Tools mirror aaiclick's CLI verbs" instructions with the exact text:

```
Tools mirror aaiclick's CLI verbs one-to-one and run against the same backends as the REST surface under /api/v0 (docs/designs/api_server.md).

Lineage triage (why does table X look wrong?):
1. oplog_subgraph(target_table) — read the operation graph and each node's sql_template first; form a hypothesis before querying.
2. list_graph_nodes(target_table) — every table in scope with kind (input/intermediate/target) and whether it is still live. A table with live=false cannot be queried: say so and stop, or re-run the job with full preservation (run_job) and retry.
3. get_table_schema(table, target_table) — always call this before querying a table; use only column names it returns.
4. query_table(sql, target_table) — read-only SELECT, scoped to the graph, row-capped. Cite the rows it returns as evidence.
```

Drop the comment "the turnkey LLM wrappers are the CLI's explain / debug verbs".

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/server/test_mcp.py aaiclick/server/test_mcp_rbac.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/mcp.py aaiclick/server/test_mcp.py
git commit -m "feat(mcp): list_graph_nodes tool and lineage triage instructions"
```

### Task 4: Point the design doc at the new modules

**Files:**
- Modify: `docs/designs/lineage.md:45`, `:134`, `:144`, `:207`
- Modify: `docs/designs/future.md:148`

- [ ] **Step 1: Update implementation references** — `aaiclick/ai/agents/lineage_tools.py` → `aaiclick/oplog/query_sandbox.py` (sandbox) / `aaiclick/oplog/lineage.py` (`classify_nodes`, `GraphNode`); in the "Agent tools" list add that the same four tools are exposed over MCP (`aaiclick/server/mcp.py`, `list_graph_nodes`, `get_table_schema`, `query_table`, `oplog_subgraph` — rendered SQL is on each node's `sql_template`). Apply the `markdown-style` and `shortify` skills.

- [ ] **Step 2: Full suite, lint, commit**

Run: `uv run pytest aaiclick -q -x -p no:cacheprovider` then `uv run ruff check . && uv run ruff format --check .`
Expected: PASS, clean.

```bash
git add docs/designs/lineage.md docs/designs/future.md
git commit -m "docs: lineage design references the oplog sandbox and MCP tools"
```

- [ ] **Step 3: Push and check CI** — `git push -u origin ccr-19a656ac-q74fns`, then the `check-pr` skill.
