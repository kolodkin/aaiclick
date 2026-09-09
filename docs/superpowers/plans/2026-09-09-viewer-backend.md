# Viewer Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the backend of `docs/designs/viewer.md`: object-scoped queries, saved queries, and dashboards in `internal_api`, exposed over REST, MCP, and CLI, with results in ClickHouse's `JSONCompact` shape.

**Architecture:** A new `aaiclick/viewer/` package holds the view models, the SQLModel tables, and the `where` guard; `aaiclick/internal_api/viewer.py` holds the verbs, built on `open_object(...).view(...)` so every SELECT is produced by the Object API. Three small prerequisites land first in `aaiclick/data`: `Object.select_sql()`, `open_object(..., job_id=)`, and a `query_text()` helper that returns ClickHouse's own output for a named format. REST, MCP, and CLI are thin renderers, as `docs/designs/api_server.md` requires.

**Tech Stack:** Python 3.10+, pydantic 2, SQLModel + Alembic, chdb / clickhouse-connect, FastAPI, fastmcp, argparse, pytest (chdb + SQLite locally).

**Spec:** `docs/designs/viewer.md` (backend sections). Frontend phases (`@data`, `@query`, `@dashboard`) are separate plans.

## Global Constraints

- Every `internal_api` verb is written once and rendered by CLI, REST (`/api/v0`), and MCP (`docs/designs/api_server.md`).
- Tenant is never a parameter: read `get_active_tenant_id()`; REST gets it from `require_tenant`.
- Scope keys: `persistent` or `job:<id|name>`; job names resolve to the latest run, like `get_job`.
- Results are `JSONCompact` with `output_format_json_quote_64bit_integers`, `_quote_decimals`, `_quote_denormals`, `_named_tuples_as_objects` all `1`; CSV is `CSVWithNames`. `limit ≤ 1000`, `max_execution_time` 30 s.
- String sets are `Literal` + module constants, never enums. NamedTuples over plain tuples. All imports at top of file (lazy import only for the documented data↔orchestration cycle, with a comment).
- Tests live beside the module (`test_*.py`), flat functions, no `@pytest.mark.asyncio`, `filterwarnings = error`.
- Migrations come from the `generate-migration` skill (GitHub Actions), never hand-written.
- No `Any` shortcuts except JSON payload values (`list[list[Any]]`), which the spec fixes.

## Decisions taken while planning (spec refinements)

- `query_text` is a module-level function in `aaiclick/data/data_context/ch_client.py` that branches on `is_chdb()`, mirroring `export_query_to_file`. There is no wrapper class for clickhouse-connect to hang a method on.
- The `where` guard rejects statement separators, DDL/DML keywords, and any `SELECT`, `FROM`, `JOIN`, `UNION`, or `WITH` keyword. Without a subquery there is no table position, so no raw table can be reached, and columns named `p_value` or `t_stamp` stay legal. This replaces the spec's "raw prefix" rule; Task 14 updates the spec.
- `fields` is applied as a projection string through `select_sql(columns=...)`, never through `View(selected_fields=[...])`, because a one-element `selected_fields` renames the column to `value`.
- `aai_id` is hidden from results: the default projection is every schema column except `AAI_ID_COLUMN`, and `fields` may not name it.
- Router tests go in `aaiclick/server/routers/test_viewer.py`, matching the existing layout.

## File Structure

| File | Responsibility |
|------|----------------|
| `aaiclick/data/object/object.py` | `Object.select_sql()` public wrapper over `_build_select`; `result()` uses it |
| `aaiclick/data/data_context/data_context.py` | `open_object(..., job_id=)`, `_build_scoped_table(..., job_id=)`, `list_job_tables(job_id)` |
| `aaiclick/data/data_context/ch_client.py` | `query_text(sql, fmt, settings)` |
| `aaiclick/ai/agents/lineage_tools.py` | `validate_where_expression(expr)` sharing the existing regexes |
| `aaiclick/internal_api/jobs.py` | public `resolve_job(ref) -> Job` raising `NotFound` |
| `aaiclick/view_models.py` | `ObjectFilter.job` |
| `aaiclick/internal_api/objects.py` | job-scope listing |
| `aaiclick/viewer/__init__.py` | empty package marker |
| `aaiclick/viewer/scope.py` | `ScopeRef` parse/format |
| `aaiclick/viewer/cell_view.py` | `cell_view_error(text)` YAML shape validation |
| `aaiclick/viewer/view_models.py` | request/response models |
| `aaiclick/viewer/models.py` | `SavedQueryRow` (`viewer_queries`), `DashboardRow` (`viewer_dashboards`) |
| `aaiclick/internal_api/viewer.py` | `ResolvedScope`, `query_object`, saved queries, dashboards, `run_dashboard` |
| `aaiclick/internal_api/__init__.py` | re-export the new verbs |
| `aaiclick/server/routers/viewer.py` | REST under `/viewer` |
| `aaiclick/server/app.py` | include the router |
| `aaiclick/server/mcp.py` | tools |
| `aaiclick/__main__.py`, `aaiclick/cli_renderers.py` | `data query`, `view queries …`, `view dashboards …` |
| `aaiclick/orchestration/migrations/env.py` | import `aaiclick.viewer.models` |
| `docs/designs/api_server.md`, `docs/designs/viewer.md`, `docs/user_guide/object.md` | docs |

---

### Task 1: `Object.select_sql()` and constraint-aware `result()`

**Files:**
- Modify: `aaiclick/data/object/object.py` (near `_build_select`, ~line 370, and `result()` ~line 500)
- Test: `aaiclick/data/object/test_select_sql.py` (new)

**Interfaces:**
- Produces: `Object.select_sql(columns: str = "*", *, order_by=_UNSET, limit=_UNSET, offset=_UNSET) -> str`, inherited by `View` and `LazyOperator`. `View.result()` now honors the view's constraints.

- [ ] **Step 1: Write the failing tests**

```python
"""``Object.select_sql`` — the SELECT an object reads itself with."""

from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value


async def test_select_sql_base_object_is_plain_select(ctx):
    obj = await create_object_from_value({"a": [1, 2], "b": ["x", "y"]})
    assert obj.select_sql() == f"SELECT * FROM {obj.table} ORDER BY aai_id"


async def test_select_sql_view_applies_where_order_limit_offset(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    view = obj.view(where="a > 1", order_by="a DESC", limit=5, offset=1)
    assert view.select_sql(columns="`a`, `b`") == (
        f"SELECT `a`, `b` FROM {obj.table} WHERE (a > 1) ORDER BY a DESC LIMIT 5 OFFSET 1"
    )


async def test_select_sql_per_call_overrides_win(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3]})
    view = obj.view(limit=5)
    assert view.select_sql(limit=2, offset=1).endswith("LIMIT 2 OFFSET 1")


async def test_view_result_honors_constraints(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3]})
    result = await obj.view(where="a >= 2").result()
    assert sorted(row[0] for row in result.result_rows) == [2, 3]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/data/object/test_select_sql.py -q -p no:cacheprovider`
Expected: FAIL with `AttributeError: 'Object' object has no attribute 'select_sql'` (three tests) and the `result()` test returning all three rows.

- [ ] **Step 3: Implement**

In `object.py`, directly after `_build_select` (keep `_build_select` unchanged):

```python
    def select_sql(
        self,
        columns: str = "*",
        *,
        order_by: Any = _UNSET,
        limit: Any = _UNSET,
        offset: Any = _UNSET,
    ) -> str:
        """The SELECT this object reads itself with.

        ``columns`` is the projection (default ``*``); ``order_by`` / ``limit``
        / ``offset`` override the View's stored values for this call only.
        ``View`` and ``LazyOperator`` inherit it; ``data()`` and ``result()``
        read through the same text.
        """
        return self._build_select(columns, default_order_by=None, order_by=order_by, limit=limit, offset=offset)
```

Replace the body of `Object.result()`:

```python
    async def result(self):
        """Query and return the raw ClickHouse query result for this object.

        Honors View constraints (WHERE / ORDER BY / LIMIT / OFFSET). Prefer
        `.data()` for normal use — it returns typed Python values and
        supports orient modes.

        Raises:
            RuntimeError: If the object is stale (already deleted).
        """
        self.checkstale()
        return await self.ch_client.query(self.select_sql())
```

- [ ] **Step 4: Run the tests and the object suite**

Run: `uv run pytest aaiclick/data/object/test_select_sql.py aaiclick/data/object/test_view.py -q -p no:cacheprovider`
Expected: all PASS. If a `test_view.py` test asserted that `result()` ignored constraints, that test encoded the old bug; report it rather than weakening it.

- [ ] **Step 5: Document in the user guide**

In `docs/user_guide/object.md`, API Quick Reference table, add a row after the `view()` entry:

```markdown
| `select_sql(columns="*", *, order_by, limit, offset)` | The SELECT text the object reads with; Views include their constraints |
```

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/test_select_sql.py docs/user_guide/object.md
git commit -m "Object.select_sql(): expose the read SELECT; result() honors View constraints"
```

---

### Task 2: `open_object(job_id=)` and `list_job_tables`

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` (`_build_scoped_table` ~line 286, `open_object` ~line 670, after `list_persistent_tables` ~line 802)
- Modify: `aaiclick/data/data_context/__init__.py` and `aaiclick/data/__init__.py` (export `list_job_tables` beside `list_persistent_tables`)
- Test: `aaiclick/data/data_context/test_persistent.py` (append)

**Interfaces:**
- Produces: `open_object(name, scope=SCOPE_JOB, *, job_id: int | None = None) -> Object`; `list_job_tables(job_id: int) -> list[str]` (tenant-checked registry rows).

- [ ] **Step 1: Write the failing tests** (append to `test_persistent.py`; it already uses the `orch_ctx`/`ctx` fixture style — match whichever fixture the file uses)

```python
async def test_open_object_with_explicit_job_id(ctx):
    obj = await create_object_from_value([1, 2], name="jscoped", scope="job")
    job_id = int(obj.table.split("_")[1])

    reopened = await open_object("jscoped", scope="job", job_id=job_id)

    assert reopened.table == f"j_{job_id}_jscoped"
    assert await reopened.data() == [1, 2]


async def test_open_object_other_job_id_is_not_found(ctx):
    await create_object_from_value([1], name="jscoped2", scope="job")
    with pytest.raises(ObjectNotFoundError):
        await open_object("jscoped2", scope="job", job_id=1)


async def test_list_job_tables_returns_only_that_job(ctx):
    obj = await create_object_from_value([1], name="jt_a", scope="job")
    await create_object_from_value([2], name="jt_p", scope="global")
    job_id = int(obj.table.split("_")[1])

    assert await list_job_tables(job_id) == [f"j_{job_id}_jt_a"]
    assert await list_job_tables(1) == []
```

Add `list_job_tables`, `open_object`, `ObjectNotFoundError` to the file's imports from `aaiclick.data.data_context` (and `import pytest` if absent).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/data/data_context/test_persistent.py -q -p no:cacheprovider -k "job_id or job_tables"`
Expected: FAIL with `TypeError: open_object() got an unexpected keyword argument 'job_id'` and `ImportError` for `list_job_tables`.

- [ ] **Step 3: Implement**

`_build_scoped_table`:

```python
def _build_scoped_table(name: str, scope: NamedScope, *, job_id: int | None = None) -> str:
    """Validate ``name`` and build the full CH table name for a scoped object.

    ``job_id`` overrides the ambient job (from ``task_scope``) for
    ``scope="job"``; callers outside a task, such as the viewer, pass it
    explicitly.
    """
    _validate_persistent_name(name)
    if scope == SCOPE_TEMP_NAMED:
        return make_scoped_table_name(scope, name, snowid=get_snowflake_id())
    if scope == SCOPE_JOB and job_id is None:
        lifecycle = get_data_lifecycle()
        job_id = lifecycle.current_job_id() if lifecycle is not None else None
    return make_scoped_table_name(scope, name, job_id=job_id, tenant_id=get_active_tenant_id())
```

`open_object` signature and first line:

```python
async def open_object(name: str, scope: PersistentScope = SCOPE_JOB, *, job_id: int | None = None) -> Object:
    """Open an existing persistent Object by name.

    Args:
        name: Persistent name (without prefix).
        scope: ``"global"`` → ``p_<name>``; ``"job"`` → ``j_<job_id>_<name>``.
        job_id: The owning job for ``scope="job"``. Defaults to the active
            orch job; pass it explicitly outside a task (REST, MCP, CLI).
    ...
    """
    from ..object import Object
    from ..object.ingest import _get_table_schema

    table_name = _build_scoped_table(name, scope, job_id=job_id)
```

The registry read in `_get_table_schema` already filters on the active tenant, so a foreign job's table raises `LookupError`; convert it where `_get_table_schema` is called:

```python
    try:
        fieldtype, columns = await _get_table_schema(table_name, ch)
    except LookupError as exc:
        raise ObjectNotFoundError(f"Persistent object '{name}' does not exist (table {table_name})") from exc
```

Keep the existing `EXISTS TABLE` check before it. New function after `list_persistent_tables`:

```python
async def list_job_tables(job_id: int) -> list[str]:
    """List the active tenant's CH table names registered under ``job_id``."""
    # Circular dep: orchestration imports the data package at import time
    # (same pattern as list_persistent_tables).
    from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
    from aaiclick.orchestration.sql_context import get_sql_session

    async with get_sql_session() as session:
        result = await session.execute(
            select(TableRegistry.table_name).where(
                TableRegistry.tenant_id == get_active_tenant_id(),
                TableRegistry.job_id == job_id,
                col(TableRegistry.table_name).startswith("j_", autoescape=True),
            )
        )
    return sorted(row[0] for row in result.all())
```

Export `list_job_tables` next to `list_persistent_tables` in `aaiclick/data/data_context/__init__.py` and `aaiclick/data/__init__.py`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/data/data_context/test_persistent.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data
git commit -m "open_object(job_id=) and list_job_tables for callers outside a task"
```

---

### Task 3: `query_text()` in ClickHouse's own output formats

**Files:**
- Modify: `aaiclick/data/data_context/ch_client.py` (after `export_query_to_file`)
- Modify: `aaiclick/data/data_context/chdb_client.py` (add `query_text` method next to `command`)
- Test: `aaiclick/data/data_context/test_query_text.py` (new)

**Interfaces:**
- Produces: `async def query_text(sql: str, fmt: str, settings: dict | None = None) -> str` in `ch_client.py`; `ChdbClient.query_text(sql, fmt, settings) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
"""``query_text``: a SELECT in one of ClickHouse's named output formats."""

from __future__ import annotations

import json

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import query_text

JSON_SETTINGS = {
    "output_format_json_quote_64bit_integers": 1,
    "output_format_json_quote_decimals": 1,
    "output_format_json_quote_denormals": 1,
    "output_format_json_named_tuples_as_objects": 1,
}


async def test_query_text_json_compact_quotes_64bit_and_keeps_structure(ctx):
    obj = await create_object_from_value({"n": [1, 2], "tags": [["a"], ["b", "c"]]})
    text = await query_text(
        f"SELECT toUInt64(n) AS n, tags FROM {obj.table} ORDER BY n", "JSONCompact", JSON_SETTINGS
    )
    doc = json.loads(text)
    assert [c["name"] for c in doc["meta"]] == ["n", "tags"]
    assert doc["data"] == [["1", ["a"]], ["2", ["b", "c"]]]


async def test_query_text_csv_with_names(ctx):
    obj = await create_object_from_value({"n": [1, 2]})
    text = await query_text(f"SELECT n FROM {obj.table} ORDER BY n", "CSVWithNames")
    assert text.splitlines() == ['"n"', "1", "2"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/data/data_context/test_query_text.py -q -p no:cacheprovider`
Expected: FAIL with `ImportError: cannot import name 'query_text'`.

- [ ] **Step 3: Implement**

`chdb_client.py`, after `command`:

```python
    async def query_text(self, query: str, fmt: str, settings: dict | None = None) -> str:
        """Run ``query`` and return ClickHouse's own output in format ``fmt``
        (``JSONCompact``, ``CSVWithNames``, …) as text."""
        result = self._session.query(_with_settings(query, settings), fmt)
        raw = result.bytes()
        return raw.decode("utf-8") if raw else ""
```

`ch_client.py`, after `export_query_to_file`:

```python
async def query_text(sql: str, fmt: str, settings: dict | None = None) -> str:
    """Run ``sql`` and return ClickHouse's own output in format ``fmt`` as text.

    Lets a caller hand ClickHouse's ``JSONCompact`` or ``CSVWithNames`` output
    through unchanged instead of re-serialising Python values. Mirrors
    ``export_query_to_file``: chdb has the method on its adapter, while the
    clickhouse-connect client exposes ``raw_query``.
    """
    client = get_ch_client()
    if is_chdb():
        return await client.query_text(sql, fmt, settings)  # type: ignore[attr-defined]
    raw = await client.raw_query(sql, fmt=fmt, settings=settings)  # type: ignore[attr-defined]
    return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
```

`is_chdb` is already imported in `ch_client.py` (used by `export_query_to_file`).

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/data/data_context/test_query_text.py -q -p no:cacheprovider`
Expected: PASS. (The distributed CI job exercises the clickhouse-connect branch.)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/data_context
git commit -m "query_text(): ClickHouse output in a named format, chdb and clickhouse-connect"
```

---

### Task 4: `validate_where_expression`

**Files:**
- Modify: `aaiclick/ai/agents/lineage_tools.py` (after `validate_select_safety`)
- Test: `aaiclick/ai/agents/test_lineage_tools.py` (append)

**Interfaces:**
- Produces: `validate_where_expression(expr: str) -> ToolError | None`.

- [ ] **Step 1: Write the failing tests** (append; the file already imports from `lineage_tools`, add `validate_where_expression`)

```python
@pytest.mark.parametrize(
    "expr",
    [
        pytest.param("amount > 100 AND name = 'x'", id="plain"),
        pytest.param("event = 'DROP TABLE'", id="keyword-inside-literal"),
        pytest.param("p_value > 1 AND t_stamp < now()", id="prefixed-column-names"),
        pytest.param("replaceAll(name, 'a', 'b') = 'x'", id="function-call"),
    ],
)
def test_validate_where_expression_accepts(expr):
    assert validate_where_expression(expr) is None


@pytest.mark.parametrize(
    "expr",
    [
        pytest.param("1 = 1; DROP TABLE x", id="statement-separator"),
        pytest.param("id IN (SELECT id FROM p_secret)", id="subquery"),
        pytest.param("EXISTS (SELECT 1)", id="exists-subquery"),
        pytest.param("a = 1 UNION ALL SELECT 1", id="union"),
        pytest.param("DELETE FROM x", id="dml"),
    ],
)
def test_validate_where_expression_rejects(expr):
    err = validate_where_expression(expr)
    assert err is not None and err.code == "not_expression"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/ai/agents/test_lineage_tools.py -q -p no:cacheprovider -k where_expression`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement** (add the regex beside the others, and the function after `validate_select_safety`)

```python
# A WHERE expression has no table position unless it opens a subquery; refusing
# these keywords is what keeps an object-scoped query inside its object.
_SUBQUERY_KEYWORDS_RE = re.compile(r"\b(SELECT|FROM|JOIN|UNION|WITH)\b", re.IGNORECASE)
```

```python
def validate_where_expression(expr: str) -> ToolError | None:
    """Reject anything that is not a single boolean expression: statement
    separators, DDL/DML keywords, and any subquery keyword."""
    scan = normalize_sql_for_scan(expr)
    if _SEMICOLON_RE.search(scan) or ";" in scan.strip().rstrip(";"):
        return ToolError("not_expression", "Only a single expression is allowed.")
    if _FORBIDDEN_KEYWORDS_RE.search(scan):
        return ToolError("not_expression", "DDL/DML keywords are rejected in a where expression.")
    if _SUBQUERY_KEYWORDS_RE.search(scan):
        return ToolError("not_expression", "Subqueries are not allowed in a where expression.")
    return None
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/ai/agents/test_lineage_tools.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/ai/agents
git commit -m "validate_where_expression: single expression, no DDL/DML, no subqueries"
```

---

### Task 5: `resolve_job` and job-scope object listing

**Files:**
- Modify: `aaiclick/internal_api/jobs.py` (after `_resolve_job`)
- Modify: `aaiclick/view_models.py` (`ObjectFilter`)
- Modify: `aaiclick/internal_api/objects.py` (`list_objects`)
- Test: `aaiclick/internal_api/test_jobs.py`, `aaiclick/internal_api/test_objects.py` (append)

**Interfaces:**
- Produces: `jobs_api.resolve_job(ref: RefId) -> Job` raising `NotFound`; `ObjectFilter.job: RefId | None`; `list_objects(ObjectFilter(scope="job", job=ref))` returning that job's objects with `scope="job"`.

- [ ] **Step 1: Write the failing tests**

`test_jobs.py` (append; the file already creates jobs through `internal_api.run_job` / factories, reuse the same helper it uses):

```python
async def test_resolve_job_by_id_and_name_and_missing(orch_ctx):
    job = await run_job(RunJobRequest(entrypoint="aaiclick.orchestration.examples.simple:main", name="rj"))
    assert (await jobs.resolve_job(job.id)).id == job.id
    assert (await jobs.resolve_job("rj")).id == job.id
    with pytest.raises(errors.NotFound):
        await jobs.resolve_job("no_such_job")
```

`test_objects.py` (append):

```python
async def test_list_objects_job_scope_by_ref():
    job = await jobs.run_job(RunJobRequest(entrypoint="aaiclick.orchestration.examples.simple:main", name="objs_job"))
    async with task_scope(task_id=get_snowflake_id(), job_id=job.id, run_id=get_snowflake_id()):
        await create_object_from_value([1, 2], name="result", scope="job")
    await create_object_from_value([3], name="persist", scope="global")

    page = await objects.list_objects(ObjectFilter(scope="job", job=job.id))
    assert [(o.name, o.scope, o.table) for o in page.items] == [("result", "job", f"j_{job.id}_result")]

    by_name = await objects.list_objects(ObjectFilter(scope="job", job="objs_job"))
    assert [o.name for o in by_name.items] == ["result"]


async def test_list_objects_job_scope_requires_job():
    with pytest.raises(errors.Invalid):
        await objects.list_objects(ObjectFilter(scope="job"))
```

Add to the imports of `test_objects.py`: `from aaiclick.internal_api import jobs`, `from aaiclick.orchestration.orch_context import task_scope`, `from aaiclick.snowflake import get_snowflake_id`, `from aaiclick.view_models import RunJobRequest`. If `run_job` needs a different entrypoint in this repo, use the one `test_jobs.py` already uses.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_jobs.py aaiclick/internal_api/test_objects.py -q -p no:cacheprovider -k "resolve_job or job_scope"`
Expected: FAIL: `AttributeError: module has no attribute 'resolve_job'`; `ValidationError` for `ObjectFilter(job=...)`.

- [ ] **Step 3: Implement**

`jobs.py`:

```python
async def resolve_job(ref: RefId) -> Job:
    """The job for ``ref``: a numeric id, or the most recent job with that name.

    Raises ``NotFound`` when no job of the active tenant matches.
    """
    job = await _resolve_job(ref)
    if job is None:
        raise NotFound(f"Job not found: {ref}")
    return job
```

`view_models.py`:

```python
class ObjectFilter(BaseModel):
    """Filter parameters for ``internal_api.list_objects``."""

    prefix: str | None = None
    scope: ObjectScope | None = None
    job: RefId | None = None  # required with scope="job": id, or name → latest run
    limit: int = 50
    cursor: str | None = None
```

`objects.py`, replace the scope check and table listing in `list_objects`:

```python
    filter = filter or ObjectFilter()
    if filter.scope == SCOPE_JOB:
        if filter.job is None:
            raise Invalid("scope='job' requires job (id or name)")
        job = await jobs_api.resolve_job(filter.job)
        tables = await list_job_tables(job.id)
        scope: ObjectScope = SCOPE_JOB
    elif filter.scope in (None, SCOPE_GLOBAL):
        tables = await list_persistent_tables()
        scope = SCOPE_GLOBAL
    else:
        raise Invalid(f"scope={filter.scope!r} not supported (global or job)")

    pairs = sorted((name_from_table(t), t) for t in tables)
```

and use `scope=scope` in the `ObjectView(...)` construction. Imports: `from aaiclick.data.data_context import list_job_tables`, `from aaiclick.data.scope import SCOPE_GLOBAL, SCOPE_JOB, ObjectScope, name_from_table`, `from . import jobs as jobs_api`. Update the module docstring: job scope is now supported via `ObjectFilter.job`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/internal_api/test_jobs.py aaiclick/internal_api/test_objects.py -q -p no:cacheprovider`
Expected: PASS, including the existing `test_list_objects_rejects_non_global_scope` (`scope="temp"` still raises `Invalid`).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api aaiclick/view_models.py
git commit -m "list_objects: job scope by id or name via resolve_job"
```

---

### Task 6: `aaiclick/viewer` package — scope keys, cell-view validation, view models

**Files:**
- Create: `aaiclick/viewer/__init__.py` (empty)
- Create: `aaiclick/viewer/scope.py`
- Create: `aaiclick/viewer/cell_view.py`
- Create: `aaiclick/viewer/view_models.py`
- Test: `aaiclick/viewer/test_scope.py`, `aaiclick/viewer/test_cell_view.py`

**Interfaces:**
- Produces:
  - `ScopeRef(kind: Literal["persistent","job"], job: RefId | None)`, `parse_scope(key: str) -> ScopeRef` raising `ValueError`, `scope_key(ref) -> str`.
  - `cell_view_error(text: object) -> str | None`.
  - `OrderBy(name: str, dir: Literal["ASC","DESC"])`, `ObjectQuery`, `ObjectQueryRequest`, `ObjectQueryResult`, `SavedQueryIn`, `SavedQuery`, `SavedQueryFilter`, `DashboardIn`, `Dashboard`, `DashboardSummary`, `DashboardResults`, `Deleted`.

- [ ] **Step 1: Write the failing tests**

`aaiclick/viewer/test_scope.py`:

```python
from __future__ import annotations

import pytest

from aaiclick.viewer.scope import ScopeRef, parse_scope, scope_key


@pytest.mark.parametrize(
    "key, expected",
    [
        pytest.param("persistent", ScopeRef("persistent", None), id="persistent"),
        pytest.param("job:123", ScopeRef("job", 123), id="job-id"),
        pytest.param("job:nightly_etl", ScopeRef("job", "nightly_etl"), id="job-name"),
    ],
)
def test_parse_scope(key, expected):
    assert parse_scope(key) == expected
    assert scope_key(expected) == key


@pytest.mark.parametrize("key", ["", "global", "job:", "job", "task:1"])
def test_parse_scope_rejects(key):
    with pytest.raises(ValueError):
        parse_scope(key)
```

`aaiclick/viewer/test_cell_view.py`:

```python
from __future__ import annotations

import pytest

from aaiclick.viewer.cell_view import cell_view_error


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param("id:\n  type: link\n  value: https://x/{cell}\n", id="link"),
        pytest.param("params:\n  - name: p\n    options: [a, b]\n", id="params-only"),
    ],
)
def test_cell_view_error_accepts(text):
    assert cell_view_error(text) is None


@pytest.mark.parametrize(
    "text, fragment",
    [
        pytest.param("col: [unclosed", "invalid cell_view", id="bad-yaml"),
        pytest.param("- a\n- b\n", "mapping", id="not-a-mapping"),
        pytest.param("id:\n  type: link\n", "value", id="missing-value"),
        pytest.param("id:\n  type: 7\n  value: x\n", "type", id="type-not-string"),
        pytest.param({"id": {"type": "link"}}, "string", id="not-text"),
    ],
)
def test_cell_view_error_rejects(text, fragment):
    err = cell_view_error(text)
    assert err is not None and fragment in err
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/viewer -q -p no:cacheprovider`
Expected: FAIL with `ModuleNotFoundError: No module named 'aaiclick.viewer'`.

- [ ] **Step 3: Implement**

`aaiclick/viewer/scope.py`:

```python
"""Scope keys: the string form of "which objects" a viewer request means."""

from __future__ import annotations

from typing import Literal, NamedTuple

from aaiclick.view_models import RefId

SCOPE_PERSISTENT = "persistent"
SCOPE_JOB_KIND = "job"
ScopeKind = Literal["persistent", "job"]


class ScopeRef(NamedTuple):
    kind: ScopeKind
    job: RefId | None  # id, or name resolving to the latest run; None for persistent


def parse_scope(key: str) -> ScopeRef:
    """``"persistent"`` or ``"job:<id|name>"`` → ``ScopeRef``; ``ValueError`` otherwise."""
    if key == SCOPE_PERSISTENT:
        return ScopeRef(SCOPE_PERSISTENT, None)
    prefix = f"{SCOPE_JOB_KIND}:"
    if key.startswith(prefix) and len(key) > len(prefix):
        ref = key[len(prefix) :]
        return ScopeRef(SCOPE_JOB_KIND, int(ref) if ref.isdigit() else ref)
    raise ValueError(f"scope must be 'persistent' or 'job:<id|name>', got {key!r}")


def scope_key(ref: ScopeRef) -> str:
    return SCOPE_PERSISTENT if ref.kind == SCOPE_PERSISTENT else f"{SCOPE_JOB_KIND}:{ref.job}"
```

`aaiclick/viewer/cell_view.py` (the YAML contract the kernel interprets; validation only):

```python
"""Shape validation for ``cell_view`` YAML — interpreted by the SPA, validated here."""

from __future__ import annotations

import yaml

PARAMS_KEY = "params"


def cell_view_error(text: object) -> str | None:
    """``None`` when ``text`` is empty or a mapping of ``col: {type: str, value: str}``
    (plus an optional ``params`` list); otherwise the reason."""
    if text is None or text == "":
        return None
    if not isinstance(text, str):
        return "cell_view must be a YAML string"
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return f"invalid cell_view YAML: {exc}"
    if doc is None:
        return None
    if not isinstance(doc, dict):
        return "cell_view must be a mapping of column name to {type, value}"
    for col, view in doc.items():
        if col == PARAMS_KEY:
            if not isinstance(view, list):
                return "cell_view params must be a list"
            continue
        if not isinstance(view, dict):
            return f"cell_view[{col!r}] must be a mapping with type and value"
        if not isinstance(view.get("type"), str):
            return f"cell_view[{col!r}].type must be a string"
        if not isinstance(view.get("value"), str):
            return f"cell_view[{col!r}].value must be a string"
    return None
```

`aaiclick/viewer/view_models.py`:

```python
"""Request / response models for the viewer verbs (``internal_api.viewer``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, Field

from aaiclick.ai.agents.lineage_tools import ColumnSchema

ORDER_ASC = "ASC"
ORDER_DESC = "DESC"
OrderDir = Literal["ASC", "DESC"]

FMT_JSON = "json"
FMT_CSV = "csv"
QueryFormat = Literal["json", "csv"]

MAX_LIMIT = 1000


class OrderBy(NamedTuple):
    name: str
    dir: OrderDir


class ObjectQuery(BaseModel):
    """What to read from one object: the part of a request that is saved."""

    scope: str = "persistent"  # "persistent" | "job:<id|name>"
    object: str
    fields: list[str] | None = None  # None = every column
    where: str | None = None  # SQL boolean expression over the object's columns
    order_by: list[OrderBy] = Field(default_factory=list)


class ObjectQueryRequest(ObjectQuery):
    limit: int = Field(default=100, ge=1, le=MAX_LIMIT)
    offset: int = Field(default=0, ge=0)
    fmt: QueryFormat = FMT_JSON


class ObjectQueryResult(BaseModel):
    """``meta`` + ``data`` for ``fmt="json"`` (ClickHouse JSONCompact), ``text`` for CSV."""

    meta: list[ColumnSchema] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    text: str | None = None


class SavedQueryIn(ObjectQuery):
    name: str
    scope: str | None = "persistent"  # None = the query is offered under every scope
    cell_view: str | None = None  # raw YAML, see aaiclick/viewer/cell_view.py


class SavedQuery(SavedQueryIn):
    updated_at: datetime


class SavedQueryFilter(BaseModel):
    scope: str | None = None  # also matches queries saved with no scope
    object: str | None = None
    limit: int = 50


class DashboardIn(BaseModel):
    name: str
    scope: str = "persistent"
    html: str
    queries: dict[str, ObjectQuery]  # panel name → query


class Dashboard(DashboardIn):
    updated_at: datetime


class DashboardSummary(BaseModel):
    name: str
    scope: str
    updated_at: datetime


class DashboardResults(BaseModel):
    """Column-oriented results per panel — the ``window.queries`` contract."""

    results: dict[str, dict[str, list[Any]]]
    meta: dict[str, list[ColumnSchema]]


class Deleted(BaseModel):
    name: str
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/viewer -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/viewer
git commit -m "viewer: scope keys, cell_view validation, view models"
```

---

### Task 7: SQLModel tables and migration

**Files:**
- Create: `aaiclick/viewer/models.py`
- Modify: `aaiclick/orchestration/migrations/env.py` (import block, lines 8-10)
- Test: `aaiclick/viewer/test_models.py`

**Interfaces:**
- Produces: `SavedQueryRow` (`viewer_queries`), `DashboardRow` (`viewer_dashboards`); a generated Alembic revision.

- [ ] **Step 1: Write the failing test**

```python
"""Round-trip the viewer tables through the SQL session."""

from __future__ import annotations

from sqlmodel import select

from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.models import DashboardRow, SavedQueryRow


async def test_viewer_rows_round_trip(orch_ctx_no_ch):
    async with get_sql_session() as session:
        session.add(SavedQueryRow(id=get_snowflake_id(), name="q", object="orders", scope="persistent", where="a > 1"))
        session.add(DashboardRow(id=get_snowflake_id(), name="d", scope="persistent", html="<p/>", queries="{}"))
        await session.commit()
    async with get_sql_session() as session:
        q = (await session.execute(select(SavedQueryRow).where(SavedQueryRow.name == "q"))).scalar_one()
        d = (await session.execute(select(DashboardRow).where(DashboardRow.name == "d"))).scalar_one()
    assert (q.object, q.where, q.tenant_id) == ("orders", "a > 1", 1)
    assert d.html == "<p/>"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest aaiclick/viewer/test_models.py -q -p no:cacheprovider`
Expected: FAIL with `ModuleNotFoundError` for `aaiclick.viewer.models`.

- [ ] **Step 3: Implement the models**

```python
"""SQLModel tables for saved viewer queries and dashboards.

``tenant_id`` is a plain ``BigInteger`` (no FK), and free-text config
(``cell_view`` YAML, ``fields`` / ``order_by`` / ``queries`` JSON) is stored
verbatim — the SPA interprets it; the server validates shape only. Same
conventions as ``aaiclick/orchestration/models.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from sqlalchemy import BigInteger, DateTime, String, Text, UniqueConstraint
from sqlmodel import Column, Field, SQLModel

from ..datetime_utils import utc_now
from ..tenancy import DEFAULT_TENANT_ID


class SavedQueryRow(SQLModel, table=True):
    """A saved object query: scope, object, filter, presentation, cell views."""

    __tablename__: ClassVar[str] = "viewer_queries"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(
        default=DEFAULT_TENANT_ID,
        sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
    )
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    scope: str | None = Field(default=None, sa_column=Column(String, nullable=True, index=True))
    object: str = Field(sa_column=Column(String, nullable=False, index=True))
    where: str | None = Field(default=None, sa_column=Column(Text, nullable=True))
    fields: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # JSON list
    order_by: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # JSON list of [name, dir]
    cell_view: str | None = Field(default=None, sa_column=Column(Text, nullable=True))  # raw YAML
    updated_at: datetime = Field(default_factory=utc_now, sa_column=Column(DateTime, nullable=False))


class DashboardRow(SQLModel, table=True):
    """An agent-authored HTML dashboard over named object queries."""

    __tablename__: ClassVar[str] = "viewer_dashboards"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tenant_id: int = Field(
        default=DEFAULT_TENANT_ID,
        sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
    )
    name: str = Field(sa_column=Column(String, nullable=False, index=True))
    scope: str = Field(sa_column=Column(String, nullable=False))
    html: str = Field(sa_column=Column(Text, nullable=False))
    queries: str = Field(sa_column=Column(Text, nullable=False))  # JSON {panel: ObjectQuery}
    updated_at: datetime = Field(default_factory=utc_now, sa_column=Column(DateTime, nullable=False))
```

In `env.py` add, after the `aaiclick.orchestration.models` import:

```python
import aaiclick.viewer.models  # noqa: F401  # register viewer_queries/viewer_dashboards
```

The test fixtures create tables from `SQLModel.metadata` (via the Alembic upgrade in `orch_context`), so the test cannot pass until the migration exists. Commit the models first, then generate.

- [ ] **Step 4: Commit the models**

```bash
git add aaiclick/viewer/models.py aaiclick/viewer/test_models.py aaiclick/orchestration/migrations/env.py
git commit -m "viewer tables: viewer_queries and viewer_dashboards models"
git push -u origin claude/queryview-aaiclick-plugin-rm5lrl
```

- [ ] **Step 5: Generate the migration with the `generate-migration` skill**

Trigger workflow `generate-migration.yaml` with `message="add viewer_queries and viewer_dashboards"` on this branch (use the `devpowers:action-run` skill through the GitHub MCP, since `gh` is unavailable in this environment), wait for it, then `git pull`. Review the new file under `aaiclick/orchestration/migrations/versions/`: it must create both tables with the unique constraints and indexes above and nothing else.

- [ ] **Step 6: Run the test and the migration suite**

Run: `uv run pytest aaiclick/viewer/test_models.py aaiclick/orchestration -q -p no:cacheprovider -k "viewer_rows or migration"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/orchestration/migrations/versions
git commit -m "migration: viewer_queries and viewer_dashboards"
```

---

### Task 8: `internal_api.viewer.query_object`

**Files:**
- Create: `aaiclick/internal_api/viewer.py`
- Modify: `aaiclick/internal_api/__init__.py` (re-export `query_object`)
- Test: `aaiclick/internal_api/test_viewer.py` (new)

**Interfaces:**
- Consumes: Task 1 `select_sql`, Task 2 `open_object(job_id=)`, Task 3 `query_text`, Task 4 `validate_where_expression`, Task 5 `resolve_job`, Task 6 models.
- Produces: `async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult`; `async def resolve_scope(key: str) -> ResolvedScope` where `ResolvedScope(kind, job_id: int | None, job_name: str | None)`; `async def open_scoped(name: str, scope: ResolvedScope) -> Object`.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for ``aaiclick.internal_api.viewer``."""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.internal_api import errors, jobs, viewer
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.view_models import RunJobRequest
from aaiclick.viewer.view_models import ObjectQueryRequest, OrderBy

pytestmark = pytest.mark.usefixtures("orch_ctx")

ENTRYPOINT = "aaiclick.orchestration.examples.simple:main"  # whatever test_jobs.py uses


async def _seed_orders():
    return await create_object_from_value(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]}, name="orders", scope="global"
    )


async def test_query_object_returns_json_compact_meta_and_data():
    await _seed_orders()
    res = await viewer.query_object(ObjectQueryRequest(object="orders", order_by=[OrderBy("id", "ASC")]))
    assert [c.name for c in res.meta] == ["id", "name", "amount"]
    assert "aai_id" not in [c.name for c in res.meta]
    assert res.data == [[1, "a", 10], [2, "b", 20], [3, "c", 30]]
    assert res.text is None


async def test_query_object_fields_where_order_limit_offset():
    await _seed_orders()
    res = await viewer.query_object(
        ObjectQueryRequest(
            object="orders", fields=["name"], where="amount >= 20", order_by=[OrderBy("amount", "DESC")], limit=1, offset=1
        )
    )
    assert [c.name for c in res.meta] == ["name"]
    assert res.data == [["b"]]


async def test_query_object_csv():
    await _seed_orders()
    res = await viewer.query_object(ObjectQueryRequest(object="orders", fields=["id"], order_by=[OrderBy("id", "ASC")], fmt="csv"))
    assert res.text is not None and res.text.splitlines() == ['"id"', "1", "2", "3"]
    assert res.meta == [] and res.data == []


async def test_query_object_job_scope_by_id_and_name():
    job = await jobs.run_job(RunJobRequest(entrypoint=ENTRYPOINT, name="viewer_job"))
    async with task_scope(task_id=get_snowflake_id(), job_id=job.id, run_id=get_snowflake_id()):
        await create_object_from_value([5, 6], name="result", scope="job")

    by_id = await viewer.query_object(ObjectQueryRequest(scope=f"job:{job.id}", object="result"))
    by_name = await viewer.query_object(ObjectQueryRequest(scope="job:viewer_job", object="result"))
    assert by_id.data == by_name.data == [[5], [6]]


@pytest.mark.parametrize(
    "request_kwargs",
    [
        pytest.param({"object": "orders", "where": "1 = 1; DROP TABLE x"}, id="separator"),
        pytest.param({"object": "orders", "where": "id IN (SELECT 1)"}, id="subquery"),
        pytest.param({"object": "orders", "fields": ["nope"]}, id="unknown-field"),
        pytest.param({"object": "orders", "fields": ["aai_id"]}, id="aai_id-field"),
        pytest.param({"object": "orders", "order_by": [OrderBy("nope", "ASC")]}, id="unknown-order-col"),
        pytest.param({"object": "orders", "scope": "task:1"}, id="bad-scope"),
    ],
)
async def test_query_object_invalid(request_kwargs):
    await _seed_orders()
    with pytest.raises(errors.Invalid):
        await viewer.query_object(ObjectQueryRequest(**request_kwargs))


async def test_query_object_unknown_object_and_job():
    with pytest.raises(errors.NotFound):
        await viewer.query_object(ObjectQueryRequest(object="missing"))
    with pytest.raises(errors.NotFound):
        await viewer.query_object(ObjectQueryRequest(scope="job:no_such", object="x"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider`
Expected: FAIL with `ImportError: cannot import name 'viewer'`.

- [ ] **Step 3: Implement**

```python
"""Internal API for the viewer: object-scoped queries, saved queries, dashboards.

Every function runs inside ``orch_context(with_ch=True)`` and the active
tenant. Queries never see a table name: ``open_object`` resolves ``(scope,
object)`` and the Object API builds the SELECT (see docs/designs/viewer.md).
"""

from __future__ import annotations

import json
from typing import NamedTuple

from aaiclick.ai.agents.lineage_tools import (
    DEFAULT_MAX_EXECUTION_TIME,
    ColumnSchema,
    validate_where_expression,
)
from aaiclick.data.data_context import ObjectNotFoundError, open_object
from aaiclick.data.data_context.ch_client import query_text
from aaiclick.data.models import AAI_ID_COLUMN
from aaiclick.data.object import Object
from aaiclick.data.scope import SCOPE_GLOBAL, SCOPE_JOB
from aaiclick.data.sql_utils import quote_identifier
from aaiclick.viewer.scope import SCOPE_JOB_KIND, SCOPE_PERSISTENT, ScopeKind, parse_scope
from aaiclick.viewer.view_models import (
    FMT_CSV,
    ObjectQueryRequest,
    ObjectQueryResult,
    OrderBy,
)

from . import jobs as jobs_api
from .errors import Invalid, NotFound

JSON_COMPACT = "JSONCompact"
CSV_WITH_NAMES = "CSVWithNames"
JSON_SETTINGS = {
    "output_format_json_quote_64bit_integers": 1,
    "output_format_json_quote_decimals": 1,
    "output_format_json_quote_denormals": 1,
    "output_format_json_named_tuples_as_objects": 1,
}


class ResolvedScope(NamedTuple):
    kind: ScopeKind
    job_id: int | None
    job_name: str | None


async def resolve_scope(key: str) -> ResolvedScope:
    """Parse a scope key and resolve a job reference to one run of the tenant."""
    try:
        ref = parse_scope(key)
    except ValueError as exc:
        raise Invalid(str(exc)) from exc
    if ref.kind == SCOPE_PERSISTENT:
        return ResolvedScope(SCOPE_PERSISTENT, None, None)
    job = await jobs_api.resolve_job(ref.job)  # NotFound for a foreign or missing job
    return ResolvedScope(SCOPE_JOB_KIND, job.id, job.name)


async def open_scoped(name: str, scope: ResolvedScope) -> Object:
    """``open_object`` for a resolved scope; ``NotFound`` when the object is missing."""
    try:
        if scope.kind == SCOPE_PERSISTENT:
            return await open_object(name, scope=SCOPE_GLOBAL)
        return await open_object(name, scope=SCOPE_JOB, job_id=scope.job_id)
    except (ObjectNotFoundError, ValueError) as exc:
        raise NotFound(f"Object not found in scope: {name}") from exc


def _projection(obj: Object, fields: list[str] | None) -> tuple[list[str], str]:
    columns = [c for c in obj.schema.columns if c != AAI_ID_COLUMN]
    if fields is None:
        chosen = columns
    else:
        unknown = [f for f in fields if f not in columns]
        if unknown:
            raise Invalid(f"unknown fields: {', '.join(unknown)}")
        chosen = list(fields)
    if not chosen:
        raise Invalid("fields must name at least one column")
    return chosen, ", ".join(quote_identifier(c) for c in chosen)


def _order_clause(obj: Object, order_by: list[OrderBy]) -> str | None:
    if not order_by:
        return None
    parts = []
    for item in order_by:
        entry = OrderBy._make(item)
        if entry.name not in obj.schema.columns or entry.name == AAI_ID_COLUMN:
            raise Invalid(f"unknown order_by column: {entry.name}")
        parts.append(f"{quote_identifier(entry.name)} {entry.dir}")
    return ", ".join(parts)


async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult:
    """Read one page of an object as ClickHouse JSONCompact (or CSV text)."""
    if request.where is not None and (err := validate_where_expression(request.where)):
        raise Invalid(err.message)
    scope = await resolve_scope(request.scope)
    obj = await open_scoped(request.object, scope)
    _, projection = _projection(obj, request.fields)
    view = obj.view(
        where=request.where, order_by=_order_clause(obj, request.order_by), limit=request.limit, offset=request.offset
    )
    sql = view.select_sql(columns=projection)
    if request.fmt == FMT_CSV:
        return ObjectQueryResult(text=await query_text(sql, CSV_WITH_NAMES))
    text = await query_text(sql, JSON_COMPACT, {**JSON_SETTINGS, "max_execution_time": DEFAULT_MAX_EXECUTION_TIME})
    doc = json.loads(text)
    return ObjectQueryResult(
        meta=[ColumnSchema(name=str(m["name"]), type=str(m["type"])) for m in doc["meta"]],
        data=[list(row) for row in doc["data"]],
    )
```

If `obj.schema.columns` does not exist under that name, use the same accessor `object_to_detail` in `aaiclick/data/object/adapters.py` uses. In `aaiclick/internal_api/__init__.py`, add `query_object` to the re-exports the way `list_objects` is exported.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider`
Expected: PASS. If a Memory-engine object without `aai_id` breaks `select_sql`'s default order (no `ORDER BY`), the tests still pass because they set `order_by` where order matters.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api
git commit -m "internal_api.viewer.query_object: object-scoped page in JSONCompact or CSV"
```

---

### Task 9: Saved queries

**Files:**
- Modify: `aaiclick/internal_api/viewer.py`, `aaiclick/internal_api/__init__.py`
- Test: `aaiclick/internal_api/test_viewer.py` (append)

**Interfaces:**
- Produces: `list_saved_queries(filter: SavedQueryFilter | None = None) -> Page[SavedQuery]`, `save_query(query: SavedQueryIn) -> SavedQuery`, `delete_saved_query(name: str) -> Deleted`, and the row↔model helpers `_row_to_saved_query`, `_apply_saved_query`.

- [ ] **Step 1: Write the failing tests** (append; add `SavedQueryFilter, SavedQueryIn` to the view_models import and `from aaiclick.tenancy import active_tenant`)

```python
async def test_save_query_round_trip_and_upsert():
    await _seed_orders()
    saved = await viewer.save_query(
        SavedQueryIn(name="big", object="orders", where="amount > 15", fields=["name"], order_by=[OrderBy("amount", "DESC")], cell_view="name:\n  type: link\n  value: https://x/{cell}\n")
    )
    assert saved.where == "amount > 15" and saved.order_by == [OrderBy("amount", "DESC")]

    again = await viewer.save_query(SavedQueryIn(name="big", object="orders", where="amount > 25"))
    page = await viewer.list_saved_queries()
    assert [q.name for q in page.items] == ["big"] and page.items[0].where == "amount > 25"
    assert again.updated_at >= saved.updated_at


async def test_list_saved_queries_filters_scope_and_object():
    await viewer.save_query(SavedQueryIn(name="any", object="orders"))
    await viewer.save_query(SavedQueryIn(name="job_only", scope="job:etl", object="result"))
    names = lambda page: sorted(q.name for q in page.items)  # noqa: E731
    assert names(await viewer.list_saved_queries(SavedQueryFilter(scope="job:etl"))) == ["any", "job_only"]
    assert names(await viewer.list_saved_queries(SavedQueryFilter(scope="persistent"))) == ["any"]
    assert names(await viewer.list_saved_queries(SavedQueryFilter(object="result"))) == ["job_only"]


async def test_save_query_validates_where_and_cell_view():
    with pytest.raises(errors.Invalid):
        await viewer.save_query(SavedQueryIn(name="x", object="orders", where="id IN (SELECT 1)"))
    with pytest.raises(errors.Invalid):
        await viewer.save_query(SavedQueryIn(name="x", object="orders", cell_view="col: [unclosed"))


async def test_saved_queries_are_tenant_scoped():
    await viewer.save_query(SavedQueryIn(name="mine", object="orders"))
    with active_tenant(2):
        assert (await viewer.list_saved_queries()).items == []
        with pytest.raises(errors.NotFound):
            await viewer.delete_saved_query("mine")
    assert (await viewer.delete_saved_query("mine")).name == "mine"
    assert (await viewer.list_saved_queries()).items == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider -k saved`
Expected: FAIL with `AttributeError: module 'aaiclick.internal_api.viewer' has no attribute 'save_query'`.

- [ ] **Step 3: Implement** (append to `viewer.py`; add imports `from sqlmodel import col, select`, `from aaiclick.orchestration.sql_context import get_sql_session`, `from aaiclick.snowflake import get_snowflake_id`, `from aaiclick.tenancy import get_active_tenant_id`, `from aaiclick.datetime_utils import utc_now`, `from aaiclick.view_models import Page`, `from aaiclick.viewer.cell_view import cell_view_error`, `from aaiclick.viewer.models import SavedQueryRow`, and `Deleted, SavedQuery, SavedQueryFilter, SavedQueryIn` from the viewer view models)

```python
def _row_to_saved_query(row: SavedQueryRow) -> SavedQuery:
    return SavedQuery(
        name=row.name,
        scope=row.scope,
        object=row.object,
        fields=json.loads(row.fields) if row.fields else None,
        where=row.where,
        order_by=[OrderBy._make(o) for o in json.loads(row.order_by)] if row.order_by else [],
        cell_view=row.cell_view,
        updated_at=row.updated_at,
    )


def _apply_saved_query(row: SavedQueryRow, query: SavedQueryIn) -> None:
    row.scope = query.scope
    row.object = query.object
    row.where = query.where
    row.fields = json.dumps(query.fields) if query.fields is not None else None
    row.order_by = json.dumps([list(OrderBy._make(o)) for o in query.order_by]) if query.order_by else None
    row.cell_view = query.cell_view or None
    row.updated_at = utc_now()


def _validate_saved_query(query: SavedQueryIn) -> None:
    if not query.name.strip():
        raise Invalid("name required")
    if query.scope is not None:
        try:
            parse_scope(query.scope)
        except ValueError as exc:
            raise Invalid(str(exc)) from exc
    if query.where is not None and (err := validate_where_expression(query.where)):
        raise Invalid(err.message)
    if err := cell_view_error(query.cell_view):
        raise Invalid(err)


async def list_saved_queries(filter: SavedQueryFilter | None = None) -> Page[SavedQuery]:
    """Saved queries of the active tenant. ``scope`` matches that scope plus
    queries saved without one; ``object`` matches exactly."""
    filter = filter or SavedQueryFilter()
    predicates = [SavedQueryRow.tenant_id == get_active_tenant_id()]
    if filter.scope is not None:
        predicates.append((SavedQueryRow.scope == filter.scope) | (col(SavedQueryRow.scope).is_(None)))
    if filter.object is not None:
        predicates.append(SavedQueryRow.object == filter.object)
    async with get_sql_session() as session:
        rows = (
            await session.execute(select(SavedQueryRow).where(*predicates).order_by(col(SavedQueryRow.name)).limit(filter.limit))
        ).scalars().all()
    return Page[SavedQuery](items=[_row_to_saved_query(r) for r in rows], total=len(rows))


async def save_query(query: SavedQueryIn) -> SavedQuery:
    """Upsert a saved query by ``(tenant, name)``."""
    _validate_saved_query(query)
    tenant_id = get_active_tenant_id()
    async with get_sql_session() as session:
        row = (
            await session.execute(select(SavedQueryRow).where(SavedQueryRow.tenant_id == tenant_id, SavedQueryRow.name == query.name))
        ).scalar_one_or_none()
        if row is None:
            row = SavedQueryRow(id=get_snowflake_id(), tenant_id=tenant_id, name=query.name, object=query.object)
            session.add(row)
        _apply_saved_query(row, query)
        await session.commit()
        await session.refresh(row)
        return _row_to_saved_query(row)


async def delete_saved_query(name: str) -> Deleted:
    async with get_sql_session() as session:
        row = (
            await session.execute(select(SavedQueryRow).where(SavedQueryRow.tenant_id == get_active_tenant_id(), SavedQueryRow.name == name))
        ).scalar_one_or_none()
        if row is None:
            raise NotFound(f"Saved query not found: {name}")
        await session.delete(row)
        await session.commit()
    return Deleted(name=name)
```

`SavedQueryIn.scope` is `str | None` (Task 6) and is stored as given: `"persistent"` by default, a job key, or `None` for a query offered under every scope. Only a non-`None` scope is parsed. A saved query is never executed by itself — `run_dashboard` (Task 10) and the frontend run it as an `ObjectQueryRequest` under the caller's scope, so `ObjectQueryRequest.scope` stays a plain `str`.

Re-export `list_saved_queries`, `save_query`, `delete_saved_query` in `internal_api/__init__.py`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api aaiclick/viewer
git commit -m "internal_api.viewer: saved queries (list, upsert, delete), tenant-scoped"
```

---

### Task 10: Dashboards and `run_dashboard`

**Files:**
- Modify: `aaiclick/internal_api/viewer.py`, `aaiclick/internal_api/__init__.py`
- Test: `aaiclick/internal_api/test_viewer.py` (append)

**Interfaces:**
- Produces: `list_dashboards() -> Page[DashboardSummary]`, `get_dashboard(name) -> Dashboard`, `save_dashboard(dashboard: DashboardIn) -> Dashboard`, `delete_dashboard(name) -> Deleted`, `run_dashboard(name) -> DashboardResults`.

- [ ] **Step 1: Write the failing tests** (append; import `DashboardIn, ObjectQuery`)

```python
async def test_dashboard_round_trip_and_run():
    await _seed_orders()
    dash = await viewer.save_dashboard(
        DashboardIn(
            name="sales",
            html="<h1>x</h1>",
            queries={"top": ObjectQuery(object="orders", fields=["name", "amount"], order_by=[OrderBy("amount", "DESC")])},
        )
    )
    assert dash.queries["top"].fields == ["name", "amount"]
    assert [d.name for d in (await viewer.list_dashboards()).items] == ["sales"]
    assert (await viewer.get_dashboard("sales")).html == "<h1>x</h1>"

    results = await viewer.run_dashboard("sales")
    assert results.results == {"top": {"name": ["c", "b", "a"], "amount": [30, 20, 10]}}
    assert [c.name for c in results.meta["top"]] == ["name", "amount"]

    assert (await viewer.delete_dashboard("sales")).name == "sales"
    with pytest.raises(errors.NotFound):
        await viewer.get_dashboard("sales")


async def test_save_dashboard_validates_panels():
    with pytest.raises(errors.Invalid):
        await viewer.save_dashboard(DashboardIn(name="bad", html="<p/>", queries={}))
    with pytest.raises(errors.Invalid):
        await viewer.save_dashboard(
            DashboardIn(name="bad", html="<p/>", queries={"p": ObjectQuery(object="orders", where="x IN (SELECT 1)")})
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider -k dashboard`
Expected: FAIL with `AttributeError` for `save_dashboard`.

- [ ] **Step 3: Implement** (append; import `DashboardRow`, and `Dashboard, DashboardIn, DashboardResults, DashboardSummary, ObjectQuery, MAX_LIMIT`)

```python
def _row_to_dashboard(row: DashboardRow) -> Dashboard:
    queries = {panel: ObjectQuery.model_validate(q) for panel, q in json.loads(row.queries).items()}
    return Dashboard(name=row.name, scope=row.scope, html=row.html, queries=queries, updated_at=row.updated_at)


def _validate_dashboard(dashboard: DashboardIn) -> None:
    if not dashboard.name.strip():
        raise Invalid("name required")
    if not dashboard.html.strip():
        raise Invalid("html required")
    if not dashboard.queries:
        raise Invalid("at least one panel query required")
    try:
        parse_scope(dashboard.scope)
    except ValueError as exc:
        raise Invalid(str(exc)) from exc
    for panel, query in dashboard.queries.items():
        if query.where is not None and (err := validate_where_expression(query.where)):
            raise Invalid(f"{panel}: {err.message}")


async def list_dashboards() -> Page[DashboardSummary]:
    async with get_sql_session() as session:
        rows = (
            await session.execute(
                select(DashboardRow).where(DashboardRow.tenant_id == get_active_tenant_id()).order_by(col(DashboardRow.name))
            )
        ).scalars().all()
    return Page[DashboardSummary](
        items=[DashboardSummary(name=r.name, scope=r.scope, updated_at=r.updated_at) for r in rows], total=len(rows)
    )


async def _dashboard_row(session, name: str) -> DashboardRow:
    row = (
        await session.execute(select(DashboardRow).where(DashboardRow.tenant_id == get_active_tenant_id(), DashboardRow.name == name))
    ).scalar_one_or_none()
    if row is None:
        raise NotFound(f"Dashboard not found: {name}")
    return row


async def get_dashboard(name: str) -> Dashboard:
    async with get_sql_session() as session:
        return _row_to_dashboard(await _dashboard_row(session, name))


async def save_dashboard(dashboard: DashboardIn) -> Dashboard:
    """Upsert a dashboard by ``(tenant, name)``."""
    _validate_dashboard(dashboard)
    tenant_id = get_active_tenant_id()
    async with get_sql_session() as session:
        row = (
            await session.execute(select(DashboardRow).where(DashboardRow.tenant_id == tenant_id, DashboardRow.name == dashboard.name))
        ).scalar_one_or_none()
        if row is None:
            row = DashboardRow(id=get_snowflake_id(), tenant_id=tenant_id, name=dashboard.name, scope=dashboard.scope, html="", queries="{}")
            session.add(row)
        row.scope = dashboard.scope
        row.html = dashboard.html
        row.queries = json.dumps({panel: q.model_dump(mode="json") for panel, q in dashboard.queries.items()})
        row.updated_at = utc_now()
        await session.commit()
        await session.refresh(row)
        return _row_to_dashboard(row)


async def delete_dashboard(name: str) -> Deleted:
    async with get_sql_session() as session:
        row = await _dashboard_row(session, name)
        await session.delete(row)
        await session.commit()
    return Deleted(name=name)


async def run_dashboard(name: str) -> DashboardResults:
    """Run every panel query under the dashboard's scope; column-oriented results."""
    dashboard = await get_dashboard(name)
    results: dict[str, dict[str, list]] = {}
    meta: dict[str, list[ColumnSchema]] = {}
    for panel, query in dashboard.queries.items():
        page = await query_object(
            ObjectQueryRequest(**query.model_dump(), scope=dashboard.scope, limit=MAX_LIMIT)  # panel scope = dashboard scope
        )
        names = [c.name for c in page.meta]
        results[panel] = {n: [row[i] for row in page.data] for i, n in enumerate(names)}
        meta[panel] = page.meta
    return DashboardResults(results=results, meta=meta)
```

`ObjectQuery.model_dump()` includes `scope`; passing `scope=dashboard.scope` afterwards overrides it — write it as `{**query.model_dump(), "scope": dashboard.scope, "limit": MAX_LIMIT}` unpacked once to avoid a duplicate-keyword error. Re-export the five functions in `internal_api/__init__.py`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/internal_api/test_viewer.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api
git commit -m "internal_api.viewer: dashboards (CRUD) and run_dashboard"
```

---

### Task 11: REST router

**Files:**
- Create: `aaiclick/server/routers/viewer.py`
- Modify: `aaiclick/server/app.py` (import line and the tenant-scoped router tuple)
- Test: `aaiclick/server/routers/test_viewer.py` (new)

**Interfaces:**
- Produces routes under `/api/v0/viewer`: `POST /query`, `GET /queries`, `PUT /queries/{name}`, `DELETE /queries/{name}`, `GET /dashboards`, `GET /dashboards/{name}`, `PUT /dashboards/{name}`, `DELETE /dashboards/{name}`, `POST /dashboards/{name}:run`.

- [ ] **Step 1: Write the failing tests**

```python
from __future__ import annotations

from aaiclick.data.data_context import create_object_from_value
from aaiclick.view_models import Page, Problem, ProblemCode
from aaiclick.viewer.view_models import Dashboard, DashboardResults, ObjectQueryResult, SavedQuery

from ..app import API_PREFIX

V = f"{API_PREFIX}/viewer"


async def test_query_object_route(orch_ctx, app_client):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="http_orders", scope="global")
    response = await app_client.post(f"{V}/query", json={"object": "http_orders", "order_by": [["id", "ASC"]]})
    assert response.status_code == 200
    result = ObjectQueryResult.model_validate(response.json())
    assert [c.name for c in result.meta] == ["id", "name"] and result.data == [[1, "a"], [2, "b"]]


async def test_query_object_route_errors(orch_ctx, app_client):
    missing = await app_client.post(f"{V}/query", json={"object": "nope"})
    assert missing.status_code == 404 and Problem.model_validate(missing.json()).code is ProblemCode.NOT_FOUND
    bad = await app_client.post(f"{V}/query", json={"object": "nope", "where": "1; DROP TABLE x"})
    assert bad.status_code == 422


async def test_saved_query_and_dashboard_routes(orch_ctx, app_client):
    await create_object_from_value({"id": [1]}, name="http_o", scope="global")
    put = await app_client.put(f"{V}/queries/q1", json={"object": "http_o", "where": "id > 0"})
    assert put.status_code == 200 and SavedQuery.model_validate(put.json()).name == "q1"
    listed = Page[SavedQuery].model_validate((await app_client.get(f"{V}/queries")).json())
    assert [q.name for q in listed.items] == ["q1"]

    put_d = await app_client.put(f"{V}/dashboards/d1", json={"html": "<p/>", "queries": {"p": {"object": "http_o"}}})
    assert put_d.status_code == 200 and Dashboard.model_validate(put_d.json()).name == "d1"
    run = await app_client.post(f"{V}/dashboards/d1:run")
    assert DashboardResults.model_validate(run.json()).results == {"p": {"id": [1]}}

    assert (await app_client.delete(f"{V}/queries/q1")).status_code == 200
    assert (await app_client.delete(f"{V}/dashboards/d1")).status_code == 200
    assert (await app_client.get(f"{V}/dashboards/d1")).status_code == 404


async def test_viewer_requires_auth(orch_ctx, anon_client):
    response = await anon_client.post(f"{V}/query", json={"object": "x"})
    assert response.status_code in (200, 401)  # 401 in distributed mode; local mode has auth off
```

For the auth test, follow what `aaiclick/server/routers/test_objects.py` or the auth tests do to assert 401 only when `config.auth_enabled()`; if that helper exists, use it and drop the `200` alternative.

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest aaiclick/server/routers/test_viewer.py -q -p no:cacheprovider`
Expected: FAIL with 404 responses (routes absent).

- [ ] **Step 3: Implement**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import viewer as viewer_api
from aaiclick.view_models import Page
from aaiclick.viewer.view_models import (
    Dashboard,
    DashboardIn,
    DashboardResults,
    DashboardSummary,
    Deleted,
    ObjectQueryRequest,
    ObjectQueryResult,
    SavedQuery,
    SavedQueryFilter,
    SavedQueryIn,
)

from ..deps import orch_scope_with_ch
from ..errors import problem_responses

router = APIRouter(prefix="/viewer", tags=["viewer"], dependencies=[Depends(orch_scope_with_ch)])


@router.post("/query", response_model=ObjectQueryResult, responses=problem_responses(404, 422))
async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult:
    return await viewer_api.query_object(request)


@router.get("/queries", response_model=Page[SavedQuery])
async def list_saved_queries(filter: SavedQueryFilter = Depends()) -> Page[SavedQuery]:
    return await viewer_api.list_saved_queries(filter)


class SavedQueryBody(SavedQueryIn):
    name: str = ""  # taken from the path


@router.put("/queries/{name}", response_model=SavedQuery, responses=problem_responses(422))
async def save_query(name: str, body: SavedQueryBody) -> SavedQuery:
    return await viewer_api.save_query(SavedQueryIn(**{**body.model_dump(), "name": name}))


@router.delete("/queries/{name}", response_model=Deleted, responses=problem_responses(404))
async def delete_saved_query(name: str) -> Deleted:
    return await viewer_api.delete_saved_query(name)


@router.get("/dashboards", response_model=Page[DashboardSummary])
async def list_dashboards() -> Page[DashboardSummary]:
    return await viewer_api.list_dashboards()


@router.get("/dashboards/{name}", response_model=Dashboard, responses=problem_responses(404))
async def get_dashboard(name: str) -> Dashboard:
    return await viewer_api.get_dashboard(name)


class DashboardBody(DashboardIn):
    name: str = ""  # taken from the path


@router.put("/dashboards/{name}", response_model=Dashboard, responses=problem_responses(422))
async def save_dashboard(name: str, body: DashboardBody) -> Dashboard:
    return await viewer_api.save_dashboard(DashboardIn(**{**body.model_dump(), "name": name}))


@router.delete("/dashboards/{name}", response_model=Deleted, responses=problem_responses(404))
async def delete_dashboard(name: str) -> Deleted:
    return await viewer_api.delete_dashboard(name)


@router.post("/dashboards/{name}:run", response_model=DashboardResults, responses=problem_responses(404, 422))
async def run_dashboard(name: str) -> DashboardResults:
    return await viewer_api.run_dashboard(name)
```

In `app.py`, add `viewer` to `from .routers import execution_workers, jobs, objects, registered_jobs, tasks, viewer` and `viewer.router,` to the tenant-scoped tuple.

- [ ] **Step 4: Run tests and regenerate the OpenAPI schema**

Run: `uv run pytest aaiclick/server/routers/test_viewer.py -q -p no:cacheprovider`
Expected: PASS.
Then run the OpenAPI/type generation the repo's CI checks (`docs/designs/frontend.md`: `npm run gen-types`) and commit `src/api/schema.ts` if it changed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server src/api/schema.ts
git commit -m "REST: /viewer routes for object queries, saved queries, dashboards"
```

---

### Task 12: MCP tools

**Files:**
- Modify: `aaiclick/server/mcp.py` (new section after the objects section)
- Test: `aaiclick/server/test_mcp.py` (`EXPECTED_TOOLS` and one tool test)

**Interfaces:**
- Produces tools: `query_object`, `list_saved_queries`, `save_query`, `delete_saved_query`, `list_dashboards`, `get_dashboard`, `save_dashboard`, `delete_dashboard`, `run_dashboard`.

- [ ] **Step 1: Write the failing tests**

Add the nine names to `EXPECTED_TOOLS`, and append:

```python
async def test_query_object_tool_round_trips(orch_ctx, mcp_client):
    await create_object_from_value({"id": [1, 2]}, name="mcp_orders", scope="global")
    result = await mcp_client.call_tool("query_object", {"request": {"object": "mcp_orders", "order_by": [["id", "DESC"]]}})
    parsed = ObjectQueryResult.model_validate(result.structured_content)
    assert parsed.data == [[2], [1]]


async def test_query_object_tool_maps_not_found(orch_ctx, mcp_client):
    with pytest.raises(ToolError):
        await mcp_client.call_tool("query_object", {"request": {"object": "missing"}})
```

Add `from aaiclick.data.data_context import create_object_from_value` and `from aaiclick.viewer.view_models import ObjectQueryResult` to the test imports.

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest aaiclick/server/test_mcp.py -q -p no:cacheprovider`
Expected: `test_registered_tools_match_expected` FAILS on the set difference; the new tests fail with unknown tool.

- [ ] **Step 3: Implement** (in `mcp.py`, import `viewer as viewer_api` from `aaiclick.internal_api` and the viewer view models)

```python
# --- viewer: object queries, saved queries, dashboards ------------------


@mcp.tool
async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult:
    """Read one page of an object by ``(scope, object)`` with optional
    ``fields`` / ``where`` / ``order_by``; JSONCompact ``meta`` + ``data``."""
    async with orch_context(with_ch=True):
        return await viewer_api.query_object(request)


@mcp.tool
async def list_saved_queries(filter: SavedQueryFilter | None = None) -> Page[SavedQuery]:
    """Saved viewer queries, optionally for one scope or object."""
    async with orch_context(with_ch=True):
        return await viewer_api.list_saved_queries(filter)


@mcp.tool
async def save_query(query: SavedQueryIn) -> SavedQuery:
    """Create or replace a saved viewer query by name."""
    async with orch_context(with_ch=True):
        return await viewer_api.save_query(query)


@mcp.tool
async def delete_saved_query(name: str) -> Deleted:
    """Delete a saved viewer query by name."""
    async with orch_context(with_ch=True):
        return await viewer_api.delete_saved_query(name)


@mcp.tool
async def list_dashboards() -> Page[DashboardSummary]:
    """Dashboards of the active tenant."""
    async with orch_context(with_ch=True):
        return await viewer_api.list_dashboards()


@mcp.tool
async def get_dashboard(name: str) -> Dashboard:
    """A dashboard's HTML and panel queries."""
    async with orch_context(with_ch=True):
        return await viewer_api.get_dashboard(name)


@mcp.tool
async def save_dashboard(dashboard: DashboardIn) -> Dashboard:
    """Create or replace a dashboard: HTML plus named object queries."""
    async with orch_context(with_ch=True):
        return await viewer_api.save_dashboard(dashboard)


@mcp.tool
async def delete_dashboard(name: str) -> Deleted:
    """Delete a dashboard by name."""
    async with orch_context(with_ch=True):
        return await viewer_api.delete_dashboard(name)


@mcp.tool
async def run_dashboard(name: str) -> DashboardResults:
    """Run every panel query of a dashboard; column-oriented results."""
    async with orch_context(with_ch=True):
        return await viewer_api.run_dashboard(name)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/server/test_mcp.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server
git commit -m "MCP: viewer tools"
```

---

### Task 13: CLI

**Files:**
- Modify: `aaiclick/__main__.py` (docstring, handlers near `_run_data_purge`, parsers after the `data purge` block, dispatch after the `data` branch)
- Modify: `aaiclick/cli_renderers.py` (after `render_objects_purged`)
- Test: `aaiclick/test_cli_viewer.py` (new)

**Interfaces:**
- Produces commands: `data query <object> [--scope] [--where] [--fields] [--order-by] [--limit] [--offset] [--csv] [--json]`; `view queries list [--scope] [--object]`, `view queries save <name> <object> [--scope] [--where] [--fields] [--order-by] [--cell-view FILE]`, `view queries delete <name>`; `view dashboards list|get <name>|save <name> --file FILE|delete <name>|run <name>`.

- [ ] **Step 1: Write the failing tests** (handlers are called directly with a Namespace, like other CLI tests that avoid subprocesses; if `aaiclick/test_cli.py` drives `main([...])`, use the same entry)

```python
"""CLI rendering of the viewer verbs."""

from __future__ import annotations

import argparse
import json

import pytest

from aaiclick import __main__ as cli
from aaiclick.data.data_context import create_object_from_value

pytestmark = pytest.mark.usefixtures("orch_ctx")


async def test_data_query_text_and_json(capsys):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="cli_orders", scope="global")
    ns = argparse.Namespace(
        object="cli_orders", scope="persistent", where=None, fields="name", order_by="id:desc",
        limit=100, offset=0, csv=False, json=False,
    )
    await cli._run_data_query(ns)
    out = capsys.readouterr().out
    assert "name" in out and out.index("b") < out.index("a")

    ns.json = True
    await cli._run_data_query(ns)
    doc = json.loads(capsys.readouterr().out)
    assert doc["data"] == [["b"], ["a"]]


async def test_view_queries_save_list_delete(capsys):
    await cli._run_view_queries_save(
        argparse.Namespace(name="q1", object="cli_orders", scope="persistent", where="id > 0", fields=None, order_by=None, cell_view=None, json=True)
    )
    assert json.loads(capsys.readouterr().out)["name"] == "q1"
    await cli._run_view_queries_list(argparse.Namespace(scope=None, object=None, json=False))
    assert "q1" in capsys.readouterr().out
    await cli._run_view_queries_delete(argparse.Namespace(name="q1", json=False))
    assert "q1" in capsys.readouterr().out
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest aaiclick/test_cli_viewer.py -q -p no:cacheprovider`
Expected: FAIL with `AttributeError: module has no attribute '_run_data_query'`.

- [ ] **Step 3: Implement**

Parsing helpers and handlers in `__main__.py` (imports: `from aaiclick.viewer.view_models import DashboardIn, ObjectQueryRequest, OrderBy, SavedQueryFilter, SavedQueryIn`):

```python
def _parse_order_by(raw: str | None) -> list[OrderBy]:
    """``col:desc,other`` → ``[OrderBy("col", "DESC"), OrderBy("other", "ASC")]``."""
    if not raw:
        return []
    out = []
    for part in raw.split(","):
        name, _, direction = part.strip().partition(":")
        out.append(OrderBy(name, "DESC" if direction.lower() == "desc" else "ASC"))
    return out


def _parse_fields(raw: str | None) -> list[str] | None:
    return [f.strip() for f in raw.split(",")] if raw else None


async def _run_data_query(args: argparse.Namespace) -> None:
    request = ObjectQueryRequest(
        scope=args.scope, object=args.object, fields=_parse_fields(args.fields), where=args.where,
        order_by=_parse_order_by(args.order_by), limit=args.limit, offset=args.offset, fmt="csv" if args.csv else "json",
    )
    result = await _run_data_api(internal_api.query_object(request))
    _render(args, result, cli_renderers.render_query_result)


async def _run_view_queries_list(args: argparse.Namespace) -> None:
    page = await _run_data_api(internal_api.list_saved_queries(SavedQueryFilter(scope=args.scope, object=args.object)))
    _render(args, page, cli_renderers.render_saved_queries_page)


async def _run_view_queries_save(args: argparse.Namespace) -> None:
    cell_view = Path(args.cell_view).read_text() if args.cell_view else None
    query = SavedQueryIn(
        name=args.name, scope=args.scope, object=args.object, where=args.where,
        fields=_parse_fields(args.fields), order_by=_parse_order_by(args.order_by), cell_view=cell_view,
    )
    _render(args, await _run_data_api(internal_api.save_query(query)), cli_renderers.render_saved_query)


async def _run_view_queries_delete(args: argparse.Namespace) -> None:
    _render(args, await _run_data_api(internal_api.delete_saved_query(args.name)), cli_renderers.render_deleted)


async def _run_view_dashboards_list(args: argparse.Namespace) -> None:
    _render(args, await _run_data_api(internal_api.list_dashboards()), cli_renderers.render_dashboards_page)


async def _run_view_dashboards_get(args: argparse.Namespace) -> None:
    _render(args, await _run_data_api(internal_api.get_dashboard(args.name)), cli_renderers.render_dashboard)


async def _run_view_dashboards_save(args: argparse.Namespace) -> None:
    doc = json.loads(Path(args.file).read_text())  # {"scope"?, "html", "queries": {panel: {...}}}
    dashboard = DashboardIn(name=args.name, **doc)
    _render(args, await _run_data_api(internal_api.save_dashboard(dashboard)), cli_renderers.render_dashboard)


async def _run_view_dashboards_delete(args: argparse.Namespace) -> None:
    _render(args, await _run_data_api(internal_api.delete_dashboard(args.name)), cli_renderers.render_deleted)


async def _run_view_dashboards_run(args: argparse.Namespace) -> None:
    _render(args, await _run_data_api(internal_api.run_dashboard(args.name)), cli_renderers.render_dashboard_results)
```

Parsers (after the `data purge` block):

```python
    # data query <object> [--scope] [--where] [--fields] [--order-by] [--limit] [--offset] [--csv]
    data_query_parser = data_subparsers.add_parser("query", help="Read rows of an object")
    data_query_parser.add_argument("object", type=str, help="Object name within the scope")
    data_query_parser.add_argument("--scope", default="persistent", help="persistent (default) or job:<id|name>")
    data_query_parser.add_argument("--where", default=None, help="SQL boolean expression over the object's columns")
    data_query_parser.add_argument("--fields", default=None, help="Comma-separated columns (default: all)")
    data_query_parser.add_argument("--order-by", dest="order_by", default=None, help="col[:asc|desc],... ")
    data_query_parser.add_argument("--limit", type=int, default=100, help="Rows per page (max 1000)")
    data_query_parser.add_argument("--offset", type=int, default=0, help="Row offset")
    data_query_parser.add_argument("--csv", action="store_true", help="Print CSV instead of a table")
    _add_json_flag(data_query_parser)

    # view queries ... / view dashboards ...
    view_parser = subparsers.add_parser("view", help="Saved viewer queries and dashboards")
    subcommands["view"] = view_parser
    view_subparsers = view_parser.add_subparsers(dest="view_command", help="View commands")

    vq = view_subparsers.add_parser("queries", help="Saved queries")
    vq_sub = vq.add_subparsers(dest="queries_command")
    p = vq_sub.add_parser("list", help="List saved queries")
    p.add_argument("--scope", default=None)
    p.add_argument("--object", default=None)
    _add_json_flag(p)
    p = vq_sub.add_parser("save", help="Create or replace a saved query")
    p.add_argument("name")
    p.add_argument("object")
    p.add_argument("--scope", default="persistent")
    p.add_argument("--where", default=None)
    p.add_argument("--fields", default=None)
    p.add_argument("--order-by", dest="order_by", default=None)
    p.add_argument("--cell-view", dest="cell_view", default=None, help="Path to a cell_view YAML file")
    _add_json_flag(p)
    p = vq_sub.add_parser("delete", help="Delete a saved query")
    p.add_argument("name")
    _add_json_flag(p)

    vd = view_subparsers.add_parser("dashboards", help="Dashboards")
    vd_sub = vd.add_subparsers(dest="dashboards_command")
    p = vd_sub.add_parser("list", help="List dashboards")
    _add_json_flag(p)
    p = vd_sub.add_parser("get", help="Show a dashboard")
    p.add_argument("name")
    _add_json_flag(p)
    p = vd_sub.add_parser("save", help="Create or replace a dashboard from a JSON file")
    p.add_argument("name")
    p.add_argument("--file", required=True, help='JSON: {"scope"?, "html", "queries": {panel: {object, ...}}}')
    _add_json_flag(p)
    p = vd_sub.add_parser("delete", help="Delete a dashboard")
    p.add_argument("name")
    _add_json_flag(p)
    p = vd_sub.add_parser("run", help="Run a dashboard's panel queries")
    p.add_argument("name")
    _add_json_flag(p)
```

Dispatch: add `elif args.data_command == "query": asyncio.run(_run_data_query(args))` in the `data` branch, and a new branch:

```python
    elif args.command == "view":
        handlers = {
            ("queries", "list"): _run_view_queries_list,
            ("queries", "save"): _run_view_queries_save,
            ("queries", "delete"): _run_view_queries_delete,
            ("dashboards", "list"): _run_view_dashboards_list,
            ("dashboards", "get"): _run_view_dashboards_get,
            ("dashboards", "save"): _run_view_dashboards_save,
            ("dashboards", "delete"): _run_view_dashboards_delete,
            ("dashboards", "run"): _run_view_dashboards_run,
        }
        key = (args.view_command, getattr(args, "queries_command", None) or getattr(args, "dashboards_command", None))
        handler = handlers.get(key)
        if handler is None:
            subcommands["view"].print_help()
        else:
            asyncio.run(handler(args))
```

Module docstring lines to add under the `data` ones:

```
    python -m aaiclick data query <object> [--scope job:<ref>] [--where EXPR]   # Read rows of an object
    python -m aaiclick view queries list|save|delete                            # Saved viewer queries
    python -m aaiclick view dashboards list|get|save|delete|run                 # Dashboards
```

Renderers in `cli_renderers.py`:

```python
def render_query_result(result: ObjectQueryResult) -> None:
    """Print a query page as an aligned text table, or the CSV text verbatim."""
    if result.text is not None:
        print(result.text)
        return
    names = [c.name for c in result.meta]
    if not names:
        print("No columns")
        return
    widths = [max(len(n), *(len(str(row[i])) for row in result.data)) if result.data else len(n) for i, n in enumerate(names)]
    print("  ".join(n.ljust(widths[i]) for i, n in enumerate(names)))
    print("  ".join("-" * w for w in widths))
    for row in result.data:
        print("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    print(f"\nRows: {len(result.data)}")


def render_saved_queries_page(page: Page[SavedQuery]) -> None:
    if not page.items:
        print("No saved queries")
        return
    print(f"{'Name':<30} {'Scope':<20} {'Object':<30} Where")
    print("-" * 100)
    for q in page.items:
        print(f"{q.name:<30} {q.scope:<20} {q.object:<30} {q.where or ''}")


def render_saved_query(q: SavedQuery) -> None:
    print(f"Name:     {q.name}")
    print(f"Scope:    {q.scope}")
    print(f"Object:   {q.object}")
    print(f"Fields:   {', '.join(q.fields) if q.fields else '*'}")
    print(f"Where:    {q.where or ''}")
    print(f"Order by: {', '.join(f'{o.name} {o.dir}' for o in q.order_by)}")
    print(f"Updated:  {q.updated_at}")


def render_dashboards_page(page: Page[DashboardSummary]) -> None:
    if not page.items:
        print("No dashboards")
        return
    print(f"{'Name':<30} {'Scope':<20} Updated")
    print("-" * 70)
    for d in page.items:
        print(f"{d.name:<30} {d.scope:<20} {d.updated_at}")


def render_dashboard(d: Dashboard) -> None:
    print(f"Name:    {d.name}")
    print(f"Scope:   {d.scope}")
    print("Panels:")
    for panel, q in d.queries.items():
        print(f"  {panel}: {q.object}" + (f" where {q.where}" if q.where else ""))
    print(f"HTML:    {len(d.html)} chars")


def render_dashboard_results(r: DashboardResults) -> None:
    for panel, columns in r.results.items():
        rows = len(next(iter(columns.values()), []))
        print(f"{panel}: {rows} row(s), columns {', '.join(columns)}")


def render_deleted(view: Deleted) -> None:
    print(f"Deleted '{view.name}'")
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest aaiclick/test_cli_viewer.py aaiclick/test_cli.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/__main__.py aaiclick/cli_renderers.py aaiclick/test_cli_viewer.py
git commit -m "CLI: data query, view queries, view dashboards"
```

---

### Task 14: Docs and full verification

**Files:**
- Modify: `docs/designs/api_server.md` (command table and view-model catalogue)
- Modify: `docs/designs/viewer.md` (implementation references; the `where` guard wording; `query_text` location)
- Test: full suite

- [ ] **Step 1: Update `api_server.md`**

Append to the CLI → internal_api → REST → MCP table:

```markdown
| `data query <object>`      | `query_object(request)`            | `POST /viewer/query`               | `query_object`            |
| `view queries list`        | `list_saved_queries(filter)`       | `GET /viewer/queries`              | `list_saved_queries`      |
| `view queries save`        | `save_query(query)`                | `PUT /viewer/queries/{name}`       | `save_query`              |
| `view queries delete`      | `delete_saved_query(name)`         | `DELETE /viewer/queries/{name}`    | `delete_saved_query`      |
| `view dashboards list`     | `list_dashboards()`                | `GET /viewer/dashboards`           | `list_dashboards`         |
| `view dashboards get`      | `get_dashboard(name)`              | `GET /viewer/dashboards/{name}`    | `get_dashboard`           |
| `view dashboards save`     | `save_dashboard(dashboard)`        | `PUT /viewer/dashboards/{name}`    | `save_dashboard`          |
| `view dashboards delete`   | `delete_dashboard(name)`           | `DELETE /viewer/dashboards/{name}` | `delete_dashboard`        |
| `view dashboards run`      | `run_dashboard(name)`              | `POST /viewer/dashboards/{name}:run` | `run_dashboard`         |
```

Add `job` to the `ObjectFilter` row of the view-model catalogue and a line pointing at `aaiclick/viewer/view_models.py` for the viewer models.

- [ ] **Step 2: Update `viewer.md`**

- In "Queries name an object, not a table": replace the raw-prefix sentence with the actual rule — the `where` expression is checked by `validate_where_expression` (no statement separator, no DDL/DML, no `SELECT`/`FROM`/`JOIN`/`UNION`/`WITH`), so no table position exists.
- In Prerequisites: `query_text` is a module function in `ch_client.py`, next to `export_query_to_file`.
- In Scope Model: `ScopeRef` lives in `aaiclick/viewer/scope.py`, not `aaiclick/viewer/view_models.py`.
- Add `**Implementation**:` references (by name, not line) under Internal API (`aaiclick/internal_api/viewer.py` — `query_object`, `save_query`, `run_dashboard`), Storage (`aaiclick/viewer/models.py`), Surfaces (`aaiclick/server/routers/viewer.py`, `aaiclick/server/mcp.py` viewer section, `aaiclick/__main__.py` `_run_data_query`), and Scope Model (`aaiclick/viewer/scope.py`).
- Rollout: mark phase 1 as landed by pointing to the code (no status icons).

Run the `shortify` skill over both docs afterwards.

- [ ] **Step 3: Full verification**

Run: `uv run pytest -q -p no:cacheprovider` and `uv run pre-commit run --all-files` and `npm run check`
Expected: all green.

- [ ] **Step 4: Commit and push, then check CI**

```bash
git add docs aaiclick
git commit -m "docs: viewer backend surfaces and implementation references"
git push -u origin claude/queryview-aaiclick-plugin-rm5lrl
```

Then run the `check-pr` skill once a PR exists.
