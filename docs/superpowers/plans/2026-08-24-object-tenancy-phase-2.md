# Object Tenancy (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scope persistent ClickHouse objects to a tenant, so two tenants can hold an object of the same name and neither can read or overwrite the other's.

**Architecture:** Two layers, both required. The ClickHouse **table name** carries the tenant (`p_<tenant_id>_<name>`; the default tenant keeps bare `p_<name>`) — that is what makes the physical name unique, and it is what prevents a cross-tenant write, because persistent creates are `CREATE TABLE IF NOT EXISTS`. A **`table_registry.tenant_id`** column carries ownership, and is what `open_object()` and object listing filter on. The active tenant comes from the existing `aaiclick.tenancy` contextvar, exactly as `jobs` / `registered_jobs` already do.

**Tech Stack:** Python 3.11, SQLModel + SQLAlchemy (async), Alembic, ClickHouse (chdb locally, clickhouse-connect distributed), pytest + pytest-asyncio (auto mode).

**Spec:** `docs/designs/tenant_rbac.md` — sections "Object Tenancy (Phase 2)" and "Migration".

## Global Constraints

- **Imports at top of file only.** No imports inside functions, methods, or test functions. Break cycles by moving shared code to a neutral module; `from __future__ import annotations` second; an inline import is a last resort and needs a one-line comment. Never use the `TYPE_CHECKING` pattern.
- **No `__all__` in `__init__.py`.** Import the name; that exports it.
- **`aaiclick/__init__.py` is public API only.** Subpackage `__init__.py` files may re-export internals.
- **Never use `Any` to dodge typing.** Prefer `Literal` over `StrEnum` for closed string sets.
- **No history comments.** Do not write `# Removed: ...` or narrate the refactor in comments.
- **Never hand-write an Alembic migration.** Use the `generate-migration` skill.
- **`tenant_id` column shape, copied from `aaiclick/orchestration/models.py`:**
  ```python
  tenant_id: int = Field(
      default=DEFAULT_TENANT_ID,
      sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
  )
  ```
  Plain `BigInteger`, **not** a DB FK to `tenants` — the auth tables live in a separate package and cross-package DDL coupling buys nothing.
- **`DEFAULT_TENANT_ID = 1`**, from `aaiclick.tenancy`. Unset active tenant means the default tenant, so local mode and existing single-tenant deployments keep working with zero configuration.
- **Tests:** flat module-level functions, no test classes, no `@pytest.mark.asyncio` (auto mode). Test files sit next to the module under test. `filterwarnings = ["error"]` — an unhandled warning fails the test.
- **Run tests with:** `uv run pytest <path> -q --no-cov`
- **Cross-tenant reads return the not-found path, never a forbidden path** — no existence leak.

---

## File Structure

| File | Responsibility | Change |
|-------------------------------------------------|---------------------------------------------------|--------|
| `aaiclick/data/scope.py`                         | Table-name prefix scheme; gains tenant awareness   | Modify |
| `aaiclick/data/test_scope.py`                    | Prefix round-trip tests                            | Modify |
| `aaiclick/orchestration/lifecycle/db_lifecycle.py` | `TableRegistry` model + `OplogTablePayload`      | Modify |
| `aaiclick/orchestration/orch_context.py`         | Stamps `tenant_id` on registry rows                | Modify |
| `aaiclick/data/data_context/data_context.py`     | Builds scoped names; lists / deletes persistent objects | Modify |
| `aaiclick/data/object/ingest.py`                 | `_get_table_schema` registry lookup                | Modify |
| `aaiclick/internal_api/objects.py`               | Object endpoints                                   | Modify |
| `aaiclick/orchestration/migrations/versions/*`   | `table_registry.tenant_id`                         | Generate |

Task order is dependency order: the naming layer (1) is independent, the column (2) must exist before anything reads it (3-5), and the endpoints (6) sit on top.

---

### Task 1: Tenant-aware table-name prefix

**Files:**
- Modify: `aaiclick/data/scope.py`
- Test: `aaiclick/data/test_scope.py` (modify — the file already exists with 17 scope tests; append, do not overwrite)

**Interfaces:**
- Consumes: `DEFAULT_TENANT_ID` from `aaiclick.tenancy`.
- Produces:
  - `make_scoped_table_name(scope: NamedScope, name: str, job_id: int | None = None, snowid: int | None = None, tenant_id: int = DEFAULT_TENANT_ID) -> str`
  - `name_from_table(table_name: str) -> str` — unchanged signature, now also strips a `p_<digits>_` tenant prefix.
  - `tenant_from_table(table_name: str) -> int` — returns `DEFAULT_TENANT_ID` for a bare `p_<name>`.
  - `GLOBAL_TENANT_RE = re.compile(r"^p_(\d+)_")`

Only `scope="global"` gains a tenant prefix. `j_*` and `t_*` are unchanged — they are reachable only through their tenant-scoped job, and the tenant prefix must never stack onto them.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/data/test_scope.py` (17 tests already live there — keep them):

```python
"""Tests for ``aaiclick.data.scope`` tenant-aware prefixing."""

from __future__ import annotations

import pytest

from aaiclick.data.scope import (
    SCOPE_GLOBAL,
    SCOPE_JOB,
    SCOPE_TEMP_NAMED,
    make_scoped_table_name,
    name_from_table,
    scope_of,
    tenant_from_table,
)
from aaiclick.tenancy import DEFAULT_TENANT_ID


def test_default_tenant_keeps_bare_global_prefix():
    """Backward compatibility: existing deployments must not need a rename."""
    table = make_scoped_table_name(SCOPE_GLOBAL, "sales", tenant_id=DEFAULT_TENANT_ID)
    assert table == "p_sales"
    assert name_from_table(table) == "sales"
    assert tenant_from_table(table) == DEFAULT_TENANT_ID


def test_other_tenants_get_a_prefixed_global_name():
    table = make_scoped_table_name(SCOPE_GLOBAL, "sales", tenant_id=7)
    assert table == "p_7_sales"
    assert scope_of(table) == SCOPE_GLOBAL
    assert name_from_table(table) == "sales"
    assert tenant_from_table(table) == 7


def test_two_tenants_may_share_an_object_name():
    """The physical name is what stops a cross-tenant overwrite."""
    a = make_scoped_table_name(SCOPE_GLOBAL, "sales", tenant_id=7)
    b = make_scoped_table_name(SCOPE_GLOBAL, "sales", tenant_id=8)
    assert a != b


def test_leading_underscore_name_does_not_look_tenant_prefixed():
    """``_5_x`` is a legal name; ``p__5_x`` must not parse as tenant 5."""
    table = make_scoped_table_name(SCOPE_GLOBAL, "_5_x", tenant_id=DEFAULT_TENANT_ID)
    assert table == "p__5_x"
    assert tenant_from_table(table) == DEFAULT_TENANT_ID
    assert name_from_table(table) == "_5_x"


def test_job_and_temp_names_never_take_a_tenant_prefix():
    """They reach their tenant through the owning job — no stacking."""
    assert make_scoped_table_name(SCOPE_JOB, "x", job_id=42, tenant_id=7) == "j_42_x"
    assert make_scoped_table_name(SCOPE_TEMP_NAMED, "x", snowid=99, tenant_id=7) == "t_x_99"


def test_job_scope_still_requires_a_job_id():
    with pytest.raises(ValueError, match="requires a job_id"):
        make_scoped_table_name(SCOPE_JOB, "x")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest aaiclick/data/test_scope.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'tenant_from_table'`

- [ ] **Step 3: Implement the tenant-aware prefix**

In `aaiclick/data/scope.py`, add the import and regex near the existing ones:

```python
from aaiclick.tenancy import DEFAULT_TENANT_ID

GLOBAL_TENANT_RE = re.compile(r"^p_(\d+)_")
```

Add `tenant_from_table`:

```python
def tenant_from_table(table_name: str) -> int:
    """Return the tenant a global-scope table belongs to.

    A bare ``p_<name>`` belongs to the default tenant. The parse is
    unambiguous because persistent names may not start with a digit
    (``_validate_persistent_name``), so no default-tenant object can
    produce a ``p_<digits>_`` prefix.
    """
    match = GLOBAL_TENANT_RE.match(table_name)
    return int(match.group(1)) if match else DEFAULT_TENANT_ID
```

Extend `name_from_table`'s global branch to strip the tenant segment too:

```python
    if scope == SCOPE_GLOBAL:
        match = GLOBAL_TENANT_RE.match(table_name)
        if match:
            return table_name[match.end() :]
        return table_name[len(GLOBAL_PREFIX) :]
```

Give `make_scoped_table_name` the new keyword and use it in the global branch only:

```python
def make_scoped_table_name(
    scope: NamedScope,
    name: str,
    job_id: int | None = None,
    snowid: int | None = None,
    tenant_id: int = DEFAULT_TENANT_ID,
) -> str:
    """Build the full CH table name for a scoped named object.

    Args:
        scope: ``"temp_named"``, ``"job"``, or ``"global"``.
        name: Validated persistent name (without prefix).
        job_id: Required when ``scope="job"``.
        snowid: Required when ``scope="temp_named"``.
        tenant_id: Owning tenant. Only ``scope="global"`` encodes it; the
            default tenant keeps the bare ``p_<name>`` form. Job- and
            temp-scoped tables reach their tenant through the owning job.
    """
    if scope == SCOPE_GLOBAL:
        if tenant_id == DEFAULT_TENANT_ID:
            return f"{GLOBAL_PREFIX}{name}"
        return f"{GLOBAL_PREFIX}{tenant_id}_{name}"
```

Leave the `temp_named` and `job` branches exactly as they are.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest aaiclick/data/test_scope.py -q --no-cov`
Expected: PASS (6 passed)

- [ ] **Step 5: Run the full suite — nothing else may move**

Run: `uv run pytest -q --no-cov`
Expected: PASS. Every existing caller omits `tenant_id`, so every name it builds is unchanged.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/scope.py aaiclick/data/test_scope.py
git commit -m "feat: tenant-aware global object table names"
```

---

### Task 2: `table_registry.tenant_id` column and migration

**Files:**
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py`
- Modify: `aaiclick/orchestration/orch_context.py`
- Test: `aaiclick/orchestration/test_persistent.py` (the `orch_ctx` fixture supplies orch_context + task_scope; `background/test_db_lifecycle.py` uses a raw `bg_db` engine and cannot exercise the write path)
- Generate: `aaiclick/orchestration/migrations/versions/<rev>_add_table_registry_tenant_id.py`

**Interfaces:**
- Consumes: `DEFAULT_TENANT_ID`, `get_active_tenant_id` from `aaiclick.tenancy`.
- Produces:
  - `TableRegistry.tenant_id: int`
  - `OplogTablePayload.tenant_id: int` (defaults to `DEFAULT_TENANT_ID`)
  - `_write_table_registry_row` inserts the column.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_persistent.py`:

```python
async def test_register_table_stamps_the_active_tenant(orch_ctx):
    """A registry row records who owns the table, for query scoping.

    Looks the row up *by tenant* rather than by table name: the stamp is
    what this task delivers, and the name gains its tenant prefix in
    Task 3. Asserting the prefixed name here would fail until then.
    """
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="owned", scope="global")

    async with get_sql_session() as session:
        result = await session.execute(select(TableRegistry.table_name).where(TableRegistry.tenant_id == 7))
        assert result.scalar_one().endswith("owned")
```

Add these imports at the top of that file (top-of-file only — never inside the test):

```python
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.tenancy import active_tenant
```

Drop any import the file already has.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: FAIL — `AttributeError: type object 'TableRegistry' has no attribute 'tenant_id'`

- [ ] **Step 3: Add the column and stamp it**

In `aaiclick/orchestration/lifecycle/db_lifecycle.py`, add to the imports:

```python
from ...tenancy import DEFAULT_TENANT_ID
```

Add the field to `TableRegistry`, directly after `table_name`:

```python
    tenant_id: int = Field(
        default=DEFAULT_TENANT_ID,
        sa_column=Column(BigInteger, nullable=False, index=True, server_default=str(DEFAULT_TENANT_ID)),
    )
```

Add the field to `OplogTablePayload`, after `schema_doc`:

```python
    tenant_id: int = DEFAULT_TENANT_ID
```

In `aaiclick/orchestration/orch_context.py`, `register_table` passes the active tenant. Add `get_active_tenant_id` to the existing `from aaiclick.tenancy import ...` line, then:

```python
    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        self._enqueue(
            DBLifecycleMessage(
                DBLifecycleOp.OPLOG_TABLE,
                oplog_table=OplogTablePayload(
                    table_name,
                    self._task_id,
                    self._job_id,
                    self._run_id,
                    schema_doc=schema_doc,
                    tenant_id=get_active_tenant_id(),
                ),
            )
        )
```

In `_write_table_registry_row`, add the column to the INSERT and the parameter dict:

```python
                    text(
                        "INSERT INTO table_registry "
                        "(table_name, tenant_id, job_id, task_id, run_id, created_at, schema_doc) "
                        "VALUES (:table_name, :tenant_id, :job_id, :task_id, :run_id, :created_at, :schema_doc) "
                        "ON CONFLICT (table_name) DO NOTHING"
                    ),
                    {
                        "table_name": p.table_name,
                        "tenant_id": p.tenant_id,
                        "job_id": p.job_id,
                        "task_id": p.task_id,
                        "run_id": p.run_id,
                        "created_at": now,
                        "schema_doc": p.schema_doc,
                    },
```

Leave `ON CONFLICT (table_name) DO NOTHING` alone. It is correct precisely because Task 1 made the physical name tenant-unique — a conflict can now only be the same tenant re-registering its own table.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: PASS

Local and dev builds tables from `SQLModel.metadata`, so no migration is needed for this step to go green.

- [ ] **Step 5: Generate the Alembic migration**

Use the `generate-migration` skill — **never hand-write the file**:

```bash
gh workflow run generate-migration.yaml -f message="add table_registry tenant_id"
```

Then `git pull` and review. The revision must add `tenant_id` as `BigInteger`, `nullable=False`, indexed, with `server_default='1'`, so existing rows backfill to the default tenant — which matches the bare `p_<name>` prefix those tables already carry.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q --no-cov`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add aaiclick/orchestration/ 
git commit -m "feat: record owning tenant on table_registry rows"
```

---

### Task 3: Scope object creation and lookup by tenant

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py`
- Modify: `aaiclick/data/object/ingest.py`
- Test: `aaiclick/orchestration/test_persistent.py`

**Interfaces:**
- Consumes: `make_scoped_table_name(..., tenant_id=...)` and `tenant_from_table` from Task 1; `TableRegistry.tenant_id` from Task 2.
- Produces: `_build_scoped_table` and `open_object` both resolve through the active tenant; `_get_table_schema(table, ch_client)` filters its registry lookup by the active tenant.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/orchestration/test_persistent.py` (add `active_tenant` and `open_object` to its top-of-file imports if absent):

```python
async def test_two_tenants_hold_distinct_objects_of_one_name(orch_ctx):
    """Same name, different tenants — separate tables, separate data."""
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="shared", scope="global")
    with active_tenant(8):
        await create_object_from_value([9], name="shared", scope="global")

    with active_tenant(7):
        seven = await open_object("shared", scope="global")
        assert await seven.data() == [1, 2, 3]
    with active_tenant(8):
        eight = await open_object("shared", scope="global")
        assert await eight.data() == [9]


async def test_open_object_does_not_cross_tenants(orch_ctx):
    """Another tenant's object is missing, not forbidden — no existence leak."""
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="private", scope="global")

    with active_tenant(8):
        with pytest.raises(ObjectNotFoundError):
            await open_object("private", scope="global")
```

Import `ObjectNotFoundError` from `aaiclick.data.errors` at the top of the file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: FAIL — tenant 8 opens tenant 7's `p_private`, because names are not yet tenant-scoped at the call site.

- [ ] **Step 3: Route name-building through the active tenant**

In `aaiclick/data/data_context/data_context.py`, add `from aaiclick.tenancy import get_active_tenant_id` to the top-of-file imports, then pass it in `_build_scoped_table`:

```python
def _build_scoped_table(name: str, scope: NamedScope) -> str:
    """Validate ``name`` and build the full CH table name for a scoped object."""
    _validate_persistent_name(name)
    if scope == SCOPE_TEMP_NAMED:
        return make_scoped_table_name(scope, name, snowid=get_snowflake_id())
    job_id: int | None = None
    if scope == SCOPE_JOB:
        lifecycle = get_data_lifecycle()
        job_id = lifecycle.current_job_id() if lifecycle is not None else None
    return make_scoped_table_name(scope, name, job_id=job_id, tenant_id=get_active_tenant_id())
```

`create_object` and `open_object` both call `_build_scoped_table`, so both become tenant-scoped with no further change.

- [ ] **Step 4: Filter the registry lookup by tenant**

In `aaiclick/data/object/ingest.py`, `_get_table_schema` currently selects on `table_name` alone. Add the tenant predicate so a registry row belonging to another tenant cannot satisfy the lookup:

```python
    async with get_sql_session() as sess:
        result = await sess.execute(
            select(TableRegistry.schema_doc).where(
                TableRegistry.table_name == table,
                TableRegistry.tenant_id == get_active_tenant_id(),
            )
        )
        row = result.one_or_none()
```

Import `get_active_tenant_id` at the **top** of the file:

```python
from aaiclick.tenancy import get_active_tenant_id
```

Do not add it to the lazy import block already inside `_get_table_schema`.
That block exists only to break an orchestration↔data cycle;
`aaiclick.tenancy` is a neutral module that imports neither side, so it
belongs at the top like any other import.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: PASS

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q --no-cov`
Expected: PASS. Tests that never set a tenant run as the default tenant and see bare `p_<name>` throughout.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/data/
git commit -m "feat: scope persistent object create and open by tenant"
```

---

### Task 4: Registry-backed, tenant-filtered listing

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py`
- Test: `aaiclick/orchestration/test_persistent.py`

**Interfaces:**
- Consumes: `TableRegistry.tenant_id` (Task 2), `name_from_table` (Task 1).
- Produces: `list_persistent_objects() -> list[str]` — unchanged signature, now reads `table_registry` filtered by the active tenant instead of scanning `system.tables`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_persistent.py`:

```python
async def test_listing_shows_only_the_active_tenants_objects(orch_ctx):
    with active_tenant(7):
        await create_object_from_value([1], name="seven_only", scope="global")
    with active_tenant(8):
        await create_object_from_value([2], name="eight_only", scope="global")

    with active_tenant(7):
        assert await list_persistent_objects() == ["seven_only"]
    with active_tenant(8):
        assert await list_persistent_objects() == ["eight_only"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py::test_listing_shows_only_the_active_tenants_objects -q --no-cov`
Expected: FAIL — the `system.tables` scan matches `p\_%`, so tenant 7 also sees `p_8_eight_only`.

- [ ] **Step 3: Read the registry instead of scanning ClickHouse**

Replace `list_persistent_objects` in `aaiclick/data/data_context/data_context.py`:

```python
async def list_persistent_objects() -> list[str]:
    """List the active tenant's persistent object names.

    Reads SQL ``table_registry`` rather than scanning ``system.tables``:
    ownership lives in SQL, and a ClickHouse scan cannot tell one tenant's
    tables from another's without re-parsing every prefix.

    Returns:
        List of persistent names (without prefix).
    """
    async with get_sql_session() as session:
        result = await session.execute(
            select(TableRegistry.table_name).where(
                TableRegistry.tenant_id == get_active_tenant_id(),
                TableRegistry.table_name.startswith(GLOBAL_PREFIX),
            )
        )
    return [name_from_table(row[0]) for row in result.all()]
```

Add `GLOBAL_PREFIX` and `name_from_table` to the existing `from ..scope import (...)` block. `TableRegistry`, `get_sql_session`, and `select` come from orchestration, which imports data — so this is a genuine cycle. Follow the pattern already established in `aaiclick/data/object/ingest.py::_get_table_schema`: a lazy import inside the function with a one-line comment naming the cycle.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -q --no-cov`
Expected: PASS.

!!! note "Expect fallout here, and fix the callers rather than the test"
    Listing now requires a SQL session, so any test or caller that listed
    objects under a bare `data_context()` will fail loudly. That is the
    intended contract — `internal_api.objects` already runs under
    `orch_context(with_ch=True)` from both the server and the CLI. Fix the
    caller; do not weaken the test.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/
git commit -m "feat: list persistent objects from the tenant-filtered registry"
```

---

### Task 5: Tenant-scoped delete and purge

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py`
- Test: `aaiclick/orchestration/test_persistent.py`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: `delete_persistent_object` and `delete_persistent_objects` act only on the active tenant, and clear the registry rows they drop.

- [ ] **Step 1: Write the failing test**

```python
async def test_purge_leaves_other_tenants_objects_alone(orch_ctx):
    """A purge is scoped to the caller's tenant, not the whole database."""
    with active_tenant(7):
        await create_object_from_value([1], name="mine", scope="global")
    with active_tenant(8):
        await create_object_from_value([2], name="theirs", scope="global")

    with active_tenant(7):
        deleted = await delete_persistent_objects(after=datetime(2000, 1, 1))
        assert deleted == ["mine"]
    with active_tenant(8):
        assert await list_persistent_objects() == ["theirs"]
```

Add `from datetime import datetime` to the file's top-of-file imports if absent.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py::test_purge_leaves_other_tenants_objects_alone -q --no-cov`
Expected: FAIL — the `system.tables` scan drops both tenants' tables.

- [ ] **Step 3: Scope the drops and clear registry rows**

`delete_persistent_object` already routes through `_build_scoped_table`, so it is tenant-correct after Task 3; it must additionally delete the registry row, or a re-create would hit `ON CONFLICT DO NOTHING` and keep the stale row:

```python
async def delete_persistent_object(name: str, scope: PersistentScope = SCOPE_JOB) -> None:
    table_name = _build_scoped_table(name, scope)
    await get_ch_client().command(f"DROP TABLE IF EXISTS {table_name}")
    await _forget_registry_rows([table_name])
```

Rewrite `delete_persistent_objects` to choose its candidates from the registry, keeping the ClickHouse `metadata_modification_time` filter for the time window:

```python
async def delete_persistent_objects(
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[str]:
    """Drop the active tenant's persistent tables, filtered by creation time.

    Candidates come from ``table_registry`` so the purge cannot reach another
    tenant's tables; the time window is still evaluated against ClickHouse
    ``system.tables.metadata_modification_time``.

    Args:
        after: Drop tables created at or after this time (inclusive).
        before: Drop tables created before this time (exclusive).

    Returns:
        List of deleted persistent names (without prefix).

    Raises:
        ValueError: If neither ``after`` nor ``before`` is specified.
    """
    if after is None and before is None:
        raise ValueError(
            "At least one of 'after' or 'before' must be specified "
            "to prevent accidental deletion of all persistent objects"
        )
    tenant_id = get_active_tenant_id()
    owned = sorted(
        make_scoped_table_name(SCOPE_GLOBAL, n, tenant_id=tenant_id) for n in await list_persistent_objects()
    )
    if not owned:
        return []

    ch = get_ch_client()
    names_lit = ", ".join(f"'{escape_sql_string(t)}'" for t in owned)
    conditions = ["database = currentDatabase()", f"name IN ({names_lit})"]
    if after is not None:
        conditions.append(f"metadata_modification_time >= '{after.strftime('%Y-%m-%d %H:%M:%S')}'")
    if before is not None:
        conditions.append(f"metadata_modification_time < '{before.strftime('%Y-%m-%d %H:%M:%S')}'")

    result = await ch.query(f"SELECT name FROM system.tables WHERE {' AND '.join(conditions)}")
    names = [row[0] for row in result.result_rows]
    for table_name in names:
        await ch.command(f"DROP TABLE IF EXISTS {table_name}")
    await _forget_registry_rows(names)
    return [name_from_table(n) for n in names]
```

Add the helper next to them:

```python
async def _forget_registry_rows(table_names: list[str]) -> None:
    """Delete ``table_registry`` rows for dropped tables.

    Without this a re-created object would hit the registry's
    ``ON CONFLICT (table_name) DO NOTHING`` and keep a stale schema_doc.
    """
    if not table_names:
        return
    async with get_sql_session() as session:
        await session.execute(delete(TableRegistry).where(TableRegistry.table_name.in_(table_names)))
        await session.commit()
```

`escape_sql_string` comes from `..sql_utils`, which `data_context.py` already
imports from — add it to that existing import line. Use the same lazy-import
comment pattern Task 4 established for `TableRegistry` / `get_sql_session`;
`delete` comes from `sqlalchemy`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest aaiclick/orchestration/test_persistent.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -q --no-cov`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/
git commit -m "feat: scope persistent object delete and purge by tenant"
```

---

### Task 6: Tenant-scoped object endpoints and docs

**Files:**
- Modify: `aaiclick/internal_api/objects.py`
- Modify: `docs/designs/tenant_rbac.md`
- Test: `aaiclick/internal_api/test_objects.py`
- Test: `aaiclick/server/routers/test_objects.py`

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: no signature changes. `list_objects`, `get_object`, `delete_object`, `purge_objects` are tenant-scoped because the layer beneath them is.

`_fetch_table_metadata` and `get_object` build table names via `make_scoped_table_name(SCOPE_GLOBAL, n)` — those calls must pass the active tenant, or metadata lookups will miss for every non-default tenant.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/internal_api/test_objects.py` (add `active_tenant` and `errors` to top-of-file imports if absent):

```python
async def test_list_objects_is_tenant_scoped():
    with active_tenant(7):
        await create_object_from_value([1], name="seven", scope="global")
    with active_tenant(8):
        await create_object_from_value([2], name="eight", scope="global")

    with active_tenant(7):
        page = await objects.list_objects()
        assert [item.name for item in page.items] == ["seven"]


async def test_get_object_across_tenants_is_not_found():
    """404, never 403 — a cross-tenant get must not leak existence."""
    with active_tenant(7):
        await create_object_from_value([1], name="seven", scope="global")

    with active_tenant(8):
        with pytest.raises(errors.NotFound):
            await objects.get_object("seven")


async def test_object_detail_carries_row_count_for_a_non_default_tenant():
    """Metadata lookup must use the tenant-prefixed table name."""
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="seven", scope="global")
        detail = await objects.get_object("seven")
        assert detail.row_count == 3
```

The file's autouse `_object_data_ctx` fixture cleans up by listing objects — extend it to clean tenants 7 and 8 as well, wrapping each sweep in `with active_tenant(...)`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_objects.py -q --no-cov`
Expected: FAIL — `row_count` is `None` for tenant 7, because `_fetch_table_metadata` was handed the bare `p_seven` name.

- [ ] **Step 3: Pass the active tenant when building table names**

In `aaiclick/internal_api/objects.py`, add `from aaiclick.tenancy import get_active_tenant_id` at the top, then update both call sites:

```python
    tables = [make_scoped_table_name(SCOPE_GLOBAL, n, tenant_id=get_active_tenant_id()) for n in paged]
```

`get_object` reads `obj.table` from the object `open_object` returned, so it already carries the right name — leave it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest aaiclick/internal_api/test_objects.py aaiclick/server/routers/test_objects.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Update the spec to reference the implementation**

In `docs/designs/tenant_rbac.md`, the "Object Tenancy (Phase 2)" section describes this as planned work. Convert it to implementation references by name — never line numbers:

- Physical namespace: `aaiclick/data/scope.py` — see `make_scoped_table_name`, `tenant_from_table`.
- Ownership: `aaiclick/orchestration/lifecycle/db_lifecycle.py` — see `TableRegistry`; listing in `aaiclick/data/data_context/data_context.py` — see `list_persistent_objects`.

Remove Phase 2 from the "Remaining" list in `docs/designs/future.md`, leaving Phase 3. Do **not** add ✅ markers — an implementation reference is the signal. Then run the `markdown-style` and `shortify` skills over both files.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q --no-cov`
Expected: PASS

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check aaiclick/ && uv run ruff format --check aaiclick/
git add aaiclick/ docs/
git commit -m "feat: tenant-scoped object endpoints"
```

---

## Out of Scope

- **Phase 3 (SPA)** — tenant switcher, membership admin UI, superadmin-gated controls.
- **ClickHouse database-per-tenant** — explicitly rejected in the spec; per-database schema migrations for the managed tables (`operation_log`, `task_logs`, `schema_migrations`) are the cost that rules it out.
- **Opaque object table names** — the end state that retires prefix parsing entirely. Recorded in `docs/designs/future.md`.
