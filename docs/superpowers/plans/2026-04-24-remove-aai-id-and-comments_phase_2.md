# Phase 2 — Schema Reads From Registry

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the `system.columns` + YAML-comment schema reconstruction with a read of `table_registry.schema_json` → `SchemaView.model_validate_json()` → in-memory `Schema` dataclass. Extend the `Schema` dataclass with a per-column `fieldtype` field so the hydrated object retains the same information the YAML comments used to provide.

**Depends on:** Phase 1 (needs `ColumnView.fieldtype`, `SchemaView.fieldtype`, `TableRegistry.schema_json`).

**Unlocks:** Phase 3 (once read path is registry-based, the write path can stop emitting COMMENTs).

---

## File Structure

| File                                           | Role                                                                                                                 |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `aaiclick/data/models.py`                      | Modify — add `fieldtype` to `ColumnInfo` dataclass.                                                                  |
| `aaiclick/data/view_models.py`                 | Modify — extend the existing `schema_to_view` / `column_info_to_view` to thread `fieldtype`; add `view_to_schema`.   |
| `aaiclick/data/test_view_models.py` (exists)   | Modify — extend with round-trip tests for the `view_to_schema` converter and `fieldtype` passthrough.                 |
| `aaiclick/data/object/ingest.py`               | Modify — `_get_table_schema` reads the registry instead of `system.columns`.                                         |
| `aaiclick/data/object/test_schema.py` (exists) | Modify — add tests for registry-backed schema reads.                                                                 |

**Do not create `aaiclick/data/schema_converters.py`.** `schema_to_view` and `column_info_to_view` already exist at `aaiclick/data/view_models.py:72,82` and are consumed by `aaiclick/internal_api/objects.py`. Extend them in place and add the reverse helper `view_to_schema(view, *, table)` alongside. Creating a second module with the same function names would leave two implementations and break the existing import in `internal_api`.

Circular-import note: `view_models.py` already imports `ColumnInfo, EngineType, Schema` from `.models` today, and the reverse edge (`models.py → view_models.py`) does not exist, so the converters stay where they are.

---

### Task 2.1: Add per-column `fieldtype` to `ColumnInfo`

**Files:**
- Modify: `aaiclick/data/models.py` — `ColumnInfo` dataclass.
- Modify: `aaiclick/data/object/test_order_by.py` (exists) — tests for `ColumnInfo` live in this package; append there rather than creating a new top-level `aaiclick/data/test_models.py`. A test_models.py file **does not exist** at `aaiclick/data/` today, and the refactor does not justify creating one.

**Background:** Today, per-column fieldtype lives only in the YAML comment. Post-refactor, the in-memory `Schema` must carry it on each `ColumnInfo`. We'll default it to `"s"` so call sites that don't yet pass it still work; Phase 3 populates real values on all write paths.

`ColumnInfo`'s real fields (see `aaiclick/data/models.py:46`) are:

```python
@dataclass
class ColumnInfo:
    type: str                 # ClickHouse base type, e.g. "Int64"
    nullable: bool = False
    array: int = False        # array depth — historical name, 0 for scalar
    low_cardinality: bool = False
```

Every test snippet below uses the real field names — **not** `ch_base_type` or `array_depth`, which were wrong in an earlier draft of this plan.

- [ ] **Step 1: Locate `ColumnInfo`**

Open `aaiclick/data/models.py` at line 46 (`class ColumnInfo`). Note that the `array` attribute is an int (despite the mismatch between the type annotation `int` and the default value `False`, which is a historical quirk — don't change it in this refactor).

- [ ] **Step 2: Write the failing test**

Append to `aaiclick/data/object/test_order_by.py` (or create `aaiclick/data/test_column_info.py` if you prefer a dedicated file — both placements are acceptable):

```python
from aaiclick.data.models import ColumnInfo


def test_column_info_carries_fieldtype():
    ci = ColumnInfo(type="Int64", nullable=False, array=0, fieldtype="a")
    assert ci.fieldtype == "a"


def test_column_info_fieldtype_defaults_to_scalar():
    ci = ColumnInfo(type="String")
    assert ci.fieldtype == "s"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_order_by.py -v -k fieldtype` (or the dedicated file).
Expected: FAIL with unexpected keyword `fieldtype`.

- [ ] **Step 4: Add the field**

In `aaiclick/data/models.py` modify `ColumnInfo`:

```python
from typing import Literal  # add if not already imported

@dataclass
class ColumnInfo:
    """..."""  # keep existing docstring
    type: str
    nullable: bool = False
    array: int = False            # historical: int-typed, defaulted to False
    low_cardinality: bool = False
    fieldtype: Literal["s", "a"] = "s"
```

Preserve field order — only `fieldtype` is new, appended last.

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest aaiclick/data/object/test_order_by.py -v -k fieldtype`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/object/test_order_by.py
git commit -m "feature: add fieldtype to ColumnInfo dataclass"
```

---

### Task 2.2: Extend `column_info_to_view` / `schema_to_view` and add `view_to_schema`

**Files:**
- Modify: `aaiclick/data/view_models.py` — existing `column_info_to_view` (line 72) and `schema_to_view` (line 82) gain `fieldtype` passthrough; add the reverse helper `view_to_schema(view, *, table) -> Schema`.
- Modify: `aaiclick/data/test_view_models.py` — append round-trip tests.

**Background:** Converters are pure functions — no DB, no I/O. They translate between the persistence shape (`SchemaView`) and the runtime shape (`Schema` dataclass). The forward helpers already exist in `view_models.py` and are used by `aaiclick/internal_api/objects.py`. Do not create a second module.

Real field names (re-asserted because earlier drafts were wrong): `ColumnInfo.type`, `ColumnInfo.nullable`, `ColumnInfo.array`, `ColumnInfo.low_cardinality`. `ColumnView.type`, `ColumnView.nullable`, `ColumnView.array_depth`, `ColumnView.low_cardinality`. The two models use different names for the array-depth field — the existing `column_info_to_view` bridges them with `array_depth=int(info.array)`.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/data/test_view_models.py`:

```python
from aaiclick.data.models import ColumnInfo, Schema
from aaiclick.data.view_models import (
    ColumnView,
    SchemaView,
    column_info_to_view,
    schema_to_view,
    view_to_schema,
)


def test_column_info_to_view_threads_fieldtype():
    info = ColumnInfo(type="Int64", array=1, fieldtype="a")
    view = column_info_to_view("votes", info)
    assert view.name == "votes"
    assert view.type == "Int64"
    assert view.array_depth == 1
    assert view.fieldtype == "a"


def test_schema_to_view_round_trip_with_fieldtypes():
    schema = Schema(
        fieldtype="d",
        columns={
            "title": ColumnInfo(type="String", fieldtype="s"),
            "votes": ColumnInfo(type="Int64", array=1, fieldtype="a"),
        },
        table="t_123",
        order_by="(title)",
        engine="MergeTree",
    )
    view = schema_to_view(schema)
    assert view.fieldtype == "d"
    assert [c.name for c in view.columns] == ["title", "votes"]
    assert view.columns[1].array_depth == 1
    assert view.columns[1].fieldtype == "a"


def test_view_to_schema_round_trip():
    view = SchemaView(
        columns=[
            ColumnView(name="x", type="Int64", array_depth=1, fieldtype="a"),
            ColumnView(name="y", type="String", nullable=True, fieldtype="s"),
        ],
        order_by="(x)",
        engine="MergeTree",
        fieldtype="d",
    )
    schema = view_to_schema(view, table="t_987")
    assert schema.table == "t_987"
    assert schema.fieldtype == "d"
    assert list(schema.columns) == ["x", "y"]
    assert schema.columns["x"].array == 1
    assert schema.columns["x"].fieldtype == "a"
    assert schema.columns["y"].nullable is True


def test_schema_view_schema_round_trip_is_identity():
    original = Schema(
        fieldtype="a",
        columns={"value": ColumnInfo(type="Float64", fieldtype="a")},
        table="t_rt",
        order_by=None,
        engine=None,
    )
    restored = view_to_schema(schema_to_view(original), table="t_rt")
    assert restored.fieldtype == original.fieldtype
    assert restored.order_by == original.order_by
    assert restored.engine == original.engine
    assert list(restored.columns) == list(original.columns)
    for k in original.columns:
        assert restored.columns[k].type == original.columns[k].type
        assert restored.columns[k].fieldtype == original.columns[k].fieldtype
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/test_view_models.py -v`
Expected: FAIL — `schema_to_view` does not yet thread `fieldtype`; `view_to_schema` does not exist.

- [ ] **Step 3: Extend the converters in `view_models.py`**

Modify `aaiclick/data/view_models.py` (the existing `column_info_to_view` and `schema_to_view` — **not a new module**):

```python
def column_info_to_view(name: str, info: ColumnInfo) -> ColumnView:
    return ColumnView(
        name=name,
        type=info.type,
        nullable=info.nullable,
        array_depth=int(info.array),
        low_cardinality=info.low_cardinality,
        fieldtype=info.fieldtype,
    )


def schema_to_view(schema: Schema) -> SchemaView:
    return SchemaView(
        columns=[column_info_to_view(name, info) for name, info in schema.columns.items()],
        order_by=schema.order_by,
        engine=schema.engine,
        fieldtype=schema.fieldtype,
    )


def view_to_schema(view: SchemaView, *, table: str) -> Schema:
    """Hydrate a Schema dataclass from a persisted SchemaView."""
    columns = {
        cv.name: ColumnInfo(
            type=cv.type,
            nullable=cv.nullable,
            array=cv.array_depth,
            low_cardinality=cv.low_cardinality,
            fieldtype=cv.fieldtype,
        )
        for cv in view.columns
    }
    return Schema(
        fieldtype=view.fieldtype,
        columns=columns,
        table=table,
        order_by=view.order_by,
        engine=view.engine,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/test_view_models.py -v`
Expected: all four new tests PASS. The existing `test_schema_view_round_trip_with_fieldtype` (added in Phase 1) continues to pass.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/view_models.py aaiclick/data/test_view_models.py
git commit -m "feature: extend schema_to_view with fieldtype; add view_to_schema"
```

---

### Task 2.3: Swap `_get_table_schema` to read from `table_registry`

**Files:**
- Modify: `aaiclick/data/object/ingest.py` — `_get_table_schema` function (currently lines 76-119).
- Modify/create: `aaiclick/data/object/test_schema.py` — read-path tests.

**Background:** Today `_get_table_schema(table, ch_client)` runs a `SELECT name, type, comment FROM system.columns`. We replace it with a SQL lookup of the registry row; if the row is missing or its `schema_json` is null we raise a clear error. There is no fallback.

The SQL session helper is at `aaiclick.orchestration.sql_context.get_sql_session` (also re-exported from `aaiclick.orchestration.orch_context` — see `aaiclick/testing.py:32`). It's an async context manager yielding an `AsyncSession`. Import and use it directly — **no** `data_ctx.sql_session()` method exists on the `data_context()` object.

**Project test fixture is `ctx` (not `data_ctx`)** — see `aaiclick/data/conftest.py:17`. The fixture yields nothing; tests call module-level helpers like `create_object_from_value([...])` and `get_ch_client()` directly. There is no `DataContext` class used at test level.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/data/object/test_schema.py` (the file already exists and is the home of schema tests for Object — use its idiomatic pattern, which is module-level async functions taking the `ctx` fixture):

```python
import json

import pytest
from sqlmodel import select, update

from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.object.ingest import _get_table_schema
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session


async def test_get_table_schema_reads_from_registry(ctx):
    obj = await create_object_from_value([1, 2, 3])  # array Object

    # Simulate what Phase 3's create_object write path will do: update the
    # registry row (oplog_record_table wrote it bare during creation).
    async with get_sql_session() as sess:
        await sess.execute(
            update(TableRegistry)
            .where(TableRegistry.table_name == obj.table)
            .values(
                schema_json=json.dumps({
                    "columns": [{
                        "name": "value", "type": "Int64", "nullable": False,
                        "array_depth": 0, "low_cardinality": False, "fieldtype": "a",
                    }],
                    "order_by": None,
                    "engine": "MergeTree",
                    "fieldtype": "a",
                })
            )
        )
        await sess.commit()

    ch_client = get_ch_client()
    fieldtype, columns = await _get_table_schema(obj.table, ch_client)

    assert fieldtype == "a"
    assert set(columns) == {"value"}
    assert columns["value"].fieldtype == "a"


async def test_get_table_schema_missing_registry_row_raises(ctx):
    ch_client = get_ch_client()
    await ch_client.command(
        "CREATE TABLE t_orphan_test (v Int64) ENGINE = Memory"
    )
    try:
        with pytest.raises(LookupError, match="not registered"):
            await _get_table_schema("t_orphan_test", ch_client)
    finally:
        await ch_client.command("DROP TABLE IF EXISTS t_orphan_test")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_schema.py -v -k "registry or orphan"`
Expected: FAIL — `ImportError` for `view_to_schema` (not yet wired into `ingest.py`), or the current implementation still reads `system.columns` instead of the registry.

- [ ] **Step 3: Rewrite `_get_table_schema`**

In `aaiclick/data/object/ingest.py`, replace the current body (lines 76-119) with:

```python
from sqlmodel import select

from aaiclick.data.view_models import SchemaView, view_to_schema
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session


async def _get_table_schema(table: str, ch_client) -> tuple[str, dict[str, ColumnInfo]]:
    """
    Load a table's schema from the registry.

    Raises LookupError if the table has no registry row or a null schema_json —
    this indicates the table was not created by aaiclick or predates the
    schema_json migration.
    """
    async with get_sql_session() as sess:
        result = await sess.execute(
            select(TableRegistry.schema_json).where(TableRegistry.table_name == table)
        )
        row = result.one_or_none()

    if row is None or row[0] is None:
        raise LookupError(
            f"Table {table!r} is not registered in table_registry (or has no "
            "schema_json). It was either not created by aaiclick, or was created "
            "by a version that predates the schema_json registry."
        )

    view = SchemaView.model_validate_json(row[0])
    schema = view_to_schema(view, table=table)
    return schema.fieldtype, schema.columns
```

The `ch_client` parameter is retained — callers still pass it today — but the new body does not touch it. Add a one-line comment: `# ch_client retained for call-site compatibility; schema now lives in SQL`. Confirm with `rg "_get_table_schema\(" aaiclick/` before considering any signature change.

Also delete the now-dead YAML-parsing code paths at the top of the function (the `ColumnMeta.from_yaml` calls and the `aai_id_fieldtype` / `is_dict` inference block around lines 96-118).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_schema.py -v -k "registry or orphan"`
Expected: both new tests PASS. Other tests in this file will fail — expected, because nothing else writes `schema_json` yet.

- [ ] **Step 5: Run the full data test suite**

Run: `pytest aaiclick/data/ -v`
Expected: widespread failures, because nothing writes `schema_json` yet. Note which tests fail — they are all expected to start passing again once Phase 3 wires the write path. **Do not** modify or skip them. Record the failing-test count so you can verify the number matches after Phase 3.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/ingest.py aaiclick/data/object/test_schema.py
git commit -m "$(cat <<'EOF'
refactor: read schema from table_registry.schema_json

_get_table_schema now loads SchemaView from the registry and
hydrates a Schema dataclass via view_to_schema, dropping the
system.columns + YAML COMMENT path.

Write side (create_object etc.) is updated in the next phase —
many data tests are expected to fail until then.
EOF
)"
```

---

## Phase 2 Complete

At this point:

- `ColumnInfo.fieldtype` carries per-column fieldtype in memory.
- `schema_converters.py` translates between `Schema` (dataclass) and `SchemaView` (Pydantic).
- `_get_table_schema` reads `table_registry.schema_json` only. There is no fallback to `system.columns`.
- Most data tests FAIL — expected, because Phase 3 hasn't wired up writes yet. Do not fix or skip them; proceed to Phase 3.
