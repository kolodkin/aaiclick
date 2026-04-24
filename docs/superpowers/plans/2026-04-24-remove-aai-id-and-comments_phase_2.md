# Phase 2 — Schema Reads From Registry

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the `system.columns` + YAML-comment schema reconstruction with a read of `table_registry.schema_json` → `SchemaView.model_validate_json()` → in-memory `Schema` dataclass. Extend the `Schema` dataclass with a per-column `fieldtype` field so the hydrated object retains the same information the YAML comments used to provide.

**Depends on:** Phase 1 (needs `ColumnView.fieldtype`, `SchemaView.fieldtype`, `TableRegistry.schema_json`).

**Unlocks:** Phase 3 (once read path is registry-based, the write path can stop emitting COMMENTs).

---

## File Structure

| File                                                                    | Role                                                                                           |
|-------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `aaiclick/data/models.py`                                               | Modify — add `fieldtype` to `ColumnInfo` dataclass.                                            |
| `aaiclick/data/schema_converters.py` (new)                              | Create — pure helpers `schema_to_view(Schema) -> SchemaView` and `view_to_schema(SchemaView, table) -> Schema`. |
| `aaiclick/data/test_schema_converters.py` (new)                         | Create — round-trip tests for the converters.                                                  |
| `aaiclick/data/object/ingest.py`                                        | Modify — `_get_table_schema` reads the registry instead of `system.columns`.                   |
| `aaiclick/data/object/test_schema.py` (create if missing)               | Create/modify — tests for registry-backed schema reads.                                        |

**Why a new `schema_converters.py` module?** Phase 3 writes will use the same converters to serialize at create time. Keeping them in a small, dependency-free module sidesteps the circular import risk between `view_models.py` (which today imports from `.models`) and `models.py`.

---

### Task 2.1: Add per-column `fieldtype` to `ColumnInfo`

**Files:**
- Modify: `aaiclick/data/models.py` — `ColumnInfo` dataclass.
- Modify: `aaiclick/data/test_models.py` (create if missing) — test that `fieldtype` round-trips through `ColumnInfo`.

**Background:** Today, per-column fieldtype lives only in the YAML comment. Post-refactor, the in-memory `Schema` must carry it on each `ColumnInfo`. We'll default it to `"s"` so call sites that don't yet pass it still work; Phase 3 populates real values on all write paths.

- [ ] **Step 1: Locate `ColumnInfo`**

Search `aaiclick/data/models.py` for `class ColumnInfo` (or `@dataclass` preceding `ColumnInfo`). Note its current fields — you'll be adding one more.

- [ ] **Step 2: Write the failing test**

Create `aaiclick/data/test_models.py` (append if exists):

```python
from aaiclick.data.models import ColumnInfo


def test_column_info_carries_fieldtype():
    ci = ColumnInfo(ch_base_type="Int64", nullable=False, array_depth=0, fieldtype="a")
    assert ci.fieldtype == "a"


def test_column_info_fieldtype_defaults_to_scalar():
    ci = ColumnInfo(ch_base_type="String")
    assert ci.fieldtype == "s"
```

Use the exact constructor fields that already exist on `ColumnInfo` — the read-only bits are `ch_base_type`, `nullable`, `array_depth`, and `low_cardinality` (check the current definition and adapt if field names differ). Only `fieldtype` is new.

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest aaiclick/data/test_models.py -v -k fieldtype`
Expected: FAIL with unexpected keyword `fieldtype`.

- [ ] **Step 4: Add the field**

In `aaiclick/data/models.py` modify `ColumnInfo`:

```python
from typing import Literal  # add if not already imported

@dataclass
class ColumnInfo:
    """..."""  # keep existing docstring
    ch_base_type: str
    nullable: bool = False
    array_depth: int = 0
    low_cardinality: bool = False
    fieldtype: Literal["s", "a"] = "s"
```

(If fields are in a different order in the current code, preserve their order and append `fieldtype` last.)

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest aaiclick/data/test_models.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/test_models.py
git commit -m "feature: add fieldtype to ColumnInfo dataclass"
```

---

### Task 2.2: Write `schema_converters.py`

**Files:**
- Create: `aaiclick/data/schema_converters.py`
- Create: `aaiclick/data/test_schema_converters.py`

**Background:** Converters are pure functions — no DB, no I/O. They translate between the persistence shape (`SchemaView`) and the runtime shape (`Schema` dataclass). Keeping them pure makes them trivially testable.

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/data/test_schema_converters.py`:

```python
from aaiclick.data.models import ColumnInfo, Schema
from aaiclick.data.schema_converters import schema_to_view, view_to_schema
from aaiclick.data.view_models import ColumnView, SchemaView


def test_schema_to_view_round_trip():
    schema = Schema(
        fieldtype="d",
        columns={
            "title": ColumnInfo(ch_base_type="String", fieldtype="s"),
            "votes": ColumnInfo(ch_base_type="Int64", array_depth=1, fieldtype="a"),
        },
        table="t_123",
        order_by="(title)",
        engine="MergeTree",
    )
    view = schema_to_view(schema)

    assert isinstance(view, SchemaView)
    assert view.fieldtype == "d"
    assert view.order_by == "(title)"
    assert view.engine == "MergeTree"
    assert [c.name for c in view.columns] == ["title", "votes"]
    assert view.columns[0].fieldtype == "s"
    assert view.columns[1].fieldtype == "a"
    assert view.columns[1].array_depth == 1


def test_view_to_schema_round_trip():
    view = SchemaView(
        columns=[
            ColumnView(name="x", type="Int64", fieldtype="a"),
            ColumnView(name="y", type="String", nullable=True, fieldtype="s"),
        ],
        order_by="(x)",
        engine="MergeTree",
        fieldtype="d",
    )
    schema = view_to_schema(view, table="t_987")

    assert isinstance(schema, Schema)
    assert schema.table == "t_987"
    assert schema.fieldtype == "d"
    assert schema.order_by == "(x)"
    assert schema.engine == "MergeTree"
    assert list(schema.columns) == ["x", "y"]
    assert schema.columns["x"].fieldtype == "a"
    assert schema.columns["x"].array_depth == 1
    assert schema.columns["y"].nullable is True


def test_schema_view_schema_round_trip_is_identity():
    original = Schema(
        fieldtype="a",
        columns={"value": ColumnInfo(ch_base_type="Float64", fieldtype="a")},
        table="t_rt",
        order_by=None,
        engine=None,
    )
    restored = view_to_schema(schema_to_view(original), table="t_rt")
    assert restored.fieldtype == original.fieldtype
    assert restored.order_by == original.order_by
    assert restored.engine == original.engine
    assert restored.columns.keys() == original.columns.keys()
    for k in original.columns:
        assert restored.columns[k].ch_base_type == original.columns[k].ch_base_type
        assert restored.columns[k].fieldtype == original.columns[k].fieldtype
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/test_schema_converters.py -v`
Expected: FAIL — `ModuleNotFoundError: aaiclick.data.schema_converters`.

- [ ] **Step 3: Implement the converters**

Create `aaiclick/data/schema_converters.py`:

```python
from __future__ import annotations

from .models import ColumnInfo, Schema
from .view_models import ColumnView, SchemaView


def schema_to_view(schema: Schema) -> SchemaView:
    """Convert an in-memory Schema into the persistence-shape SchemaView."""
    columns = [
        ColumnView(
            name=name,
            type=info.ch_base_type,
            nullable=info.nullable,
            array_depth=info.array_depth,
            low_cardinality=info.low_cardinality,
            fieldtype=info.fieldtype,
        )
        for name, info in schema.columns.items()
    ]
    return SchemaView(
        columns=columns,
        order_by=schema.order_by,
        engine=schema.engine,
        fieldtype=schema.fieldtype,
    )


def view_to_schema(view: SchemaView, *, table: str) -> Schema:
    """Hydrate a Schema dataclass from a persisted SchemaView."""
    columns = {
        cv.name: ColumnInfo(
            ch_base_type=cv.type,
            nullable=cv.nullable,
            array_depth=cv.array_depth,
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

Run: `pytest aaiclick/data/test_schema_converters.py -v`
Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/schema_converters.py aaiclick/data/test_schema_converters.py
git commit -m "feature: add schema_to_view / view_to_schema converters"
```

---

### Task 2.3: Swap `_get_table_schema` to read from `table_registry`

**Files:**
- Modify: `aaiclick/data/object/ingest.py` — `_get_table_schema` function (currently lines 76-119).
- Modify/create: `aaiclick/data/object/test_schema.py` — read-path tests.

**Background:** Today `_get_table_schema(table, ch_client)` runs a `SELECT name, type, comment FROM system.columns`. We replace it with a SQL lookup of the registry row; if the row is missing or its `schema_json` is null we raise a clear error. There is no fallback.

The SQL session helper is already available via the orchestration layer — look for a `get_sql_session()` or `sql_session()` context manager in `aaiclick/orchestration/lifecycle/` or `aaiclick/orchestration/db.py`. Use whatever the existing registry-read code uses (grep for `from .* import.*TableRegistry`).

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/data/object/test_schema.py` (or append):

```python
import pytest

from aaiclick import DataContext
from aaiclick.data.object.ingest import _get_table_schema


async def test_get_table_schema_reads_from_registry(data_ctx: DataContext):
    obj = await data_ctx.create("votes", [1, 2, 3])  # array Object
    fieldtype, columns = await _get_table_schema(obj.table, data_ctx.ch_client)

    assert fieldtype == "a"
    assert set(columns) == {"value"}
    assert columns["value"].fieldtype == "a"


async def test_get_table_schema_missing_registry_row_raises(data_ctx: DataContext):
    # Create a raw ClickHouse table that has no registry row.
    await data_ctx.ch_client.command(
        "CREATE TABLE t_orphan_test (v Int64) ENGINE = Memory"
    )
    try:
        with pytest.raises(LookupError, match="not registered"):
            await _get_table_schema("t_orphan_test", data_ctx.ch_client)
    finally:
        await data_ctx.ch_client.command("DROP TABLE IF EXISTS t_orphan_test")
```

The test uses the existing `data_ctx` fixture (check `aaiclick/conftest.py`; if the fixture name differs, use the project's standard).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_schema.py -v`
Expected: FAIL — the first test fails once a registry row is required but not yet written (Phase 3 handles writes), OR it currently passes via `system.columns`. Either way, we flip it to registry-driven now; for this phase we'll temporarily populate `schema_json` inside the test via a direct `UPDATE`, so that the test proves the read path works before Phase 3 wires up the write path.

Amend the first test to directly insert a `schema_json` value after `data_ctx.create()` so the test is self-contained:

```python
async def test_get_table_schema_reads_from_registry(data_ctx: DataContext):
    obj = await data_ctx.create("votes", [1, 2, 3])

    # Simulate what Phase 3's create_object write path will do:
    import json
    from sqlmodel import update
    from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
    async with data_ctx.sql_session() as sess:
        await sess.execute(
            update(TableRegistry)
            .where(TableRegistry.table_name == obj.table)
            .values(schema_json=json.dumps({
                "columns": [
                    {"name": "value", "type": "Int64", "nullable": False,
                     "array_depth": 0, "low_cardinality": False, "fieldtype": "a"}
                ],
                "order_by": None,
                "engine": "MergeTree",
                "fieldtype": "a",
            }))
        )
        await sess.commit()

    fieldtype, columns = await _get_table_schema(obj.table, data_ctx.ch_client)
    assert fieldtype == "a"
    assert columns["value"].fieldtype == "a"
```

(Replace `data_ctx.sql_session()` with the real helper name — grep for `sql_session` in the project first.)

- [ ] **Step 3: Rewrite `_get_table_schema`**

In `aaiclick/data/object/ingest.py`, replace the current body (lines 76-119) with:

```python
from sqlmodel import select

from aaiclick.data.schema_converters import view_to_schema
from aaiclick.data.view_models import SchemaView
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry


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

Replace `get_sql_session` with the project's actual helper. Remove the now-unused `ch_client` parameter only if no caller still passes it — if callers do pass it, keep the parameter and simply ignore it inside the body (adding a one-line comment: `# ch_client retained for call-site compatibility; schema now lives in SQL`). Grep for call sites with `rg "_get_table_schema\(" aaiclick/` before deleting the param.

Also delete the now-dead YAML-parsing code paths at the top of the function (the `ColumnMeta.from_yaml` calls and the `aai_id_fieldtype` / `is_dict` inference block).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_schema.py -v`
Expected: both tests PASS.

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
