# Phase 3 — DDL Without `aai_id` or `COMMENT`s

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop emitting the hidden `aai_id` column and ClickHouse column `COMMENT`s on every table aaiclick creates. On every write path (create, copy, insert, concat) build a `SchemaView` and persist it to `table_registry.schema_doc`. Fix `build_order_by_clause` to stop force-appending `aai_id`. Drop the now-redundant `col_fieldtype` field from `Schema` / `CopyInfo`.

**Depends on:** Phase 2 (needs `schema_to_view`, registry read path).

**Unlocks:** Phase 4 (once tables have no `aai_id`, operator SQL can stop referencing it).

After Phase 3, the data-module tests that started failing in Phase 2 should all pass again — because create/copy/ingest now populate `schema_doc`, which Phase 2's `_get_table_schema` reads.

---

## File Structure

| File                                                                  | Role                                                                            |
|-----------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `aaiclick/data/models.py`                                             | Modify — `build_order_by_clause` (drop `aai_id`); remove `col_fieldtype` fields. |
| `aaiclick/data/data_context/data_context.py`                          | Modify — `create_object` omits `aai_id`/COMMENTs and writes the registry row.   |
| `aaiclick/data/object/operators.py`                                   | Modify — copy path inherits registry write via `create_object`; drop `col_fieldtype` threading. |
| `aaiclick/data/object/ingest.py`                                      | Modify — `copy_db`, `copy_db_selected_fields`, `insert_objects_db`, `concat_objects_db` inherit registry write via `create_object`; prune `aai_id` SELECT/exclude branches. |
| `aaiclick/data/object/test_order_by.py` (exists)                      | Modify — the four existing `test_build_order_by_clause_*` tests hard-code the old behaviour. Rewrite them (don't create a new file). |
| `aaiclick/data/object/test_schema.py` (exists)                        | Modify — assertions that `aai_id` appears in schema columns flip to assertions of its absence. |
| `aaiclick/data/data_context/test_data_context.py` (**new** — create alongside `data_context.py`) | Create — DDL-level assertions: no `aai_id`, no COMMENTs, `schema_doc` populated, `aai_id` no longer reserved. |
| `aaiclick/data/object/test_datetime.py`, `test_nullable.py` (exist)   | Modify — drop `col_fieldtype=...` from `Schema(...)` constructor calls (`test_datetime.py:156,205`, `test_nullable.py:116`). |
| `aaiclick/orchestration/lifecycle/db_lifecycle.py`                    | Modify — add `schema_doc: str | None = None` field to `OplogTablePayload`.      |
| `aaiclick/orchestration/orch_context.py`                              | Modify — `DBLifecycleHandler.oplog_record_table` accepts and forwards `schema_doc`; `_write_table_registry_row` INSERT extends column list and parameter dict. |
| `aaiclick/oplog/oplog_api.py`                                         | Modify — `oplog_record_table(table_name, schema_doc=None)` signature; forward to handler. |
| `aaiclick/data/data_context/lifecycle.py`                             | Modify — no-op `LocalLifecycleHandler.oplog_record_table` takes the new kwarg (signature must match). |
| `aaiclick/orchestration/oplog_backfill.py`                            | Modify — the raw `INSERT INTO table_registry (...)` at line ~74 extends to include `schema_doc`. |
| `aaiclick/orchestration/background/conftest.py`                       | Modify — `insert_table_registry` test helper accepts and passes `schema_doc`. |

**There is no `aaiclick/data/test_data_context.py` today.** The new file goes next to `data_context.py` (per the project's co-location rule) at `aaiclick/data/data_context/test_data_context.py`. An earlier draft of this plan said the file already existed — it does not.

---

### Task 3.1: `build_order_by_clause` no longer appends `aai_id`

**Files:**
- Modify: `aaiclick/data/models.py` — `build_order_by_clause` (currently lines 277-291).
- Modify: `aaiclick/data/object/test_order_by.py` (**exists**) — rewrite the four existing `test_build_order_by_clause_*` tests at lines 13-30 that hard-code the old `aai_id`-appending behaviour. Also update `test_object_init_order_by_sets_schema` at lines 33-40 which asserts `schema.order_by == "(date, aai_id)"`.

**Important:** an earlier draft of this plan wrote new tests to `aaiclick/data/test_models.py` — that file does not exist. Rewriting the existing tests in `test_order_by.py` is the correct move; creating a new top-level test file would leave the stale tests failing.

- [ ] **Step 1: Rewrite the existing tests**

In `aaiclick/data/object/test_order_by.py`, replace the four existing tests (lines 13-30) and update the Object-init test at lines 33-40:

```python
def test_build_order_by_clause_empty_returns_tuple():
    """build_order_by_clause with no columns yields tuple() — no implicit key."""
    assert build_order_by_clause([]) == "tuple()"


def test_build_order_by_clause_single_column():
    """build_order_by_clause preserves a single user column verbatim."""
    assert build_order_by_clause(["date"]) == "(date)"


def test_build_order_by_clause_multiple_columns():
    """build_order_by_clause preserves multiple user columns verbatim."""
    assert build_order_by_clause(["date", "category"]) == "(date, category)"


def test_build_order_by_clause_does_not_append_aai_id():
    """build_order_by_clause never injects aai_id — the column does not exist."""
    assert "aai_id" not in build_order_by_clause(["date"])
    assert "aai_id" not in build_order_by_clause([])
```

Then update `test_object_init_order_by_sets_schema` — the fixture `schema` was built with `{"aai_id": ColumnInfo("UInt64"), "date": ColumnInfo("String")}`. Drop the `aai_id` entry (the column no longer exists) and change the assertion to `assert obj._schema.order_by == "(date)"`.

Delete `test_build_order_by_clause_dedup_aai_id` and `test_build_order_by_clause_only_aai_id` — the scenarios they cover are now impossible.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_order_by.py -v -k build_order_by`
Expected: FAIL — current implementation appends `aai_id`.

- [ ] **Step 3: Replace the function body**

In `aaiclick/data/models.py` (around lines 277-291):

```python
def build_order_by_clause(columns: list[str]) -> str:
    """Build an ORDER BY clause string from column names.

    Empty input yields ``tuple()`` — the ClickHouse "no ordering key" form.
    Otherwise columns are joined into a parenthesised list as given.
    """
    if not columns:
        return "tuple()"
    return f"({', '.join(columns)})"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_order_by.py -v`
Expected: PASS — all (remaining) tests in the file pass.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/object/test_order_by.py
git commit -m "refactor: build_order_by_clause no longer appends aai_id"
```

---

### Task 3.2: `create_object` emits clean DDL + writes registry row

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` — `create_object` (currently starts line 249).
- Create: `aaiclick/data/data_context/test_data_context.py` — DDL-level assertions.

**Background:** Today `create_object` iterates `schema.columns`, appends an `aai_id` column, writes `COMMENT '{fieldtype: ...}'` on each column, and calls `oplog_record_table(obj.table)` (which enqueues a `DBLifecycleMessage` that eventually writes a bare registry row via raw SQL). We need it to: (1) emit only user columns, no COMMENTs; (2) call `schema_to_view(schema)` and thread the JSON through the whole plumbing chain so the INSERT SQL populates `schema_doc`.

**The registry write plumbing is raw SQL, not ORM.** Before Task 3.2 can end green, these sites all need updating in lock-step:

| Site                                                                | Change                                                                                 |
|---------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `aaiclick/orchestration/lifecycle/db_lifecycle.py::OplogTablePayload` | Add `schema_doc: str | None = None` dataclass field.                                  |
| `aaiclick/orchestration/orch_context.py::DBLifecycleHandler.oplog_record_table` | Accept `schema_doc: str | None = None`, include it in the `OplogTablePayload(...)` construction. |
| `aaiclick/orchestration/orch_context.py::DBLifecycleHandler._write_table_registry_row` | Extend the raw INSERT's column list and parameter dict with `schema_doc`.             |
| `aaiclick/oplog/oplog_api.py::oplog_record_table`                   | Accept `schema_doc: str | None = None`; forward to the handler.                        |
| `aaiclick/data/data_context/lifecycle.py::LifecycleHandler.oplog_record_table` (abstract base + `LocalLifecycleHandler` no-op override) | Signature must match — accept `schema_doc`.                                           |
| `aaiclick/orchestration/oplog_backfill.py` (raw INSERT at line 74)  | Extend columns + bound params.                                                         |
| `aaiclick/orchestration/background/conftest.py::insert_table_registry` (test helper) | Accept `schema_doc=None` kwarg for background-cleanup tests.                          |

All other `create_object()` callers (`aaiclick/data/object/operators.py`, `aaiclick/data/object/ingest.py::copy_db`, `join.py`, `url.py`) flow through `create_object(schema)` — once Task 3.2 changes `create_object` to compute `schema_to_view(schema).model_dump_json()` and pass it into `oplog_record_table(...)`, every caller inherits the registry write. **No separate task is needed to wire them.**

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/data/data_context/test_data_context.py` (no such file exists today; it sits alongside `data_context.py` per the project's co-location rule). Use the real fixture `ctx` and the real module-level helpers — there is no `DataContext` class or `data_ctx.create()` / `data_ctx.sql_session()` method:

```python
import json

from sqlmodel import select

from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session


async def test_create_object_emits_no_aai_id_column(ctx):
    obj = await create_object_from_value([1.0, 2.0, 3.0])
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}' ORDER BY position"
    )
    names = [r[0] for r in result.result_rows]
    assert "aai_id" not in names
    assert names == ["value"]


async def test_create_object_emits_no_comment_clauses(ctx):
    obj = await create_object_from_value([1, 2, 3])
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name, comment FROM system.columns WHERE table = '{obj.table}'"
    )
    for name, comment in result.result_rows:
        assert comment == "", f"column {name} has unexpected comment {comment!r}"


async def test_create_object_writes_schema_doc(ctx):
    obj = await create_object_from_value([1, 2, 3])
    async with get_sql_session() as sess:
        result = await sess.execute(
            select(TableRegistry.schema_doc).where(TableRegistry.table_name == obj.table)
        )
        raw = result.scalar_one()
    assert raw is not None
    parsed = json.loads(raw)
    assert parsed["fieldtype"] == "a"
    assert [c["name"] for c in parsed["columns"]] == ["value"]
    assert parsed["columns"][0]["fieldtype"] == "a"


async def test_create_object_allows_user_column_named_aai_id(ctx):
    # aai_id is no longer reserved — users can define a column with that name.
    obj = await create_object_from_value({"aai_id": [1, 2], "label": ["a", "b"]})
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}'"
    )
    names = {r[0] for r in result.result_rows}
    assert "aai_id" in names
    assert "label" in names
```

`create_object_from_value` dispatches on the input value type: list → array Object, scalar → scalar Object, dict → dict Object. There is no separate `create_dict` helper.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/data_context/test_data_context.py -v`
Expected: FAIL — tables still carry `aai_id`, COMMENTs are still emitted, `schema_doc` is null, and `create_object_from_value({"aai_id": ..., "label": ...})` raises `ValueError` for the reserved-name collision.

- [ ] **Step 3: Thread `schema_doc` through the plumbing**

Before touching `create_object` itself, update every site in the lock-step table above. Order matters: the signature extensions must land together so the handler call compiles.

1. `aaiclick/orchestration/lifecycle/db_lifecycle.py` — add `schema_doc: str | None = None` to `OplogTablePayload`.
2. `aaiclick/orchestration/orch_context.py::DBLifecycleHandler.oplog_record_table(self, table_name, schema_doc=None)` — include in the `OplogTablePayload(...)` construction.
3. `aaiclick/orchestration/orch_context.py::DBLifecycleHandler._write_table_registry_row` — extend the raw INSERT's column list and `:parameter` bind dict:
   ```python
   await session.execute(
       text(
           "INSERT INTO table_registry "
           "(table_name, job_id, task_id, run_id, created_at, schema_doc) "
           "VALUES (:table_name, :job_id, :task_id, :run_id, :created_at, :schema_doc) "
           "ON CONFLICT (table_name) DO NOTHING"
       ),
       {"table_name": p.table_name, "job_id": p.job_id, "task_id": p.task_id,
        "run_id": p.run_id, "created_at": now, "schema_doc": p.schema_doc},
   )
   ```
4. `aaiclick/oplog/oplog_api.py::oplog_record_table(table_name, schema_doc=None)` — forward the kwarg to `lc.oplog_record_table(...)`.
5. `aaiclick/data/data_context/lifecycle.py::LifecycleHandler.oplog_record_table(self, table_name, schema_doc=None)` — keep the abstract signature in sync (same for the `LocalLifecycleHandler` no-op subclass).
6. `aaiclick/orchestration/oplog_backfill.py:74` — extend its raw INSERT the same way.
7. `aaiclick/orchestration/background/conftest.py::insert_table_registry` — add `schema_doc=None` kwarg and bind it.

- [ ] **Step 4: Update `create_object`**

In `aaiclick/data/data_context/data_context.py`, replace the current DDL-building block (around lines 283-301) with column-definition code that does not add `aai_id` or COMMENTs:

```python
# Build column definitions for CREATE TABLE.
column_defs = [
    f"{quote_identifier(col_name)} {col_def.ch_type()}"
    for col_name, col_def in schema.columns.items()
]
```

Delete the nearby validation that raised on `col_name == "aai_id" and col_def.nullable` — the name is no longer reserved. Delete the `ColumnMeta(...).to_yaml()` / `COMMENT '...'` block.

Then, at the `oplog_record_table(obj.table)` call (around line 338), extend the write:

```python
from aaiclick.data.view_models import schema_to_view

# ... inside create_object, after the CREATE TABLE command succeeds:
oplog_record_table(
    obj.table,
    schema_doc=schema_to_view(schema).model_dump_json(),
)
```

The import lives in the existing `aaiclick.data.view_models` module — **not** in a new `schema_converters.py`. See Phase 2 Task 2.2 for why.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest aaiclick/data/data_context/test_data_context.py -v`
Expected: all four tests PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/data_context/data_context.py aaiclick/data/data_context/test_data_context.py \
        aaiclick/orchestration/lifecycle/db_lifecycle.py \
        aaiclick/orchestration/orch_context.py \
        aaiclick/oplog/oplog_api.py \
        aaiclick/data/data_context/lifecycle.py \
        aaiclick/orchestration/oplog_backfill.py \
        aaiclick/orchestration/background/conftest.py
git commit -m "$(cat <<'EOF'
refactor: create_object emits clean DDL + persists SchemaView

- drop aai_id column and ClickHouse COMMENT clauses from CREATE TABLE
- thread schema_doc through OplogTablePayload, the lifecycle handler,
  oplog_record_table, and the raw INSERT in _write_table_registry_row;
  extend oplog_backfill's raw INSERT identically
- aai_id is no longer a reserved column name
EOF
)"
```

---

### Task 3.3: Remove `col_fieldtype`; prune `aai_id` plumbing in copy / insert / concat

**Files:**
- Modify: `aaiclick/data/models.py` — remove `col_fieldtype` from `Schema` and `CopyInfo`.
- Modify: `aaiclick/data/object/ingest.py` — prune every `aai_id`-exclusion / `aai_id`-carrier branch in `copy_db`, `copy_db_selected_fields`, `insert_objects_db`, `concat_objects_db` (lines 96, 103-116, 172-174, 193-199, 231, 236, 239, 248, 268, 291, 315, 319, 338, 395, 401).
- Modify: `aaiclick/data/object/operators.py` — any `"aai_id": ColumnInfo(...)` entries in destination-schema dicts.
- Modify: `aaiclick/data/object/test_datetime.py` (lines 156, 205), `aaiclick/data/object/test_nullable.py` (line 116) — these today pass `col_fieldtype=FIELDTYPE_*` to `Schema(...)`; Phase 3 drops the field, so these must be updated in the same commit or the suite goes red with `TypeError: Schema.__init__() got an unexpected keyword argument 'col_fieldtype'`.

**Background:** Every `CREATE TABLE` for an aaiclick-managed table goes through `create_object(schema)` (verified by `rg "await create_object" aaiclick/data/`). Task 3.2 made that one site write `schema_doc`. `operators.py`, `ingest.py::copy_db` / `copy_db_selected_fields` / `concat_objects_db` / `insert_objects_db`, `url.py::create_object_from_url`, and `join.py` all route through `create_object(schema)`, so they **inherit the registry write automatically** — no per-site plumbing is needed here. An earlier draft of this plan said otherwise; it was wrong.

The non-trivial work in this task is:

1. Deleting `col_fieldtype` from `Schema` and `CopyInfo` (no longer needed — `ColumnInfo.fieldtype` carries the same information per-column).
2. Pruning the large set of `aai_id`-exclusion branches that currently litter `ingest.py` copy/insert/concat helpers.

- [ ] **Step 1: Write failing tests asserting schema_doc exists for copy and concat**

Append to `aaiclick/data/object/test_schema.py` using the real fixture and API:

```python
import json

from sqlmodel import select

from aaiclick import create_object_from_value
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session


async def test_copy_derivation_writes_schema_doc(ctx):
    src = await create_object_from_value([1, 2, 3])
    copy = await src.copy()

    async with get_sql_session() as sess:
        row = (await sess.execute(
            select(TableRegistry.schema_doc).where(TableRegistry.table_name == copy.table)
        )).scalar_one()

    parsed = json.loads(row)
    assert parsed["fieldtype"] == "a"
    assert [c["name"] for c in parsed["columns"]] == ["value"]


async def test_concat_writes_schema_doc(ctx):
    a = await create_object_from_value([1, 2])
    b = await create_object_from_value([3, 4])
    c = await a.concat(b)

    async with get_sql_session() as sess:
        row = (await sess.execute(
            select(TableRegistry.schema_doc).where(TableRegistry.table_name == c.table)
        )).scalar_one()

    assert row is not None
    parsed = json.loads(row)
    assert parsed["fieldtype"] == "a"
```

`Object.concat` (see `aaiclick/data/object/object.py:761`) is a method on `Object`, not on `data_context()`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_schema.py -v -k "copy or concat"`
Expected: FAIL — the `aai_id`-exclusion branches still exist in `ingest.py` and break Phase-2's `_get_table_schema` lookup (or the COMMENT-based read still runs). Once Task 3.3 prunes them, the registry write done by `create_object` in Task 3.2 suffices.

- [ ] **Step 3: Prune `aai_id` plumbing in `ingest.py`**

Delete every branch that special-cases `aai_id`:

- Line 96 `aai_id_fieldtype = None`
- Lines 103-118 — YAML-parsing fallback that reads the old `aai_id` COMMENT. (Already dead after Phase 2's `_get_table_schema` rewrite; confirm and delete.)
- Lines 172-174, 193-199 — "exclude `aai_id` from copy data columns" logic.
- Line 231 — `{"aai_id": ColumnInfo("UInt64"), "value": ...}` — drop the `aai_id` entry.
- Lines 236, 248 — `SELECT aai_id, value FROM ...` → drop `aai_id` from the select list.
- Line 239 — `columns = {"aai_id": ColumnInfo("UInt64")}` — drop.
- Lines 268, 291, 315, 319, 338, 395, 401 — `c != "aai_id"` filters (the column is no longer in `source_columns`, so the filter is a no-op — delete the condition).

**Note:** `ingest.py:236` and `:248` used to rely on the source carrying an `aai_id` the copy could preserve. Without `aai_id`, the copy has no implicit row-order guarantee; that is the intended behaviour per the spec. Callers that need determinism pass `order_by=...`.

- [ ] **Step 4: Remove `col_fieldtype` from `Schema` and `CopyInfo`**

In `aaiclick/data/models.py`, delete the `col_fieldtype` field from `Schema` (line 249) and from `CopyInfo` (line 272 ballpark). Grep for remaining references:

```bash
rg "col_fieldtype" aaiclick/ --type py
```

Fix each — `aaiclick/data/object/test_datetime.py:156,205` and `aaiclick/data/object/test_nullable.py:116` construct `Schema(..., col_fieldtype=FIELDTYPE_*)`. Drop the kwarg. If the test genuinely needed the value (it won't — per-column fieldtype lives on `ColumnInfo.fieldtype` now), set the value on each `ColumnInfo` instead.

- [ ] **Step 5: Prune `operators.py` destination-schema dicts**

In `aaiclick/data/object/operators.py`, search for `"aai_id": ColumnInfo` — every hit is a destination-schema construction that must drop the entry. (Details continue in Phase 4.)

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_schema.py -v`
Expected: PASS for the new `test_copy_derivation_writes_schema_doc` / `test_concat_writes_schema_doc` tests.

- [ ] **Step 7: Run the full data suite**

Run: `pytest aaiclick/data/ -v`
Expected: the failure set from end of Phase 2 is now back near zero. Some operator tests (`test_arithmetic*.py`) will still fail because their SQL still references `aai_id` — that's Phase 4. Any other failure is a regression; fix before committing.

- [ ] **Step 8: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/object/operators.py aaiclick/data/object/ingest.py \
        aaiclick/data/object/test_schema.py aaiclick/data/object/test_datetime.py aaiclick/data/object/test_nullable.py
git commit -m "$(cat <<'EOF'
refactor: drop col_fieldtype; prune aai_id plumbing from ingest

- remove col_fieldtype from Schema and CopyInfo (per-column fieldtype
  now lives on ColumnInfo.fieldtype)
- prune aai_id-exclusion branches from copy / insert / concat helpers
- existing test_datetime / test_nullable schemas drop col_fieldtype kwarg
EOF
)"
```

---

## Phase 3 Complete

At this point:

- New tables contain only user columns — no `aai_id`, no ClickHouse COMMENTs.
- `table_registry.schema_doc` is populated for every aaiclick-managed table.
- `_get_table_schema` (from Phase 2) round-trips successfully against those rows.
- `build_order_by_clause` returns `tuple()` for empty input and preserves user columns otherwise.
- `col_fieldtype` is gone from `Schema` and `CopyInfo`; per-column fieldtype lives on `ColumnInfo.fieldtype`.

Operators still reference `aai_id` in their SQL — that's Phase 4.
