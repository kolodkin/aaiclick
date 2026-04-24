# Phase 3 — DDL Without `aai_id` or `COMMENT`s

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop emitting the hidden `aai_id` column and ClickHouse column `COMMENT`s on every table aaiclick creates. On every write path (create, copy, insert, concat) build a `SchemaView` and persist it to `table_registry.schema_json`. Fix `build_order_by_clause` to stop force-appending `aai_id`. Drop the now-redundant `col_fieldtype` field from `Schema` / `CopyInfo`.

**Depends on:** Phase 2 (needs `schema_to_view`, registry read path).

**Unlocks:** Phase 4 (once tables have no `aai_id`, operator SQL can stop referencing it).

After Phase 3, the data-module tests that started failing in Phase 2 should all pass again — because create/copy/ingest now populate `schema_json`, which Phase 2's `_get_table_schema` reads.

---

## File Structure

| File                                                         | Role                                                                            |
|--------------------------------------------------------------|---------------------------------------------------------------------------------|
| `aaiclick/data/models.py`                                    | Modify — `build_order_by_clause` (drop `aai_id`); remove `col_fieldtype` fields. |
| `aaiclick/data/data_context/data_context.py`                 | Modify — `create_object` omits `aai_id`/COMMENTs and writes the registry row.   |
| `aaiclick/data/object/operators.py`                          | Modify — copy path writes registry rows; drop `col_fieldtype` threading.        |
| `aaiclick/data/object/ingest.py`                             | Modify — `insert_objects_db` / `concat_objects_db` write registry rows.         |
| `aaiclick/data/test_models.py`                               | Modify — `build_order_by_clause` tests.                                         |
| `aaiclick/data/test_data_context.py` (exists)                | Modify — DDL-level assertions.                                                  |

---

### Task 3.1: `build_order_by_clause` no longer appends `aai_id`

**Files:**
- Modify: `aaiclick/data/models.py` — `build_order_by_clause` (currently lines 277-291).
- Modify: `aaiclick/data/test_models.py`.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/data/test_models.py`:

```python
from aaiclick.data.models import build_order_by_clause


def test_build_order_by_clause_empty_returns_tuple():
    assert build_order_by_clause([]) == "tuple()"


def test_build_order_by_clause_preserves_user_columns():
    assert build_order_by_clause(["date", "id"]) == "(date, id)"


def test_build_order_by_clause_does_not_append_aai_id():
    result = build_order_by_clause(["date"])
    assert "aai_id" not in result
    assert result == "(date)"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/test_models.py -v -k build_order_by`
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

Run: `pytest aaiclick/data/test_models.py -v -k build_order_by`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/test_models.py
git commit -m "refactor: build_order_by_clause no longer appends aai_id"
```

---

### Task 3.2: `create_object` emits clean DDL + writes registry row

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` — `create_object` (currently lines 248-339).
- Modify: `aaiclick/data/test_data_context.py` — DDL-level assertions.

**Background:** Today `create_object` iterates `schema.columns`, appends an `aai_id` column, writes `COMMENT '{fieldtype: ...}'` on each column, and calls `oplog_record_table(obj.table)` (which writes a bare registry row). We need it to: (1) emit only user columns, no COMMENTs; (2) call `schema_to_view(schema)` and write the JSON alongside the registry row — in the same transaction that calls `oplog_record_table`.

Grep for `oplog_record_table` to see whether it's the right layer to extend, or whether the insert into `table_registry` happens in a helper that already exists. Likely the write lives in `aaiclick/orchestration/lifecycle/db_lifecycle.py` — extend that helper to accept `schema_json`, then update every call site.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/data/test_data_context.py`:

```python
import json

from aaiclick import DataContext
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from sqlmodel import select


async def test_create_object_emits_no_aai_id_column(data_ctx: DataContext):
    obj = await data_ctx.create("v", [1.0, 2.0, 3.0])
    result = await data_ctx.ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}' ORDER BY position"
    )
    names = [r[0] for r in result.result_rows]
    assert "aai_id" not in names
    assert names == ["value"]


async def test_create_object_emits_no_comment_clauses(data_ctx: DataContext):
    obj = await data_ctx.create("v", [1, 2, 3])
    result = await data_ctx.ch_client.query(
        f"SELECT name, comment FROM system.columns WHERE table = '{obj.table}'"
    )
    for name, comment in result.result_rows:
        assert comment == "", f"column {name} has unexpected comment {comment!r}"


async def test_create_object_writes_schema_json(data_ctx: DataContext):
    obj = await data_ctx.create("v", [1, 2, 3])
    async with data_ctx.sql_session() as sess:
        result = await sess.execute(
            select(TableRegistry.schema_json).where(TableRegistry.table_name == obj.table)
        )
        raw = result.scalar_one()
    assert raw is not None
    parsed = json.loads(raw)
    assert parsed["fieldtype"] == "a"
    assert [c["name"] for c in parsed["columns"]] == ["value"]
    assert parsed["columns"][0]["fieldtype"] == "a"


async def test_create_object_allows_user_column_named_aai_id(data_ctx: DataContext):
    # aai_id is no longer reserved.
    obj = await data_ctx.create_dict("d", {"aai_id": [1, 2], "label": ["a", "b"]})
    result = await data_ctx.ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}'"
    )
    names = {r[0] for r in result.result_rows}
    assert "aai_id" in names
    assert "label" in names
```

If the project exposes a different `create_dict`/dict-creation API, adapt the last test to whichever call creates a dict-shaped Object.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/test_data_context.py -v -k "aai_id or schema_json or comment_clauses"`
Expected: FAIL — tables still carry `aai_id`, COMMENTs are still emitted, `schema_json` is null.

- [ ] **Step 3: Update `create_object`**

In `aaiclick/data/data_context/data_context.py`, replace the current DDL-building block (around lines 283-301) with column-definition code that does not add `aai_id` or COMMENTs:

```python
# Build column definitions for CREATE TABLE.
column_defs = [
    f"{quote_identifier(col_name)} {col_def.ch_type()}"
    for col_name, col_def in schema.columns.items()
]
```

Delete the nearby validation that raised on `col_name == "aai_id" and col_def.nullable` — the name is no longer reserved. Delete the `ColumnMeta(...).to_yaml()` / `COMMENT '...'` block.

Then, where the function currently calls `oplog_record_table(obj.table)`, extend the persistence so the registry row carries `schema_json`:

```python
from aaiclick.data.schema_converters import schema_to_view

# ... inside create_object, after the CREATE TABLE command succeeds:
schema_view = schema_to_view(schema)
oplog_record_table(obj.table, schema_json=schema_view.model_dump_json())
```

Update `oplog_record_table` (or whatever helper writes to `table_registry`) to accept `schema_json: str | None = None` and pass it through to the SQL insert. Find the function with `rg "def oplog_record_table" aaiclick/`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest aaiclick/data/test_data_context.py -v -k "aai_id or schema_json or comment_clauses"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/data_context/data_context.py aaiclick/data/test_data_context.py
# plus whichever helper module you modified (db_lifecycle.py / oplog helper):
git add aaiclick/orchestration/lifecycle/db_lifecycle.py
git commit -m "$(cat <<'EOF'
refactor: create_object emits clean DDL + persists SchemaView

- drop aai_id column and ClickHouse COMMENT clauses from CREATE TABLE
- write SchemaView as JSON into table_registry.schema_json
- aai_id is no longer a reserved column name
EOF
)"
```

---

### Task 3.3: Copy / insert / concat paths write the registry row

**Files:**
- Modify: `aaiclick/data/object/operators.py` — copy-path SQL (search for `CopyInfo`).
- Modify: `aaiclick/data/object/ingest.py` — `insert_objects_db`, `concat_objects_db` (search for `IngestQueryInfo`).
- Modify: `aaiclick/data/models.py` — remove `col_fieldtype` from `Schema` and `CopyInfo`.
- Modify: tests alongside each module.

**Background:** Every code path that issues `CREATE TABLE` for an aaiclick-managed table must follow up by writing a registry row including `schema_json`. The spec lists three: `create_object` (done in 3.2), copy-based derivations (`CopyInfo`), and ingest-based derivations (`IngestQueryInfo`).

- [ ] **Step 1: Inventory the write call sites**

Run:

```bash
rg "CREATE TABLE" aaiclick/data/ --type py -n
```

Every hit that creates an aaiclick-managed table needs a paired registry write. Make a list — typical call sites:

- `aaiclick/data/data_context/data_context.py::create_object` (already done).
- `aaiclick/data/object/operators.py` — copy helpers that build a result table from a source.
- `aaiclick/data/object/ingest.py` — `insert_objects_db`, `concat_objects_db`, and any helper they share for the destination table.

Add temp tables (`Engine = Memory` scratch tables inside operator SQL) only to the allow-list in the cleanup sweep if they already are today — they are not registered in `table_registry`, so do NOT start registering them now.

- [ ] **Step 2: Write failing tests for each path**

Append to `aaiclick/data/object/test_schema.py`:

```python
import json

from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from sqlmodel import select


async def test_copy_derivation_writes_schema_json(data_ctx):
    src = await data_ctx.create("src", [1, 2, 3])
    # pick an operation that currently uses the Copy path — e.g. a View materialisation
    # or an operator that returns a Copy. Adapt to whatever the codebase uses.
    copy = await src.copy("copy_dst")

    async with data_ctx.sql_session() as sess:
        row = (await sess.execute(
            select(TableRegistry.schema_json).where(TableRegistry.table_name == copy.table)
        )).scalar_one()

    parsed = json.loads(row)
    assert parsed["fieldtype"] == "a"
    assert [c["name"] for c in parsed["columns"]] == ["value"]


async def test_concat_writes_schema_json(data_ctx):
    a = await data_ctx.create("a", [1, 2])
    b = await data_ctx.create("b", [3, 4])
    c = await data_ctx.concat("c", [a, b])

    async with data_ctx.sql_session() as sess:
        row = (await sess.execute(
            select(TableRegistry.schema_json).where(TableRegistry.table_name == c.table)
        )).scalar_one()

    assert row is not None
    parsed = json.loads(row)
    assert parsed["fieldtype"] == "a"
```

Adjust fixture names and method names to the real API.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_schema.py -v -k "copy or concat"`
Expected: FAIL — these paths do not yet populate `schema_json`.

- [ ] **Step 4: Wire each call site**

For each CREATE-TABLE site in `operators.py` / `ingest.py`:

- Build a `Schema` dataclass for the destination (the existing code already knows `columns`, `fieldtype`, `order_by`, `engine` from `CopyInfo` / `IngestQueryInfo`).
- Call `schema_to_view(schema).model_dump_json()` and pass it to the same registry-write helper you extended in Task 3.2.

Example pattern (pseudocode — adapt to each call site):

```python
from aaiclick.data.schema_converters import schema_to_view

dst_schema = Schema(
    fieldtype=info.fieldtype,
    columns=info.columns,
    table=dst_table,
    order_by=info.order_by,
    engine=effective_engine,
)
await ch_client.command(f"CREATE TABLE {dst_table} ({...}) {engine_clause}")
oplog_record_table(dst_table, schema_json=schema_to_view(dst_schema).model_dump_json())
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_schema.py -v`
Expected: PASS.

- [ ] **Step 6: Remove `col_fieldtype`**

Per the spec: "The `col_fieldtype` field on `CopyInfo` is removed" and "`col_fieldtype` field is redundant and removed" from `Schema`.

Delete those fields from the dataclasses. Grep for remaining references:

```bash
rg "col_fieldtype" aaiclick/ --type py
```

Fix each call site — information that used to live in `col_fieldtype` is now carried per-column on `ColumnInfo.fieldtype` (added in Task 2.1). When you need the old "uniform per-column fieldtype", either look it up on a specific `ColumnInfo` or derive from `schema.fieldtype`.

- [ ] **Step 7: Run the full data suite**

Run: `pytest aaiclick/data/ -v`
Expected: the failure set from end of Phase 2 is now back to zero (or matches the pre-Phase-2 baseline). No `aai_id` related assertions should fail. Any remaining failures are genuine regressions — fix them before committing.

- [ ] **Step 8: Commit**

```bash
git add aaiclick/data/models.py aaiclick/data/object/operators.py aaiclick/data/object/ingest.py aaiclick/data/object/test_schema.py
git commit -m "$(cat <<'EOF'
refactor: persist SchemaView for copy and ingest derivations

Every aaiclick-managed CREATE TABLE now writes a table_registry row
with schema_json. col_fieldtype is removed from Schema and CopyInfo;
per-column fieldtype lives on ColumnInfo.fieldtype instead.
EOF
)"
```

---

## Phase 3 Complete

At this point:

- New tables contain only user columns — no `aai_id`, no ClickHouse COMMENTs.
- `table_registry.schema_json` is populated for every aaiclick-managed table.
- `_get_table_schema` (from Phase 2) round-trips successfully against those rows.
- `build_order_by_clause` returns `tuple()` for empty input and preserves user columns otherwise.
- `col_fieldtype` is gone from `Schema` and `CopyInfo`; per-column fieldtype lives on `ColumnInfo.fieldtype`.

Operators still reference `aai_id` in their SQL — that's Phase 4.
