Remove `aai_id`, Drop Table Comments, Move Schema Metadata to Registry JSON
---

# Motivation

Two drivers, in priority order:

1. **Performance** — every aaiclick table currently carries an `aai_id UInt64 DEFAULT generateSnowflakeID()` column appended at create time. That is one extra column written on every insert plus a Snowflake call per row. Schema reads also touch ClickHouse `system.columns` plus per-column `COMMENT` YAML parsing. Both costs are paid on every table touched.
2. **Simplicity** — `aai_id` is a hidden column that leaks into schemas, docs, and operator semantics. Per-column fieldtype lives in ClickHouse `COMMENT` YAML; object-level fieldtype is smuggled onto the `aai_id` column's comment. Two metadata stores, neither queryable from SQL. Consolidating into one JSON column on the SQL `table_registry` collapses three special cases into one.

This is a greenfield refactor. There are no production users; existing databases are disposable.

# Scope

In scope:

- Drop the `aai_id` column from all aaiclick-created ClickHouse tables.
- Drop ClickHouse `COMMENT` clauses from `CREATE TABLE` DDL.
- Add a `schema_json` column to the SQL `table_registry`, holding the full schema as a Pydantic-serialized JSON document.
- Make schema reconstruction read from the registry JSON instead of `system.columns` + COMMENT YAML.
- Tighten the operator contract: binary elementwise ops between two array Objects from different sources require an explicit `View(order_by=...)` on both sides, detected at call time. `.data()` gains keyword-only `order_by`, `offset`, and `limit` parameters (with `limit=1000` as a safety default) so callers can read rows from any Object without building a `View`.

Out of scope (explicitly):

- Schema evolution / `ALTER TABLE` reconciliation. aaiclick does not mutate table schemas after creation today.
- External (non-aaiclick-created) ClickHouse tables. The registry is authoritative for aaiclick-managed tables.
- Lineage metadata or column descriptions in `schema_json`. Pure metadata move; no new fields.
- Backwards-compatibility migration for existing databases.

# Physical Table Shape

Before:

```sql
CREATE TABLE t_123 (
  title String COMMENT '{fieldtype: s}',
  votes Int64 COMMENT '{fieldtype: a}',
  aai_id UInt64 DEFAULT generateSnowflakeID() COMMENT '{fieldtype: a}'
)
ENGINE = MergeTree
ORDER BY (aai_id)
```

After:

```sql
CREATE TABLE t_123 (
  title String,
  votes Int64
)
ENGINE = MergeTree
ORDER BY tuple()
```

Changes:

- No `aai_id` column emitted; no `DEFAULT generateSnowflakeID()`.
- No column `COMMENT`s.
- `build_order_by_clause()` no longer auto-appends `aai_id`. It returns the user-provided ORDER BY as-is, or `tuple()` for empty input.
- `aai_id` is no longer a reserved name; users may use it as an ordinary column name.

# Registry Metadata

A new `schema_json` column on `table_registry` (SQL, via Alembic migration). The contents are a Pydantic-serialized `SchemaView` — the existing pydantic model in `aaiclick/data/view_models.py` — extended with `fieldtype` at both the object and column levels:

```python
# aaiclick/data/view_models.py (extended in place)
class ColumnView(BaseModel):
    name: str
    type: str                          # ClickHouse base type, e.g. "Int64"
    nullable: bool = False
    array_depth: int = 0
    low_cardinality: bool = False
    fieldtype: Literal["s", "a"]       # NEW: per-column fieldtype

class SchemaView(BaseModel):
    columns: list[ColumnView] = Field(default_factory=list)
    order_by: str | None = None
    engine: EngineType | None = None
    fieldtype: Literal["s", "a", "d"]  # NEW: object-level fieldtype
```

`SchemaView` is already the API-response shape; after this change it doubles as the persistence shape stored in `schema_json`. No new model types are introduced.

The dataclass named `ColumnMeta` in `aaiclick/data/models.py` — whose only purpose today is YAML serialization into ClickHouse column comments — is deleted along with the YAML-comment code path.

Sync invariant: aaiclick owns all DDL on its tables. Registry rows and ClickHouse tables are created in the same code path, in order:

```
create_object(name, schema, rows):
    1. ch_client.execute(CREATE TABLE ...)        # DDL
    2. table_registry.insert(table=..., schema_json=...)
    3. ch_client.insert(table, rows)              # data
```

Failure modes:

- Step 1 fails — nothing written.
- Step 2 fails — orphan ClickHouse table; existing cleanup sweep handles it (queries `table_registry`; absence of a row is the orphan signal).
- Step 3 fails — table + registry row exist with no data; existing cleanup handles this case today and is unchanged.

The registry is authoritative for schema reads. There is no second `ALTER`-time write because there is no schema evolution today.

# Operator Contract

| Operation                                          | Requires explicit `order_by`?        |
|----------------------------------------------------|--------------------------------------|
| `a + b` (two array Objects, different sources)     | Yes — both sides as `View(order_by)` |
| `array_obj.data()`                                 | No — defaults to `None`; safety cap is `limit=1000` |
| `a + a` (same Object, or same-table fast path)     | No                                   |
| `array_obj + scalar_obj` (broadcast)               | No                                   |
| Aggregations producing arrays                      | No                                   |

Detection is at call time. `Object` and `View` already exist as separate classes; the check in operator code is structural — for cross-table binary ops, both operands must be `View` instances with `_order_by is not None`.

`.data()` accepts the ordering and paging kwargs directly so callers do not need to build a `View` for a one-shot read:

```python
async def data(
    self,
    orient: str = ORIENT_DICT,
    *,
    order_by: str | None = None,
    offset: int | None = None,
    limit: int | None = 1000,
): ...
```

The existing `orient` positional parameter is preserved for compatibility with existing call sites; `order_by`, `offset`, and `limit` are new keyword-only additions.

Resolution rules:

- If `self` is a `View`, the kwargs on `data()` override the `View`'s own `_order_by` / `_offset` / `_limit` when provided; otherwise the `View`'s values are used.
- `order_by=None` is permitted on any Object (including arrays); the result is whatever order ClickHouse returns. Determinism is opt-in via an explicit `order_by`.
- `limit=1000` is the default safety cap that makes calling `data()` without `order_by` safe. Callers wanting all rows pass `limit=None` explicitly.
- Scalars and dicts ignore `order_by`, `offset`, `limit` (single-row results); passing them is allowed but has no effect.

Errors:

```python
TypeError(
    "Binary elementwise ops on array Objects from different sources "
    "require an explicit row order. Wrap both sides with "
    ".view(order_by=...) before combining.\n"
    "  Got: <left repr> + <right repr>"
)
```

`data()` does not raise on missing `order_by`; the `limit=1000` cap is the safety mechanism.

SQL changes in `aaiclick/data/object/operators.py`:

- Cross-table array⊗array: `row_number() OVER (ORDER BY aai_id)` becomes `row_number() OVER (ORDER BY <left.order_by>)` on the left side and `... <right.order_by>` on the right.
- Same-table fast path: unchanged (single SELECT, no alignment needed).
- Scalar broadcast: drop the "preserve `aai_id`" SELECT clause; rely on the array side's natural source order.
- Aggregation result rows: drop the `generateSnowflakeID()` SELECT clause; aggregations no longer emit an `aai_id`.

# Schema Reconstruction & Data Flow

Today (`aaiclick/data/object/schema.py`, `_get_table_schema`):

1. `SELECT name, type, comment FROM system.columns WHERE table = ?` (ClickHouse).
2. Parse each comment YAML into a `ColumnMeta.fieldtype`.
3. Pull object-level fieldtype from the `aai_id` row's comment.
4. Build the `Schema` dataclass.

After:

1. `SELECT schema_json FROM table_registry WHERE table_name = ?` (PostgreSQL/SQLite).
2. `SchemaView.model_validate(schema_json)`.
3. Hydrate the in-memory `Schema` dataclass from `SchemaView`. The dataclass keeps its current shape so the rest of the runtime is undisturbed.

A missing registry row raises a clear error: the table either was not created by aaiclick or was created by a previous (pre-refactor) version. There is no fallback to `system.columns`.

Touched call sites:

- `aaiclick/data/data_context/data_context.py` — `create_object` family: emit DDL without `aai_id`/COMMENTs; build `SchemaView`; insert registry row.
- Copy operations (`CopyInfo.col_fieldtype` in `aaiclick/data/models.py`) — instead of carrying `col_fieldtype` through to recompose comments, propagate the source's `SchemaView` and write it for the copied table. The `col_fieldtype` field on `CopyInfo` is removed.
- `insert_objects_db`, `concat_objects_db` (`IngestQueryInfo.columns`) — already carries column info via `ColumnInfo`; gains writing the registry row for the new table.
- `Schema` dataclass (`aaiclick/data/models.py`) — gains per-column `fieldtype` (today only on YAML `ColumnMeta`); `col_fieldtype` field is redundant and removed.
- `build_order_by_clause` (`aaiclick/data/models.py`) — no longer auto-appends `aai_id`; returns `tuple()` for empty input.
- CLI renderers (`aaiclick/cli_renderers.py`) — drops the `if col.name == "aai_id": skip` filter; the column simply does not exist.
- Cleanup background tasks (`aaiclick/orchestration/background/`) — no logic change; they already query `table_registry`.

`aai_id` was not used as a stable lineage identifier (`table_registry.table_name` is the lineage key), so lineage and oplog code is unchanged.

Documentation updates:

- `docs/object.md` — remove the order-preservation-via-`aai_id` section; document the explicit `View(order_by=...)` contract for cross-table operators and the new `data(order_by=, offset=, limit=)` kwargs (including the `limit=1000` safety default).
- `docs/data_context.md` — update the schema-storage description to reference `table_registry.schema_json`.
- `docs/glossary.md` — remove the `aai_id` entry.
- `docs/lineage_implementation_plan.md`, `docs/insert_advisory_lock.md` — remove `aai_id` references.
- `docs/future.md` — strike any items made obsolete.

# Testing

Per `CLAUDE.md` testing rules: tests live alongside the modules they test, are flat module-level functions (no classes), are not decorated with `@pytest.mark.asyncio`, and do not test plain Python defaults or constructor passthrough.

New / updated test files:

- `aaiclick/data/test_models.py` — `SchemaView` round-trips (model → JSON → model) including the new `fieldtype` fields; `build_order_by_clause` no longer appends `aai_id`; empty input yields `tuple()`.
- `aaiclick/data/object/test_schema.py` — schema reconstruction reads from the registry, not `system.columns`; missing registry row raises a clear error.
- `aaiclick/data/test_data_context.py` — `create_object` emits DDL without `aai_id`/COMMENTs; the registry row is inserted with correct `schema_json`; `aai_id` is no longer reserved.
- `aaiclick/data/object/test_arithmetic.py` (and the other operator test files) — extend with the contract: cross-table `a + b` without Views raises `TypeError` at call time; with Views, `a.view(order_by=...) + b.view(order_by=...)` produces correctly aligned results; same-table fast path still works without Views; scalar broadcast still works without Views; aggregations still work.
- The test file covering `.data()` on Objects — `array_obj.data()` succeeds and returns at most 1000 rows in arbitrary order (no ordering required); `array_obj.data(order_by="...")` returns rows in the specified order; `array_obj.view(order_by=...).data()` succeeds; `array_obj.data(limit=None)` returns all rows; `View` values for `order_by`/`offset`/`limit` are overridden when kwargs are passed to `data()`; scalar and dict `.data()` work without any kwargs.
- `aaiclick/orchestration/migrations/versions/<new>_add_schema_json_to_table_registry.py` — Alembic migration adds the `schema_json` column.

SQL patterns are validated with the `chdb-eval` skill: no `ORDER BY aai_id` anywhere; `row_number() OVER (ORDER BY <user_order>)` in cross-table operator SQL; `ORDER BY tuple()` in `CREATE TABLE` for tables without a user-supplied order key.

Deleted:

- All assertions checking that `aai_id` appears in inferred schemas.
- COMMENT-parsing tests.
- `_pin_chdb_session` workarounds tied to `aai_id` reordering, if any (verified during implementation).
