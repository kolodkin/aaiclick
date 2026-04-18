Join
---

# Overview

`join()` combines two Objects on one or more key columns into a new Object, mirroring the familiar `pandas.DataFrame.merge` / `Spark DataFrame.join` shape. The result is a fresh ClickHouse table — same materialization pattern as `concat()` (see `aaiclick/data/object/ingest.py` → `concat_objects_db`).

Status: ⚠️ NOT YET IMPLEMENTED — tracked in `docs/future.md`.

```python
basics  = await create_object_from_value([...])   # tconst, title, year
ratings = await create_object_from_value([...])   # tconst, rating, votes

joined = await basics.join(ratings, on="tconst", how="left")
await joined.data(orient="records")
# → [{"tconst": "tt0000001", "title": "Carmencita", "year": 1894,
#     "rating": 5.7, "votes": 2068}, ...]
```

# API

```python
async def join(
    self,
    other: Object,
    *,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    how: Literal["inner", "left", "right", "full", "cross"] = "inner",
    suffixes: tuple[str, str] | None = None,
) -> Object
```

**Arguments**:

| Name       | Type                                      | Notes                                                                 |
|------------|-------------------------------------------|-----------------------------------------------------------------------|
| `other`    | `Object`                                  | Right-hand Object. Must be a dict fieldtype (`d`).                   |
| `on`       | `str \| list[str] \| None`                | Key column(s) present under the same name in both Objects.           |
| `left_on`  | `str \| list[str] \| None`                | Left-side key(s) when names differ. Requires matching `right_on`.    |
| `right_on` | `str \| list[str] \| None`                | Right-side key(s) when names differ. Requires matching `left_on`.    |
| `how`      | `"inner" \| "left" \| "right" \| "full" \| "cross"` | Join type. Default `"inner"`.                                  |
| `suffixes` | `tuple[str, str] \| None`                 | Applied to non-key columns that collide. When `None`, a collision is a `ValueError`. |

**Mutual exclusion**: exactly one of `{on}` or `{left_on, right_on}` must be set — except for `how="cross"`, where all three must be unset. Violations raise `ValueError` before any SQL is emitted.

**Self-join**: `a.join(a, on="id")` works; the SQL aliases the two sources as `l` / `r`.

# Join Types

ClickHouse supports all five natively. Nullable promotion applies to the "outer" side only:

| `how`    | ClickHouse keyword | Left cols nullable? | Right cols nullable? | Notes                              |
|----------|--------------------|---------------------|----------------------|------------------------------------|
| `inner`  | `INNER JOIN`       | no                  | no                   | Default. Only matching rows.       |
| `left`   | `LEFT JOIN`        | no                  | **yes** (promoted)   | All left rows; right NULL on miss. |
| `right`  | `RIGHT JOIN`       | **yes** (promoted)  | no                   | Symmetric to `left`.               |
| `full`   | `FULL OUTER JOIN`  | **yes** (promoted)  | **yes** (promoted)   | Union of both.                     |
| `cross`  | `CROSS JOIN`       | no                  | no                   | Cartesian product. No keys.        |

!!! warning "`right` / `full` may be expensive on large left sides"
    ClickHouse implements `RIGHT`/`FULL` by building a hash table of the right relation and scanning the left. For very large left Objects, consider swapping the call (`b.join(a, how="left")`) or materializing a filter first.

Deferred to follow-ups: `semi`, `anti`, `asof`. Each has semantic nuance (multiplicity, sort-order requirement) worth separate consideration.

# Column Semantics

## Key columns

- **`on="tconst"`** (same name both sides) → emitted as SQL `USING (tconst)`. One copy survives in the result, under the original name.
- **`left_on="id"`, `right_on="tconst"`** → emitted as `ON l.id = r.tconst`. Both columns survive under their original names. (Renaming after the join is cheap via `.rename()`.)
- Keys must compare-equal at the ClickHouse type level via `_are_types_compatible` (same helper used by `concat`). `String` ↔ `FixedString` is accepted; `String` ↔ `Int64` is not.

## Non-key collisions

Given a left column `name` and a right column `name`, both non-key:

- `suffixes=None` (default) → `ValueError: join column collision on {'name'}; pass suffixes=('_x', '_y') or rename first`.
- `suffixes=("_l", "_r")` → result columns are `name_l`, `name_r`. Both sides always get a suffix — no silent asymmetry.

Empty suffixes (`("", "_r")`) are rejected to avoid resurrecting the collision on one side.

## `aai_id`

The result gets fresh `generateSnowflakeID()` values, exactly like `concat()`. Source `aai_id` columns are dropped before projection — they never appear in the output schema even under `suffixes`.

## Column order

`[keys..., left non-key cols in left order..., right non-key cols in right order...]`. Keys appear once under `USING`, twice (left-first) under `left_on`/`right_on`.

## Nullable / LowCardinality / Array

- Nullable: promoted per the table above. `LowCardinality(String)` on the outer side becomes `LowCardinality(Nullable(String))`.
- LowCardinality: preserved on both sides — ClickHouse handles `JOIN` on `LowCardinality` keys natively.
- Array: allowed for non-key columns; join keys must be scalar.

# Result Schema & Fieldtype

The result is always a **dict** Object (`FIELDTYPE_DICT`), even when both inputs are arrays — joins logically produce row-oriented data. Callers that need an array can unwrap via column selection (`joined["rating"]`).

`order_by` on the result is unset — join output has no intrinsic order. `data()` still returns rows via `ORDER BY aai_id`, giving stable-but-arbitrary order. Callers who need deterministic order should `.view(order_by=...)` on the result.

# Implementation Sketch

Follows the `concat_objects_db` shape in `aaiclick/data/object/ingest.py`:

```python
# aaiclick/data/object/join.py

async def join_objects_db(
    left: IngestQueryInfo,
    right: IngestQueryInfo,
    *,
    left_keys: list[str],
    right_keys: list[str],
    how: str,
    suffixes: tuple[str, str] | None,
    ch_client,
) -> Object:
    # 1. Validate: key existence, type compatibility, collision policy.
    # 2. Build result Schema:
    #    - keys (deduped if names match)
    #    - left non-key columns (suffixed on collision)
    #    - right non-key columns (suffixed on collision)
    #    - nullable promotion per `how`
    # 3. result = await create_object(schema)
    # 4. Emit:
    #      INSERT INTO {result.table} ({cols})
    #      SELECT {projection}
    #      FROM {left.source} AS l
    #      {HOW} JOIN {right.source} AS r
    #      {USING (k) | ON l.k = r.k}
    # 5. oplog_record_sample(result.table, "join",
    #        kwargs={"left": left.base_table, "right": right.base_table,
    #                "on": ..., "how": how})
    return result
```

The `Object.join()` method on `aaiclick/data/object/object.py` resolves the `on` / `left_on` / `right_on` triangle into explicit `left_keys` / `right_keys` lists, then calls `join_objects_db`.

# Examples

## Basic inner join

```python
users  = await create_object_from_value([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
])
orders = await create_object_from_value([
    {"user_id": 1, "total": 99.5},
    {"user_id": 1, "total": 14.0},
    {"user_id": 3, "total": 42.0},
])

joined = await users.join(orders, left_on="id", right_on="user_id")
await joined.data(orient="records")
# → [{"id": 1, "user_id": 1, "name": "Alice", "total": 99.5},
#    {"id": 1, "user_id": 1, "name": "Alice", "total": 14.0}]
```

## Left join with nullable right side

```python
enriched = await users.join(orders, left_on="id", right_on="user_id", how="left")
# → Alice rows keep their totals; Bob gets total=NULL
# Schema: total is Nullable(Float64) in the result.
```

## Suffixes on collision

```python
a = await create_object_from_value([{"id": 1, "score": 10}])
b = await create_object_from_value([{"id": 1, "score": 99}])

merged = await a.join(b, on="id", suffixes=("_l", "_r"))
# → [{"id": 1, "score_l": 10, "score_r": 99}]
```

## Cross join

```python
colors = await create_object_from_value([{"c": "red"}, {"c": "blue"}])
sizes  = await create_object_from_value([{"s": "S"}, {"s": "M"}])
skus   = await colors.join(sizes, how="cross")
# → 4 rows, every color × every size
```

# Oplog / Lineage

```python
oplog_record_sample(
    result.table,
    "join",
    kwargs={
        "left":  left.base_table,
        "right": right.base_table,
        "left_on":  left_keys,
        "right_on": right_keys,
        "how": how,
    },
)
```

The recorded `kwargs` are sufficient to reconstruct the call; any SQL-level variant (`USING` vs `ON`) is derivable from `left_keys == right_keys`.

# Distributed Considerations

Non-goals for v1 — the default chdb + SQLite backend is single-shard and the naive `JOIN` suffices. Recorded here for the distributed backend follow-up:

- **Broadcast vs shuffle**: ClickHouse's default is hash join, materializing the right side in memory. For a small right Object joined against a large left, this is correct. For two large Objects on a sharded cluster, `GLOBAL JOIN` is required so the right side is collected once per node rather than per-shard.
- **Strategy hints**: `join_algorithm='parallel_hash'`, `'partial_merge'`, `'grace_hash'` — revisit once a real distributed benchmark exists.
- **Collocation**: when both Objects share a sharding key equal to the join key, a local `JOIN` is correct without `GLOBAL`. aaiclick has no sharding-key metadata today, so assume non-collocated.

Nothing in the v1 API precludes later adding a `strategy=` kwarg.

# Interaction With Other Work

- **Lazy Operator Results** (`docs/future.md`): joins typically produce large materialized results, so eager CREATE + INSERT remains the right default. The `LazyView` wrapper, if/when it lands, could cover `a.join(b).view(limit=10)` without materializing — orthogonal to this feature.
- **Two-tier persistent tables** (`p_*` / `j_*`): join results default to transient `t_*` like every other operator. No special casing.

# Open Questions

1. **Asof join**: ClickHouse's `ASOF LEFT JOIN` is uniquely useful for time-series. Worth a separate mini-spec once the base join ships — needs `order_by` on the right side and a `tolerance` parameter.
2. **Semi / anti**: trivially supportable as `how="semi" | "anti"`, but the result schema is "left columns only", diverging from the table above. Defer with asof.
3. **Key renaming ergonomics**: `left_on` + `right_on` both survive in output. Is that surprising vs pandas (which drops one)? Current call: keep both, document clearly, let `.rename()` handle the cleanup.
