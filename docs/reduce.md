reduce Orchestration Helper — Design
---

# Overview

`reduce(function, obj, initializer=None, *, partition, args, kwargs)` implements a
**layered parallel reduction** over an Object. Each layer partitions the current
input into Views, applies the function to each partition concurrently (all
INSERTing results into a single pre-allocated layer Object), then repeats until
only one row remains.

The design follows the same **pre-allocate + concurrent INSERT** pattern as
`map()`: `_expand_reduce` creates a `layer_obj` before spawning partition tasks,
and each `_reduce_part` task writes its result directly into `layer_obj` without
a separate collect/concat step.

The function must be **homomorphic**: its output schema must match its input
schema, because each layer's `layer_obj` becomes the next layer's input.

## Motivating Example

```
Input: Object (1300 rows), partition=500

Layer 0 — _expand_reduce creates layer0_obj (empty), spawns 3 tasks:
  _reduce_part(view rows 0–499,    dest=layer0_obj)  ─┐
  _reduce_part(view rows 500–999,  dest=layer0_obj)  ├─ parallel INSERT
  _reduce_part(view rows 1000–1299,dest=layer0_obj)  ─┘
  _after_reduce(layer0_obj, depends_on=all 3 above)

_after_reduce: count(layer0_obj) = 3 → continue
  → creates layer1_obj, spawns 1 task:
    _reduce_part(view rows 0–2 of layer0_obj, dest=layer1_obj)
    _after_reduce(layer1_obj, depends_on=above)

_after_reduce: count(layer1_obj) = 1 → return layer1_obj (final result)
```

# API

```python
def reduce(
    function: Callable,           # async def f(partition: Object, *args, **kwargs) -> Object
    obj: Union[Task, Object],
    initializer=None,             # Optional Python value; prepended to input before reduction
    *,
    partition: int = 5000,        # Rows per partition at each layer
    args: tuple = (),             # Extra positional args forwarded to function
    kwargs: Optional[dict] = None,
) -> Group
```

Returns a `Group` (consistent with `map()`). The group's final task result is
the reduced Object.

## Function Contract

```python
async def my_reduce_fn(partition: Object, *args, **kwargs) -> Object:
    ...
```

- Receives an Object (a partition View of the current layer's input)
- Must return an Object whose schema matches its input schema
- Should perform the aggregation in ClickHouse (not fetch rows to Python)
- The framework INSERTs the returned Object's rows into the pre-allocated
  `layer_obj`; the returned Object is then dropped

### Example: distributed sum

```python
async def sum_partition(partition: Object) -> Object:
    return await partition.sum()  # returns scalar Object (1 row, same type)

result_group = reduce(sum_partition, my_object, partition=5000)
```

### Example: with initializer

```python
result_group = reduce(sum_partition, my_object, initializer=0, partition=5000)
# equivalent to: prepend Object([0]) to my_object, then reduce
```

# Execution Model

## Layer-by-Layer Reduction

```
Layer 0:
  Input:    Object with N rows
  Pre-alloc: layer_obj (empty, schema = input schema)
  Split:    M = ceil(N / partition) Views (LIMIT + OFFSET ORDER BY aai_id)
  Spawn:    M _reduce_part tasks in parallel, each INSERTs 1 result row into layer_obj
  After:    _after_reduce waits for all M tasks → layer_obj has M rows

Layer 1:
  Input:    layer_obj (M rows)
  Pre-alloc: next_layer_obj (empty, same schema)
  Split:    K = ceil(M / partition) Views of layer_obj
  Spawn:    K _reduce_part tasks in parallel, each INSERTs into next_layer_obj
  After:    _after_reduce waits for all K tasks → next_layer_obj has K rows

...repeat until layer_obj has exactly 1 row...
```

Termination: `_after_reduce` returns `layer_obj` as the final result when
`count(layer_obj) == 1`. Special case: if `M == 1` on the first layer, a single
partition task runs and `_after_reduce` immediately returns the result.

## Initializer Handling

When `initializer` is not None:

1. Create an Object from the initializer value (`create_object_from_value`)
2. Concat initializer Object with `obj` → new input Object
3. Proceed with normal layered reduction

When `initializer` is None and `obj` is empty (0 rows):
- Raise `TypeError("reduce() of empty sequence with no initial value")`
  (mirrors Python's `functools.reduce` behavior)

# Task Graph Structure

```
Group (name="reduce")
└── _expand_reduce(input_obj, ...)
    ├── Queries count(input_obj)
    ├── Handles empty input (raise or prepend initializer)
    ├── Pre-allocates layer_obj (schema = input_obj.schema)
    ├── Creates M _reduce_part tasks (parallel, all writing to layer_obj)
    └── Creates _after_reduce(layer_obj, parts_done=[task_0..task_M-1], ...)
        ├── Depends on all M _reduce_part tasks (via parts_done list)
        ├── If count(layer_obj) == 1: return layer_obj (final)
        └── Else:
            ├── Pre-allocates next_layer_obj
            ├── Creates K _reduce_part tasks (next layer)
            └── Creates next _after_reduce(next_layer_obj, ...)
                └── (dynamic children returned for registration)
```

The `parts_done` parameter passes all `_reduce_part` Task objects as a list.
`_collect_upstreams` recurses into lists, so the framework automatically creates
dependencies on all M tasks without any special mechanism.

## Task Descriptions

### `_expand_reduce(function, obj, partition, group_id, layer, cbk_args, cbk_kwargs)`

Expander task. Decorated with `@task`. Runs at execution time.

1. Query: `SELECT count() FROM obj.table`
2. If `count == 0`: raise `TypeError` (initializer handling done upstream)
3. Compute `M = ceil(count / partition)`
4. `layer_obj = await create_object(obj.schema)`  ← pre-allocate empty dest
5. Create M `_reduce_part` tasks, each receiving a View and `layer_obj`:
   `View(limit=partition, offset=i*partition, order_by="aai_id")`
6. Create `_after_reduce` task with `layer_obj` + `parts_done=[*reduce_part_tasks]`
7. Return `[*reduce_part_tasks, after_task]` for dynamic registration

### `_reduce_part(function, part_view, layer_obj, cbk_args, cbk_kwargs) -> None`

Worker task. Decorated with `@task`. Runs once per partition per layer.

1. `temp = await function(part_view, *cbk_args, **cbk_kwargs)` (await if async)
2. `INSERT INTO layer_obj SELECT * FROM temp`
3. Drop `temp`
4. Return `None` (result is in `layer_obj`, not returned)

All M partition tasks write concurrently to the same `layer_obj`. ClickHouse
MergeTree engines support concurrent inserts.

### `_after_reduce(layer_obj, parts_done, function, partition, group_id, layer, cbk_args, cbk_kwargs)`

Collector task. Decorated with `@task`. Depends on all `_reduce_part` tasks
via the `parts_done` list parameter.

1. `parts_done` resolved at runtime to `[None, None, ...]` (just used for deps)
2. Query `count(layer_obj)`
3. If `count == 1`: return `layer_obj` (final result)
4. Else:
   a. `next_layer_obj = await create_object(layer_obj.schema)`
   b. Create K `_reduce_part` tasks for next layer (partitioning `layer_obj`)
   c. Create next `_after_reduce(next_layer_obj, parts_done=[...], layer=layer+1)`
   d. Return `[*reduce_part_tasks, after_task]` as dynamic children

The `layer` counter is for debugging/logging only.

# Intermediate Object Lifecycle

Each layer creates:
- 1 `layer_obj` (pre-allocated by `_expand_reduce` or `_after_reduce`, written
  to by M concurrent `_reduce_part` tasks, then read by the next `_after_reduce`)
- M temporary `temp` Objects (created inside each `_reduce_part` call, dropped
  immediately after INSERT)

Compare to previous concat-based design:
- **New**: 1 layer Object + M short-lived temp Objects per layer
- **Old**: M partial Objects + 1 concat Object per layer (more total Objects)

Lifecycle is managed automatically:
- **Local mode**: `LocalLifecycleHandler` drops tables when refcount reaches 0
- **Distributed mode**: `PgLifecycleHandler` pin/claim pattern; `PgCleanupWorker`
  drops tables after all references are released

# Design Constraints & Trade-offs

## Function must be homomorphic

The same function is applied at every layer. Output schema must match input
schema because `layer_obj` is created with the input schema and becomes the
next layer's input. The most natural pattern: function reduces any number of
rows to exactly 1 row.

## Partition argument controls fan-out

`partition=P` means each task processes at most P rows:
- Larger P → fewer tasks, less parallelism, less overhead
- Smaller P → more tasks, more parallelism, more overhead

Default `partition=5000` matches `map()`.

## Concurrent writes to layer_obj

All M `_reduce_part` tasks INSERT into the same `layer_obj` table concurrently.
ClickHouse MergeTree tables support concurrent inserts natively. Row ordering
within `layer_obj` follows Snowflake ID insertion order (non-deterministic
across parallel tasks), which is acceptable since the function must be
commutative anyway.

## No cross-partition state

The function cannot depend on ordering or accumulate state across partitions.
Must be associative and commutative for deterministic results.

Correct: sum, product, min, max, bitwise AND/OR
Incorrect: running median, sequential state machines, order-dependent operations

# Usage Examples

## Basic sum

```python
@task
async def sum_part(partition: Object) -> Object:
    return await partition.sum()

@job("total_sum")
def compute_total(data: Object):
    return reduce(sum_part, data, initializer=0, partition=5000)
```

## Custom aggregation (max absolute value)

```python
@task
async def max_abs_part(partition: Object) -> Object:
    abs_obj = await partition.abs()
    return await abs_obj.max()

@job("max_abs")
def compute_max_abs(data: Object):
    return reduce(max_abs_part, data, partition=5000)
```

## Word count merge

```python
@task
async def merge_counts(partition: Object) -> Object:
    # partition has columns: word (String), count (Int64)
    return await (await partition.group_by("word")).sum("count")

@job("word_count")
def aggregate_word_counts(partial_counts: Object):
    return reduce(merge_counts, partial_counts, partition=5000)
```

# Relation to map()

`reduce()` complements `map()`. A typical pipeline:

```python
@task
async def process_row(row, factor):
    ...

@task
async def aggregate_partition(partition: Object) -> Object:
    return await partition.sum()

@job("etl")
def pipeline(data: Object):
    mapped = map(process_row, data, partition=5000, kwargs={"factor": 2})
    reduced = reduce(aggregate_partition, mapped, partition=5000)
    return reduced
```

The result of `map()` (a Group) can be passed as `obj` to `reduce()`.
`_expand_reduce` waits for the map Group to complete via the standard task
dependency mechanism before querying the row count.

Both `map()` and `reduce()` follow the same pattern:
- Define-time: create Group + expander task (no DB access)
- Runtime: expander pre-allocates output Object, spawns partition tasks
- Partition tasks write concurrently into the pre-allocated Object

The difference: `reduce()` adds `_after_reduce` to check termination and
optionally spawn the next layer.

# Open Questions (for implementation)

1. **Schema for layer_obj**: `create_object(input_obj.schema)` assumes the
   function's output schema matches the input schema (homomorphic). Should
   `_expand_reduce` validate this on a 1-row sample before creating all tasks,
   or defer to runtime INSERT failures?

2. **Termination condition**: `count == 1` or `count <= 1`? Strictly 1 is
   cleaner; `<= 1` handles the edge case where a buggy function returns 0 rows.
   A layer cap (max layers) would guard against infinite loops.

3. **parts_done resolution**: At `_after_reduce` runtime, `parts_done` resolves
   to `[None, None, ...]` (since `_reduce_part` returns None). Verify the
   execution engine handles lists of None upstream results correctly.

4. **layer_obj ownership across tasks**: `layer_obj` is created by
   `_expand_reduce` and referenced by both `_reduce_part` tasks and
   `_after_reduce`. In distributed mode, the pin/claim lifecycle handler must
   keep `layer_obj` alive until `_after_reduce` completes. Confirm `PgLifecycleHandler`
   handles this multi-task reference correctly.
