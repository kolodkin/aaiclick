reduce Orchestration Helper — Design
---

# Overview

`reduce(function, obj, initializer=None, *, partition, args, kwargs)` implements a
**parallel tree reduction** over an Object. Unlike Python's sequential
`functools.reduce`, this variant achieves O(n log_P n) depth by applying the
function to P-row partitions in parallel and feeding the partial results back
through the same function until a single result remains.

The function must therefore be **homomorphic**: its output schema must be
compatible with its input schema, because outputs of one layer become inputs
of the next.

## Motivation

Tree reduction is the standard distributed pattern for associative operations
(sum, product, min, max, concat, merge, custom aggregation). Parallelism is
proportional to the number of partitions at each layer; depth is O(log_P n)
layers.

```
Input (N=8 rows, P=2):

Layer 0:   [a b] [c d] [e f] [g h]   ← 4 partition tasks (parallel)
               ↓     ↓     ↓     ↓
Layer 1:  [p1 p2] [p3 p4]            ← 2 partition tasks (parallel)
               ↓       ↓
Layer 2:  [q1   q2]                  ← 1 partition task
               ↓
           result                    ← final Object
```

Each layer creates new intermediate Objects and drops them when no longer needed.

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

- Receives an Object (a partition View or intermediate layer Object)
- Must return an Object whose schema is compatible with its own input schema
  (output rows become inputs at the next layer)
- Should perform the actual aggregation in ClickHouse (not fetch rows to Python)

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
  Input:  Object with N rows
  Split:  M = ceil(N / partition) Views (LIMIT + OFFSET + ORDER BY aai_id)
  Apply:  function(view_i) → partial_i  for i in 0..M-1   (parallel)
  Concat: concat(partial_0, ..., partial_M-1) → layer_1_obj (M rows)

Layer 1:
  Input:  layer_1_obj (M rows)
  Split:  K = ceil(M / partition) Views
  Apply:  function(view_j) → partial_j  for j in 0..K-1   (parallel)
  Concat: concat results → layer_2_obj (K rows)

...repeat until count == 1...

Layer k:
  Input:  layer_k_obj (1 row)
  → return layer_k_obj as final result  (no further reduction needed)
```

Termination: when the concat result has exactly 1 row, that row is the result.
Special case: M == 1 on any layer means only one partition exists; apply
function once and the result is final.

## Initializer Handling

When `initializer` is not None:

1. Create an Object from the initializer value (using `create_object_from_value`)
2. Schema-check: the initializer Object must be compatible with `obj`
3. Concat initializer Object with `obj` → new input Object
4. Proceed with normal tree reduction

When `initializer` is None and `obj` is empty (0 rows):
- Raise `TypeError("reduce() of empty sequence with no initial value")`
  (mirrors Python's `functools.reduce` behavior)

# Task Graph Structure

```
Group (name="reduce")
└── _expand_reduce            ← expander task; runs at execution time
    ├── Queries row count of input Object
    ├── Handles empty input (raise or return initializer)
    ├── If M == 1: creates single _reduce_part + _finalize_reduce
    └── If M > 1:
        ├── Creates M _reduce_part tasks (parallel)
        └── _collect_reduce (depends on all M _reduce_part tasks)
            ├── Concats M partial result Objects → layer_obj
            ├── If count(layer_obj) > 1:
            │   └── Creates next-layer _expand_reduce (dynamic child)
            └── If count(layer_obj) == 1:
                └── Returns layer_obj as final result
```

The dynamic task creation pattern follows `_expand_map`: tasks returned from
`_expand_reduce` and `_collect_reduce` are registered as children by the
orchestration engine.

## Task Descriptions

### `_expand_reduce(function, obj, partition, group_id, layer, cbk_args, cbk_kwargs)`

Expander task. Decorated with `@task`. Runs at execution time.

1. Query: `SELECT count() FROM obj.table`
2. If `count == 0`: raise (initializer handled upstream before this task)
3. Compute `M = ceil(count / partition)`
4. Create M `_reduce_part` tasks, each with a View:
   `View(limit=partition, offset=i*partition, order_by="aai_id")`
5. Create `_collect_reduce` task that depends on all M `_reduce_part` tasks
6. Return `[*reduce_part_tasks, collect_task]` for dynamic registration

### `_reduce_part(function, part, cbk_args, cbk_kwargs) -> Object`

Worker task. Decorated with `@task`. Runs once per partition.

1. Call `function(part, *cbk_args, **cbk_kwargs)` (awaiting if async)
2. Return the resulting Object

The returned Object is serialized via `_serialize_ref()` and stored as the
task result. `_collect_reduce` reads these results via task output resolution.

### `_collect_reduce(part_results, group_id, function, partition, layer, cbk_args, cbk_kwargs) -> Object`

Collector task. Decorated with `@task`. Runs after all `_reduce_part` tasks
in the current layer complete.

1. Receive list of partial result Objects (resolved from `_reduce_part` results)
2. If `len(part_results) == 1`: return `part_results[0]` (already final)
3. Concat all partial Objects → `layer_obj`
4. Query `count(layer_obj)`
5. If `count == 1`: return `layer_obj` (done)
6. Else: create next-layer `_expand_reduce(function, layer_obj, ...)`
   with `layer=layer+1` and return new expander task for dynamic registration

The `layer` counter is for debugging/logging only (no functional effect).

# Intermediate Object Lifecycle

Each layer creates:
- M temporary partial Objects (returned by `_reduce_part`, consumed by `_collect_reduce`)
- 1 layer concat Object (created by `_collect_reduce`, passed to next `_expand_reduce`)

Lifecycle is managed automatically:
- **Local mode**: `LocalLifecycleHandler` drops tables when refcount reaches 0
- **Distributed mode**: `PgLifecycleHandler` pin/claim pattern transfers ownership
  between tasks; `PgCleanupWorker` drops tables after all references are released

No special cleanup code needed in reduce itself — the existing machinery handles it.

# Design Constraints & Trade-offs

## Function must be homomorphic

The same function is applied at every layer. This means:
- Output schema must be compatible with input schema (same column names/types)
- Output typically has fewer rows than input (aggregation)
- The most natural pattern: function reduces any number of rows to a single row

This is not a hard requirement — function could return multiple rows — but
reduction terminates only when a single row remains, so multi-row outputs
extend the number of layers.

## Partition argument controls fan-out

`partition=P` means each task processes at most P rows. Larger P:
- Fewer tasks per layer → less orchestration overhead
- More work per task → less parallelism

Smaller P:
- More tasks per layer → more parallelism
- More orchestration overhead

For most aggregations, `partition=5000` (same default as `map`) is a
reasonable starting point.

## No cross-partition state

Unlike a sequential reduce, the function cannot depend on ordering or
accumulate state across partitions. The function must be associative and
(if initializer is provided) the initializer must be the identity element
for the operation.

Correct: sum, product, min, max, bitwise AND/OR, string concat (with separator)
Incorrect: running median, sequential state machines, order-dependent operations

## Argument order does not affect result

Partial results are concatenated via `concat()`, which preserves Snowflake ID
order (creation time). Since partition tasks run in parallel and creation order
is not guaranteed, the reduce function must be commutative as well as
associative for results to be deterministic.

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

## Word count merge (dict Object)

```python
@task
async def merge_counts(partition: Object) -> Object:
    # partition has columns: word (String), count (Int64)
    # group by word, sum counts
    return await (await partition.group_by("word")).sum("count")

@job("word_count")
def aggregate_word_counts(partial_counts: Object):
    return reduce(merge_counts, partial_counts, partition=5000)
```

# Relation to map()

`reduce()` is designed to complement `map()`. A typical pipeline:

```python
@task
async def process_row(row, factor):
    ...  # row-level work

@task
async def aggregate_partition(partition: Object) -> Object:
    return await partition.sum()

@job("etl")
def pipeline(data: Object):
    mapped = map(process_row, data, partition=5000, kwargs={"factor": 2})
    reduced = reduce(aggregate_partition, mapped, partition=5000)
    return reduced
```

The result of `map()` (a Group) can be passed as `obj` to `reduce()`. The
`_expand_reduce` expander task will wait for the map Group to complete before
querying the row count, using the standard task dependency mechanism.

# Open Questions (for implementation)

1. **`_collect_reduce` dependency resolution**: The task framework resolves
   upstream task results for known parameter names. `_collect_reduce` needs
   all `_reduce_part` results as a list. This may require a wrapper that
   collects N results into a list — or a new mechanism in the execution engine
   for "collect N results from a group".

2. **Termination condition**: Should termination be `count == 1` or
   `count <= 1`? Strictly 1 is cleaner; `<= 1` handles edge case where
   function returns 0 rows (which would be a bug in the user's function, but
   worth considering for robustness).

3. **Layer cap**: Should there be a maximum layer count to prevent accidental
   infinite loops (e.g., function always returns multiple rows)?

4. **Schema validation**: Should `_expand_reduce` validate that function's
   output schema is compatible with input schema before creating all tasks?
   This would require a speculative function call on a 1-row sample.
