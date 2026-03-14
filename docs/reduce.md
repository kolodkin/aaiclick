reduce Orchestration Helper — Design
---

# Overview

`reduce(function, obj, initializer=None, *, partition, args, kwargs)` implements a
**layered parallel reduction** over an Object. Each layer partitions the current
input into Views, applies the function to each partition concurrently (all
INSERTing results into a single pre-allocated layer Object), then repeats until
only one row remains.

**All layers, subgroups, and tasks are created at once** inside a single
`_expand_reduce` execution. `_expand_reduce` queries the row count once, computes
the full layer structure upfront, pre-allocates all layer Objects, and returns
every task and subgroup as dynamic children in one shot. There is no lazy
layer-by-layer expansion.

The function must be **homomorphic**: its output schema must match its input
schema, because each layer's `layer_obj` becomes the next layer's input.

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
) -> Task[Object]
```

Returns `_expand_reduce` as a `Task[Object]`. The task's result value is
`layer_last_obj` — the final single-row Object produced by the reduction.
The Group("reduce") and all layer subgroups are created internally and
registered through `_expand_reduce`'s dynamic children.

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

# Layer Count

Given `N` input rows and partition size `P`, the number of layers is:

```
layers = ⌈log_P(N)⌉
```

Computed iteratively (handles all edge cases):

```python
def num_layers(N: int, P: int) -> int:
    layers = 0
    while N > 1:
        N = ceil(N / P)
        layers += 1
    return layers
```

**Example — 1300 rows, partition=500:**

```
Layer 0  input=1300  tasks=⌈1300/500⌉=3   layer_0_obj → 3 rows
Layer 1  input=3     tasks=⌈3/500⌉   =1   layer_1_obj → 1 row  ✓
```
2 layers, 4 `_reduce_part` tasks total.

**Example — 210 rows, partition=10:**

```
Layer 0  input=210  tasks=⌈210/10⌉=21  layer_0_obj → 21 rows
Layer 1  input=21   tasks=⌈21/10⌉ = 3  layer_1_obj →  3 rows
Layer 2  input=3    tasks=⌈3/10⌉  = 1  layer_2_obj →  1 row  ✓
```
3 layers, 25 `_reduce_part` tasks total.

# Task Graph Structure

`reduce()` is called at define-time and creates two things:

1. **Group "reduce"** — top-level group (internal, not returned to caller)
2. **`_expand_reduce` task** — returned to the caller as `Task[Object]`

All remaining structure is created inside `_expand_reduce` at runtime, in one
shot. `_expand_reduce` returns two things to the framework:

- **Task result**: `layer_last_obj` — the final Object (1 row, same schema as input)
- **Dynamic children**: all layer subgroups and `_reduce_part` tasks

```
[define-time]
Group "reduce"
└── _expand_reduce   ← returned to caller as Task[Object]

[_expand_reduce runtime — all created at once]
Group "reduce"
└── _expand_reduce → result: layer_last_obj (Object)
    └── dynamic children:
        ├── Group "layer_0"
        │   ├── _reduce_part(view[0:P]   → layer_0_obj)
        │   ├── _reduce_part(view[P:2P]  → layer_0_obj)
        │   └── _reduce_part(view[2P:N]  → layer_0_obj)
        └── Group "layer_1"  ← depends on Group "layer_0"
            └── _reduce_part(view[0:M0] of layer_0_obj → layer_1_obj)
```

`layer_1_obj` above IS `layer_last_obj` — the same Object returned as the
task result. No separate terminal task is needed.

Each `Group "layer_L+1"` depends on `Group "layer_L"` completing — enforces
sequential layer execution while all tasks within a layer run in parallel.

## Concrete Example: 1300 rows, partition=500

```
Group "reduce"
└── _expand_reduce → result: layer_1_obj (Object, 1 row)
    └── dynamic children (all registered at once):
        ├── Group "layer_0"
        │   ├── _reduce_part(view rows    0– 499  → layer_0_obj)
        │   ├── _reduce_part(view rows  500– 999  → layer_0_obj)
        │   └── _reduce_part(view rows 1000–1299  → layer_0_obj)
        └── Group "layer_1"  [depends on Group "layer_0"]
            └── _reduce_part(view rows 0–2 of layer_0_obj → layer_1_obj)
```

`layer_1_obj` (3 rows written by layer_0, reduced to 1 row in layer_1) is the
final Object returned by `_expand_reduce` as its task result.

## Concrete Example: 210 rows, partition=10

```
Group "reduce"
└── _expand_reduce → result: layer_2_obj (Object, 1 row)
    └── dynamic children (all registered at once):
        ├── Group "layer_0"
        │   ├── _reduce_part(view rows   0–  9  → layer_0_obj)
        │   ├── _reduce_part(view rows  10– 19  → layer_0_obj)
        │   │   ...  (21 tasks total)
        │   └── _reduce_part(view rows 200–209  → layer_0_obj)
        ├── Group "layer_1"  [depends on Group "layer_0"]
        │   ├── _reduce_part(view rows  0– 9 of layer_0_obj → layer_1_obj)
        │   ├── _reduce_part(view rows 10–19 of layer_0_obj → layer_1_obj)
        │   └── _reduce_part(view rows 20–20 of layer_0_obj → layer_1_obj)
        └── Group "layer_2"  [depends on Group "layer_1"]
            └── _reduce_part(view rows 0–2 of layer_1_obj → layer_2_obj)
```

# Task Descriptions

## `_expand_reduce(function, obj, partition, cbk_args, cbk_kwargs) -> Object`

Expander task. Decorated with `@task`. Runs once at execution time.
Returns `layer_last_obj` (the final Object) as its task result value, plus
all layer subgroups and `_reduce_part` tasks as dynamic children.

1. Query: `SELECT count() FROM obj`
2. If `count == 0`: raise `TypeError("reduce() of empty sequence with no initial value")`
3. Compute full layer structure using `num_layers(count, partition)`:
   - `sizes = [count]`; iterate: `sizes.append(ceil(sizes[-1] / partition))` until last == 1
4. Pre-allocate all layer Objects: `layer_objs = [await create_object(obj.schema) for _ in layers]`
5. Build all tasks and subgroups:
   - For each layer `L`:
     - `src = obj if L == 0 else layer_objs[L-1]`
     - `M = ceil(sizes[L] / partition)`
     - Create `Group(f"layer_{L}")` depending on previous layer's Group (if L > 0)
     - Create M `_reduce_part(function, View(src, offset=i*P, limit=P), layer_objs[L], cbk_args, cbk_kwargs)` tasks inside `Group("layer_L")`
6. Return `(layer_objs[-1], [*all_groups, *all_tasks])`:
   - First element: the final Object (task result value, 1 row)
   - Second element: flat list of all Groups and tasks (dynamic children for registration)

## `_reduce_part(function, part_view, layer_obj, cbk_args, cbk_kwargs) -> None`

Worker task. Decorated with `@task`. Runs once per partition per layer.

1. `temp = await function(part_view, *cbk_args, **cbk_kwargs)` (await if async)
2. `INSERT INTO layer_obj SELECT * FROM temp`
3. Drop `temp`
4. Return `None` (result written into `layer_obj`, not returned)

All M tasks within a layer write concurrently to the same `layer_obj`.
ClickHouse MergeTree tables support concurrent inserts natively.

# Initializer Handling

When `initializer` is not None:

1. `init_obj = await create_object_from_value(initializer)`
2. Concat `init_obj` with `obj` → new input Object
3. `_expand_reduce` receives the combined Object and proceeds normally

When `initializer` is None and `obj` is empty (0 rows):
- `_expand_reduce` raises `TypeError("reduce() of empty sequence with no initial value")`
  (mirrors Python's `functools.reduce` behavior)

# Intermediate Object Lifecycle

`_expand_reduce` creates all layer Objects upfront:
- **`layer_L_obj`**: pre-allocated by `_expand_reduce`, written by M concurrent
  `_reduce_part` tasks, read by the next layer's tasks
- **`temp` Objects**: created inside each `_reduce_part` call, dropped immediately
  after INSERT

Lifecycle managed automatically:
- **Local mode**: `LocalLifecycleHandler` drops tables when refcount reaches 0
- **Distributed mode**: `PgLifecycleHandler` pin/claim; `PgCleanupWorker` drops
  tables after all references are released

All layer Objects are created by `_expand_reduce` and referenced by tasks
registered as its dynamic children. `layer_last_obj` is also the task's return
value, so the lifecycle handler keeps it alive until all downstream consumers
release their reference.

# Design Constraints & Trade-offs

## Function must be homomorphic

The same function is applied at every layer. Output schema must match input
schema because all `layer_L_obj` Objects are created with the input schema.
The most natural pattern: function reduces any number of rows to exactly 1 row.

## Partition argument controls fan-out

`partition=P` means each task processes at most P rows:
- Larger P → fewer tasks per layer, less parallelism, less overhead
- Smaller P → more tasks per layer, more parallelism, more overhead

Default `partition=5000` matches `map()`.

## Concurrent writes to layer_obj

All M `_reduce_part` tasks within a layer INSERT into the same `layer_obj`
concurrently. Row ordering within `layer_obj` is non-deterministic across
parallel tasks — acceptable since the function must be associative and
commutative for correct results.

## No cross-partition state

The function cannot depend on ordering or accumulate state across partitions.

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
    return reduced  # Task[Object] — result is the final single-row Object
```

The result of `map()` (a Group or Task) can be passed as `obj` to `reduce()`.
`_expand_reduce` depends on `obj` completing before it queries the row count —
standard task dependency via `_collect_upstreams`.

Both `map()` and `reduce()` follow the same pattern:
- **Define-time**: create Group + expander task (no DB access)
- **Runtime**: expander queries count, pre-allocates output Object(s), spawns
  all partition tasks at once, returns final Object as task result

The difference: `reduce()` computes multiple layers in `_expand_reduce` and
organises them into layer subgroups with inter-layer Group dependencies.
`reduce()` returns `Task[Object]` directly; `map()` returns a `Group` (its
output is an Object written by multiple parallel partition tasks).

# Open Questions (for implementation)

1. **Schema for layer_obj**: `create_object(input_obj.schema)` assumes homomorphic
   output. Should `_expand_reduce` validate schema on a 1-row sample before
   pre-allocating all layer Objects, or defer to runtime INSERT failures?

2. **Expander task result + dynamic children**: The framework must support
   expander tasks that return both a result value (`layer_last_obj`) and a list
   of dynamic children. Confirm the task protocol for returning `(result, children)`
   and that `_expand_reduce`'s result value flows to downstream consumers correctly.

3. **layer_obj ownership**: `layer_last_obj` is returned as `_expand_reduce`'s
   task result. Confirm `PgLifecycleHandler` keeps it alive until all downstream
   consumers that depend on `_expand_reduce` complete and release their pin.

4. **Group dependency registration**: Layer subgroups depend on the previous
   layer's Group. Confirm the execution engine resolves Group→Group dependencies
   correctly (Group is COMPLETED only when all its tasks are COMPLETED).
