Parallel Operators
---

# Parallel Operators

`map`, `foreach` and `reduce` fan a computation out over partitions of an
`Object`:

<!-- req: parallel-operators -->
- `map(cbk, obj, partition=5000)` — partitions the Object and creates one
  child task per partition; `cbk(row, *args, **kwargs)` is applied to each row
  (a value for single-column Objects, a `dict` for multi-column ones) and its
  return value is appended to the output Object. A `None` return adds no row.
  Output rows land in partition-completion order, not input order.
- `foreach(cbk, obj, partition=5000)` — `map()` for side effects (write a
  file, call an API): the return value is discarded, no output Object is
  allocated, and the expander's result is `None`.
- `reduce(cbk, obj, partition=5000)` — layered parallel reduction; each layer
  reduces partitions down until a single row remains. `cbk(partition, output)`
  receives an input partition and a pre-allocated output Object and writes via
  `output.insert()`. The callback must be homomorphic: output schema equals
  input schema.

<!-- req: operator-result -->
All three accept a `Task` or an `Object` as input and return the expander
`Task`. Its result is the output Object (`None` for `foreach`), produced by a
finalize task that runs after every partition task, so a consumer sees it
filled. See [Examples: Orchestration Operators](../../examples/orchestration_operators.md).

!!! warning "Callbacks must be module-level functions"
    <!-- req: module-level-callbacks -->
    A worker imports the callback by path, and the same holds for any function
    passed as a task argument. A lambda, nested function, bound method or
    `functools.partial` raises `TypeError` when the task is created; pass
    extra values through `args` / `kwargs` instead.

!!! warning "Output schema equals input schema"
    <!-- req: operator-output-schema -->
    Both operators allocate the output from the input's schema, and ClickHouse
    casts returned values to it on insert: a `map()` callback returning `2.5`
    for an integer column stores `2`. Map over a float column, or convert in
    the callback, when the type changes. Create the input with `aai_id=True`
    (or an `order_by` view) so the LIMIT/OFFSET partitions are disjoint.
