# Custom Operators: `map()` and `reduce()`

## Overview

Custom operators enable parallel data processing at the orchestration level. `map()` partitions an Object into Views and creates parallel tasks for each partition. These are plain `@task`-decorated functions — no special subclasses.

**Implementation**: `aaiclick/orchestration/dynamic.py`

## API

### `map(cbk, obj, partition=5000) -> Task` ✅ IMPLEMENTED

Like Python's `map(func, iterable)`, but partitions an Object into Views and creates parallel `map_part` tasks for each partition.

**Parameters**:

| Parameter   | Type     | Default | Description                                    |
|-------------|----------|---------|------------------------------------------------|
| `cbk`       | Callable |         | Callback applied to each row: `cbk(row) -> None` |
| `obj`       | Object   |         | Object to partition                            |
| `partition` | int      | 5000    | Number of rows per partition                   |

**Returns**: `Task` (via `TaskFactory.__call__()`).

**At runtime** (worker executes the map task):
1. Queries `SELECT count() FROM {obj.table}`
2. Creates output Object with same schema as input
3. Creates a Group for partition tasks
4. Creates N `map_part` child tasks, each with a View (offset, limit)
5. Returns `[out, group, *tasks]` — dynamically registered via `register_returned_tasks()`

### `map_part(cbk, part, out) -> None` ✅ IMPLEMENTED

Internal task created by `map()`. Applies `cbk` to each row in a partition View.

**Parameters**:

| Parameter | Type     | Description                            |
|-----------|----------|----------------------------------------|
| `cbk`     | Callable | Callback: `cbk(row) -> None`           |
| `part`    | View     | Partition View of the source Object    |
| `out`     | Object   | Output Object to write results to      |

### `reduce()` ⚠️ NOT YET IMPLEMENTED

Will collect results from all partition tasks in a Group and pass them to a reduce function. Planned to accept a Group and wait for all partition tasks to complete before executing.

## Callable Serialization

Callbacks passed as task kwargs are serialized/deserialized automatically:

**Serialize** (`_serialize_value` in `decorators.py`):
- Plain callable → `{"ref_type": "callable", "entrypoint": "module.path.func"}`
- TaskFactory → `{"ref_type": "callable", "entrypoint": factory.entrypoint}`

**Deserialize** (`_deserialize_value` in `execution.py`):
- `{"ref_type": "callable", ...}` → `import_callback(entrypoint)` → original function

Existing utilities reused: `_callable_to_string()` from `factories.py`, `import_callback()` from `execution.py`.

## Usage

```python
from aaiclick.orchestration import job, task, map

@task
async def process_row(row):
    # arbitrary Python logic per row
    ...

@job("parallel_pipeline")
def pipeline():
    data = load_data()
    mapped = map(cbk=process_row, obj=data, partition=5000)
    return [data, mapped]
```

## Execution Flow

```
Job definition time:               Runtime (worker executes map task):
┌──────────────────────┐           ┌──────────────────────────────────┐
│ map(cbk, obj, 5000)  │           │ map() runtime:                   │
│   ↓                  │           │  1. SELECT count() → row_count   │
│ TaskFactory.__call__()│           │  2. Create output Object         │
│   ↓                  │           │  3. Create Group                 │
│ Returns Task         │── runs ──→│  4. Create N map_part child tasks│
└──────────────────────┘           │  5. Return [out, group, *tasks]  │
                                   └──────────────────────────────────┘
                                                   │
                                                   ↓
                                     N map_part tasks run in parallel
                                     each receives View(limit, offset)
                                     calls cbk(row) for each row
```

## Spark Methods vs aaiclick Capabilities

| Spark Method           | aaiclick Equivalent               | Notes                                       |
|------------------------|-----------------------------------|---------------------------------------------|
| `map(func)`            | Object operators (`+`, `*`, etc.) | Element-wise SQL operations                 |
| `mapPartitions(func)`  | **`map(cbk, obj)`** ✅           | Custom Python logic per partition           |
| `reduce(func)`         | ⚠️ NOT YET IMPLEMENTED           | Collect and aggregate partition results     |
| `filter(pred)`         | `View(where=...)`                 | SQL WHERE clause                            |
| `groupByKey`           | `obj.group_by(...)`               | SQL GROUP BY                                |
| `count()`              | `obj.count()`                     | SQL COUNT aggregation                       |
| `sum/mean/min/max/std` | `obj.sum()` etc.                  | SQL aggregation functions                   |
| `union/concat`         | `concat(a, b)`                    | INSERT INTO ... SELECT                      |
| `sort/orderBy`         | `View(order_by=...)`              | SQL ORDER BY                                |
| `flatMap(func)`        | ⚠️ NOT YET IMPLEMENTED           | Variant of map() for variable-output tasks  |
| `join`                 | ⚠️ NOT YET IMPLEMENTED           | SQL JOIN                                    |

## Future Extensions

- **`reduce()`**: Collect partition results and aggregate
- **`flatMap()`**: Variant of `map()` where each partition task can return multiple Objects
- **`filter()` at orchestration level**: For Python predicates that can't be expressed in SQL
- **Adaptive partitioning**: Auto-tune partition size based on data characteristics
- **Tree reduce**: Hierarchical reduction for better parallelism with many partitions
