# Dynamic Task Creation: `map()` and `reduce()` Operators

## Overview

Dynamic task creation allows workflows to create parallel tasks at runtime based on actual data characteristics. The `map()` and `reduce()` operators follow Python's native interfaces and use an **expander task pattern** to partition Objects into parallel work units.

**Implementation**: `aaiclick/orchestration/dynamic.py`

## API

### `map(func, obj, partition_size=1000, **kwargs) -> MapHandle`

Like Python's `map(func, iterable)`, but partitions an Object into Views and creates parallel tasks for each partition. See `map()` function in `aaiclick/orchestration/dynamic.py` and the module docstring for a usage example.

**Parameters**:

| Parameter        | Type                        | Description                                        |
|------------------|-----------------------------|----------------------------------------------------|
| `func`           | Callable or TaskFactory     | Function applied to each partition                 |
| `obj`            | Object, Task, or MapHandle  | Data to partition; if Task/MapHandle, waits for it |
| `partition_size` | int (default 1000)          | Number of rows per partition                       |
| `**kwargs`       | Any                         | Extra kwargs passed to each partition task          |

**Returns**: `MapHandle` containing the expander task and output group.

### `reduce(func, mapped) -> Task`

Like Python's `functools.reduce(func, iterable)`. Waits for all map partitions to complete, then passes their results as a list to `func`. See `reduce()` function in `aaiclick/orchestration/dynamic.py` and the module docstring for a usage example.

**Parameters**:

| Parameter | Type       | Description                                   |
|-----------|------------|-----------------------------------------------|
| `func`    | Callable   | Function receiving list of all partition results |
| `mapped`  | MapHandle  | Handle from a `map()` call                    |

**Returns**: Task that executes after all map partitions complete.

### `MapHandle`

**Implementation**: `aaiclick/orchestration/dynamic.py` — see `MapHandle` class

A dataclass representing a pending map operation. It is a **job-definition-time placeholder** — actual partition tasks don't exist yet. They are created dynamically at runtime by the expander after it queries ClickHouse for the row count.

**Important**: `MapHandle` is used in `@job` function bodies (graph definition), never inside `@task` functions (execution). Workers never see `MapHandle` objects — they receive plain serialized kwargs.

| Field      | Type    | Purpose                                                                  |
|------------|---------|--------------------------------------------------------------------------|
| `expander` | `Task`  | The expander task that runs at runtime to count rows and create partitions |
| `group`    | `Group` | Container for all dynamically created partition tasks                    |

**Dependency operators** (see `MapHandle.__rshift__`, `__rrshift__`, and `depends_on` methods):

- `mapped >> task` — downstream depends on all map tasks (via group)
- `task >> mapped` — expander depends on upstream task
- `[a, b] >> mapped` / `mapped >> [a, b]` — list support for multiple dependencies
- `mapped.depends_on(other)` — fluent API
- `map(func2, first_map)` — chaining maps (second waits for all first partitions)

**Lifecycle**:

```
Job definition time:           Runtime (worker executes expander):
┌─────────────┐                ┌──────────────────────────────────┐
│ map() call  │                │ _expand_map():                   │
│  ↓          │                │  1. Resolve object → table name  │
│ Creates:    │                │  2. SELECT count() → row_count   │
│  • expander │──── runs ────→│  3. N = ceil(count/partition_sz) │
│  • group    │                │  4. Create N child Tasks in group│
└─────────────┘                └──────────────────────────────────┘
      │                                      │
      ↓                                      ↓
  MapHandle                        N partition tasks run in parallel
  (expander + group)               each receives a View(limit, offset)
```

## Architecture: Expander Task Pattern

### Job Definition Time

When `map()` is called in a `@job` function:

1. Creates an **expander task** (entrypoint: `aaiclick.orchestration.dynamic._expand_map`)
2. Creates a **Group** for the future partition tasks
3. Returns `MapHandle(expander, group)`
4. Dependencies are wired: `obj >> expander`, `group >> reduce_task`

### Execution Time (Worker Runs Expander)

When a worker executes the expander task:

1. Resolves the Object reference to get the ClickHouse table name
2. Queries `SELECT count() FROM {table}` for row count
3. Calculates `N = ceil(row_count / partition_size)`
4. Creates N child tasks, each receiving a View with `OFFSET`/`LIMIT`
5. Assigns all child tasks to the map group
6. Commits new tasks to PostgreSQL via `commit_tasks()`

Workers automatically pick up the new PENDING tasks via the existing claiming mechanism.

### Reduce Time

The reduce task depends on the map group. The existing `claim_next_task()` dependency check ensures it only runs after ALL tasks in the group are COMPLETED. The reduce function receives a list of all partition results.

## Worker Context

**Implementation**: `aaiclick/orchestration/worker_context.py`

Expander tasks need access to `job_id` and `task_id` during execution. The `worker_context` module provides a `ContextVar`-based mechanism:

- `set_current_task_info(task_id, job_id)` — called by `execute_task()` before running any task
- `get_current_task_info() -> TaskInfo` — used by `_expand_map()` to know its job context

## Database Changes

### `is_expander` Column on Tasks ✅ IMPLEMENTED

**Migration**: `aaiclick/orchestration/migrations/versions/b5e2a1d93f67_add_is_expander_to_tasks.py`

Boolean column (default `False`) marking tasks that dynamically create child tasks.

## Dependency Flow Diagram

```
Job Definition:                    Runtime:

  load_data (Task)                   load_data completes
      │                                  │
      ▼                                  ▼
  _expand_map (Task, is_expander)    expander queries count(),
      │                              creates N partition tasks
      │                                  │
      ▼                                  ▼
  map_group (Group, empty)           map_group now contains:
      │                                partition_0 (Task)
      │                                partition_1 (Task)
      │                                ...
      │                                partition_N (Task)
      │                                  │
      ▼                                  ▼ (all complete)
  _execute_reduce (Task)             reduce collects results,
                                     calls user's function
```

## Spark Methods vs Existing Capabilities

| Spark Method           | aaiclick Equivalent                 | Notes                                       |
|------------------------|-------------------------------------|---------------------------------------------|
| `map(func)`            | Object operators (`+`, `*`, etc.)   | Element-wise SQL operations                 |
| `mapPartitions(func)`  | **`map(func, obj)`** ✅            | Custom Python logic per partition           |
| `reduce(func)`         | **`reduce(func, mapped)`** ✅      | Collect and aggregate partition results     |
| `filter(pred)`         | `View(where=...)`                   | SQL WHERE clause                            |
| `groupByKey`           | `obj.group_by(...)`                 | SQL GROUP BY                                |
| `count()`              | `obj.count()`                       | SQL COUNT aggregation                       |
| `sum/mean/min/max/std` | `obj.sum()` etc.                    | SQL aggregation functions                   |
| `union/concat`         | `concat(a, b)`                      | INSERT INTO ... SELECT                      |
| `sort/orderBy`         | `View(order_by=...)`                | SQL ORDER BY                                |
| `flatMap(func)`        | ⚠️ NOT YET IMPLEMENTED             | Variant of map() for variable-output tasks  |
| `join`                 | ⚠️ NOT YET IMPLEMENTED             | SQL JOIN                                    |

## Future Extensions

- **`flatMap()`**: Variant of `map()` where each partition task can return multiple Objects
- **`filter()` at orchestration level**: For Python predicates that can't be expressed in SQL
- **Adaptive partitioning**: Auto-tune partition_size based on data characteristics
- **Tree reduce**: Hierarchical reduction for better parallelism with many partitions
