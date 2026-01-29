# Distributed Sorting Implementation Plan

This document outlines the phased implementation of distributed sorting capabilities for aaiclick.

## Overview

Building on the [Distributed Sorting Research](./distributed_sorting_research.md), this plan describes concrete implementation steps.

## Phase 1: Simple Sort (ClickHouse-Native) ⚠️ NOT YET IMPLEMENTED

**Objective**: Add `sort()` method to Object class using ClickHouse's native sorting.

### Design Decisions

1. **New Object Creation**: Sorting creates a new Object (immutable pattern)
2. **ID Regeneration**: New Snowflake IDs assigned in sorted order
3. **Stable Sort**: Default to stable sort using `aai_id` as tiebreaker

### API Design

```python
# Basic usage
sorted_obj = await obj.sort()  # Sort by 'value' ascending

# With options
sorted_obj = await obj.sort(key="value", descending=True)

# Multi-column sort (dict objects)
sorted_obj = await obj.sort(key=["age", "name"], descending=[True, False])
```

### Implementation Details

**File**: `aaiclick/data/object.py`

```python
async def sort(
    self,
    key: str | list[str] = "value",
    descending: bool | list[bool] = False,
    stable: bool = True,
) -> "Object":
    """Create a new Object with data sorted by the specified key(s)."""
    from .data_context import get_data_context
    from .factories import create_object
    from ..snowflake_id import get_snowflake_ids

    ctx = get_data_context()

    # Normalize to lists
    keys = [key] if isinstance(key, str) else key
    descs = [descending] * len(keys) if isinstance(descending, bool) else descending

    # Build ORDER BY clause
    order_parts = []
    for k, d in zip(keys, descs):
        order_parts.append(f"{k} {'DESC' if d else 'ASC'}")
    if stable:
        order_parts.append("aai_id ASC")  # Tiebreaker for stability
    order_clause = ", ".join(order_parts)

    # Get count and generate new IDs
    count_result = await ctx.ch_client.query(f"SELECT count() FROM {self.table}")
    count = count_result.result_rows[0][0]

    if count == 0:
        return await create_object(self.schema)

    new_ids = get_snowflake_ids(count)

    # Create new object
    new_obj = await create_object(self.schema)

    # Build column list (excluding aai_id)
    columns = [c for c in self.schema.columns if c != "aai_id"]
    col_str = ", ".join(columns)

    # Insert sorted data with new IDs using window function
    query = f"""
        INSERT INTO {new_obj.table} (aai_id, {col_str})
        SELECT
            {list(new_ids)}[toUInt64(row_number() OVER (ORDER BY {order_clause}))] as aai_id,
            {col_str}
        FROM {self.table}
    """
    await ctx.ch_client.command(query)

    return new_obj
```

### Tasks

- [ ] Add `sort()` method to Object class
- [ ] Handle single column sort
- [ ] Handle multi-column sort for dict objects
- [ ] Handle stable vs unstable sorting
- [ ] Write tests for all sort scenarios
- [ ] Update documentation

---

## Phase 2: Top-K Selection ⚠️ NOT YET IMPLEMENTED

**Objective**: Efficient partial sorting for top/bottom K elements.

### API Design

```python
# Get top 10 largest values
top_obj = await obj.top_k(10)

# Get bottom 10 (smallest)
bottom_obj = await obj.top_k(10, largest=False)

# Top K by specific column
top_obj = await obj.top_k(10, key="score")
```

### Implementation

Uses ClickHouse's `ORDER BY ... LIMIT` which is optimized for partial sorting:

```python
async def top_k(
    self,
    k: int,
    key: str = "value",
    largest: bool = True,
) -> "Object":
    """Return a new Object with only the top K elements."""
    order = "DESC" if largest else "ASC"

    # ClickHouse optimizes ORDER BY + LIMIT
    # No need to fully sort - uses heap selection
    new_obj = await create_object(self.schema)

    columns = [c for c in self.schema.columns if c != "aai_id"]
    col_str = ", ".join(columns)

    new_ids = get_snowflake_ids(k)

    query = f"""
        INSERT INTO {new_obj.table} (aai_id, {col_str})
        SELECT
            {list(new_ids)}[toUInt64(row_number() OVER (ORDER BY {key} {order}))] as aai_id,
            {col_str}
        FROM {self.table}
        ORDER BY {key} {order}
        LIMIT {k}
    """
    await ctx.ch_client.command(query)

    return new_obj
```

### Tasks

- [ ] Implement `top_k()` method
- [ ] Optimize for cases where k << n
- [ ] Write tests

---

## Phase 3: Distributed Merge Sort ⚠️ NOT YET IMPLEMENTED

**Objective**: Sort datasets larger than single-node capacity using orchestration.

### Architecture

```
                         ┌─────────────────┐
                         │   Input Object  │
                         │   (N rows)      │
                         └────────┬────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ↓             ↓             ↓
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │ Partition 1 │ │ Partition 2 │ │ Partition 3 │
            │  (N/K rows) │ │  (N/K rows) │ │  (N/K rows) │
            └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
                   │               │               │
                   ↓               ↓               ↓
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │ Task:       │ │ Task:       │ │ Task:       │
            │ local_sort_1│ │ local_sort_2│ │ local_sort_3│
            └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
                   │               │               │
                   └───────────────┼───────────────┘
                                   ↓
                         ┌─────────────────┐
                         │ Task:           │
                         │ k_way_merge     │
                         └────────┬────────┘
                                  ↓
                         ┌─────────────────┐
                         │  Sorted Object  │
                         └─────────────────┘
```

### Implementation Strategy

**Step 1: Partitioning**

Partition input data into roughly equal chunks:

```python
async def partition_for_sort(obj: Object, num_partitions: int) -> list[Object]:
    """Partition an Object into chunks for distributed sorting."""
    count = await obj.count()
    chunk_size = math.ceil(count / num_partitions)

    partitions = []
    for i in range(num_partitions):
        offset = i * chunk_size
        # Use view to define partition, then materialize
        view = obj.view(offset=offset, limit=chunk_size)
        partition = await view.materialize()  # Creates new Object
        partitions.append(partition)

    return partitions
```

**Step 2: Local Sort Tasks**

```python
async def local_sort_task(
    ctx: OrchContext,
    partition_table: str,
    key: str,
    descending: bool,
) -> str:
    """Task function: Sort a single partition."""
    data_ctx = ctx.data_context

    # Load partition as Object
    partition = await Object.from_table(partition_table)

    # Sort locally
    sorted_partition = await partition.sort(key=key, descending=descending)

    return sorted_partition.table  # Return table name for merge phase
```

**Step 3: K-way Merge Task**

```python
async def k_way_merge_task(
    ctx: OrchContext,
    sorted_tables: list[str],
    key: str,
    descending: bool,
) -> str:
    """Task function: K-way merge of sorted partitions."""
    data_ctx = ctx.data_context

    # Use ClickHouse's UNION ALL with global ORDER BY
    # ClickHouse can optimize this since inputs are pre-sorted

    union_query = " UNION ALL ".join(
        f"SELECT * FROM {table}" for table in sorted_tables
    )

    order = "DESC" if descending else "ASC"

    # Generate final IDs
    total_count = sum(
        await data_ctx.ch_client.query(f"SELECT count() FROM {t}")
        for t in sorted_tables
    )
    final_ids = get_snowflake_ids(total_count)

    # Create merged result
    result = await create_object(schema)

    await data_ctx.ch_client.command(f"""
        INSERT INTO {result.table}
        SELECT
            {list(final_ids)}[row_number() OVER (ORDER BY {key} {order})] as aai_id,
            {other_columns}
        FROM ({union_query})
        ORDER BY {key} {order}
    """)

    return result.table
```

**Step 4: Job Definition**

```python
from aaiclick.orchestration import Job, Task

def create_distributed_sort_job(
    input_table: str,
    num_partitions: int,
    key: str = "value",
    descending: bool = False,
) -> Job:
    """Create a distributed sort job."""

    # Partition tasks
    partition_tasks = [
        Task(
            name=f"local_sort_{i}",
            fn="aaiclick.sorting:local_sort_task",
            params={
                "partition_idx": i,
                "num_partitions": num_partitions,
                "input_table": input_table,
                "key": key,
                "descending": descending,
            },
        )
        for i in range(num_partitions)
    ]

    # Merge task
    merge_task = Task(
        name="k_way_merge",
        fn="aaiclick.sorting:k_way_merge_task",
        params={
            "key": key,
            "descending": descending,
        },
        depends_on=[f"local_sort_{i}" for i in range(num_partitions)],
    )

    return Job(
        name="distributed_sort",
        tasks=partition_tasks + [merge_task],
    )
```

### Tasks

- [ ] Implement `partition_for_sort()` function
- [ ] Implement `local_sort_task()` function
- [ ] Implement `k_way_merge_task()` function
- [ ] Create `distributed_sort()` API that wraps the job
- [ ] Handle edge cases (empty partitions, single partition)
- [ ] Add progress tracking
- [ ] Write integration tests

---

## Phase 4: Sample Sort for Skewed Data ⚠️ NOT YET IMPLEMENTED

**Objective**: Handle highly skewed data distributions efficiently.

### When to Use

- Data has non-uniform distribution (many duplicates, clusters)
- Simple partitioning would cause load imbalance
- Very large datasets (> 100M rows)

### Algorithm

1. **Sample**: Take random samples from each partition
2. **Select Splitters**: Choose p-1 values that divide data into equal buckets
3. **Redistribute**: Send each element to appropriate bucket based on splitters
4. **Local Sort**: Sort each bucket independently

### Implementation Sketch

```python
async def sample_sort(
    obj: Object,
    num_workers: int,
    sample_factor: int = 10,
) -> Object:
    """Sort using sample sort algorithm for skewed data."""

    # Phase 1: Sampling
    sample_size = num_workers * sample_factor
    samples = await obj.sample(sample_size)
    sorted_samples = await samples.sort()

    # Phase 2: Select splitters
    splitters = []
    for i in range(1, num_workers):
        idx = i * sample_size // num_workers
        splitter = await sorted_samples.get_value_at(idx)
        splitters.append(splitter)

    # Phase 3: Bucket assignment and redistribution
    buckets = await assign_to_buckets(obj, splitters)

    # Phase 4: Sort each bucket (as Tasks)
    sorted_buckets = await parallel_sort_buckets(buckets)

    # Combine (already in order due to splitter-based bucketing)
    return await concat(*sorted_buckets)
```

### Tasks

- [ ] Implement sampling mechanism
- [ ] Implement splitter selection
- [ ] Implement bucket assignment using range queries
- [ ] Create sample sort job definition
- [ ] Compare performance with merge sort
- [ ] Write tests for skewed distributions

---

## Phase 5: Bitonic Sort for GPUs ⚠️ NOT YET IMPLEMENTED

**Objective**: Support GPU-accelerated sorting via bitonic networks.

### Design

Bitonic sort is ideal for GPU execution because:
- Fixed comparison pattern (no data-dependent branches)
- All comparisons at each stage are independent
- Excellent memory access patterns

### ClickHouse Integration

ClickHouse doesn't have native GPU support, but we could:
1. Extract data to GPU memory
2. Execute bitonic sort on GPU
3. Write results back

This requires external GPU libraries (CuPy, RAPIDS).

### Alternative: ClickHouse-Based Bitonic

For smaller datasets, we can simulate bitonic sort using SQL:

```python
async def bitonic_sort_stage(obj: Object, stage: int, substage: int) -> Object:
    """Execute one stage of bitonic sort."""

    # Generate comparison pairs
    pairs = generate_bitonic_pairs(n, stage, substage)

    # Execute compare-and-swap as SQL CASE expressions
    # This is educational but not practical for large data

    ...
```

### Tasks

- [ ] Research GPU integration options
- [ ] Prototype bitonic sort with small data
- [ ] Benchmark against merge sort
- [ ] Document use cases and limitations

---

## Testing Strategy

### Unit Tests

```python
# test_sorting.py

async def test_sort_ascending():
    async with DataContext():
        obj = await create_object_from_value([3, 1, 4, 1, 5, 9, 2, 6])
        sorted_obj = await obj.sort()
        data = await sorted_obj.data()
        assert data == [1, 1, 2, 3, 4, 5, 6, 9]

async def test_sort_descending():
    async with DataContext():
        obj = await create_object_from_value([3, 1, 4, 1, 5])
        sorted_obj = await obj.sort(descending=True)
        data = await sorted_obj.data()
        assert data == [5, 4, 3, 1, 1]

async def test_sort_stable():
    async with DataContext():
        # Create dict object with duplicate keys
        obj = await create_object_from_value({
            "key": [1, 2, 1, 2],
            "value": ["a", "b", "c", "d"]
        })
        sorted_obj = await obj.sort(key="key", stable=True)
        data = await sorted_obj.data()
        # Stable sort preserves original order for equal keys
        assert data["key"] == [1, 1, 2, 2]
        assert data["value"] == ["a", "c", "b", "d"]

async def test_sort_dict_multicolumn():
    async with DataContext():
        obj = await create_object_from_value({
            "name": ["Alice", "Bob", "Alice", "Bob"],
            "age": [30, 25, 25, 30]
        })
        sorted_obj = await obj.sort(key=["name", "age"])
        data = await sorted_obj.data()
        assert data["name"] == ["Alice", "Alice", "Bob", "Bob"]
        assert data["age"] == [25, 30, 25, 30]

async def test_top_k():
    async with DataContext():
        obj = await create_object_from_value([3, 1, 4, 1, 5, 9, 2, 6])
        top3 = await obj.top_k(3)
        data = await top3.data()
        assert data == [9, 6, 5]
```

### Integration Tests (Distributed Sort)

```python
async def test_distributed_sort_correctness():
    """Verify distributed sort produces same result as local sort."""
    async with DataContext():
        # Create large dataset
        values = list(range(10000))
        random.shuffle(values)
        obj = await create_object_from_value(values)

        # Local sort
        local_sorted = await obj.sort()

        # Distributed sort
        job = create_distributed_sort_job(obj.table, num_partitions=4)
        result = await job.run()
        distributed_sorted = await Object.from_table(result)

        # Compare
        assert await local_sorted.data() == await distributed_sorted.data()
```

---

## Performance Benchmarks

### Metrics to Track

1. **Sort time** vs data size (n)
2. **Parallel efficiency** vs number of workers
3. **Memory usage** per worker
4. **Network traffic** during merge phase

### Benchmark Plan

| Data Size | Workers | Expected Behavior |
|-----------|---------|-------------------|
| 1K | 1 | Local sort < 1s |
| 100K | 1 | Local sort < 5s |
| 1M | 4 | Distributed matches local |
| 10M | 4 | Linear scaling |
| 100M | 8 | Sub-linear (communication overhead) |

---

## Timeline

| Phase | Estimated Effort | Dependencies |
|-------|------------------|--------------|
| Phase 1: Simple Sort | Small | None |
| Phase 2: Top-K | Small | Phase 1 |
| Phase 3: Distributed Merge | Medium | Phase 1, Orchestration |
| Phase 4: Sample Sort | Medium | Phase 3 |
| Phase 5: Bitonic/GPU | Large | External research |

---

## Open Questions

1. **ID Regeneration Strategy**: Should we always regenerate IDs, or provide an option to keep original IDs?

2. **Sort Key Expressions**: Support arbitrary ClickHouse expressions as sort keys?
   ```python
   await obj.sort(key="length(name)")  # Sort by string length
   ```

3. **Null Handling**: ClickHouse has `NULLS FIRST`/`NULLS LAST` - expose this?

4. **Parallel Merge**: For very large data, can we parallelize the merge phase itself?

5. **Streaming Sort**: Support sorting data that arrives incrementally?
