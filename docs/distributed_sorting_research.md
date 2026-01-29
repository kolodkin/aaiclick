# Distributed Sorting Algorithms Research

This document explores sorting algorithms suitable for distributed computing environments, with focus on divide-and-conquer approaches that exhibit O(n log n) complexity through problem subdivision.

## Table of Contents

1. [Introduction](#introduction)
2. [Classical Divide-and-Conquer Sorting](#classical-divide-and-conquer-sorting)
3. [Distributed Sorting Algorithms](#distributed-sorting-algorithms)
4. [Algorithm Comparison](#algorithm-comparison)
5. [Application to aaiclick](#application-to-aaiclick)
6. [Proposed Implementation](#proposed-implementation)

---

## Introduction

### The log₂(n) Factor in Sorting

The theoretical lower bound for comparison-based sorting is Ω(n log n). This bound arises from information theory: with n! possible permutations, we need at least log₂(n!) ≈ n log₂(n) comparisons to distinguish between all possibilities.

Divide-and-conquer algorithms achieve O(n log n) by:
1. **Dividing** the problem into smaller subproblems (typically halves)
2. **Conquering** by recursively solving subproblems
3. **Combining** solutions back together

The recursion depth is log₂(n), and at each level we do O(n) work, giving O(n log n) total.

### Challenges in Distributed Sorting

Distributed sorting introduces unique challenges:

| Challenge | Description |
|-----------|-------------|
| **Data Distribution** | Data spread across nodes; movement is expensive |
| **Network Latency** | Communication overhead between nodes |
| **Load Balancing** | Ensuring equal work distribution |
| **Data Skew** | Uneven key distributions cause hotspots |
| **Fault Tolerance** | Handling node failures during sort |
| **Memory Constraints** | Each node has limited memory |

---

## Classical Divide-and-Conquer Sorting

### Merge Sort

**Complexity**: O(n log n) time, O(n) space

```
       [38, 27, 43, 3, 9, 82, 10]
              /           \
      [38, 27, 43, 3]    [9, 82, 10]
        /       \          /      \
    [38, 27]  [43, 3]   [9, 82]  [10]
      /  \      /  \      /  \     |
   [38] [27] [43] [3]  [9] [82]  [10]
      \  /      \  /      \  /     |
    [27, 38]  [3, 43]   [9, 82]  [10]
        \       /          \      /
      [3, 27, 38, 43]    [9, 10, 82]
              \           /
       [3, 9, 10, 27, 38, 43, 82]
```

**Key Properties**:
- **Stable**: Equal elements maintain relative order
- **Predictable**: Always O(n log n), regardless of input
- **Parallelizable**: Merge operations can run independently
- **External sorting friendly**: Works well with disk/distributed storage

**Algorithm**:
```python
def merge_sort(arr):
    if len(arr) <= 1:
        return arr

    mid = len(arr) // 2
    left = merge_sort(arr[:mid])    # Recursive divide
    right = merge_sort(arr[mid:])   # Recursive divide

    return merge(left, right)        # Combine

def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result
```

### Quick Sort

**Complexity**: O(n log n) average, O(n²) worst case

```
Pivot selection → Partition → Recurse

[38, 27, 43, 3, 9, 82, 10]  pivot=27
        ↓ partition
[3, 9, 10] [27] [38, 43, 82]
    ↓              ↓
 recurse        recurse
```

**Key Properties**:
- **In-place**: O(log n) space (stack)
- **Not stable**: Equal elements may be reordered
- **Cache-friendly**: Sequential memory access
- **Pivot-dependent**: Performance depends on pivot selection

**Parallel Challenge**: The partition step is inherently sequential.

### Heap Sort

**Complexity**: O(n log n) time, O(1) space

- In-place but not stable
- Poor cache performance (jumping through heap)
- Difficult to parallelize (heap operations are sequential)

---

## Distributed Sorting Algorithms

### 1. Distributed Merge Sort

The most natural extension of merge sort to distributed systems.

**Architecture**:
```
                    [Coordinator]
                    /    |    \
                   /     |     \
             [Node1] [Node2] [Node3]
               ↓        ↓        ↓
            Sort     Sort     Sort     ← Local sort phase
            Local    Local    Local
               ↓        ↓        ↓
               └────────┼────────┘
                        ↓
                   K-way Merge          ← Merge phase
                        ↓
                   [Sorted Output]
```

**Phases**:
1. **Distribute**: Split data across N nodes
2. **Local Sort**: Each node sorts its partition (O(n/N × log(n/N)))
3. **Merge**: K-way merge of sorted partitions (O(n × log N))

**Total Complexity**: O(n/N × log(n/N) + n × log N) with N nodes

**K-way Merge Algorithm**:
```python
import heapq

def k_way_merge(sorted_lists):
    """Merge K sorted lists efficiently using a min-heap."""
    # Heap entries: (value, list_index, element_index)
    heap = []

    # Initialize with first element from each list
    for i, lst in enumerate(sorted_lists):
        if lst:
            heapq.heappush(heap, (lst[0], i, 0))

    result = []
    while heap:
        val, list_idx, elem_idx = heapq.heappop(heap)
        result.append(val)

        # Add next element from same list
        if elem_idx + 1 < len(sorted_lists[list_idx]):
            next_val = sorted_lists[list_idx][elem_idx + 1]
            heapq.heappush(heap, (next_val, list_idx, elem_idx + 1))

    return result
```

**Advantages**:
- Simple to implement
- Predictable performance
- Works well with external storage

**Disadvantages**:
- Final merge is a bottleneck (single node)
- All data must flow through merge node

---

### 2. Bitonic Sort

A comparison-based sorting network optimized for parallel execution.

**Complexity**: O(n log² n) comparisons, O(log² n) parallel time

**Key Concept**: A bitonic sequence is one that first increases then decreases (or can be rotated to this form).

```
Bitonic sequences:
[1, 3, 5, 7, 6, 4, 2]  ← increases then decreases
[6, 4, 2, 1, 3, 5, 7]  ← rotated bitonic
```

**Bitonic Sort Network (n=8)**:
```
Input:  a0  a1  a2  a3  a4  a5  a6  a7
        │   │   │   │   │   │   │   │
Stage 1:├───┤   ├───┤   ├───┤   ├───┤   (pairs)
        │   │   │   │   │   │   │   │
Stage 2:├───────┤   │   ├───────┤   │   (quads)
        │   ├───┼───┤   │   ├───┼───┤
        │   │   │   │   │   │   │   │
Stage 3:├───────────────┤   │   │   │   (full)
        │   ├───────────┼───┤   │   │
        │   │   ├───────┼───┼───┤   │
        │   │   │   ├───┼───┼───┼───┤
        │   │   │   │   │   │   │   │
Output: sorted sequence
```

**Algorithm Structure**:
```python
def bitonic_sort(arr, low, count, ascending):
    if count > 1:
        k = count // 2
        # Sort first half ascending
        bitonic_sort(arr, low, k, True)
        # Sort second half descending
        bitonic_sort(arr, low + k, k, False)
        # Merge the bitonic sequence
        bitonic_merge(arr, low, count, ascending)

def bitonic_merge(arr, low, count, ascending):
    if count > 1:
        k = count // 2
        for i in range(low, low + k):
            compare_and_swap(arr, i, i + k, ascending)
        bitonic_merge(arr, low, k, ascending)
        bitonic_merge(arr, low + k, k, ascending)

def compare_and_swap(arr, i, j, ascending):
    if (arr[i] > arr[j]) == ascending:
        arr[i], arr[j] = arr[j], arr[i]
```

**Parallel Execution**:
- All comparisons at each stage can run simultaneously
- With n processors: O(log² n) time
- With n/2 processors: Each stage runs in O(1)

**Advantages**:
- Highly parallelizable (fixed comparison pattern)
- Oblivious algorithm (same comparisons regardless of data)
- Excellent for GPU/SIMD architectures
- No data-dependent branches

**Disadvantages**:
- O(n log² n) work (not work-optimal)
- Requires power-of-2 size (padding needed)
- Not stable

---

### 3. Sample Sort (Parallel Quicksort)

A sophisticated distributed sorting algorithm that avoids quicksort's bottlenecks.

**Complexity**: O(n/p × log(n/p) + p × log p) with high probability

**Architecture**:
```
Phase 1: Local Sampling
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Node 1  │  │ Node 2  │  │ Node 3  │
│ Sample  │  │ Sample  │  │ Sample  │
│  [s1]   │  │  [s2]   │  │  [s3]   │
└────┬────┘  └────┬────┘  └────┬────┘
     └───────────┬───────────┘
                 ↓
Phase 2: Select Splitters
         ┌─────────────┐
         │ Sort samples│
         │ Pick p-1    │
         │ splitters   │
         └──────┬──────┘
                ↓
         [s1, s2, ..., sp-1]
                ↓
Phase 3: Partition & Redistribute
         ↓            ↓            ↓
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ Node 1  │  │ Node 2  │  │ Node 3  │
    │ x < s1  │  │s1≤x<s2  │  │ x ≥ s2  │
    └────┬────┘  └────┬────┘  └────┬────┘
         ↓            ↓            ↓
Phase 4: Local Sort
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ Sort    │  │ Sort    │  │ Sort    │
    └─────────┘  └─────────┘  └─────────┘
```

**Algorithm**:
```python
def sample_sort(data, num_processors):
    p = num_processors
    n = len(data)

    # Phase 1: Each processor samples its local data
    sample_size = p - 1  # oversample by factor for better balance
    local_samples = random.sample(data, min(sample_size * p, n))

    # Phase 2: Gather samples and select splitters
    all_samples = gather_all(local_samples)  # MPI_Allgather
    all_samples.sort()

    # Select p-1 evenly spaced splitters
    splitters = []
    for i in range(1, p):
        idx = i * len(all_samples) // p
        splitters.append(all_samples[idx])

    # Phase 3: Partition local data by splitters
    buckets = [[] for _ in range(p)]
    for item in data:
        bucket_idx = bisect.bisect_left(splitters, item)
        buckets[bucket_idx].append(item)

    # Redistribute: send bucket[i] to processor i
    local_data = all_to_all_exchange(buckets)

    # Phase 4: Local sort
    local_data.sort()

    return local_data
```

**Key Insight**: By using sampling to determine splitters, we achieve good load balancing with high probability, even for skewed data distributions.

**Advantages**:
- Excellent scalability
- Handles data skew well (with oversampling)
- Minimal communication rounds
- Work-optimal: O(n log n)

**Disadvantages**:
- Not stable
- Requires all-to-all communication
- Complex implementation

---

### 4. External Merge Sort (for Disk/Distributed Storage)

When data exceeds memory, external sorting is essential.

**Architecture**:
```
┌────────────────────────────────────────┐
│             Large File (TB)            │
└────────────────────────────────────────┘
                    │
                    ↓ Read chunks
┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
│Chunk1│ │Chunk2│ │Chunk3│ │Chunk4│  ← Fits in memory
└──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘
   │        │        │        │
   ↓ sort   ↓ sort   ↓ sort   ↓ sort
┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
│Sorted│ │Sorted│ │Sorted│ │Sorted│  ← Sorted runs
│ Run1 │ │ Run2 │ │ Run3 │ │ Run4 │
└──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘
   │        │        │        │
   └────────┴────┬───┴────────┘
                 │
                 ↓ K-way merge
        ┌────────────────┐
        │  Sorted Output │
        └────────────────┘
```

**Phases**:
1. **Run Generation**: Read chunks, sort in-memory, write sorted runs
2. **Merge Passes**: K-way merge runs until single sorted output

**Optimal K-way Merge**:
- With M memory and B block size
- K = M/B - 1 (one buffer for output)
- Number of passes: ⌈log_K(N/M)⌉

**I/O Complexity**: O(N/B × log_{M/B}(N/M)) I/O operations

---

### 5. Radix Sort (Non-Comparison)

**Complexity**: O(n × k) where k = number of digits/bytes

Not comparison-based, so can beat O(n log n) for certain data types.

**Distributed Radix Sort**:
```
Pass 1: Sort by least significant digit
┌─────────┐      ┌─────────┐
│ Node 1  │ ──→  │Bucket 0 │ ──→ Node handling 0-3
│ 23, 45  │      │Bucket 1 │ ──→ Node handling 4-7
│ 12, 78  │      │   ...   │
└─────────┘      └─────────┘

Pass 2: Sort by next digit (on redistributed data)
... repeat for each digit position ...
```

**Advantages**:
- Linear time for fixed-width integers
- Highly parallelizable
- No comparison needed

**Disadvantages**:
- Only works for specific data types
- Large alphabet = many buckets = high memory
- Not stable without additional bookkeeping

---

## Algorithm Comparison

### Time Complexity

| Algorithm | Sequential | Parallel (p processors) | Work Optimal |
|-----------|------------|------------------------|--------------|
| Merge Sort | O(n log n) | O(n/p × log n) | Yes |
| Bitonic Sort | O(n log² n) | O(log² n) | No |
| Sample Sort | O(n log n) | O(n/p × log(n/p)) | Yes |
| Radix Sort | O(n × k) | O(n/p × k) | Yes |

### Communication Complexity

| Algorithm | Communication Rounds | Data Moved |
|-----------|---------------------|------------|
| Distributed Merge | O(log p) | O(n) |
| Bitonic Sort | O(log² n) | O(n log² n / p) |
| Sample Sort | O(1) | O(n) |
| Radix Sort | O(k) | O(n × k) |

### Practical Considerations

| Algorithm | Stability | Load Balance | Implementation |
|-----------|-----------|--------------|----------------|
| Distributed Merge | Stable | Guaranteed | Simple |
| Bitonic Sort | Not stable | Perfect | Moderate |
| Sample Sort | Not stable | Probabilistic | Complex |
| Radix Sort | Can be stable | Depends on data | Moderate |

### Recommendation Matrix

| Scenario | Recommended Algorithm |
|----------|----------------------|
| Small data, many nodes | Bitonic Sort |
| Large data, few nodes | Distributed Merge Sort |
| Skewed distributions | Sample Sort |
| Fixed-width integers | Radix Sort |
| Stability required | Distributed Merge Sort |
| GPU/SIMD hardware | Bitonic Sort |

---

## Application to aaiclick

### Current Architecture Fit

aaiclick's architecture has several properties relevant to distributed sorting:

1. **ClickHouse Backend**: All data stored in columnar database
2. **Snowflake IDs**: Natural temporal ordering via creation time
3. **Immutable Objects**: Operations create new tables
4. **Database-Level Operations**: Prefer SQL over Python round-trips

### ClickHouse Sorting Capabilities

ClickHouse has built-in sorting that we can leverage:

```sql
-- ORDER BY in queries (doesn't create sorted table)
SELECT * FROM table ORDER BY value;

-- Creating sorted MergeTree tables
CREATE TABLE sorted_data ENGINE = MergeTree()
ORDER BY (value)
AS SELECT * FROM source;

-- Window functions for ranking
SELECT *, row_number() OVER (ORDER BY value) as rank
FROM table;
```

### Proposed Distributed Sort Design

Given aaiclick's architecture, here's a hybrid approach:

**Approach 1: ClickHouse-Native Sort**

For moderate data sizes that fit in a single ClickHouse cluster:

```python
async def sort(self, key: str = "value", descending: bool = False) -> "Object":
    """Create a new Object with data sorted by key."""
    # Create new table with sorted data
    order = "DESC" if descending else "ASC"
    new_obj = await create_object(self.schema)

    # Database-internal sorted insert
    await ch_client.command(f"""
        INSERT INTO {new_obj.table}
        SELECT * FROM {self.table}
        ORDER BY {key} {order}
    """)

    # Regenerate Snowflake IDs to preserve new order
    # (critical for aaiclick's ordering semantics)

    return new_obj
```

**Challenge**: Snowflake IDs encode creation time, not sorted order. A new sorted Object needs new IDs to maintain the invariant that `ORDER BY aai_id` returns the logical order.

**Approach 2: Distributed Merge Sort for Large Data**

For data spread across multiple ClickHouse shards or when orchestration is needed:

```
                    ┌─────────────┐
                    │   Job:      │
                    │ sort_large  │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           ↓               ↓               ↓
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ Task:      │  │ Task:      │  │ Task:      │
    │ sort_chunk1│  │ sort_chunk2│  │ sort_chunk3│
    └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
          │               │               │
          └───────────────┼───────────────┘
                          ↓
                   ┌────────────┐
                   │ Task:      │
                   │ k_way_merge│
                   └────────────┘
```

**Implementation using aaiclick orchestration**:

```python
from aaiclick.orchestration import Job, Task

# Define distributed sort job
sort_job = Job(
    name="distributed_sort",
    tasks=[
        # Phase 1: Partition and local sort
        Task(
            name="sort_chunk",
            fn=sort_chunk_fn,
            params={"chunk_id": i}
        )
        for i in range(num_chunks)
    ] + [
        # Phase 2: K-way merge
        Task(
            name="merge_sorted",
            fn=k_way_merge_fn,
            depends_on=["sort_chunk_*"]
        )
    ]
)
```

**Approach 3: Bitonic Sort for Fixed-Size Parallel Operations**

For scenarios where we have many nodes and need predictable parallelism:

```python
async def bitonic_sort_distributed(obj: Object, ascending: bool = True) -> Object:
    """Bitonic sort using ClickHouse shards as parallel units."""
    n = await obj.count()

    # Pad to power of 2 if necessary
    padded_n = 2 ** math.ceil(math.log2(n))

    # Each stage of bitonic sort
    for stage in range(int(math.log2(padded_n))):
        for substage in range(stage + 1):
            # Generate comparison pairs for this (stage, substage)
            pairs = bitonic_pairs(padded_n, stage, substage, ascending)

            # Execute all comparisons in parallel (database-level)
            await execute_compare_swap_batch(obj, pairs)

    return obj
```

---

## Proposed Implementation

### Phase 1: Simple ClickHouse-Based Sort

**Goal**: Add `sort()` method to Object class that creates a new sorted Object.

```python
# In aaiclick/data/object.py

async def sort(
    self,
    key: str = "value",
    descending: bool = False,
    stable: bool = True
) -> "Object":
    """
    Create a new Object with data sorted by the specified key.

    Args:
        key: Column name to sort by (default: "value")
        descending: Sort in descending order if True
        stable: Maintain relative order of equal elements

    Returns:
        New Object with sorted data and regenerated Snowflake IDs
    """
    ctx = get_data_context()
    order = "DESC" if descending else "ASC"

    # For stable sort, use aai_id as tiebreaker
    order_clause = f"{key} {order}"
    if stable:
        order_clause += ", aai_id ASC"

    # Create new object with same schema
    new_obj = await create_object(self.schema)

    # Get sorted data count for ID generation
    count = await self.count()
    new_ids = get_snowflake_ids(count)

    # Build query with row numbering for ID assignment
    columns = ", ".join(c for c in self.schema.columns if c != "aai_id")

    # Use database-internal operation with new IDs
    await ctx.ch_client.command(f"""
        INSERT INTO {new_obj.table} (aai_id, {columns})
        SELECT
            arrayElement({list(new_ids)}, row_number() OVER (ORDER BY {order_clause})) as aai_id,
            {columns}
        FROM {self.table}
        ORDER BY {order_clause}
    """)

    return new_obj
```

### Phase 2: Distributed Sort for Large Data

**Goal**: Implement distributed merge sort using orchestration layer.

Key components:
1. **Partitioner**: Split data into chunks by range or hash
2. **Local Sorter**: Sort each partition (Task)
3. **K-way Merger**: Combine sorted partitions (Task with dependencies)

### Phase 3: Advanced Sorting Algorithms

**Goal**: Implement specialized algorithms for specific use cases.

Options:
- Bitonic sort for GPU-accelerated workloads
- Sample sort for highly parallel environments
- Radix sort for integer keys

---

## Open Questions

1. **Snowflake ID Semantics**: Should sorted objects have new IDs reflecting sort order, or keep original IDs? This affects `concat()` behavior.

2. **Partial Sorting**: Should we support `top_k()` or `nth_element()` operations that don't fully sort?

3. **Sort Key Types**: How to handle composite keys, expressions, or custom comparators?

4. **Memory Bounds**: For external sort, how to configure memory limits per worker?

5. **Fault Tolerance**: How to handle node failures during distributed sort?

---

## References

1. Blelloch, G. E. (1996). *Programming parallel algorithms*. Communications of the ACM.
2. Sanders, P., & Winkel, S. (2004). *Super scalar sample sort*. ESA.
3. Batcher, K. E. (1968). *Sorting networks and their applications*. AFIPS.
4. Knuth, D. E. (1998). *The Art of Computer Programming, Vol. 3: Sorting and Searching*.
5. ClickHouse Documentation: https://clickhouse.com/docs/en/sql-reference/statements/select/order-by

---

## Summary

For aaiclick's distributed sorting needs:

| Data Size | Recommended Approach |
|-----------|---------------------|
| < 1M rows | ClickHouse-native `ORDER BY` + ID regeneration |
| 1M - 100M rows | Distributed Merge Sort via orchestration |
| > 100M rows | Sample Sort for better load balancing |
| Integer keys | Consider Radix Sort |
| GPU available | Bitonic Sort |

The divide-and-conquer nature of merge sort (log₂(n) recursion depth) makes it naturally suited for distributed execution, while maintaining the stability guarantees that may be important for aaiclick's temporal ordering semantics.
