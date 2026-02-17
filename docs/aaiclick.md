# aaiclick

aaiclick is a framework that converts Python functions into ClickHouse operations flows, enabling distributed data processing with automatic persistence and flow tracking.

## Async Architecture

**All database queries and commands are async using clickhouse-connect.** The framework is built on asynchronous operations, requiring the use of `async`/`await` syntax throughout. This ensures:
- Non-blocking database operations
- Efficient concurrent execution
- Scalable performance for distributed processing

## Architecture

The framework consists of three base components:

### 1. Function Decorator: `@aaiclick`

Functions decorated with `@aaiclick` are automatically converted into ClickHouse operations flows. The decorator handles:

- Automatic conversion of function logic into ClickHouse operations
- Transparent execution flow management
- Integration with the Object wrapper system

**Example:**
```python
@aaiclick
async def process_data(input_data, threshold):
    filtered = await (input_data > threshold)
    result = await (filtered * 2)
    return result
```

**Note:** All decorated functions must be `async` and all operations must be awaited, as they execute asynchronous ClickHouse queries.

### 2. Object Instances Wrapper

Each parameter (both positional args and keyword args) is wrapped in an Object class that manages ClickHouse table operations.

See [Object Class](object.md) for detailed documentation on:
- Table naming conventions
- Operator overloading and immutability
- Column metadata with datatype/fieldtype

### 3. Flow Management

The execution flow and operation results are tracked in a dedicated flow table.

**Flow Table Responsibilities:**
- Track operation sequence
- Record intermediate results
- Maintain execution metadata
- Enable flow reconstruction and debugging

## Design Principles

1. **Async-First**: All database queries and commands are async using clickhouse-connect, enabling non-blocking operations and efficient concurrency
2. **Immutability**: All operations create new tables rather than modifying existing ones
3. **Transparency**: ClickHouse operations are abstracted behind Python syntax
4. **Persistence**: All intermediate results are stored in ClickHouse
5. **Traceability**: Complete operation history via flow tracking
6. **Distributed Order Preservation**: Order is maintained via Snowflake IDs containing creation timestamps, eliminating need for explicit ordering

## Snowflake IDs & Order Preservation

aaiclick uses **Snowflake IDs** (64-bit identifiers) for all row identifiers (`aai_id`):

**Snowflake ID Structure:**
- **Timestamp** (41 bits): Millisecond-precision creation time (~69 years range)
- **Machine ID** (10 bits): Worker/machine identifier (1024 machines)
- **Sequence** (12 bits): Per-millisecond counter (4096 IDs/ms)

**Benefits for Distributed Computing:**
- **Temporal ordering**: IDs naturally preserve chronological order across all nodes
- **No coordination needed**: Each machine generates unique IDs independently
- **Simplified operations**: `insert()` and `concat()` don't need explicit ORDER BY clauses
- **ID preservation**: Operations preserve existing Snowflake IDs from source data
  - IDs already encode temporal order from creation time
  - No renumbering or conflict detection needed
  - More efficient - direct database operations

**Critical Insight - Creation Order Matters:**

Result order is **always based on creation time** (Snowflake ID timestamps), not concat argument order!

```python
# Example 1: obj_a created first
obj_a = await create_object_from_value([1, 2, 3])  # T1
obj_b = await create_object_from_value([4, 5, 6])  # T2
result = await obj_a.concat(obj_b)  # [1, 2, 3, 4, 5, 6]
result = await obj_b.concat(obj_a)  # [1, 2, 3, 4, 5, 6] - same!

# Example 2: obj_b created first
obj_b = await create_object_from_value([4, 5, 6])  # T1
obj_a = await create_object_from_value([1, 2, 3])  # T2
result = await obj_a.concat(obj_b)  # [4, 5, 6, 1, 2, 3]
result = await obj_b.concat(obj_a)  # [4, 5, 6, 1, 2, 3] - same!
```

**Why This Matters - Temporal Causality in Distributed Systems:**

This design ensures **temporal causality** - a fundamental requirement for distributed computing:

1. **Causality Preservation**: Events that happen earlier in time always appear before events that happen later, regardless of where or how they're processed.

2. **No Race Conditions**: Concat order doesn't matter - `concat(a, b)` and `concat(b, a)` give the same result. This eliminates a whole class of distributed computing bugs.

This approach differs from traditional array operations where `concat([1,2,3], [4,5,6])` ≠ `concat([4,5,6], [1,2,3])`. In aaiclick, operation order is irrelevant - only creation time matters. This makes distributed computing much simpler and more reliable.

## Future Extensions

- Extended operation set for Object class
- Flow visualization tools
- Optimization strategies for operation chains
- Advanced flow control mechanisms
