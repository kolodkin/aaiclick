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

#### Table Naming Convention

- Each parameter gets a dedicated ClickHouse table named: `attr_name_[oid]`
- The table name is stored in the Object class instance

#### Object Class Features

**Operator Overloading:**
- All basic Python operations are overloaded (arithmetic, comparison, logical, etc.)
- Each operation creates a new table with the operation result asynchronously
- No in-place operations - all operations are immutable (return new Objects)
- All operations return awaitables that execute async ClickHouse queries via clickhouse-connect

**Extended Operations:**
- Set of framework-specific operations (to be defined in future releases)

**Example:**
```python
# Each operation creates a new table (all operations are async)
obj1 = await Object(data1)  # Creates table: data1_[oid1]
obj2 = await Object(data2)  # Creates table: data2_[oid2]
result = await (obj1 + obj2)  # Creates new table: result_[oid3]
```

#### Immutability Principle

All operations are immutable - no in-place modifications. Each operation returns a new Object with a new underlying ClickHouse table. This ensures:

- Operation history preservation
- Reproducibility
- Safe concurrent execution
- Clear data lineage

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

## Future Extensions

- Extended operation set for Object class
- Flow visualization tools
- Optimization strategies for operation chains
- Advanced flow control mechanisms
