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

## Future Extensions

- Extended operation set for Object class
- Flow visualization tools
- Optimization strategies for operation chains
- Advanced flow control mechanisms
