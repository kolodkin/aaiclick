# Agent Guidelines for aaiclick

This document contains guidelines for AI agents (like Claude Code) working on the aaiclick project.

## Test Execution Strategy

**IMPORTANT: Do NOT run tests in the Claude cloud environment.**

- All tests run automatically in GitHub Actions when code is pushed
- After pushing changes, check pull request status using the `review-pr-checks` skill or run `.claude/skills/review-pr-checks/review-pr-checks.sh`
- Local test execution is unnecessary and should be avoided
- CI/CD pipeline handles all testing and validation

## Testing Guidelines

- **Async tests**: Do NOT use `@pytest.mark.asyncio` decorator - it's not required
- pytest-asyncio is configured in `pyproject.toml` to automatically detect async test functions
- Simply define async test functions with `async def test_*():`


## Code Quality

- **Fail on warnings**: pytest is configured with `-W error` flag
- All warnings are treated as errors in tests
- Use `--strict-markers` for pytest marker validation
- Code coverage reporting is enabled via pytest-cov

## Coding Guidelines

- **No history comments**: Do NOT add comments about removed code (e.g., `# Removed: ...`)
  - Keep code clean - version control tracks history
  - Remove outdated comments during refactoring

- **Import ordering**: Organize imports in three groups, separated by blank lines:
  1. Python native (standard library): `import asyncio`, `import json`
  2. External packages (from pyproject.toml): `import pytest`, `import numpy`
  3. Current package imports: `from aaiclick import Context`

- **Circular imports**: Use two-pattern approach
  - Type annotations: Add `from __future__ import annotations` at top of file
    - Allows `Object` instead of `"Object"` in type hints
    - Required for Python 3.10+
  - Runtime imports: Use lazy imports inside methods when modules need each other
    - Example: `object.py` imports `operators` inside `__add__()` method, not at module level

### ClickHouse Client Guidelines

**Minimize data transfer between Python and ClickHouse - prefer database-internal operations.**

- **Use `ch_client.insert()` for in-memory data** (not string-formatted INSERT):
  ```python
  # GOOD
  data = [[id1, val1], [id2, val2]]
  await ch_client.insert(table_name, data)

  # BAD
  values = ", ".join(f"({id_val}, {val})" for ...)
  await ch_client.command(f"INSERT INTO {table} VALUES {values}")
  ```

- **Prefer database operations over fetching to Python**:
  - **Good**: `INSERT...SELECT`, `JOIN`, subqueries, window functions
  - **Bad**: Query → Python processing → Insert
  ```python
  # GOOD: Database-internal
  await ch_client.command(f"INSERT INTO {dest} SELECT ... FROM {src} JOIN ...")

  # BAD: Python round-trip
  rows = await ch_client.query(f"SELECT * FROM {src}")
  data = [[process(row) for row in rows]]
  await ch_client.insert(dest, data)
  ```

## Environment Variables

ClickHouse connection (all optional with sensible defaults):
- `CLICKHOUSE_HOST` (default: "localhost")
- `CLICKHOUSE_PORT` (default: 8123)
- `CLICKHOUSE_USER` (default: "default")
- `CLICKHOUSE_PASSWORD` (default: "")
- `CLICKHOUSE_DB` (default: "default")

## Project Structure

```
aaiclick/
├── aaiclick/          # Main package
│   ├── context.py     # Context manager and connection pool management
│   ├── object.py      # Core Object class
│   ├── factories.py   # Factory functions for creating objects (internal)
│   └── __init__.py    # Package exports
├── tests/             # Test suite
├── pyproject.toml     # Project configuration
└── CLAUDE.md          # This file
```

## Architecture

- **Context**: Primary API for creating and managing Objects
  - Manages ClickHouse client lifecycle
  - Tracks Objects via weakref for automatic cleanup
  - Provides `create_object()` and `create_object_from_value()` methods
- **Connection Pool**: Shared urllib3 PoolManager across all Context instances
  - Defined in `context.py` as global `_pool`
  - All clients share the same connection pool for efficiency
  - `get_ch_client()` creates clients using the shared pool
- **Factories**: Internal functions (not exported in `__init__.py`)
  - Called by Context methods
  - Accept `ch_client` parameter (mandatory)

## Distributed Computing & Order Preservation

aaiclick is a **distributed computing framework** where order is automatically preserved via **Snowflake IDs**:

- **Snowflake IDs encode timestamps**: Each ID contains creation timestamp (millisecond precision)
- **Temporal ordering**: IDs naturally preserve chronological order across distributed operations
- **No explicit ordering needed**: Operations like `insert()` and `concat()` don't need ORDER BY clauses
- **Insert/Concat behavior**: Preserve existing Snowflake IDs from source data
  - IDs already encode temporal order from when data was created
  - Order maintained when data is retrieved (via `.data()`)
  - Simpler logic - no ID renumbering or conflict detection needed
  - More efficient - direct database operations without Python round-trips

**Important**: Order after concat/insert is **always creation order**, not argument order!

**Example showing creation order**:
```python
# Scenario 1: obj_a created first
obj_a = await ctx.create_object_from_value([1, 2, 3])  # Created at time T1
obj_b = await ctx.create_object_from_value([4, 5, 6])  # Created at time T2
result = await concat(obj_a, obj_b)  # Result: [1, 2, 3, 4, 5, 6]
result = await concat(obj_b, obj_a)  # Result: [1, 2, 3, 4, 5, 6] (same!)

# Scenario 2: obj_b created first
obj_b = await ctx.create_object_from_value([4, 5, 6])  # Created at time T1
obj_a = await ctx.create_object_from_value([1, 2, 3])  # Created at time T2
result = await concat(obj_a, obj_b)  # Result: [4, 5, 6, 1, 2, 3]
result = await concat(obj_b, obj_a)  # Result: [4, 5, 6, 1, 2, 3] (same!)
```

The concat argument order doesn't matter - results are always ordered by Snowflake ID timestamps from when objects were created. This ensures temporal causality in distributed systems.

## Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Check PR status via `review-pr-checks` skill (do NOT run tests locally)
