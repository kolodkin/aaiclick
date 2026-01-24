# Agent Guidelines for aaiclick

This document contains guidelines for AI agents (like Claude Code) working on the aaiclick project.

## Test Execution Strategy

**IMPORTANT: Do NOT run tests in the Claude cloud environment.**

- All tests run automatically in GitHub Actions when code is pushed
- After pushing changes, check pull request status using the `pr-checks` skill or run `.claude/skills/pr-checks/pr-checks.sh`
- Local test execution is unnecessary and should be avoided
- CI/CD pipeline handles all testing and validation

## Testing Guidelines

- **Test file location**: Place test files alongside the modules they test
  - `aaiclick/data/test_context.py` tests `aaiclick/data/data_context.py`
  - `aaiclick/orchestration/test_orchestration_factories.py` tests `aaiclick/orchestration/factories.py`
  - Shared fixtures go in `aaiclick/conftest.py`

- **Flat test structure**: Do NOT use test classes - keep tests as flat module functions
  - Tests should be simple `async def test_*():` or `def test_*():` functions
  - Group related tests by file, not by class
  - This keeps tests simple and reduces boilerplate

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
  3. Current package imports: `from aaiclick import DataContext`

- **Circular imports**: Use two-pattern approach
  - Type annotations: Add `from __future__ import annotations` at top of file
    - Allows `Object` instead of `"Object"` in type hints
    - Required for Python 3.10+
  - Runtime imports: Use lazy imports inside methods when modules need each other
    - Example: `object.py` imports `operators` inside `__add__()` method, not at module level

- **No __all__ in __init__.py**: Do NOT define `__all__` in `__init__.py` files
  - Simply import what needs to be exported
  - Python will automatically make imported names available
  - Reduces maintenance burden (no need to update two lists)
  - Example:
    ```python
    # GOOD - Just import
    from .models import Job, Task, Worker

    # BAD - Don't add __all__
    __all__ = ["Job", "Task", "Worker"]
    ```

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

### Alembic Migration Guidelines

**Always use Alembic built-in commands for creating migrations:**

- **Create new migration**: Use `alembic revision -m "description"` or `alembic revision --autogenerate -m "description"`
  ```bash
  # Create empty migration file (manual)
  alembic revision -m "add user table"

  # Auto-generate migration from model changes (requires database connection)
  alembic revision --autogenerate -m "add user table"
  ```

- **Apply migrations**: Use `alembic upgrade head` or `alembic upgrade +1`
  ```bash
  # Apply all pending migrations
  alembic upgrade head

  # Apply next migration
  alembic upgrade +1
  ```

- **Rollback migrations**: Use `alembic downgrade -1` or `alembic downgrade <revision>`
  ```bash
  # Rollback last migration
  alembic downgrade -1

  # Rollback to specific revision
  alembic downgrade abc123
  ```

- **Check status**: Use `alembic current` and `alembic history`
  ```bash
  # Show current revision
  alembic current

  # Show migration history
  alembic history --verbose
  ```

**Important**:
- Never manually create migration files from scratch
- Always use `alembic revision` to generate the migration file skeleton
- Fill in `upgrade()` and `downgrade()` functions with actual migration code
- Test both upgrade and downgrade paths

## Environment Variables

ClickHouse connection (all optional with sensible defaults):
- `CLICKHOUSE_HOST` (default: "localhost")
- `CLICKHOUSE_PORT` (default: 8123)
- `CLICKHOUSE_USER` (default: "default")
- `CLICKHOUSE_PASSWORD` (default: "")
- `CLICKHOUSE_DB` (default: "default")

PostgreSQL connection for orchestration backend (required when using orchestration):
- `POSTGRES_HOST` (default: "localhost")
- `POSTGRES_PORT` (default: 5432)
- `POSTGRES_USER` (default: "aaiclick")
- `POSTGRES_PASSWORD` (default: "secret")
- `POSTGRES_DB` (default: "aaiclick")

Orchestration logging (optional):
- `AAICLICK_LOG_DIR` - Override default OS-dependent log directory
  - macOS default: `~/.aaiclick/logs`
  - Linux default: `/var/log/aaiclick`

## Project Structure

```
aaiclick/
├── aaiclick/                # Main package
│   ├── conftest.py          # Shared test fixtures
│   ├── snowflake_id.py      # Snowflake ID generation
│   ├── test_snowflake.py    # Tests for snowflake_id.py
│   ├── data/                # Data module
│   │   ├── data_context.py  # DataContext manager
│   │   ├── object.py        # Core Object class
│   │   ├── test_*.py        # Tests alongside modules
│   │   └── ...
│   ├── examples/            # Example scripts
│   │   ├── run_all.py       # Run all examples
│   │   └── ...
│   └── orchestration/       # Orchestration module
│       ├── factories.py
│       ├── test_*.py        # Tests alongside modules
│       └── ...
├── pyproject.toml           # Project configuration
└── CLAUDE.md                # This file
```

## Architecture

- **DataContext**: Primary API for managing Object lifecycle
  - Manages ClickHouse client lifecycle
  - Tracks Objects via weakref for automatic cleanup
  - Uses ContextVar for async-safe global context management
  - Accessed via `async with DataContext():` pattern or `get_data_context()` function
- **Module-level Functions**: `create_object()` and `create_object_from_value()`
  - Exported from package for direct use
  - Use `get_data_context()` internally to access the current context
  - No need to pass context explicitly
- **Connection Pool**: Shared urllib3 PoolManager across all DataContext instances
  - Defined in `data_context.py` as global `_pool`
  - All clients share the same connection pool for efficiency
  - `get_ch_client()` creates clients using the shared pool

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
obj_a = await create_object_from_value([1, 2, 3])  # Created at time T1
obj_b = await create_object_from_value([4, 5, 6])  # Created at time T2
result = await concat(obj_a, obj_b)  # Result: [1, 2, 3, 4, 5, 6]
result = await concat(obj_b, obj_a)  # Result: [1, 2, 3, 4, 5, 6] (same!)

# Scenario 2: obj_b created first
obj_b = await create_object_from_value([4, 5, 6])  # Created at time T1
obj_a = await create_object_from_value([1, 2, 3])  # Created at time T2
result = await concat(obj_a, obj_b)  # Result: [4, 5, 6, 1, 2, 3]
result = await concat(obj_b, obj_a)  # Result: [4, 5, 6, 1, 2, 3] (same!)
```

The concat argument order doesn't matter - results are always ordered by Snowflake ID timestamps from when objects were created. This ensures temporal causality in distributed systems.

## Specification-Driven Development

**Write detailed specifications BEFORE implementing complex features.**

### Workflow

1. **Create Specification Document** (`docs/<feature>.md`):
   - Describe architecture, data models, and APIs
   - Include code examples showing intended usage
   - Document design decisions and trade-offs
   - Specify all data types, enums, and schemas
   - Keep specifications detailed and comprehensive

2. **Create Implementation Plan** (`docs/<feature>_implementation_plan.md`) for complex features:
   - Break feature into phases with clear objectives
   - List specific tasks for each phase
   - Define deliverables and success criteria
   - Track progress with ✅ for completed phases
   - Include file references as implementation progresses

3. **Implement Phase by Phase**:
   - Follow the implementation plan sequentially
   - Write comprehensive tests for each phase
   - Commit working code frequently
   - Update implementation plan with ✅ and file references

4. **Update Documentation to Reference Implementation**:
   - **Add implementation references**: Point to actual code files and line numbers
   - **Example**: `**Implementation**: aaiclick/orchestration/factories.py:30-107`
   - **Remove duplication**: Once code exists, reference it instead of duplicating
   - **Mark status**: Use ✅ IMPLEMENTED or ⚠️ NOT YET IMPLEMENTED
   - **Keep unimplemented specs**: Detailed descriptions serve as design docs for future work

### Documentation Patterns

**For Implemented Features**:
```markdown
### Feature Name ✅ IMPLEMENTED

**Implementation**: `path/to/file.py:line-start-line-end`

Brief description with link to code instead of duplicating implementation details.
See actual code for complete implementation.
```

**For Unimplemented Features**:
```markdown
### Feature Name ⚠️ NOT YET IMPLEMENTED (Phase N+)

Detailed specification with code examples, data models, and API design.
This serves as the design document for future implementation.
```

**For Data Models**:
```markdown
### ModelName

**Implementation**: `aaiclick/module/models.py:line-start-line-end`

**Note**: Actual implementation details (e.g., "uses UPPERCASE enums", "BIGINT for IDs")

```python
# Show actual code structure from implementation
class ModelName:
    field: Type = ...
```
```

### Example: Orchestration Backend

See `docs/orchestration.md` and `docs/orchestration_implementation_plan.md` for reference:

- **Specification**: Comprehensive design document with all phases planned
- **Implementation Plan**: Phase-by-phase breakdown with progress tracking
- **Phase 2 Complete**: Implementation plan updated with ✅ and file references
- **Documentation Updated**: orchestration.md references actual code, marks implementation status
- **No Duplication**: Implemented features point to code instead of duplicating

## Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Check PR status via `pr-checks` skill (do NOT run tests locally)
