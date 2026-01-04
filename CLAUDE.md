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

- **Type annotations with circular dependencies**: Use `from __future__ import annotations`
  - Allows direct use of types without quotes: `Object` instead of `"Object"`
  - Avoids circular import issues in type annotations (annotations aren't evaluated at runtime)
  - Standard Python 3.7+ practice (PEP 563)
  - Note: This import becomes redundant in Python 3.13+ (default behavior)
  - Example pattern:
    ```python
    from __future__ import annotations

    from .object import Object  # Direct import, no TYPE_CHECKING needed

    async def my_function(obj: Object) -> Object:  # No quotes needed
        ...
    ```

- **Lazy imports for circular dependencies**: When modules need each other at runtime
  - Use lazy imports inside functions/methods instead of module-level imports
  - Breaks circular import chain while keeping clean type hints
  - Example: `object.py` methods import `operators` module lazily
  - Pattern:
    ```python
    # At module level: NO import operators

    async def __add__(self, other: Object) -> Object:
        from . import operators  # Lazy import inside method
        return await operators.add(self, other)
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

## Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Check PR status via `review-pr-checks` skill (do NOT run tests locally)
