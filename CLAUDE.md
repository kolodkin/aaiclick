# Git Workflow

After completing each task, use the `check-pr` skill to verify GitHub Actions workflows are successful.

If any workflows fail, analyze the error logs and fix issues automatically.

## Commit Guidelines

This project uses pre-commit hooks that may modify files during commit (formatting, linting, etc.).

**Commit message format** (conventional commits):

- `feature:` for new features
- `bugfix:` for bug fixes
- `refactor:` for code refactoring
- `cleanup:` for code cleanup
- Multiple types can be combined: `[feature, cleanup]: description`

**Creating commits**:

1. Check staged changes with `git status` and `git diff --staged --stat`
2. Suggest commit message following the format above
3. Get user approval before committing
4. Use HEREDOC for commit messages:

   ```bash
   git commit -m "$(cat <<'EOF'
   <type>: <short description>

   <optional longer description>
   EOF
   )"
   ```

**Handling pre-commit hook failures**:

- Hooks run BEFORE commit is created, so no commit exists yet
- Only re-stage the originally staged files (NOT all modified files)
- Do NOT use `git add -u` (stages unrelated changes)
- Do NOT use `--amend` (no commit exists to amend)
- Re-run commit with same message

**Important**:

- NEVER amend commits authored by others or already pushed
- Always use HEREDOC for multi-line messages
- Always get user approval before committing
- Do NOT include "Generated with ..." in commit messages

# Test Execution Strategy

**Local testing is supported with the default backend (chdb + SQLite).**

- Default URLs use chdb + SQLite — no infrastructure needed
- Tests also run in GitHub Actions with both local and distributed backends
- For distributed testing, set `AAICLICK_CH_URL` and `AAICLICK_SQL_URL` to remote servers

# Testing Guidelines

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

- **Unrelated test failures**: When tests outside the scope of your changes break, fix the implementation — not the tests
  - These failures indicate your changes have unintended side effects
  - Do NOT modify, skip, or weaken unrelated tests to make them pass
  - If unsure whether the test or the implementation is wrong, ask the user

- **Object API test file alignment**: Each section in the `docs/object.md` API Quick Reference table must have a dedicated test file in `aaiclick/data/object/`. Name the file after the section (e.g. `test_comparison.py`, `test_bitwise.py`, `test_domain_helpers.py`). When adding a new API section, also create the corresponding test file. When a domain helper is tightly coupled to an operator (e.g., `with_isin` ↔ `isin`), tests go in the operator's test file (`test_isin.py`), not `test_domain_helpers.py`.


# Code Quality

- **No unhandled warnings**: `filterwarnings = ["error"]` in `pyproject.toml` turns any unhandled warning into a test failure. When a third-party library emits a known warning, suppress it with `warnings.catch_warnings()` around the call that triggers it. This keeps the suppression scoped and next to the code that causes it.
- Use `--strict-markers` for pytest marker validation
- Code coverage reporting is enabled via pytest-cov

# Coding Guidelines

- **No history comments**: Do NOT add comments about removed code (e.g., `# Removed: ...`)
  - Keep code clean - version control tracks history
  - Remove outdated comments during refactoring

- **Imports**: **ALL imports MUST be at the top of the file.** No exceptions for test functions.
  - Organize in three groups separated by blank lines:
    1. Standard library: `import asyncio`, `import json`
    2. External packages: `import pytest`, `import numpy`
    3. Current package: `from aaiclick import DataContext`
  - **Never** import inside functions, methods, loops, conditionals, or test functions
  - **Only exception** — lazy imports to break circular dependencies:
    - Use `from __future__ import annotations` for type hints (defers evaluation)
    - Use inline imports inside methods only when two modules need each other at runtime
    - Do NOT use `TYPE_CHECKING` pattern — prefer restructuring code instead
    ```python
    # GOOD — top of file
    from sqlmodel import select
    from .models import Task

    async def get_task(task_id: int):
        result = await session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one()

    # BAD — inline import
    async def get_task(task_id: int):
        from sqlmodel import select  # Don't do this!
        result = await session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one()

    # OK — lazy import to break circular dependency
    def method(self):
        from .other_module import something  # Circular dep workaround
        something()
    ```

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

- **Top-level `__init__.py` is public API only**: `aaiclick/__init__.py` exports only user-facing symbols; subpackage `__init__.py` files may also re-export internals for intra-package import convenience

- **No compromising on typing**: Never use `Any` as a shortcut to avoid proper typing
  - When breaking circular imports, use module-level imports (`from . import module as mod`) combined with `from __future__ import annotations` so types resolve correctly
  - Prefer `obj: mod.ClassName` over `obj: Any`
  - If restructuring is needed to get proper types, do it

- **Prefer NamedTuples over plain tuples in APIs**: When a function accepts or returns tuples with fixed fields, define a `NamedTuple` instead
  - Use named attributes (`.op`, `.alias`) in internal code — not positional unpacking
  - Convert plain tuples to NamedTuples at API boundaries via `Cls._make(t)` to validate input format
  - Example:
    ```python
    from typing import NamedTuple, Literal

    # GOOD — NamedTuple with named access
    class Agg(NamedTuple):
        op: Literal["sum", "mean"]
        alias: str

    for source_col, agg in entries:
        sql_func = FUNCTIONS[agg.op]
        result[agg.alias] = compute(source_col, agg.op)

    # BAD — anonymous tuple, positional unpacking
    for source_col, agg_func, alias in triples:
        sql_func = FUNCTIONS[agg_func]
        result[alias] = compute(source_col, agg_func)
    ```

- **Example files** (`aaiclick/data/examples/*.py`, `aaiclick/orchestration/examples/*.py`): Add `# →` output comments inline next to `print()` calls that show computed results. Only annotate data results — not headers, separators, or static text. Skip loop bodies.
  ```python
  # GOOD — result is visible where the reader's eyes are
  print(f"Addition (a + b): {await result.data()}")  # → [12.0, 24.0, 35.0]

  # BAD — no output shown
  print(f"Addition (a + b): {await result.data()}")

  # SKIP — headers and loops don't need output comments
  print("Example 1: Arithmetic operators")
  for row in rows:
      print(f"  {row}")
  ```

## ClickHouse Client Guidelines

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

## Alembic Migration Guidelines

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

# Future Plans

`docs/future.md` is the single source of truth for unimplemented features. Move planned work there instead of marking it `⚠️ NOT YET IMPLEMENTED` inline. Spec docs may briefly reference it. Remove items when implemented.

# Specification-Driven Development

**Write detailed specifications BEFORE implementing complex features.**

## Workflow

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

## Documentation Guidelines

**Quality reference**: [FastAPI docs](https://fastapi.tiangolo.com/) — progressive disclosure, concise admonitions, copy-paste-ready examples with output shown inline.

**Avoid line numbers in implementation references** - they become stale as code changes. Instead, refer to classes, methods, or functions by name:

```markdown
# BAD - line numbers become stale
**Implementation**: `aaiclick/orchestration/context.py:129-175`

# GOOD - method names are stable
**Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext.apply()` method
```

**Markdown heading style** — use setext style for document titles, ATX (`#`) for sections:

```markdown
# GOOD - setext title + ATX sections (one level deep)
Document Title
---

# Section One

## Subsection

# BAD - ATX title with deep nesting
# Document Title

## Section One

### Subsection
```

- Document title: setext underline with `---`
- Top-level sections: `#`
- Subsections: `##`
- Avoid `###` and deeper where possible — restructure instead

**Markdown table formatting** - align columns with padding for human readability:

```markdown
# GOOD - aligned columns, padded with spaces
| Guard                                   | Scenario                                                  |
|-----------------------------------------|-----------------------------------------------------------|
| `sys.is_finalizing()`                   | Interpreter shutdown — skip to avoid thread safety issues |
| `_data_ctx_ref is None`                 | Object was never registered                               |

# BAD - minimal separators, hard to read
| Guard | Scenario |
|-------|----------|
| `sys.is_finalizing()` | Interpreter shutdown — skip to avoid thread safety issues |
| `_data_ctx_ref is None` | Object was never registered |
```

**Admonitions** — use `!!! tip`, `!!! warning`, `??? info` only at genuine pitfall points
where a user would hit a confusing error without the callout. Never for emphasis, decoration,
or restating what surrounding prose already says. Collapsible `???` for optional context.

```markdown
# GOOD — real pitfall, saves debugging time
!!! warning "`or_where()` requires a prior `where()`"
    Calling `or_where()` without a preceding `where()` raises `ValueError`.

# GOOD — optional context, reader can skip
??? info "Which deployment mode?"
    Start with the default (chdb + SQLite) — it needs zero setup.

# BAD — restating what the code already shows
!!! tip
    Use `await` to get the result of an operation.

# BAD — decorating a reference table
!!! info "ClickHouse uses RE2 regex syntax"
    No lookaheads or lookbehinds.
```

# Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Run `check-pr` skill to verify CI passes

