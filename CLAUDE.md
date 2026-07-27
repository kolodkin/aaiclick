# Git Workflow

After completing each task, use the `check-pr` skill to verify GitHub Actions workflows are successful.

If any workflows fail, analyze the error logs and fix issues automatically.

# Test Execution Strategy

**Local testing is supported with the default backend (chdb + SQLite).**

- Default URLs use chdb + SQLite — no infrastructure needed
- Tests also run in GitHub Actions with both local and distributed backends
- For distributed testing, set `AAICLICK_CH_URL` and `AAICLICK_SQL_URL` to remote servers

**Architecture**: `docs/designs/testing.md` — fixture layout, chdb session constraint, module-split rules for mp-worker tests.

# Testing

Use the `python-testing-style` skill for test layout, async test rules, Object API alignment, and what NOT to test.

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
  - **Circular dependencies — prefer restructuring over inline imports**. When two modules pull each other in at import time, try these in order:
    1. **Move the shared code to a common/neutral module** that neither of the cyclic modules has to import from the other. This is the preferred fix — it surfaces the right module boundary.
    2. Use `from __future__ import annotations` so type-hint imports resolve lazily.
    3. **Only as a last resort**, use an inline import inside a method with a one-line comment explaining why restructuring was not possible.
    - Do NOT use the `TYPE_CHECKING` pattern — prefer restructuring code instead.
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

    # BEST — extract the shared logic to a neutral module so the cycle disappears
    # (e.g. move the helper to aaiclick/common/foo.py that both sides import)

    # LAST RESORT — lazy import inside a method, only when restructuring is blocked
    def method(self):
        from .other_module import something  # Circular dep: restructuring blocked by X.
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

- **Prefer `Literal` over `StrEnum` / `(str, Enum)` for string constants**: Use `typing.Literal` for closed sets of string values. Reach for a real `Enum` class only when something forces it.
  - Define a `Literal` type alias for the validated value set.
  - Export module-level constants for the individual values so callers don't repeat string literals at call sites.
  - For DB-mapped fields, store the value as a plain `sa_column=Column(String, ...)` typed with the `Literal`. Do **not** add a DB CHECK constraint — closed string sets are enforced by the `Literal` type plus boundary validation (SQLModel/Pydantic on write, the CLI's `choices=`, API request models). Keeping enforcement in code means widening a set is a one-line code change, not a hand-written constraint migration (Alembic autogenerate can't emit CHECK changes). Avoid native ENUM types too — `ALTER TYPE ADD VALUE` is non-transactional in Postgres, while a plain `String` round-trips cleanly through both Postgres and SQLite.
  - **Docstring convention — public-API only**: In public-API parameter docstrings (functions a user calls from outside the module), show the literal value (` ``"NONE"`` `, ` ``"dict"`` `) rather than the constant name (`PRESERVATION_NONE`, `ORIENT_DICT`). The reader copy-pastes the value into a call. Use sphinx double-backticks to match the surrounding doc style.
    - **Don't** rewrite test docstrings — they sit next to test code that uses the constant name, and a docstring/code mismatch is more confusing than the constant.
    - **Don't** rewrite cryptic abbreviations — `FIELDTYPE_ARRAY` is self-documenting, while `"a"` is opaque. Keep the constant name in docstrings when the literal is shorter than its meaning.
    - **Do** rewrite when the literal is a clear word (`"NONE"`, `"dict"`, `"sum"`, `"temp_named"`).
  - Example:
    ```python
    from typing import Literal

    APPLES = "apples"
    BANANAS = "bananas"
    FruitType = Literal["apples", "bananas"]
    FRUIT_TYPES: list[FruitType] = [APPLES, BANANAS]

    def eat_fruit(fruit: FruitType) -> None:
        ...

    # GOOD — type-checked string literal
    eat_fruit(APPLES)
    eat_fruit("bananas")

    # BAD — StrEnum adds runtime class, IntEnum-style indirection, and
    # imports for what is fundamentally just a string
    class FruitType(StrEnum):
        APPLES = "apples"
        BANANAS = "bananas"

    eat_fruit(FruitType.APPLES)
    ```

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

## Alembic Migrations

Use the `generate-migration` skill. Never hand-write migration files.

# Future Plans

`docs/designs/future.md` is the single source of truth for unimplemented features. Move planned work there instead of marking it `⚠️ NOT YET IMPLEMENTED` inline. Spec docs may briefly reference it. Remove items when implemented.

# Specification-Driven Development

**Write detailed specifications BEFORE implementing complex features.**

## Workflow

1. **Create Specification Document** (`docs/designs/<feature>.md`):
   - Describe architecture, data models, and APIs
   - Include code examples showing intended usage
   - Document design decisions and trade-offs
   - Specify all data types, enums, and schemas
   - Keep specifications detailed and comprehensive

2. **Plan and Implement with superpowers**: Use the `superpowers:writing-plans` and `superpowers:executing-plans` skills to break the feature into phases and execute them:
   - Write comprehensive tests for each phase
   - Commit working code frequently

3. **Update Documentation to Reference Implementation**:
   - **Add implementation references**: Point to actual code files and line numbers
   - **Example**: `**Implementation**: aaiclick/orchestration/factories.py:30-107`
   - **Remove duplication**: Once code exists, reference it instead of duplicating
   - **No status icons**: The implementation reference itself signals a feature is built — do not add ✅ IMPLEMENTED markers. Unimplemented work lives in `docs/designs/future.md` (see Future Plans), not inline ⚠️ NOT YET IMPLEMENTED markers.
   - **Keep unimplemented specs**: Detailed descriptions serve as design docs for future work

4. **Remove superpowers plan and spec once implemented**: Delete the superpowers plan (`docs/superpowers/plans/`) and the feature's spec document after the feature lands, and fix any references pointing at them — the code and its user-guide docs are the record. (Unimplemented specs stay, per step 3.)

## Documentation

- Use the `shortify` skill after writing or editing docs in subdirectories.
- Use the `markdown-style` skill for heading style, table formatting, admonitions, and implementation references.
- Skip root-level `.md` files (`CLAUDE.md`, `README.md`, `CHANGELOG.md`).
