---
name: python-testing-style
description: Project conventions for writing pytest tests in aaiclick — file layout, flat structure, async test rules, what NOT to test, Object API alignment. TRIGGER when creating or editing `test_*.py` files, `conftest.py`, or when asked to write/review tests.
---

# python-testing-style

Project conventions for pytest tests in aaiclick.

## File location — alongside the module under test

- `aaiclick/data/test_context.py` tests `aaiclick/data/data_context.py`
- `aaiclick/orchestration/test_orchestration_factories.py` tests `aaiclick/orchestration/factories.py`
- Shared fixtures live in `aaiclick/conftest.py`.
- **Exception**: end-to-end suites that exercise the deployed package live in `./test_e2e/<suite>/` (e.g. `test_e2e/docker/`). They are not picked up by the default `pytest` invocation and only run via dedicated workflows.

## Flat structure — no test classes

Tests are flat module-level functions: `def test_*():` or `async def test_*():`. Group related tests by file, not by class.

## Async tests — no decorator needed

Do NOT use `@pytest.mark.asyncio`. `pytest-asyncio` is configured in `pyproject.toml` to auto-detect async test functions. Just write `async def test_*():`.

## Unrelated test failures — fix the implementation, not the test

When tests outside the scope of your changes break, your changes have unintended side effects.

- Do NOT modify, skip, or weaken unrelated tests to make them pass.
- If unsure whether the test or the implementation is wrong, ask the user.

## Object API test file alignment

Each section in the `docs/user_guide/object.md` API Quick Reference table must have a dedicated test file in `aaiclick/data/object/` named after the section (e.g. `test_comparison.py`, `test_bitwise.py`, `test_domain_helpers.py`).

When adding a new API section, create the corresponding test file. When a domain helper is tightly coupled to an operator (e.g., `with_isin` ↔ `isin`), tests go in the operator's test file (`test_isin.py`), not `test_domain_helpers.py`.

## Don't test Python defaults or plain assignment

Python is already tested — trust it.

**Skip**:
- Constructing an object and asserting constructor-assigned fields equal the inputs.
- Asserting default values of dataclass / Pydantic / NamedTuple fields (`assert obj.x is None`).
- Decorator tests that only check `@task(name="x")` stores `name == "x"`.
- Trivial factory passthrough (`factory(a, b)` → assert fields match `a`, `b`).

**Test real behavior**: branching logic, computations, validation errors, DB round-trips, schema inference, format output, ID uniqueness, env-var parsing.

```python
# BAD — only checks Python assignment works
def test_task_default_max_retries():
    t = create_task("mod.fn")
    assert t.max_retries == 0

# GOOD — tests real validation behavior
def test_strategy_mode_requires_strategy():
    with pytest.raises(ValueError, match="requires a non-empty sampling_strategy"):
        resolve_job_config(PreservationMode.STRATEGY, None, None)

# GOOD — tests branching logic
def test_data_list_single_vs_multiple():
    assert data_list("only").data == "only"
    assert data_list("a", "b").data == ["a", "b"]
```

## Warnings — `filterwarnings = ["error"]` turns warnings into failures

`pyproject.toml` sets `filterwarnings = ["error"]`, so any unhandled warning fails the test. When a third-party library emits a known warning, suppress it with `warnings.catch_warnings()` scoped around the call that triggers it.
