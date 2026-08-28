---
name: python-testing-style
description: Project conventions for writing pytest tests in aaiclick — file layout, flat structure, async test rules, what NOT to test, Object API alignment, when to parametrize, when a test is safe to delete. TRIGGER when creating or editing `test_*.py` files, `conftest.py`, or when asked to write, review, consolidate, parametrize, deduplicate, or remove tests.
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

## Parametrize input/expected clusters

When several tests drive the same call and differ only in inputs and expected values, fold them into one `@pytest.mark.parametrize`. Consolidate only when **all** of these hold:

1. **One call shape** — same function or method; only literal arguments and expected values differ.
2. **One outcome kind** — never merge tests asserting a returned value with tests asserting a raised exception. `pytest.raises` clusters parametrize separately.
3. **Zero added logic** — no `if`, loop, `getattr`, or operator indirection introduced to absorb the variants. Needing one means don't consolidate.
4. **Same fixtures and decorators.**
5. **Intent survives** — every case gets a descriptive `id=`, and a docstring that explained one case becomes a comment on its param.

Leave alone: different methods (`match` vs `like` — would need `getattr`), different call chains (`having` vs `or_having`), and tests whose setup bodies differ.

```python
# GOOD — one call, only literals vary, ids carry the intent
@pytest.mark.parametrize(
    "value, pattern, expected",
    [
        pytest.param(["apple", "banana"], "^a", [1, 0], id="array"),
        pytest.param("hello", "ell", 1, id="scalar"),
    ],
)
async def test_match(ctx, value, pattern, expected):
    obj = await create_object_from_value(value)
    assert await (await obj.match(pattern)).data() == expected

# BAD — merges raises with asserts, and needs a branch to do it
@pytest.mark.parametrize("value, expected, raises", [...])
async def test_match(ctx, value, expected, raises):
    if raises:
        with pytest.raises(ValueError):
            await create_object_from_value(value)
    else:
        ...
```

## Removing a redundant test

Delete only when redundancy is **mechanically provable** — not because a test looks trivial:

- **Exact duplicate** — identical statements, constants included. Keep the copy in the module whose docstring claims that contract.
- **Strict subset** — a sibling asserts everything this test does, plus more. Fold any rationale the removed test documented into the survivor's docstring.

Verify before committing: diff per-file `executed_lines` from `--cov-report=json` before and after, ignoring `test_*.py` entries, and require **zero production lines lost**. A green suite is not evidence on its own — a deleted test cannot fail.

Looking trivial is not proof. Check first whether the test covers the negative branch of a conditional, or construction behavior that isn't free (positional args on a Pydantic model). Both read like default-value assertions and are neither.

## Warnings — `filterwarnings = ["error"]` turns warnings into failures

`pyproject.toml` sets `filterwarnings = ["error"]`, so any unhandled warning fails the test. When a third-party library emits a known warning, suppress it with `warnings.catch_warnings()` scoped around the call that triggers it.
