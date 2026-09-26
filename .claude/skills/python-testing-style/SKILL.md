---
name: python-testing-style
description: Project conventions for writing pytest tests in aaiclick — file layout, flat structure, async test rules, prefer end-to-end over internal tests, what NOT to test, Object API alignment, when to parametrize, when a test is safe to delete. TRIGGER when creating or editing `test_*.py` files, `conftest.py`, or when asked to write, review, consolidate, parametrize, deduplicate, or remove tests.
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
- SQL text sent to a (mocked) client. Assert the outcome — rows, tables remaining, state — unless the user explicitly asks for it or the test docstring says the SQL shape is the contract (e.g. `select_sql()` returns SQL).
- The outcome of a specific DB migration (Alembic revision or `aaiclick/oplog/migrations/NNNN_*.sql`) — e.g. "after 0002 the column is X". Test the migration runner, not individual scripts; when a migration breaks an existing test, update that test.

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

## Avoid internal tests — prefer end-to-end

Test through the surface a user touches, not the implementation behind it. A test that asserts on private helpers (`_foo()`), private attributes, in-memory wiring (`task.previous_dependencies`), serialized dict shapes, or generated SQL text breaks on every refactor and can pass while the real flow is broken.

Pick the highest public entry point that exercises the behavior:

| Area | Entry point | Assert on |
|------|-------------|-----------|
| Orchestration | a real `@job` / `@task` pipeline run with `ajob_test` | job status, `get_job_result`, persisted rows |
| Object API | `create_object_from_value` inside `data_context()` + the operator | `await obj.data()`, schema |
| Server / API | an HTTP request against the app, or an `internal_api` call on a real DB | status code, response body |
| CLI | the CLI entry point | output, exit code |

A bug fix gets an end-to-end test that reproduces the user-visible failure. Run it against the unfixed code and confirm it fails before you commit.

```python
# BAD — checks in-memory wiring; says nothing about whether the job runs
def test_same_upstream_twice():
    consumer = add(left=upstream, right=[upstream])
    assert len(consumer.previous_dependencies) == 1

# GOOD — runs the pipeline the user wrote and checks what they get back
async def test_same_upstream_in_two_kwargs_runs(orch_ctx):
    j = await ajob_test(same_upstream_twice_pipeline, value=21)
    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == 42
```

Internal tests are fine only when an end-to-end run can't reach the case: crash recovery, race windows, dead-worker cleanup, retry and backoff timing, or a pure function whose output *is* the contract (a parser, a SQL param set). Say which in the docstring (e.g. "pure function: the returned schema is the contract").

## Parametrize input/expected clusters

When several tests drive the same call and differ only in inputs and expected values, fold them into one `@pytest.mark.parametrize`. Consolidate only when **all** of these hold:

1. **One call shape** — same function or method; only literal arguments and expected values differ.
2. **One outcome kind** — never merge tests asserting a returned value with tests asserting a raised exception. `pytest.raises` clusters parametrize separately.
3. **Zero added logic** — the body makes one call; no `if`, loop, `match`, dispatch helper, or `getattr(obj, name)` absorbs the variants. When the variants are operators or methods, the callable itself is the param (`pytest.param(operator.add, 15, id="add")` with `op(obj, 5)`, or `Object.with_year` with `op(obj, "ts")`) — that is one call shape.
4. **Same fixtures and decorators.**
5. **Intent survives** — every case gets a descriptive `id=`, and a docstring that explained one case becomes a comment on its param.

Leave alone: different call chains (`having` vs `or_having`) and tests whose setup bodies differ.

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

A warning raised at garbage collection or teardown (an unawaited coroutine, `PytestUnraisableExceptionWarning`) fires after the call returns, so `catch_warnings()` can't catch it. Fix ours (our code, our mocks). Filter only a third-party leak: an exact-message `filterwarnings` mark on the affected tests, applied in one place with an upstream reference and a TODO (see `aaiclick/ai/conftest.py`).
