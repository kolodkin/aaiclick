Phase 2 — Resolution & Public API
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Wire up the user-facing API. After this phase callers can pass `preserve=...` to `create_job()` and `@register_job(...)`. The `preserve` value is persisted and resolved correctly, but no lifecycle behavior changes yet — `task_scope()` still uses the old machinery.

---

## Task 1: `resolve_preserve()` with TDD

**Files:**
- Create: `aaiclick/orchestration/test_preserve_resolution.py`
- Modify: `aaiclick/orchestration/factories.py`

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/orchestration/test_preserve_resolution.py`:

```python
"""Tests for resolve_preserve() — precedence: explicit > registered > None."""

import pytest

from aaiclick.orchestration.factories import resolve_preserve


def test_explicit_none_falls_through_to_registered():
    assert resolve_preserve(explicit=None, registered=["a"]) == ["a"]


def test_explicit_list_overrides_registered():
    assert resolve_preserve(explicit=["a"], registered=["b"]) == ["a"]


def test_explicit_empty_list_is_explicit_no_preserve():
    # `[]` is "explicitly nothing" — must NOT fall through.
    assert resolve_preserve(explicit=[], registered=["b"]) == []


def test_explicit_star_overrides_registered():
    assert resolve_preserve(explicit="*", registered=["b"]) == "*"


def test_registered_none_returns_none():
    assert resolve_preserve(explicit=None, registered=None) is None


def test_registered_star_passes_through():
    assert resolve_preserve(explicit=None, registered="*") == "*"


def test_explicit_invalid_type_raises():
    with pytest.raises(TypeError, match="preserve"):
        resolve_preserve(explicit=42, registered=None)  # type: ignore[arg-type]
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_preserve_resolution.py -x --no-cov -q
```

Expected: `ImportError` for `resolve_preserve`.

- [ ] **Step 3: Implement `resolve_preserve()`**

In `aaiclick/orchestration/factories.py`, after the existing imports, add:

```python
from aaiclick.orchestration.models import Preserve


_UNSET: object = object()


def resolve_preserve(explicit: Preserve | object = _UNSET, registered: Preserve = None) -> Preserve:
    """Resolve effective preserve value.

    Precedence:
        1. ``explicit`` if non-None (including ``[]`` — explicit empty list is honored).
        2. ``registered`` (the RegisteredJob default).
        3. ``None``.

    The sentinel is needed to distinguish ``explicit=None`` (caller didn't supply
    anything; fall through) from ``explicit=[]`` (caller explicitly said nothing
    should be preserved; do NOT fall through).
    """
    if explicit is _UNSET or explicit is None:
        chosen = registered
    else:
        if not (explicit == "*" or isinstance(explicit, list)):
            raise TypeError(
                f"preserve must be None, '*', or list[str]; got {type(explicit).__name__}"
            )
        if isinstance(explicit, list) and not all(isinstance(x, str) for x in explicit):
            raise TypeError("preserve list must contain only str")
        chosen = explicit  # type: ignore[assignment]

    if chosen is None:
        return None
    if chosen == "*":
        return "*"
    return list(chosen)  # defensive copy
```

Update the test signature to use the same sentinel — `resolve_preserve(explicit=None, registered=...)` should treat `None` as fall-through, matching test 1. The implementation above does this.

- [ ] **Step 4: Run tests to confirm pass**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_preserve_resolution.py -x --no-cov -q
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/test_preserve_resolution.py
git commit -m "$(cat <<'EOF'
feature: resolve_preserve() with explicit > registered > None precedence

Sentinel-based fall-through so explicit=[] is honored as 'preserve nothing'
instead of falling back to the RegisteredJob default. Type validation on
explicit prevents silent acceptance of garbage.
EOF
)"
```

---

## Task 2: Wire `preserve` into `create_job()`

**Files:**
- Modify: `aaiclick/orchestration/factories.py` (add `preserve` param to `create_job`)
- Modify: `aaiclick/orchestration/test_orchestration_factories.py` (add a test)

- [ ] **Step 1: Find `create_job()` signature**

```bash
grep -n "^async def create_job\|^def create_job" /home/user/aaiclick/aaiclick/orchestration/factories.py
```

Note the line. Read 30 lines starting there to understand current params.

- [ ] **Step 2: Add the test first**

Append to `aaiclick/orchestration/test_orchestration_factories.py`:

```python
async def test_create_job_persists_explicit_preserve(orch_session):
    job = await create_job(
        registered_name="example",   # adjust to whatever fixture exists
        preserve=["foo", "bar"],
    )
    refreshed = await orch_session.get(Job, job.id)
    assert refreshed.preserve == ["foo", "bar"]


async def test_create_job_uses_registered_preserve_default(orch_session, registered_factory):
    registered_factory(name="example_with_default", preserve=["default_table"])
    job = await create_job(registered_name="example_with_default")
    refreshed = await orch_session.get(Job, job.id)
    assert refreshed.preserve == ["default_table"]


async def test_create_job_explicit_overrides_registered(orch_session, registered_factory):
    registered_factory(name="example_override", preserve=["from_registered"])
    job = await create_job(
        registered_name="example_override",
        preserve=["from_explicit"],
    )
    refreshed = await orch_session.get(Job, job.id)
    assert refreshed.preserve == ["from_explicit"]
```

If `registered_factory` doesn't exist, look in `aaiclick/orchestration/conftest.py` for the existing way to create a `RegisteredJob` test fixture and adapt accordingly.

- [ ] **Step 3: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_orchestration_factories.py -x --no-cov -q -k "preserve"
```

Expected: failures complaining `preserve` isn't an accepted kwarg.

- [ ] **Step 4: Update `create_job()` signature**

Add `preserve: Preserve | None = None` to the parameter list (place it after existing optional params). Inside the function, replace any `preservation_mode` resolution with:

```python
registered_preserve = registered.preserve if registered is not None else None
job.preserve = resolve_preserve(explicit=preserve, registered=registered_preserve)
```

Locate the existing line that sets `job.preservation_mode = ...` (it's still there from Phase 1; Phase 6 deletes it). Leave the old line in place for now — both columns are populated until Phase 6 cuts the old one over. **Important:** the old `resolve_job_config()` call must NOT raise if `mode=None` and `registered.preservation_mode=None`. If the existing implementation already supports that, no change is needed.

- [ ] **Step 5: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_orchestration_factories.py -x --no-cov -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/factories.py aaiclick/orchestration/test_orchestration_factories.py
git commit -m "$(cat <<'EOF'
feature: create_job(preserve=...) parameter

Persists the resolved preserve value on the Job row. Old preservation_mode
column continues to be populated until Phase 6 cuts it over.
EOF
)"
```

---

## Task 3: Wire `preserve` into `@register_job` decorator

**Files:**
- Modify: `aaiclick/orchestration/decorators.py` (or `registered_jobs.py` — find which has the decorator)
- Modify: a test file in `aaiclick/orchestration/` covering the decorator

- [ ] **Step 1: Locate the decorator**

```bash
grep -rn "^def register_job\|^async def register_job\|register_job =" /home/user/aaiclick/aaiclick/orchestration/decorators.py /home/user/aaiclick/aaiclick/orchestration/registered_jobs.py
```

- [ ] **Step 2: Write the failing test**

Append to `aaiclick/orchestration/test_registered_jobs.py`:

```python
async def test_register_job_stores_preserve(orch_session):
    @register_job(name="my_preserved_job", preserve=["a", "b"])
    async def my_job(ctx):
        pass

    refreshed = await orch_session.exec(
        select(RegisteredJob).where(RegisteredJob.name == "my_preserved_job")
    )
    row = refreshed.one()
    assert row.preserve == ["a", "b"]


async def test_register_job_preserve_star(orch_session):
    @register_job(name="star_job", preserve="*")
    async def star_job(ctx):
        pass

    refreshed = await orch_session.exec(
        select(RegisteredJob).where(RegisteredJob.name == "star_job")
    )
    assert refreshed.one().preserve == "*"
```

(Adjust imports — `select`, `RegisteredJob`, `register_job` — to match the existing test file's pattern.)

- [ ] **Step 3: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_registered_jobs.py -x --no-cov -q -k "preserve"
```

Expected: TypeError on unexpected `preserve` kwarg.

- [ ] **Step 4: Add `preserve` to the decorator**

In the decorator implementation, add `preserve: Preserve = None` to the kwargs and propagate to the `RegisteredJob` row creation:

```python
def register_job(
    *,
    name: str,
    preserve: Preserve = None,
    # ... existing params ...
):
    # ... existing body ...
    registered = RegisteredJob(
        name=name,
        preserve=preserve,
        # ... existing fields ...
    )
```

Validation: reuse `resolve_preserve(explicit=preserve, registered=None)` to normalize the value (defensive copy + type check) before storing.

- [ ] **Step 5: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_registered_jobs.py -x --no-cov -q
```

Expected: green.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/decorators.py aaiclick/orchestration/registered_jobs.py aaiclick/orchestration/test_registered_jobs.py
git commit -m "feature: @register_job(preserve=...) parameter"
```

---

## Task 4: Phase 2 sanity check

- [ ] **Step 1: Run the full orchestration test suite**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS. New tests added in this phase pass; no regression in existing tests.

- [ ] **Step 2: Spot-check the public API surface**

```bash
python -c "
import asyncio, inspect
from aaiclick.orchestration.factories import create_job, resolve_preserve
sig = inspect.signature(create_job)
assert 'preserve' in sig.parameters, sig
print('create_job has preserve param: ok')
"
```

Expected: `create_job has preserve param: ok`.

- [ ] **Step 3: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-gwqt4
```

---

# Done When

- `resolve_preserve()` exists with documented precedence rules and type validation.
- `create_job(preserve=...)` accepts and persists the value.
- `@register_job(preserve=...)` accepts and persists the default.
- Existing `PreservationMode` callers still work (untouched until Phase 6).
- Test suite is green.
