Phase 3 — Task Name Locks
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Add the SQL model + acquire/release/sweep operations for `task_name_locks`. The lock guards the invariant *"at most one live task in a job holds a non-preserved name."*

After this phase the lock infrastructure exists and is unit-tested, but `task_scope()` doesn't yet acquire it. Phase 4 wires it in.

---

## Task 1: Define the `TaskNameLock` SQL model

**Files:**
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py`

- [ ] **Step 1: Add the SQLModel class**

Append to `aaiclick/orchestration/lifecycle/db_lifecycle.py`:

```python
from datetime import datetime
from sqlmodel import SQLModel, Field


class TaskNameLock(SQLModel, table=True):
    """One row per `(job_id, name)` held by a live task.

    Acquired by `task_scope()` when a task creates `j_<id>_<name>` for a
    name not in the job's `preserve` list. Released on task exit (success
    or failure) and by the BackgroundWorker dead-worker sweep.
    """

    __tablename__ = "task_name_locks"

    job_id: int = Field(primary_key=True)
    name: str = Field(primary_key=True, max_length=255)
    task_id: int = Field(index=True)
    acquired_at: datetime = Field(default_factory=datetime.utcnow)
```

Match the style of the surrounding `TablePinRef` / `TableContextRef` models — use the same import patterns and column types.

- [ ] **Step 2: Verify the class loads**

```bash
cd /home/user/aaiclick && python -c "from aaiclick.orchestration.lifecycle.db_lifecycle import TaskNameLock; print(TaskNameLock.__tablename__)"
```

Expected: `task_name_locks`.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/lifecycle/db_lifecycle.py
git commit -m "feature: TaskNameLock SQLModel for non-preserved name coordination"
```

---

## Task 2: Acquire/release operations with TDD

**Files:**
- Create: `aaiclick/orchestration/lifecycle/test_task_name_locks.py`
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py` (add helper functions)

- [ ] **Step 1: Write failing tests**

Create `aaiclick/orchestration/lifecycle/test_task_name_locks.py`:

```python
"""Tests for task_name_locks acquire / release / sweep operations.

These coordinate non-preserved named tables between concurrent tasks in
the same job: only one live task can hold a given (job_id, name) at a time.
"""

import pytest
from sqlmodel import select

from aaiclick.orchestration.lifecycle.db_lifecycle import (
    TableNameCollision,
    TaskNameLock,
    acquire_task_name_lock,
    release_task_name_locks_for_task,
    release_task_name_locks_for_dead_tasks,
)


async def test_acquire_succeeds_when_free(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    rows = (await orch_session.exec(select(TaskNameLock).where(TaskNameLock.job_id == 1))).all()
    assert len(rows) == 1
    assert rows[0].name == "foo"
    assert rows[0].task_id == 100


async def test_acquire_idempotent_for_same_task(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    # Same task re-acquiring its own lock is a no-op.
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 1


async def test_acquire_collision_raises(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    with pytest.raises(TableNameCollision) as exc_info:
        await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=200)
    assert exc_info.value.held_by_task_id == 100
    assert exc_info.value.name == "foo"


async def test_acquire_different_names_isolated(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    await acquire_task_name_lock(orch_session, job_id=1, name="bar", task_id=200)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 2


async def test_acquire_different_jobs_isolated(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    await acquire_task_name_lock(orch_session, job_id=2, name="foo", task_id=100)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 2


async def test_release_for_task_clears_only_that_task(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    await acquire_task_name_lock(orch_session, job_id=1, name="bar", task_id=100)
    await acquire_task_name_lock(orch_session, job_id=1, name="baz", task_id=200)
    await release_task_name_locks_for_task(orch_session, task_id=100)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 1
    assert rows[0].name == "baz"


async def test_release_after_release_allows_acquire(orch_session):
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=100)
    await release_task_name_locks_for_task(orch_session, task_id=100)
    # Now task 200 can take the same name.
    await acquire_task_name_lock(orch_session, job_id=1, name="foo", task_id=200)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 1
    assert rows[0].task_id == 200


async def test_dead_task_sweep_releases_locks(orch_session, task_factory):
    # Create two tasks: one alive (RUNNING), one dead (FAILED).
    alive_task = task_factory(status="RUNNING")
    dead_task = task_factory(status="FAILED")
    await acquire_task_name_lock(orch_session, job_id=1, name="alive", task_id=alive_task.id)
    await acquire_task_name_lock(orch_session, job_id=1, name="dead", task_id=dead_task.id)
    await release_task_name_locks_for_dead_tasks(orch_session)
    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert len(rows) == 1
    assert rows[0].name == "alive"
```

If `task_factory` doesn't exist in `conftest.py`, look for the existing pattern that creates `Task` rows in tests and adapt — or add a helper to the test file that constructs a `Task` row directly via the session.

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/lifecycle/test_task_name_locks.py -x --no-cov -q
```

Expected: ImportError on `acquire_task_name_lock` (and friends).

- [ ] **Step 3: Implement the helpers**

Add to `aaiclick/orchestration/lifecycle/db_lifecycle.py`:

```python
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from aaiclick.orchestration.models import Task, TaskStatus


async def acquire_task_name_lock(
    session: AsyncSession, *, job_id: int, name: str, task_id: int
) -> None:
    """Take the (job_id, name) lock for ``task_id``.

    Idempotent: re-acquiring an already-held lock for the same task is a no-op.
    Raises ``TableNameCollision`` if another task currently holds the lock.
    """
    existing = (
        await session.exec(
            select(TaskNameLock).where(
                TaskNameLock.job_id == job_id,
                TaskNameLock.name == name,
            )
        )
    ).first()
    if existing is not None:
        if existing.task_id == task_id:
            return
        raise TableNameCollision(name=name, held_by_task_id=existing.task_id)
    session.add(TaskNameLock(job_id=job_id, name=name, task_id=task_id))
    await session.flush()


async def release_task_name_locks_for_task(session: AsyncSession, *, task_id: int) -> None:
    """Drop every lock held by ``task_id``."""
    rows = (
        await session.exec(
            select(TaskNameLock).where(TaskNameLock.task_id == task_id)
        )
    ).all()
    for row in rows:
        await session.delete(row)
    await session.flush()


_LIVE_STATUSES = (TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING)


async def release_task_name_locks_for_dead_tasks(session: AsyncSession) -> None:
    """Drop locks whose holding task is in a terminal state.

    Called by the BackgroundWorker dead-worker sweep — keeps the lock table
    from accumulating stale rows when tasks die without going through their
    own ``__aexit__``.
    """
    locks = (await session.exec(select(TaskNameLock))).all()
    if not locks:
        return
    task_ids = {lock.task_id for lock in locks}
    live_rows = (
        await session.exec(
            select(Task.id).where(
                Task.id.in_(task_ids),
                Task.status.in_(_LIVE_STATUSES),
            )
        )
    ).all()
    live_ids = set(live_rows)
    for lock in locks:
        if lock.task_id not in live_ids:
            await session.delete(lock)
    await session.flush()
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/lifecycle/test_task_name_locks.py -x --no-cov -q
```

Expected: 8 passed. If `task_factory` is missing, see Step 1 — adapt to the codebase's existing pattern.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/lifecycle/db_lifecycle.py aaiclick/orchestration/lifecycle/test_task_name_locks.py
git commit -m "$(cat <<'EOF'
feature: task_name_locks acquire/release/sweep helpers

Coordinates non-preserved named tables between concurrent tasks in
the same job. Idempotent acquire for same task; collision raises
TableNameCollision; sweep clears locks for terminal tasks.
EOF
)"
```

---

## Task 3: Phase 3 sanity check

- [ ] **Step 1: Run full orchestration tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS.

- [ ] **Step 2: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-aNOnA
```

---

# Done When

- `TaskNameLock` model exists.
- `acquire_task_name_lock`, `release_task_name_locks_for_task`, `release_task_name_locks_for_dead_tasks` are implemented and unit-tested.
- Test suite is green.
