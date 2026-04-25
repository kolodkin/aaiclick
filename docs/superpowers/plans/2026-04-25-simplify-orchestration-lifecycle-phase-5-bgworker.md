Phase 5 — BackgroundWorker
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Wire up the BackgroundWorker side of the new lifecycle:

- `_cleanup_failed_task_tables()` — drop non-preserved `j_<id>_*` for PENDING_CLEANUP tasks.
- `_cleanup_orphan_scratch_tables()` — drop unpinned `t_*` whose owning task is dead.
- `_cleanup_at_job_completion()` — at job terminal transition, drop every CH table tied to the job.
- Extend `_cleanup_dead_workers()` to release `task_name_locks`.

The old `_cleanup_unreferenced_tables()` stays in this phase (still works against the migrated schema since it referenced `table_run_refs`, but that table is gone — see Phase 6 for the actual delete). For Phase 5, ensure callers don't invoke it; it's already dead code.

---

## Task 1: `_cleanup_at_job_completion()`

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`
- Modify: `aaiclick/orchestration/background/handler.py` (and its sqlite/pg variants)
- Create: `aaiclick/orchestration/background/test_cleanup_at_job_completion.py`

- [ ] **Step 1: Write the failing test**

Create the test file:

```python
"""At job completion, BackgroundWorker drops every CH table tied to the job.

Includes:
- j_<id>_<name> preserved
- j_<id>_<name> not preserved (already dropped earlier, but the sweep is idempotent)
- t_* pinned (Object outputs that haven't been pin-cleaned yet)

After the call: table_pin_refs and task_name_locks rows for the job are gone.
"""

import pytest
from sqlmodel import select

from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.lifecycle.db_lifecycle import (
    TableRegistry,
    TablePinRef,
    TaskNameLock,
)


async def test_completion_drops_all_job_tables(ch_client, orch_session, simple_job, bg_worker):
    job_id = simple_job.id

    # Seed: a preserved named table, a pinned t_*, and registry rows for both.
    preserved = f"j_{job_id}_training_set"
    pinned = "t_pinned_obj_output"
    await ch_client.command(f"CREATE TABLE {preserved} (x Int64) ENGINE = Memory")
    await ch_client.command(f"CREATE TABLE {pinned} (x Int64) ENGINE = Memory")

    orch_session.add(TableRegistry(table_name=preserved, job_id=job_id, task_id=1, preserved=True))
    orch_session.add(TableRegistry(table_name=pinned, job_id=job_id, task_id=1, preserved=False))
    orch_session.add(TablePinRef(table_name=pinned, task_id=99))
    await orch_session.commit()

    # Act: invoke job-completion cleanup directly.
    await bg_worker._cleanup_at_job_completion(job_id=job_id)

    # Assert: both tables gone, pin/lock rows gone.
    assert (await ch_client.query(f"EXISTS TABLE {preserved}")).first_row[0] == 0
    assert (await ch_client.query(f"EXISTS TABLE {pinned}")).first_row[0] == 0
    pin_rows = (await orch_session.exec(select(TablePinRef).where(TablePinRef.table_name == pinned))).all()
    assert pin_rows == []


async def test_completion_skips_other_jobs(ch_client, orch_session, bg_worker):
    """Tables for other jobs must not be touched."""
    other_id = 9999
    other_table = f"j_{other_id}_keepme"
    await ch_client.command(f"CREATE TABLE {other_table} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=other_table, job_id=other_id, task_id=1, preserved=True))
    await orch_session.commit()

    await bg_worker._cleanup_at_job_completion(job_id=1)  # different job

    assert (await ch_client.query(f"EXISTS TABLE {other_table}")).first_row[0] == 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_cleanup_at_job_completion.py -x --no-cov -q
```

Expected: AttributeError on `_cleanup_at_job_completion`.

- [ ] **Step 3: Implement the method**

In `aaiclick/orchestration/background/background_worker.py`, add:

```python
async def _cleanup_at_job_completion(self, *, job_id: int) -> None:
    """Drop every CH table tied to a completed job.

    Called from ``try_complete_job()`` once a job's terminal status is set.
    Idempotent: tables already dropped by task_scope inline are simply
    skipped (DROP TABLE IF EXISTS).
    """
    async with self._db.session() as session:
        registry_rows = (
            await session.exec(
                select(TableRegistry).where(TableRegistry.job_id == job_id)
            )
        ).all()
        table_names = [row.table_name for row in registry_rows]

        for name in table_names:
            try:
                await self._ch.command(f"DROP TABLE IF EXISTS {name}")
            except Exception as exc:
                self._logger.warning("DROP failed for %s: %s", name, exc)

        # Clear SQL bookkeeping for this job.
        for row in registry_rows:
            await session.delete(row)
        pin_rows = (
            await session.exec(
                select(TablePinRef).where(TablePinRef.table_name.in_(table_names))
            )
        ).all() if table_names else []
        for row in pin_rows:
            await session.delete(row)
        lock_rows = (
            await session.exec(
                select(TaskNameLock).where(TaskNameLock.job_id == job_id)
            )
        ).all()
        for row in lock_rows:
            await session.delete(row)
        await session.commit()
```

`self._db.session()`, `self._ch`, `self._logger` should match whatever the existing `BackgroundWorker.__init__` sets up — read the current `__init__` and adapt.

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_cleanup_at_job_completion.py -x --no-cov -q
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_cleanup_at_job_completion.py
git commit -m "$(cat <<'EOF'
feature: BackgroundWorker._cleanup_at_job_completion

Drops every CH table tied to a completed job (preserved, non-preserved,
and pinned t_* outputs alike). Clears table_registry, table_pin_refs,
and task_name_locks rows for the job.
EOF
)"
```

---

## Task 2: Integrate into `try_complete_job()`

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`

- [ ] **Step 1: Find existing `try_complete_job()`**

```bash
grep -n "def try_complete_job\|async def try_complete_job" /home/user/aaiclick/aaiclick/orchestration/background/background_worker.py
```

Read the current implementation. It transitions a job to terminal status (COMPLETED / FAILED / CANCELLED) when all tasks are terminal.

- [ ] **Step 2: Hook in cleanup**

After the job is marked terminal AND the session has committed:

```python
# Existing: set job.status = ...; await session.commit()

# NEW: clean up CH tables and SQL bookkeeping for the job.
await self._cleanup_at_job_completion(job_id=job.id)
```

If the existing function uses a single session and you'd rather keep cleanup transactional with the status update, either:
- Wrap both in the same session, or
- Move cleanup before commit (cleanup uses DDL which can't roll back, so this is fine to do after commit).

- [ ] **Step 3: Update `test_try_complete_job.py`**

Add at least one test that asserts `_cleanup_at_job_completion` was called for a job that just transitioned to COMPLETED. Look at existing tests in that file for the pattern (mock or real). Example:

```python
async def test_try_complete_calls_cleanup(bg_worker, monkeypatch, simple_job):
    called = []
    async def fake_cleanup(*, job_id):
        called.append(job_id)
    monkeypatch.setattr(bg_worker, "_cleanup_at_job_completion", fake_cleanup)

    # Mark all tasks for the job as COMPLETED, then call try_complete_job.
    # ...

    await bg_worker.try_complete_job(job_id=simple_job.id)
    assert called == [simple_job.id]
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_try_complete_job.py -x --no-cov -q
```

Expected: green.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_try_complete_job.py
git commit -m "feature: try_complete_job invokes _cleanup_at_job_completion"
```

---

## Task 3: `_cleanup_failed_task_tables()`

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`
- Update: `aaiclick/orchestration/background/test_pending_cleanup.py`

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/background/test_pending_cleanup.py`:

```python
async def test_cleanup_failed_task_drops_non_preserved_named(ch_client, orch_session, bg_worker, simple_job, failed_task_factory):
    failed = failed_task_factory(job_id=simple_job.id)  # status=PENDING_CLEANUP

    preserved_name = f"j_{simple_job.id}_keep"
    scratch_name = f"j_{simple_job.id}_scratch"
    await ch_client.command(f"CREATE TABLE {preserved_name} (x Int64) ENGINE = Memory")
    await ch_client.command(f"CREATE TABLE {scratch_name} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=preserved_name, job_id=simple_job.id, task_id=failed.id, preserved=True))
    orch_session.add(TableRegistry(table_name=scratch_name, job_id=simple_job.id, task_id=failed.id, preserved=False))
    await orch_session.commit()

    await bg_worker._cleanup_failed_task_tables()

    # Preserved keeps; scratch goes.
    assert (await ch_client.query(f"EXISTS TABLE {preserved_name}")).first_row[0] == 1
    assert (await ch_client.query(f"EXISTS TABLE {scratch_name}")).first_row[0] == 0


async def test_cleanup_failed_task_skips_pinned_t(ch_client, orch_session, bg_worker, simple_job, failed_task_factory):
    """Pinned t_* tables created by a failed task survive — consumer might still need them."""
    failed = failed_task_factory(job_id=simple_job.id)
    pinned_t = "t_failed_but_pinned"
    await ch_client.command(f"CREATE TABLE {pinned_t} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=pinned_t, job_id=simple_job.id, task_id=failed.id, preserved=False))
    orch_session.add(TablePinRef(table_name=pinned_t, task_id=12345))  # consumer
    await orch_session.commit()

    await bg_worker._cleanup_failed_task_tables()
    assert (await ch_client.query(f"EXISTS TABLE {pinned_t}")).first_row[0] == 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_pending_cleanup.py -x --no-cov -q -k "failed_task"
```

Expected: AttributeError on `_cleanup_failed_task_tables`.

- [ ] **Step 3: Implement**

```python
async def _cleanup_failed_task_tables(self) -> None:
    """For each PENDING_CLEANUP task, drop its non-preserved + non-pinned tables.

    Pinned t_* are skipped — consumers downstream of the failed producer
    may still need them. Preserved j_<id>_<name> are skipped — they
    survive until job completion.
    """
    async with self._db.session() as session:
        failed_tasks = (
            await session.exec(
                select(Task).where(Task.status == TaskStatus.PENDING_CLEANUP)
            )
        ).all()
        if not failed_tasks:
            return

        for task in failed_tasks:
            registry_rows = (
                await session.exec(
                    select(TableRegistry).where(
                        TableRegistry.task_id == task.id,
                        TableRegistry.preserved == False,  # noqa: E712
                    )
                )
            ).all()
            for row in registry_rows:
                # Skip if pinned.
                pinned = (
                    await session.exec(
                        select(TablePinRef).where(TablePinRef.table_name == row.table_name)
                    )
                ).first()
                if pinned is not None:
                    continue
                try:
                    await self._ch.command(f"DROP TABLE IF EXISTS {row.table_name}")
                except Exception as exc:
                    self._logger.warning("Failed-task DROP of %s failed: %s", row.table_name, exc)
                else:
                    await session.delete(row)
        await session.commit()
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_pending_cleanup.py -x --no-cov -q
```

Expected: green.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_pending_cleanup.py
git commit -m "feature: BackgroundWorker._cleanup_failed_task_tables"
```

---

## Task 4: `_cleanup_orphan_scratch_tables()`

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`
- Create: `aaiclick/orchestration/background/test_cleanup_orphan_scratch.py`

- [ ] **Step 1: Write the failing test**

```python
"""Sweeps t_* tables whose owning task is dead and that aren't pinned."""

import pytest
from sqlmodel import select

from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry, TablePinRef


async def test_orphan_scratch_dropped(ch_client, orch_session, bg_worker, simple_job, failed_task_factory):
    failed = failed_task_factory(job_id=simple_job.id)
    name = "t_orphan_scratch"
    await ch_client.command(f"CREATE TABLE {name} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=name, job_id=simple_job.id, task_id=failed.id, preserved=False))
    await orch_session.commit()

    await bg_worker._cleanup_orphan_scratch_tables()

    assert (await ch_client.query(f"EXISTS TABLE {name}")).first_row[0] == 0


async def test_orphan_scratch_skips_pinned(ch_client, orch_session, bg_worker, simple_job, failed_task_factory):
    failed = failed_task_factory(job_id=simple_job.id)
    name = "t_orphan_pinned"
    await ch_client.command(f"CREATE TABLE {name} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=name, job_id=simple_job.id, task_id=failed.id, preserved=False))
    orch_session.add(TablePinRef(table_name=name, task_id=99999))
    await orch_session.commit()

    await bg_worker._cleanup_orphan_scratch_tables()

    assert (await ch_client.query(f"EXISTS TABLE {name}")).first_row[0] == 1


async def test_orphan_scratch_skips_live_owner(ch_client, orch_session, bg_worker, simple_job, running_task_factory):
    alive = running_task_factory(job_id=simple_job.id)
    name = "t_alive_scratch"
    await ch_client.command(f"CREATE TABLE {name} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=name, job_id=simple_job.id, task_id=alive.id, preserved=False))
    await orch_session.commit()

    await bg_worker._cleanup_orphan_scratch_tables()

    assert (await ch_client.query(f"EXISTS TABLE {name}")).first_row[0] == 1


async def test_orphan_scratch_skips_named_j_tables(ch_client, orch_session, bg_worker, simple_job, failed_task_factory):
    """j_<id>_<name> tables are NOT this method's concern — leaves them for cleanup_failed_task or cleanup_at_job_completion."""
    failed = failed_task_factory(job_id=simple_job.id)
    name = f"j_{simple_job.id}_named"
    await ch_client.command(f"CREATE TABLE {name} (x Int64) ENGINE = Memory")
    orch_session.add(TableRegistry(table_name=name, job_id=simple_job.id, task_id=failed.id, preserved=False))
    await orch_session.commit()

    await bg_worker._cleanup_orphan_scratch_tables()

    assert (await ch_client.query(f"EXISTS TABLE {name}")).first_row[0] == 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_cleanup_orphan_scratch.py -x --no-cov -q
```

- [ ] **Step 3: Implement**

```python
_LIVE_TASK_STATUSES = (TaskStatus.PENDING, TaskStatus.CLAIMED, TaskStatus.RUNNING)


async def _cleanup_orphan_scratch_tables(self) -> None:
    """Drop unpinned t_* CH tables whose owning task is dead.

    Only touches names starting with `t_` — j_<id>_* tables are handled
    by `_cleanup_failed_task_tables` (per-task) or
    `_cleanup_at_job_completion` (per-job).
    """
    async with self._db.session() as session:
        rows = (
            await session.exec(
                select(TableRegistry).where(TableRegistry.table_name.like("t_%"))
            )
        ).all()
        if not rows:
            return

        task_ids = {row.task_id for row in rows}
        live_ids = set(
            (
                await session.exec(
                    select(Task.id).where(
                        Task.id.in_(task_ids),
                        Task.status.in_(_LIVE_TASK_STATUSES),
                    )
                )
            ).all()
        )

        for row in rows:
            if row.task_id in live_ids:
                continue
            pinned = (
                await session.exec(
                    select(TablePinRef).where(TablePinRef.table_name == row.table_name)
                )
            ).first()
            if pinned is not None:
                continue
            try:
                await self._ch.command(f"DROP TABLE IF EXISTS {row.table_name}")
            except Exception as exc:
                self._logger.warning("Orphan scratch DROP failed: %s", exc)
            else:
                await session.delete(row)
        await session.commit()
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_cleanup_orphan_scratch.py -x --no-cov -q
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_cleanup_orphan_scratch.py
git commit -m "feature: BackgroundWorker._cleanup_orphan_scratch_tables"
```

---

## Task 5: Extend `_cleanup_dead_workers()` to release name locks

**Files:**
- Modify: `aaiclick/orchestration/background/background_worker.py`
- Update: existing tests for `_cleanup_dead_workers`

- [ ] **Step 1: Find existing `_cleanup_dead_workers`**

```bash
grep -n "_cleanup_dead_workers\|cleanup_dead_workers" /home/user/aaiclick/aaiclick/orchestration/background/background_worker.py
```

- [ ] **Step 2: Write the test**

In an appropriate test file (likely `test_cleanup.py` or new `test_cleanup_dead_workers.py`):

```python
async def test_dead_worker_sweep_releases_name_locks(orch_session, bg_worker, simple_job):
    from aaiclick.orchestration.lifecycle.db_lifecycle import (
        TaskNameLock,
        acquire_task_name_lock,
    )
    # Set up a dead task with a held lock.
    dead_task = ...  # use existing test factory; status=FAILED
    await acquire_task_name_lock(orch_session, job_id=simple_job.id, name="x", task_id=dead_task.id)
    await orch_session.commit()

    await bg_worker._cleanup_dead_workers()

    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert rows == []
```

- [ ] **Step 3: Modify `_cleanup_dead_workers`**

Add a call at the end:

```python
async def _cleanup_dead_workers(self) -> None:
    # ... existing logic to mark tasks PENDING_CLEANUP ...

    # NEW: release any task_name_locks held by dead tasks.
    async with self._db.session() as session:
        await release_task_name_locks_for_dead_tasks(session)
        await session.commit()
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/background/test_cleanup.py -x --no-cov -q
```

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/background/background_worker.py aaiclick/orchestration/background/test_cleanup.py
git commit -m "feature: dead-worker sweep releases task_name_locks"
```

---

## Task 6: End-to-end integration test (preserve + pin together)

**Files:**
- Append to: `aaiclick/orchestration/test_object_lifecycle_e2e.py`

- [ ] **Step 1: Write the integration test**

```python
async def test_preserve_and_pin_together(ch_client, orch_session, bg_worker, registered_factory):
    """A two-task chain that uses BOTH preserve and pin.

    Task A: creates `training_set` (preserved); returns Object referencing
            an anonymous t_* table consumed by B.
    Task B: reads training_set, reads the Object from A.

    After the job completes:
    - training_set is dropped by _cleanup_at_job_completion.
    - Object output is dropped by _cleanup_at_job_completion.
    """
    registered = registered_factory(name="e2e_test", preserve=["training_set"])
    job = await create_job(registered_name="e2e_test")
    # ... drive the run via whatever orchestration helper exists ...
    # ... wait for job COMPLETED ...

    # Both tables should be gone.
    assert (await ch_client.query(f"EXISTS TABLE j_{job.id}_training_set")).first_row[0] == 0
```

This test depends on a job-runner helper that may or may not exist as a fixture. If no such helper is available, **leave a SKIP marker with a clear comment**: `pytest.skip("requires job-runner helper not yet available; verify manually with example script")`. Don't fake-drive the worker loop here — that's a separate scaffolding task.

- [ ] **Step 2: Run it (or skip)**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_object_lifecycle_e2e.py -x --no-cov -q -k "preserve_and_pin"
```

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/test_object_lifecycle_e2e.py
git commit -m "feature: e2e test for preserve + pin co-existing in a job"
```

---

## Task 7: Phase 5 sanity check

- [ ] **Step 1: Run all orchestration tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS.

- [ ] **Step 2: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-gwqt4
```

---

# Done When

- `_cleanup_at_job_completion()` drops every CH table tied to a completed job and clears SQL bookkeeping.
- `try_complete_job()` calls it.
- `_cleanup_failed_task_tables()` drops only non-preserved + non-pinned tables for PENDING_CLEANUP tasks.
- `_cleanup_orphan_scratch_tables()` sweeps `t_*` tables whose owning task is dead and which aren't pinned.
- `_cleanup_dead_workers()` releases stale `task_name_locks`.
- Test suite is green.
