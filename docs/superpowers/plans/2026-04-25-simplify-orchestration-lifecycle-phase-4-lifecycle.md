Phase 4 — Lifecycle Refactor (the core of the change)
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Replace the body of `task_scope()` with a new `TaskLifecycleHandler` (a `LocalLifecycleHandler` subclass) that:

1. Tracks every CH table the task touches with its `(preserved, pinned)` flags (inherited from `LocalLifecycleHandler`).
2. Owns the SQL-side writes the old `OrchLifecycleHandler` did — `table_registry` rows including `schema_doc`, `pin_refs`, name-lock acquisition. **This is non-negotiable**: `_get_table_schema` reads `table_registry.schema_doc` (added in commit `82aef62`'s migration `161cfe0f1117`); roughly 1000 data tests route through `Object.data()` and will `LookupError` if the new handler doesn't write the row.
3. Exposes `current_job_id()` so `create_object_from_value(scope="job")` keeps building `j_<job_id>_<name>` table names.
4. On successful exit, drops unpinned + unpreserved tables **through the inherited `AsyncTableWorker` queue** (NOT via parallel `asyncio.gather(*ch_client.command(DROP...))` — chdb sessions aren't reentrant).
5. On failure exit, drops only unpinned `t_*` tables; leaves all `j_<id>_*` for the BackgroundWorker.
6. Acquires `task_name_locks` for non-preserved named tables on creation.
7. Releases the locks (success or failure) before returning.

The old `OrchLifecycleHandler` stays in the codebase through this phase so existing behavior keeps working until the cutover at the end of Phase 4 Task 5. After this phase the new implementation is the active one; Phase 6 deletes the dead `OrchLifecycleHandler` class.

> **Critical invariant for this phase:** `aaiclick/data/conftest.py::ctx` enters `orch_context() + task_scope()` because the data suite depends on the active handler writing `table_registry.schema_doc`. After Phase 4 Task 2's task_scope rewrite, the data fixture must still produce a handler with the same write semantics. Run `pytest aaiclick/data/ -x` at the end of Phase 4 — green is the acceptance gate.

---

## Task 1: Extend `LocalLifecycleHandler` with table-flag tracking

**Files:**
- Modify: `aaiclick/data/data_context/lifecycle.py`
- Modify: `aaiclick/data/data_context/test_lifecycle.py` (or create if absent)

- [ ] **Step 1: Write failing tests**

Create or append to `aaiclick/data/data_context/test_lifecycle.py`:

```python
"""Tests for LocalLifecycleHandler table-flag tracking.

The orchestration task_scope() relies on iter_tracked_tables() to decide
which tables to inline-drop on exit and which to leave for the
BackgroundWorker.
"""

import pytest

from aaiclick.data.data_context.lifecycle import LocalLifecycleHandler
from aaiclick.data.data_context.ch_client import ChClient


async def test_track_table_records_default_flags(ch_client_local: ChClient):
    handler = LocalLifecycleHandler(ch_client_local)
    async with handler:
        handler.track_table("t_123")
        tracked = list(handler.iter_tracked_tables())
        assert len(tracked) == 1
        assert tracked[0].name == "t_123"
        assert tracked[0].preserved is False
        assert tracked[0].pinned is False


async def test_track_table_with_preserved_flag(ch_client_local):
    handler = LocalLifecycleHandler(ch_client_local)
    async with handler:
        handler.track_table("j_42_training_set", preserved=True)
        tracked = list(handler.iter_tracked_tables())
        assert tracked[0].preserved is True


async def test_mark_pinned_after_track(ch_client_local):
    handler = LocalLifecycleHandler(ch_client_local)
    async with handler:
        handler.track_table("t_999")
        handler.mark_pinned("t_999")
        tracked = list(handler.iter_tracked_tables())
        assert tracked[0].pinned is True


async def test_mark_pinned_unknown_table_is_silent(ch_client_local):
    """Pin can be set by the serializer for tables not registered in this
    handler — be tolerant."""
    handler = LocalLifecycleHandler(ch_client_local)
    async with handler:
        handler.mark_pinned("t_unknown")
        # No exception, nothing tracked.
        assert list(handler.iter_tracked_tables()) == []
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/data/data_context/test_lifecycle.py -x --no-cov -q
```

Expected: AttributeError on `track_table`.

- [ ] **Step 3: Extend `LocalLifecycleHandler`**

In `aaiclick/data/data_context/lifecycle.py`, add a `NamedTuple` and the methods:

```python
from typing import NamedTuple


class TrackedTable(NamedTuple):
    name: str
    preserved: bool
    pinned: bool


# Add to LifecycleHandler base class as a no-op default:

class LifecycleHandler(ABC):
    # ... existing methods ...

    def track_table(self, table_name: str, *, preserved: bool = False) -> None:
        """Record that this handler's lifetime owns ``table_name``. Default no-op."""

    def mark_pinned(self, table_name: str) -> None:
        """Flag a tracked table as pinned (consumer-bound). Default no-op."""

    def iter_tracked_tables(self):  # -> Iterable[TrackedTable]
        """Yield ``(name, preserved, pinned)`` for each tracked table. Default empty."""
        return iter(())
```

In `LocalLifecycleHandler`, replace the existing class body's tail with:

```python
class LocalLifecycleHandler(LifecycleHandler):
    def __init__(self, ch_client: ChClient):
        self._worker = AsyncTableWorker(ch_client)
        self._tracked: dict[str, TrackedTable] = {}

    # ... existing start/stop/decref/flush/claim ...

    def incref(self, table_name: str) -> None:
        # Auto-track on incref so every existing data_context.create_object
        # caller (and any other incref site) automatically participates in
        # task_scope's inline-DROP decision. Without this we would have to
        # enumerate every incref call site and add a parallel track_table().
        self._worker.incref(table_name)
        self.track_table(table_name)

    def track_table(self, table_name: str, *, preserved: bool = False) -> None:
        existing = self._tracked.get(table_name)
        if existing is None:
            self._tracked[table_name] = TrackedTable(table_name, preserved, False)
        elif preserved and not existing.preserved:
            self._tracked[table_name] = existing._replace(preserved=True)

    def mark_pinned(self, table_name: str) -> None:
        existing = self._tracked.get(table_name)
        if existing is None:
            return
        if not existing.pinned:
            self._tracked[table_name] = existing._replace(pinned=True)

    def iter_tracked_tables(self):
        return iter(list(self._tracked.values()))
```

Add one extra test asserting `incref` auto-tracks:

```python
async def test_incref_auto_tracks_table(ch_client_local: ChClient):
    handler = LocalLifecycleHandler(ch_client_local)
    async with handler:
        handler.incref("t_auto")
        tracked = list(handler.iter_tracked_tables())
        assert len(tracked) == 1
        assert tracked[0].name == "t_auto"
```

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/data/data_context/test_lifecycle.py -x --no-cov -q
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/data_context/lifecycle.py aaiclick/data/data_context/test_lifecycle.py
git commit -m "$(cat <<'EOF'
feature: LocalLifecycleHandler tracks (preserved, pinned) per table

Adds track_table / mark_pinned / iter_tracked_tables on the base
LifecycleHandler (no-ops) and LocalLifecycleHandler (real). The
orchestration task_scope() reads these flags on exit to decide
inline-drop vs leave-for-backworker.
EOF
)"
```

---

## Task 1.5: Build `TaskLifecycleHandler`

**Files:**
- Create: `aaiclick/orchestration/lifecycle/task_lifecycle.py`
- Create: `aaiclick/orchestration/lifecycle/test_task_lifecycle.py`

**Why this exists:** `LocalLifecycleHandler` knows nothing about SQL, `task_id`, `job_id`, `run_id`, `table_registry`, or `pin_refs`. The old `OrchLifecycleHandler` did all of that. We need a subclass that re-adds those responsibilities so today's data layer (`_get_table_schema`, `create_object_from_value(scope="job")`, the `ctx` test fixture) keeps working when `task_scope` swaps handlers in Task 2.

- [ ] **Step 1: Read the existing OrchLifecycleHandler**

```bash
sed -n '48,260p' /home/user/aaiclick/aaiclick/orchestration/orch_context.py
```

Note which methods do SQL writes: `register_table` (via `OPLOG_TABLE` queue → `_write_table_registry_row`), `pin` (via `OPLOG_PIN`), `unpin`, `oplog_record`. These are the methods the new class inherits-and-replaces.

- [ ] **Step 2: Write failing tests**

Create `aaiclick/orchestration/lifecycle/test_task_lifecycle.py`:

```python
"""Tests for TaskLifecycleHandler — the orch-side LocalLifecycleHandler.

The data test fixture in aaiclick/data/conftest.py depends on this:
register_table() must write a table_registry row including schema_doc
so _get_table_schema can read it back during Object.data().
"""

from sqlmodel import select

from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry, TablePinRef
from aaiclick.orchestration.lifecycle.task_lifecycle import TaskLifecycleHandler


async def test_register_table_writes_registry_row(ch_client, orch_session):
    handler = TaskLifecycleHandler(task_id=1, job_id=2, run_id=3, ch_client=ch_client)
    async with handler:
        handler.register_table("t_x", schema_doc='{"columns":[]}')
        await handler.flush()  # synchronous-flush the registry write

    row = (await orch_session.exec(
        select(TableRegistry).where(TableRegistry.table_name == "t_x")
    )).one()
    assert row.job_id == 2
    assert row.task_id == 1
    assert row.schema_doc == '{"columns":[]}'


async def test_current_job_id_returns_constructor_value(ch_client):
    handler = TaskLifecycleHandler(task_id=1, job_id=42, run_id=3, ch_client=ch_client)
    assert handler.current_job_id() == 42


async def test_pin_writes_pin_ref_and_marks_pinned(ch_client, orch_session, simple_dependency_chain):
    """pin() fans out to TablePinRef rows AND flips the local mark_pinned flag."""
    producer_id, consumer_id = simple_dependency_chain
    handler = TaskLifecycleHandler(task_id=producer_id, job_id=1, run_id=1, ch_client=ch_client)
    async with handler:
        handler.incref("t_pinned")          # auto-tracks
        handler.pin("t_pinned")
        await handler.flush()
        tracked = {t.name: t for t in handler.iter_tracked_tables()}
        assert tracked["t_pinned"].pinned is True

    pin_rows = (await orch_session.exec(
        select(TablePinRef).where(TablePinRef.table_name == "t_pinned")
    )).all()
    assert {r.task_id for r in pin_rows} == {consumer_id}
```

`simple_dependency_chain` is a fixture that returns `(producer_task_id, consumer_task_id)` with a row in `dependencies` linking them. If it doesn't exist, add a minimal version to `aaiclick/orchestration/conftest.py`.

- [ ] **Step 3: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/lifecycle/test_task_lifecycle.py -x --no-cov -q
```

Expected: ImportError on `task_lifecycle`.

- [ ] **Step 4: Implement `TaskLifecycleHandler`**

```python
# aaiclick/orchestration/lifecycle/task_lifecycle.py
"""Per-task lifecycle handler.

Subclass of LocalLifecycleHandler that adds the SQL-side writes that
OrchLifecycleHandler used to do — table_registry rows (with schema_doc),
table_pin_refs fan-out, task_name_locks acquisition / release. The
inherited LocalLifecycleHandler queue handles the actual DROP TABLE
calls so chdb sessions are not entered concurrently.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import text

from aaiclick.data.data_context.ch_client import ChClient
from aaiclick.data.data_context.lifecycle import LocalLifecycleHandler
from aaiclick.orchestration.sql_context import get_sql_session

logger = logging.getLogger(__name__)


class TaskLifecycleHandler(LocalLifecycleHandler):
    def __init__(self, task_id: int, job_id: int, run_id: int, ch_client: ChClient):
        super().__init__(ch_client)
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id

    def current_job_id(self) -> int:
        return self._job_id

    def register_table(self, table_name: str, schema_doc: str | None = None) -> None:
        # Synchronous INSERT (matches the post-fix path in 82aef62: the
        # registry write is idempotent ON CONFLICT DO NOTHING and not
        # ordered against incref/decref, so callers that immediately read
        # back schema_doc don't have to flush the lifecycle queue first).
        # Track locally too, so __aexit__ can decide what to inline-DROP.
        self.track_table(table_name)
        # The actual write is async; schedule it on the running loop.
        # Use a fire-and-forget task with logged failure.
        import asyncio  # local import: tight to this method
        loop = asyncio.get_running_loop()
        loop.create_task(self._write_registry_row(table_name, schema_doc))

    async def _write_registry_row(self, table_name: str, schema_doc: str | None) -> None:
        now = datetime.utcnow()
        try:
            async with get_sql_session() as session:
                await session.execute(
                    text(
                        "INSERT INTO table_registry "
                        "(table_name, job_id, task_id, created_at, preserved, schema_doc) "
                        "VALUES (:table_name, :job_id, :task_id, :created_at, :preserved, :schema_doc) "
                        "ON CONFLICT (table_name) DO NOTHING"
                    ),
                    {
                        "table_name": table_name,
                        "job_id": self._job_id,
                        "task_id": self._task_id,
                        "created_at": now,
                        "preserved": False,
                        "schema_doc": schema_doc,
                    },
                )
                await session.commit()
        except Exception:
            logger.error("Failed to write table_registry for %s", table_name, exc_info=True)

    def pin(self, table_name: str) -> None:
        # Local flag for inline-DROP decision.
        self.mark_pinned(table_name)
        import asyncio
        loop = asyncio.get_running_loop()
        loop.create_task(self._write_pin_refs(table_name))

    async def _write_pin_refs(self, table_name: str) -> None:
        try:
            async with get_sql_session() as session:
                result = await session.execute(
                    text(
                        "SELECT next_id FROM dependencies "
                        "WHERE previous_id = :task_id "
                        "AND previous_type = 'task' AND next_type = 'task'"
                    ),
                    {"task_id": self._task_id},
                )
                consumer_ids = [row[0] for row in result.fetchall()]
                for cid in consumer_ids:
                    await session.execute(
                        text(
                            "INSERT INTO table_pin_refs (table_name, task_id) "
                            "VALUES (:table_name, :task_id) "
                            "ON CONFLICT (table_name, task_id) DO NOTHING"
                        ),
                        {"table_name": table_name, "task_id": cid},
                    )
                await session.commit()
        except Exception:
            logger.error("Failed to write pin_refs for %s", table_name, exc_info=True)

    def unpin(self, table_name: str) -> None:
        import asyncio
        loop = asyncio.get_running_loop()
        loop.create_task(self._delete_pin_ref(table_name))

    async def _delete_pin_ref(self, table_name: str) -> None:
        try:
            async with get_sql_session() as session:
                await session.execute(
                    text(
                        "DELETE FROM table_pin_refs "
                        "WHERE table_name = :table_name AND task_id = :task_id"
                    ),
                    {"table_name": table_name, "task_id": self._task_id},
                )
                await session.commit()
        except Exception:
            logger.error("Failed to delete pin_ref for %s", table_name, exc_info=True)

    async def flush(self) -> None:
        # Flush both the inherited AsyncTableWorker queue AND any pending
        # SQL writes scheduled via create_task above. Tests that immediately
        # read back state need this barrier.
        await super().flush()
        # Yield once so create_task'd coroutines run to completion before
        # the test's next await reads SQL.
        import asyncio
        await asyncio.sleep(0)
```

> Per `CLAUDE.md`, lift the `import asyncio` and `from sqlalchemy import text` to the top of the file in your final commit. The skeleton above keeps them inline only to mark which methods need the import; pre-commit will catch the rest.

Open question for the implementer: oplog writes (`oplog_record`) currently flow through the `OrchLifecycleHandler` queue. They have nothing to do with table lifecycle, but they *do* live on the same handler today. **Decide before writing the code** whether to (a) keep oplog on `TaskLifecycleHandler` as a parallel concern, (b) split it into a separate `OplogRecorder` injected into `task_scope`. Recommend (a) for now (smaller blast radius); revisit in a follow-up. Either way, port the existing `oplog_record` / `_write_oplog_row` from `OrchLifecycleHandler`.

- [ ] **Step 5: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/lifecycle/test_task_lifecycle.py -x --no-cov -q
```

Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/lifecycle/task_lifecycle.py aaiclick/orchestration/lifecycle/test_task_lifecycle.py
git commit -m "$(cat <<'EOF'
feature: TaskLifecycleHandler — orch-aware LocalLifecycleHandler subclass

Owns the SQL-side writes the soon-to-be-deleted OrchLifecycleHandler
did: table_registry rows including schema_doc (read by
_get_table_schema), table_pin_refs fan-out, current_job_id() for
create_object_from_value(scope='job'). Drops still flow through the
inherited AsyncTableWorker queue so chdb sessions stay serial.
EOF
)"
```

---

## Task 2: New `task_scope()` — success path with inline DROP

**Files:**
- Modify: `aaiclick/orchestration/orch_context.py` — replace `task_scope()` body.
- Create: `aaiclick/orchestration/test_task_scope_lifecycle.py`

> **Caution:** This is the largest single change. The current `task_scope()` uses `OrchLifecycleHandler`, which talks to `table_run_refs` (deleted by the Phase 1 migration). On a fresh DB the old code will already be broken — the migration is applied. So we must replace the body in this task, not later.

- [ ] **Step 1: Write the failing success-path test**

Create `aaiclick/orchestration/test_task_scope_lifecycle.py`:

```python
"""Tests for task_scope success path.

After a task body finishes without raising, task_scope.__aexit__:
- DROPs every tracked t_* table (unless pinned).
- DROPs every tracked j_<id>_<name> table that is NOT preserved.
- LEAVES preserved tables — BackgroundWorker drops them at job completion.
- LEAVES pinned tables — BackgroundWorker drops them when pin_refs == 0.
- Releases task_name_locks for this task.
"""

import pytest

from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.lifecycle.db_lifecycle import TaskNameLock
from sqlmodel import select


async def test_task_scope_drops_anonymous_tables_inline(ch_client, orch_session, simple_job):
    # Arrange: enter task_scope, create a t_* table, exit cleanly.
    async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
        await ch_client.command("CREATE TABLE t_777 (x Int64) ENGINE = Memory")
        # Manually track since ctx.table() integration comes in Task 4.
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        get_data_lifecycle().track_table("t_777")

    # Assert: table is gone.
    rows = await ch_client.query("EXISTS TABLE t_777")
    assert rows.first_row[0] == 0


async def test_task_scope_keeps_preserved_table(ch_client, orch_session, preserved_job):
    """Preserved names are NOT inline-dropped."""
    async with task_scope(task_id=1, job_id=preserved_job.id, run_id=1):
        table = f"j_{preserved_job.id}_training_set"
        await ch_client.command(f"CREATE TABLE {table} (x Int64) ENGINE = Memory")
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        get_data_lifecycle().track_table(table, preserved=True)

    # Assert: still there. BackgroundWorker drops it later.
    rows = await ch_client.query(f"EXISTS TABLE j_{preserved_job.id}_training_set")
    assert rows.first_row[0] == 1


async def test_task_scope_keeps_pinned_table(ch_client, orch_session, simple_job):
    """Pinned tables are NOT inline-dropped (consumer task still needs them)."""
    async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
        await ch_client.command("CREATE TABLE t_555 (x Int64) ENGINE = Memory")
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        handler = get_data_lifecycle()
        handler.track_table("t_555")
        handler.mark_pinned("t_555")

    rows = await ch_client.query("EXISTS TABLE t_555")
    assert rows.first_row[0] == 1


async def test_task_scope_drops_non_preserved_named_table(ch_client, orch_session, simple_job):
    """A j_<id>_<name> table whose name is NOT in preserve is inline-dropped."""
    table = f"j_{simple_job.id}_scratch"
    async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
        await ch_client.command(f"CREATE TABLE {table} (x Int64) ENGINE = Memory")
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        get_data_lifecycle().track_table(table, preserved=False)

    rows = await ch_client.query(f"EXISTS TABLE {table}")
    assert rows.first_row[0] == 0


async def test_task_scope_releases_name_locks_on_success(orch_session, simple_job):
    """Locks held during task body are released on clean exit."""
    from aaiclick.orchestration.lifecycle.db_lifecycle import acquire_task_name_lock

    async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
        await acquire_task_name_lock(orch_session, job_id=simple_job.id, name="foo", task_id=1)

    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert rows == []
```

The fixtures `simple_job`, `preserved_job`, `ch_client`, `orch_session` need to exist in `aaiclick/orchestration/conftest.py`. If they don't, add minimal versions before the test runs:

```python
@pytest.fixture
async def simple_job(orch_session):
    from aaiclick.orchestration.factories import create_job
    return await create_job(registered_name=None, preserve=None, parameters={})

@pytest.fixture
async def preserved_job(orch_session):
    from aaiclick.orchestration.factories import create_job
    return await create_job(registered_name=None, preserve=["training_set"], parameters={})
```

If `create_job` requires `registered_name` to be a real RegisteredJob, adapt the fixture to register an unnamed one first.

- [ ] **Step 2: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_task_scope_lifecycle.py -x --no-cov -q
```

Expected: failures — either the new behavior is missing or the old `OrchLifecycleHandler` errors against the migrated schema.

- [ ] **Step 3: Rewrite `task_scope()`**

Read the current `task_scope()` in `aaiclick/orchestration/orch_context.py`. Replace the entire body. Note: imports go at the top of the file per `CLAUDE.md`; the skeleton below shows them inline only for clarity:

```python
@asynccontextmanager
async def task_scope(
    task_id: int,
    job_id: int,
    run_id: int,
) -> AsyncIterator[None]:
    """Per-task lifecycle scope nested inside ``orch_context``.

    Uses :class:`TaskLifecycleHandler` (a :class:`LocalLifecycleHandler`
    subclass that owns the SQL-side writes — table_registry,
    table_pin_refs, task_name_locks). On exit:

    - **Success:** route every tracked table where ``preserved=False``
      and ``pinned=False`` through the inherited ``decref`` path so the
      ``AsyncTableWorker`` queue serialises the DROP. Preserved + pinned
      tables survive for the BackgroundWorker.
    - **Failure:** drop only tracked ``t_*`` tables that are not pinned.
      Every ``j_<id>_*`` is left for the BackgroundWorker to handle at
      job completion.

    Releases all ``task_name_locks`` held by ``task_id`` regardless of outcome.
    """
    ch_client = get_ch_client()
    handler = TaskLifecycleHandler(
        task_id=task_id, job_id=job_id, run_id=run_id, ch_client=ch_client,
    )

    objects: dict[int, weakref.ref] = {}
    await init_oplog_tables(ch_client)
    await migrate_table_registry_to_sql(ch_client)

    await handler.start()
    lc_token = _lifecycle_var.set(handler)
    obj_token = _objects_var.set(objects)
    registry_token = _task_registry_var.set({})

    success = False
    try:
        yield
        success = True
    finally:
        _task_registry_var.reset(registry_token)

        # Stale-mark all weakref-tracked Objects. Decref is no longer
        # the drop trigger — track_table+iter_tracked_tables decides
        # what to drop. We still mark stale so callers using the object
        # post-exit get a clear error.
        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True

        try:
            tracked = list(handler.iter_tracked_tables())
            to_drop = [
                tt.name
                for tt in tracked
                if not tt.pinned
                and not tt.preserved
                and (success or tt.name.startswith("t_"))
            ]
            # Route through the AsyncTableWorker queue (inherited from
            # LocalLifecycleHandler). decref serialises drops; chdb's
            # non-reentrant Session is what makes this mandatory rather
            # than a stylistic choice. Do NOT replace this with
            # asyncio.gather(*ch_client.command(...)) — chdb will deadlock
            # or corrupt session state.
            for name in to_drop:
                handler.decref(name)
            await handler.flush()
        finally:
            await handler.stop()
            _lifecycle_var.reset(lc_token)
            _objects_var.reset(obj_token)

            # Release name locks regardless of outcome.
            async with get_sql_session() as session:
                await release_task_name_locks_for_task(session, task_id=task_id)
                await session.commit()
```

Confirm `decref` actually triggers the worker's drop loop in `LocalLifecycleHandler` (i.e. that refcount-zero on a never-incref'd table still produces a DROP). If not, expose a worker-routed `drop(name)` helper on `LocalLifecycleHandler` and call that here instead. The point is: every `DROP TABLE` goes through the existing serial queue, never `ch_client.command` directly.

Top-of-file imports to add (per `CLAUDE.md`): `from aaiclick.orchestration.lifecycle.task_lifecycle import TaskLifecycleHandler` and `from aaiclick.orchestration.lifecycle.db_lifecycle import release_task_name_locks_for_task`. `get_sql_session`, `asyncio`, and module-level `logger` are already imported in `orch_context.py`.

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_task_scope_lifecycle.py -x --no-cov -q
```

Expected: 5 passed. If existing tests now fail because they relied on `OrchLifecycleHandler` semantics, mark those for triage in Task 5 — they'll be cleaned up there.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/orch_context.py aaiclick/orchestration/test_task_scope_lifecycle.py aaiclick/orchestration/conftest.py
git commit -m "$(cat <<'EOF'
feature: task_scope success-path inline DROP via LocalLifecycleHandler

Replaces OrchLifecycleHandler with LocalLifecycleHandler for per-task
lifecycle. Success path drops unpinned + unpreserved tables inline.
Failure path drops only unpinned t_*. Name locks released on exit
regardless of outcome.
EOF
)"
```

---

## Task 3: Failure-path test

**Files:**
- Create: `aaiclick/orchestration/test_task_scope_failure.py`

- [ ] **Step 1: Write tests**

Create the file:

```python
"""Tests for task_scope failure path.

When the task body raises:
- Tracked t_* tables (unpinned) are dropped inline — they're orphan scratch.
- Tracked j_<id>_* tables are LEFT for the BackgroundWorker to clean at
  job completion.
- Pinned t_* tables are LEFT (consumers may still need them).
- task_name_locks are released so a retry can re-take the names.
"""

import pytest
from sqlmodel import select

from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.lifecycle.db_lifecycle import (
    TaskNameLock,
    acquire_task_name_lock,
)


async def test_failure_drops_anonymous_table(ch_client, orch_session, simple_job):
    with pytest.raises(RuntimeError):
        async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
            await ch_client.command("CREATE TABLE t_888 (x Int64) ENGINE = Memory")
            from aaiclick.data.data_context.lifecycle import get_data_lifecycle
            get_data_lifecycle().track_table("t_888")
            raise RuntimeError("boom")

    rows = await ch_client.query("EXISTS TABLE t_888")
    assert rows.first_row[0] == 0


async def test_failure_keeps_named_job_table(ch_client, orch_session, simple_job):
    table = f"j_{simple_job.id}_partial"
    with pytest.raises(RuntimeError):
        async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
            await ch_client.command(f"CREATE TABLE {table} (x Int64) ENGINE = Memory")
            from aaiclick.data.data_context.lifecycle import get_data_lifecycle
            get_data_lifecycle().track_table(table, preserved=False)
            raise RuntimeError("boom")

    rows = await ch_client.query(f"EXISTS TABLE {table}")
    assert rows.first_row[0] == 1


async def test_failure_keeps_pinned_anonymous(ch_client, orch_session, simple_job):
    with pytest.raises(RuntimeError):
        async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
            await ch_client.command("CREATE TABLE t_999 (x Int64) ENGINE = Memory")
            from aaiclick.data.data_context.lifecycle import get_data_lifecycle
            handler = get_data_lifecycle()
            handler.track_table("t_999")
            handler.mark_pinned("t_999")
            raise RuntimeError("boom")

    rows = await ch_client.query("EXISTS TABLE t_999")
    assert rows.first_row[0] == 1


async def test_failure_releases_name_locks(orch_session, simple_job):
    with pytest.raises(RuntimeError):
        async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
            await acquire_task_name_lock(
                orch_session, job_id=simple_job.id, name="foo", task_id=1
            )
            raise RuntimeError("boom")

    rows = (await orch_session.exec(select(TaskNameLock))).all()
    assert rows == []
```

- [ ] **Step 2: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_task_scope_failure.py -x --no-cov -q
```

Expected: 4 passed (the implementation in Task 2 already handles failure correctly).

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/test_task_scope_failure.py
git commit -m "feature: tests for task_scope failure path"
```

---

## Task 4: Hook `ctx.table(name)` into name lock + preserved-name registration

**Files:**
- Modify: the table-creation entry point used by tasks. Find it:
  ```bash
  grep -rn "def table\b\|create_object_from_value\|make_persistent_table_name" /home/user/aaiclick/aaiclick/data/ /home/user/aaiclick/aaiclick/orchestration/ | head
  ```
- Create: `aaiclick/orchestration/test_task_named_table_collision.py`

- [ ] **Step 1: Trace the table-creation path**

Read the file containing `def table` on `ctx`. Identify:
- Where the user-facing name (e.g. `"training_set"`) is converted to a physical name.
- Where the CH `CREATE TABLE` is issued.
- Whether the function is called from inside `task_scope` (it should be — find via callers).

- [ ] **Step 2: Write the collision test**

Create `aaiclick/orchestration/test_task_named_table_collision.py`:

```python
"""Test that two concurrent tasks taking the same non-preserved name collide."""

import asyncio
import pytest

from aaiclick.orchestration.lifecycle.db_lifecycle import TableNameCollision
from aaiclick.orchestration.orch_context import task_scope


async def test_concurrent_non_preserved_name_raises(ch_client, orch_session, simple_job):
    """Task B tries to create the same name while Task A still holds it."""

    async def task_a():
        async with task_scope(task_id=10, job_id=simple_job.id, run_id=1):
            ctx = ...  # actual ctx — adapt to whatever data_context exposes inside task_scope
            await ctx.table("scratch")  # acquires lock for name 'scratch'
            await asyncio.sleep(0.5)

    async def task_b():
        await asyncio.sleep(0.1)
        async with task_scope(task_id=20, job_id=simple_job.id, run_id=1):
            ctx = ...
            with pytest.raises(TableNameCollision):
                await ctx.table("scratch")

    await asyncio.gather(task_a(), task_b())


async def test_preserved_name_no_collision(ch_client, orch_session, preserved_job):
    """Preserved names skip the lock — concurrent CREATE IF NOT EXISTS is fine."""

    async def make():
        async with task_scope(task_id=task_id, job_id=preserved_job.id, run_id=1):
            ctx = ...
            await ctx.table("training_set")

    # Run two concurrently — neither should error.
    await asyncio.gather(make(), make())
```

The `ctx = ...` placeholder is intentional — fill it in with the actual context-getter once you've traced the table-creation path. Stop and ask if `ctx` is reachable only via `data_context()` (in which case the test needs nesting).

- [ ] **Step 3: Modify the table-creation function**

In whatever module owns `ctx.table(name)`:

1. Branch on whether the name is in the current job's `preserve` list (or if `preserve == "*"`):
   - **Preserved:** issue `CREATE TABLE IF NOT EXISTS j_<job_id>_<name> ...`, call `handler.track_table(physical_name, preserved=True)`. No lock.
   - **Non-preserved:** call `acquire_task_name_lock(session, job_id, name, task_id)`. If it raises `TableNameCollision`, propagate. Otherwise issue `CREATE TABLE j_<job_id>_<name> ...` (regular create — should fail if it exists, since lock guarantees we're the only owner). Call `handler.track_table(physical_name, preserved=False)`.
2. The `task_id` and `job_id` come from `task_scope()` ContextVars — search for an existing `_task_id_var` / `_job_id_var` in `orch_context.py` (or expose them if they don't exist).

- [ ] **Step 4: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_task_named_table_collision.py -x --no-cov -q
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/test_task_named_table_collision.py <table-ctor file>
git commit -m "$(cat <<'EOF'
feature: ctx.table(name) acquires name lock for non-preserved names

Preserved names use CREATE IF NOT EXISTS without a lock (idempotent).
Non-preserved names take the (job_id, name) row in task_name_locks;
collision raises TableNameCollision.
EOF
)"
```

---

## Task 5: Object output → `mark_pinned` integration

**Files:**
- Modify: the Object serializer that sets pin_refs.

- [ ] **Step 1: Find the pin-writer**

```bash
grep -rn "TablePinRef(" /home/user/aaiclick/aaiclick/orchestration/ /home/user/aaiclick/aaiclick/data/ | grep -v test_
```

Identify the function that writes a `TablePinRef` row. Capture the module path + function name — they're what the test in Step 2 imports.

- [ ] **Step 2: Write the test**

Append to `aaiclick/orchestration/test_object_lifecycle_e2e.py` (already exists). Replace `<MODULE>` and `<PIN_FN>` with the values from Step 1:

```python
async def test_object_output_table_marked_pinned_in_handler(ch_client, orch_session, simple_job):
    """When a task returns an Object, the underlying table must be marked
    pinned in the local handler so task_scope.__aexit__ leaves it alone."""

    async with task_scope(task_id=1, job_id=simple_job.id, run_id=1):
        await ch_client.command("CREATE TABLE t_obj (x Int64) ENGINE = Memory")
        from aaiclick.data.data_context.lifecycle import get_data_lifecycle
        handler = get_data_lifecycle()
        handler.track_table("t_obj")

        # Simulate the serializer call that pins the table for a consumer.
        from <MODULE> import <PIN_FN>
        await <PIN_FN>(
            session=orch_session,
            table="t_obj",
            consumer_task_id=2,
        )

        tracked = list(handler.iter_tracked_tables())
        assert any(t.name == "t_obj" and t.pinned for t in tracked)
```

- [ ] **Step 3: Run to confirm failure**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_object_lifecycle_e2e.py -x --no-cov -q -k "marked_pinned"
```

Expected: failure — pin function doesn't call `mark_pinned`.

- [ ] **Step 4: Modify the pin function**

In the function identified in Step 1, after the `TablePinRef` row is written:

```python
from aaiclick.data.data_context.lifecycle import get_data_lifecycle

handler = get_data_lifecycle()
if handler is not None:
    handler.mark_pinned(table)
```

- [ ] **Step 5: Run tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/test_object_lifecycle_e2e.py -x --no-cov -q
```

Expected: green.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/test_object_lifecycle_e2e.py <pin-function file>
git commit -m "feature: pin function marks table pinned in local lifecycle handler"
```

---

## Task 6: Phase 4 sanity check

- [ ] **Step 1: Run full orchestration tests**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS. Some old tests may have been broken by the rewrite — diagnose and fix in this step (or convert them to obsolete tests to be deleted in Phase 6).

- [ ] **Step 2: Run the data tests too**

```bash
cd /home/user/aaiclick && pytest aaiclick/data/ -x --no-cov -q
```

Expected: PASS.

- [ ] **Step 3: Push**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-aNOnA
```

---

# Done When

- `task_scope()` uses `TaskLifecycleHandler`. `OrchLifecycleHandler` is unused but not yet deleted.
- `TaskLifecycleHandler.register_table` writes `table_registry` rows including `schema_doc`. `current_job_id()` returns the constructor-supplied job_id.
- All inline DROPs route through the inherited `AsyncTableWorker` queue (no parallel `ch_client.command` calls).
- Success-path test green.
- Failure-path test green.
- Concurrent name-collision test green.
- Preserved-name idempotent-create test green.
- Object pin marks the table on the local handler AND writes `table_pin_refs` rows.
- **`pytest aaiclick/data/ -x --no-cov -q` is green** — the data fixture (`aaiclick/data/conftest.py::ctx`) still produces a handler that writes `schema_doc`, so `Object.data()` reads succeed.
- Full `pytest aaiclick/ -x` is green.
