# Change Signal Consumers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `cli_wait.wait_for_job` react to change signals instead of polling on a fixed interval, and consolidate the wait primitive it would otherwise duplicate.

**Architecture:** A new `cross_process` property on `SignalTransport` says whether a transport can carry signals committed by other processes. `wait_for_job` runs the transport's `feed` and waits on an `EventBus` subscription only when that is true; otherwise it keeps today's plain poll. Separately, the private `_wait_or_timeout` helper moves to a neutral top-level module and replaces four hand-rolled `try/except asyncio.TimeoutError` blocks.

**Tech Stack:** Python 3.11+, asyncio, SQLAlchemy/SQLModel, pytest + pytest-asyncio (auto mode), asyncpg (distributed extra only).

**Spec:** `docs/superpowers/specs/2026-09-15-change-signal-consumers-design.md`

## Global Constraints

- **All imports at the top of the file.** No imports inside functions, methods, or test functions. Three groups separated by blank lines: stdlib, external packages, current package.
- **No `__all__` in `__init__.py`.** Import the names; Python exports them.
- **No history comments.** Never write `# Removed: ...` or similar. Version control tracks history.
- **Async tests use no decorator.** `pytest-asyncio` is in auto mode — write `async def test_*()` directly, never `@pytest.mark.asyncio`.
- **Tests live beside the module under test.** `aaiclick/test_async_wait.py` tests `aaiclick/async_wait.py`. Flat module-level functions, no test classes.
- **`filterwarnings = ["error"]`** — any unhandled warning fails the test.
- **Prefer `Literal` over enums** for closed string sets.
- Run the full check before each commit: `python -m pytest aaiclick/ -x -q` plus `ruff check aaiclick/` and `ruff format --check aaiclick/`.

---

### Task 1: The `wait_or_timeout` primitive

**Files:**
- Create: `aaiclick/async_wait.py`
- Test: `aaiclick/test_async_wait.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `async def wait_or_timeout(event: asyncio.Event, timeout: float) -> bool` — `True` once `event` is set, `False` when `timeout` elapses first.

- [ ] **Step 1: Write the failing test**

Create `aaiclick/test_async_wait.py`:

```python
"""Tests for the shared "event set, or timeout?" wait primitive."""

import asyncio

from aaiclick.async_wait import wait_or_timeout


async def test_returns_true_when_event_already_set():
    event = asyncio.Event()
    event.set()
    assert await wait_or_timeout(event, 5.0) is True


async def test_returns_true_when_event_set_during_wait():
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, event.set)
    assert await wait_or_timeout(event, 5.0) is True


async def test_returns_false_when_timeout_elapses_first():
    assert await wait_or_timeout(asyncio.Event(), 0.01) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest aaiclick/test_async_wait.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'aaiclick.async_wait'`

- [ ] **Step 3: Write minimal implementation**

Create `aaiclick/async_wait.py`:

```python
"""Shared asyncio wait primitives.

Neutral module: the poll loops that use these live in
``orchestration/background``, ``orchestration/execution``,
``orchestration/events`` and the CLI, so the helper belongs to none of them.
"""

from __future__ import annotations

import asyncio


async def wait_or_timeout(event: asyncio.Event, timeout: float) -> bool:
    """True once ``event`` is set, False when ``timeout`` elapses first.

    Lets a poll loop express "sleep, but wake early on shutdown" as its
    ``while`` condition instead of a ``try`` / ``except TimeoutError`` block.
    """
    try:
        await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
        return False
    return True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest aaiclick/test_async_wait.py -q`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add aaiclick/async_wait.py aaiclick/test_async_wait.py
git commit -m "feat: add shared wait_or_timeout primitive"
```

---

### Task 2: Convert the five callers to `wait_or_timeout`

**Files:**
- Modify: `aaiclick/orchestration/events/postgres.py` — delete `_wait_or_timeout`, import the shared one
- Modify: `aaiclick/orchestration/background/background_worker.py` — `_cleanup_loop`
- Modify: `aaiclick/orchestration/logging.py` — `run`
- Modify: `aaiclick/orchestration/execution/execution_worker.py` — `_heartbeat_while_waiting`, `_watch_for_cancellation`

**Interfaces:**
- Consumes: `wait_or_timeout` from Task 1.
- Produces: no new names. Behaviour is unchanged at every site.

This task has no new tests — it is a pure refactor guarded by the existing suite. The four inline sites each lose a `try` / `except asyncio.TimeoutError`.

- [ ] **Step 1: Replace the helper in `postgres.py`**

Delete the `_wait_or_timeout` function (its docstring becomes the shared one's) and add the import. In the imports block, add to the current-package group:

```python
from aaiclick.async_wait import wait_or_timeout
```

Then rename both call sites — in `_keep_alive`:

```python
        while not await wait_or_timeout(stop, PING_INTERVAL):
            await conn.execute("SELECT 1")
```

and in `_back_off`:

```python
        stopped = await wait_or_timeout(stop, self._backoff)
```

- [ ] **Step 2: Convert `background_worker._cleanup_loop`**

Replace:

```python
    async def _cleanup_loop(self) -> None:
        while not self._shutdown.is_set():
            await self._do_cleanup()
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._poll_interval,
                )
            except asyncio.TimeoutError:
                pass
```

with:

```python
    async def _cleanup_loop(self) -> None:
        while not self._shutdown.is_set():
            await self._do_cleanup()
            await wait_or_timeout(self._shutdown, self._poll_interval)
```

Add `from aaiclick.async_wait import wait_or_timeout` to the imports. Leave the `import asyncio` line alone unless nothing else in the file uses it — check with `grep -n "asyncio\." aaiclick/orchestration/background/background_worker.py` before removing.

- [ ] **Step 3: Convert `logging.run`**

Replace:

```python
    async def run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=LOG_FLUSH_INTERVAL)
                return
            except asyncio.TimeoutError:
                pass
            await self.flush_pending()
```

with:

```python
    async def run(self) -> None:
        while not await wait_or_timeout(self._stop, LOG_FLUSH_INTERVAL):
            await self.flush_pending()
```

Add the import. Check `asyncio.` usage before touching `import asyncio`.

- [ ] **Step 4: Convert both `execution_worker` loops**

Replace `_heartbeat_while_waiting`'s body:

```python
    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            await heartbeat_fn(execution_worker_id)
```

with:

```python
    while not await wait_or_timeout(done, interval):
        await heartbeat_fn(execution_worker_id)
```

Replace `_watch_for_cancellation`'s body:

```python
    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=poll_interval)
            return
        except asyncio.TimeoutError:
            pass
        if await vehicle.poll_cancelled(task):
            cancelled.set()
            await vehicle.terminate(handle)
            return
```

with:

```python
    while not await wait_or_timeout(done, poll_interval):
        if await vehicle.poll_cancelled(task):
            cancelled.set()
            await vehicle.terminate(handle)
            return
```

Both drop the redundant leading `done.is_set()` check: `wait_or_timeout` on an already-set event returns `True` immediately, so the loop exits without calling the body. Add the import.

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest aaiclick/ -x -q`
Expected: PASS, same count as before this task. The execution-worker and background-worker suites are the ones that exercise these loops.

- [ ] **Step 6: Lint and commit**

```bash
ruff check aaiclick/ && ruff format --check aaiclick/
git add aaiclick/orchestration/events/postgres.py \
        aaiclick/orchestration/background/background_worker.py \
        aaiclick/orchestration/logging.py \
        aaiclick/orchestration/execution/execution_worker.py
git commit -m "refactor: use shared wait_or_timeout in the five poll loops"
```

---

### Task 3: The `cross_process` transport capability

**Files:**
- Modify: `aaiclick/orchestration/events/transport.py` — add to the `SignalTransport` protocol
- Modify: `aaiclick/orchestration/events/local.py` — `cross_process = False`
- Modify: `aaiclick/orchestration/events/postgres.py` — `cross_process = True`
- Modify: `aaiclick/orchestration/events/__init__.py` — export the three state constants
- Test: `aaiclick/orchestration/test_events.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `SignalTransport.cross_process -> bool`; `STATE_IDLE`, `STATE_LISTENING`, `STATE_RECONNECTING` importable from `aaiclick.orchestration.events`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_events.py`:

```python
def test_local_transport_is_not_cross_process():
    """chdb's file lock confines local-mode writers to one process, so a
    waiter in another process can never receive their signals."""
    assert LocalTransport().cross_process is False
```

Add `LocalTransport` to that file's imports if it is not already there:

```python
from aaiclick.orchestration.events.local import LocalTransport
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest aaiclick/orchestration/test_events.py::test_local_transport_is_not_cross_process -q`
Expected: FAIL — `AttributeError: 'LocalTransport' object has no attribute 'cross_process'`

- [ ] **Step 3: Add the property to the protocol**

In `transport.py`, inside `class SignalTransport(Protocol)`, directly after the `state` property:

```python
    @property
    def cross_process(self) -> bool:
        """Whether ``feed`` can deliver signals committed by *other* processes.

        ``state`` answers whether this transport's link is up, which is not
        the same question: a transport can be healthily "listening" and still
        only ever see its own process's commits. Only a waiter in a separate
        process from the writers needs this distinction, and for it a false
        value means signals will never arrive — poll instead.
        """
        ...
```

- [ ] **Step 4: Implement on both transports**

In `local.py`, inside `class LocalTransport`, directly after the `state` property:

```python
    @property
    def cross_process(self) -> bool:
        # chdb's file lock allows one process, so every writer is this one.
        return False
```

In `postgres.py`, inside `class PostgresTransport`, directly after the `state` property:

```python
    @property
    def cross_process(self) -> bool:
        # Postgres fans each NOTIFY out to every LISTEN connection, whichever
        # process holds it.
        return True
```

- [ ] **Step 5: Export the state constants**

In `events/__init__.py`, widen the state import:

```python
from .state import STATE_IDLE, STATE_LISTENING, STATE_RECONNECTING
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest aaiclick/orchestration/test_events.py -q`
Expected: PASS, including the new test.

- [ ] **Step 7: Commit**

```bash
ruff check aaiclick/ && ruff format --check aaiclick/
git add aaiclick/orchestration/events/
git commit -m "feat: add cross_process capability to SignalTransport"
```

---

### Task 4: Event-driven `wait_for_job`

**Files:**
- Modify: `aaiclick/cli_wait.py`
- Test: `aaiclick/test_cli_wait.py`

**Interfaces:**
- Consumes: `cross_process` from Task 3; `get_transport`, `signal_transport`, `event_bus`, `EventBus`, `SignalTransport`, `STATE_LISTENING` from `aaiclick.orchestration.events`.
- Produces: `wait_for_job(..., signal_poll_interval: float = DEFAULT_SIGNAL_POLL_INTERVAL)`; module constant `DEFAULT_SIGNAL_POLL_INTERVAL = 10.0`; private `_wake_interval(transport, poll_interval, signal_poll_interval) -> float`.

!!! warning "The interval trap"
    `LocalTransport.state` is `STATE_LISTENING` unconditionally. Selecting the
    slow interval on `state` alone makes local-mode `job wait` ten times
    *less* responsive than today. `_wake_interval` must require
    `cross_process` **and** `STATE_LISTENING`. The `local-always-fast`
    parametrized case below is the regression guard.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/test_cli_wait.py`:

```python
class _SignallingTransport:
    """Cross-process transport whose feed wakes every subscriber on a timer."""

    cross_process = True
    state = STATE_LISTENING

    def before_commit(self, session) -> None:
        return None

    def after_commit(self, session) -> None:
        return None

    async def feed(self, bus, *, stop) -> None:
        while not stop.is_set():
            await asyncio.sleep(0.01)
            bus.publish()


class _SilentTransport:
    """Cross-process transport that never publishes; records that feed ran."""

    cross_process = True
    state = STATE_LISTENING

    def __init__(self) -> None:
        self.feed_started = False
        self.feed_finished = False

    def before_commit(self, session) -> None:
        return None

    def after_commit(self, session) -> None:
        return None

    async def feed(self, bus, *, stop) -> None:
        self.feed_started = True
        try:
            await stop.wait()
        finally:
            self.feed_finished = True


class _LocalLikeTransport(_SilentTransport):
    """Mirrors LocalTransport: listening, but signals never cross processes."""

    cross_process = False


@pytest.mark.parametrize(
    "cross_process, state, expected",
    [
        pytest.param(True, STATE_LISTENING, 10.0, id="listening-uses-slow-poll"),
        pytest.param(True, STATE_RECONNECTING, 1.0, id="reconnecting-uses-fast-poll"),
        pytest.param(True, STATE_IDLE, 1.0, id="idle-uses-fast-poll"),
        # LocalTransport.state is always LISTENING, so cross_process is the
        # only thing standing between local mode and a 10s wait.
        pytest.param(False, STATE_LISTENING, 1.0, id="local-always-fast"),
    ],
)
def test_wake_interval(cross_process, state, expected):
    transport = SimpleNamespace(cross_process=cross_process, state=state)
    assert _wake_interval(transport, poll_interval=1.0, signal_poll_interval=10.0) == expected


async def test_signal_advances_loop_without_waiting_slow_poll():
    """Both intervals are 30s, so only a signal can finish this inside 2s."""
    with _patch_stats(_stats("RUNNING", {"RUNNING": 1}), _stats("COMPLETED", {"COMPLETED": 1})):
        with signal_transport(_SignallingTransport()):
            result = await asyncio.wait_for(
                wait_for_job(1, timeout=30.0, poll_interval=30.0, signal_poll_interval=30.0),
                timeout=2.0,
            )
    assert result.job_status == "COMPLETED"


async def test_signal_published_during_fetch_is_not_lost():
    """The subscription opens before the first fetch, so a commit landing
    mid-fetch is queued in the depth-1 mailbox rather than dropped."""
    seen: list[int] = []

    async def _stats_publishing_once(_ref):
        seen.append(1)
        if len(seen) == 1:
            # A commit lands while this very fetch is in flight.
            get_event_bus().publish()
            return _stats("RUNNING", {"RUNNING": 1})
        return _stats("COMPLETED", {"COMPLETED": 1})

    with patch.multiple(
        "aaiclick.cli_wait.internal_api",
        job_stats=AsyncMock(side_effect=_stats_publishing_once),
        get_job=AsyncMock(return_value=SimpleNamespace(id=1)),
    ):
        with signal_transport(_SilentTransport()):
            result = await asyncio.wait_for(
                wait_for_job(1, timeout=30.0, poll_interval=30.0, signal_poll_interval=30.0),
                timeout=2.0,
            )
    assert result.job_status == "COMPLETED"


async def test_feed_is_torn_down_when_the_wait_times_out():
    """The Postgres LISTEN connection must not outlive a failed wait.

    The intervals are small but non-zero so the loop really suspends: awaiting
    an ``AsyncMock`` never yields to the event loop, so a zero-timeout wait
    would raise before the feed task was ever scheduled.
    """
    transport = _SilentTransport()
    with _patch_stats(_stats("RUNNING", {"RUNNING": 1})):
        with signal_transport(transport):
            with pytest.raises(JobWaitTimeout):
                await wait_for_job(1, timeout=0.05, poll_interval=0.01, signal_poll_interval=0.01)
    assert transport.feed_started is True
    assert transport.feed_finished is True


async def test_no_feed_started_when_transport_is_not_cross_process():
    transport = _LocalLikeTransport()
    with _patch_stats(_stats("COMPLETED", {"COMPLETED": 1})):
        with signal_transport(transport):
            await wait_for_job(1, timeout=5.0, poll_interval=0)
    assert transport.feed_started is False
```

Extend that file's imports — stdlib first, then the package group:

```python
import asyncio

from aaiclick.cli_wait import JobWaitTimeout, _wake_interval, wait_for_job
from aaiclick.orchestration.events import (
    STATE_IDLE,
    STATE_LISTENING,
    STATE_RECONNECTING,
    get_event_bus,
    signal_transport,
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest aaiclick/test_cli_wait.py -q`
Expected: FAIL — `ImportError: cannot import name '_wake_interval' from 'aaiclick.cli_wait'`

- [ ] **Step 3: Implement the wake-up context manager**

In `cli_wait.py`, extend the imports:

```python
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from time import monotonic

from aaiclick import internal_api
from aaiclick.orchestration.events import (
    STATE_LISTENING,
    EventBus,
    SignalTransport,
    event_bus,
    get_transport,
    signal_transport,
)
from aaiclick.orchestration.models import TERMINAL_JOB_STATUSES
from aaiclick.orchestration.view_models import JobStatsView
from aaiclick.view_models import RefId
```

Add the constant beside the existing ones:

```python
DEFAULT_SIGNAL_POLL_INTERVAL = 10.0
```

Then add both helpers above `wait_for_job`:

```python
def _wake_interval(transport: SignalTransport, poll_interval: float, signal_poll_interval: float) -> float:
    """How long to wait before refetching unprompted.

    The slow interval applies only while signals can actually arrive.
    ``state`` alone is not enough: ``LocalTransport`` reports ``LISTENING``
    unconditionally, so keying off it would slow local-mode waits down rather
    than speed them up.
    """
    if transport.cross_process and transport.state == STATE_LISTENING:
        return signal_poll_interval
    return poll_interval


@asynccontextmanager
async def _wake_ups(transport: SignalTransport) -> AsyncIterator[Callable[[float], Awaitable[None]]]:
    """Yield ``await wake(seconds)``, which returns early on a change signal.

    A transport that cannot carry signals between processes yields plain
    ``asyncio.sleep`` — local mode executes jobs only inside the ``local
    start`` server process, so a CLI waiting here would never be woken.

    The subscription opens before the caller's first fetch and stays open for
    the whole loop. The mailbox is depth-1, so a commit landing during a fetch
    is queued and the next wait returns at once instead of being lost.
    """
    if not transport.cross_process:
        yield asyncio.sleep
        return

    bus = EventBus()
    stop = asyncio.Event()
    with event_bus(bus), signal_transport(transport):
        feed = asyncio.create_task(transport.feed(bus, stop=stop))
        try:
            with bus.subscription() as sub:

                async def wake(seconds: float) -> None:
                    # A signal, a timeout and a closed bus all mean the same
                    # thing — refetch now — so the reason is never read.
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(sub.wait(), seconds)

                yield wake
        finally:
            stop.set()
            feed.cancel()
            with suppress(asyncio.CancelledError):
                await feed
```

- [ ] **Step 4: Rewrite the loop**

Give `wait_for_job` the new keyword argument, documented in the existing Args block:

```python
        signal_poll_interval: Seconds between polls while change signals are
            arriving, where the poll is only a safety net. Ignored when the
            backend cannot deliver signals to this process.
```

Then wrap the loop. Everything from `stats = await internal_api.job_stats(job_id)` down is unchanged except the final wait:

```python
    transport = get_transport()
    async with _wake_ups(transport) as wake:
        while True:
            stats = await internal_api.job_stats(job_id)
            terminal = stats.job_status in TERMINAL_JOB_STATUSES

            # ``terminal or`` guarantees a final report even when the last tick's
            # counts are unchanged — tasks finish, then the job row flips.
            if on_change is not None and (terminal or stats.status_counts != last_counts):
                on_change(stats)
                last_counts = stats.status_counts

            if terminal:
                return stats

            if monotonic() >= deadline:
                raise JobWaitTimeout(
                    f"Job {stats.job_name!r} (id={stats.job_id}) did not reach a terminal "
                    f"status within {timeout}s; last status was {stats.job_status}",
                    stats,
                )

            await wake(_wake_interval(transport, poll_interval, signal_poll_interval))
```

Update the function docstring's first paragraph to say it blocks on change signals where the backend delivers them and falls back to polling otherwise.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest aaiclick/test_cli_wait.py -q`
Expected: PASS — the new tests plus every pre-existing test in the file unchanged.

- [ ] **Step 6: Run the full suite**

Run: `python -m pytest aaiclick/ -x -q`
Expected: PASS. `aaiclick/test_cli.py` patches `wait_for_job` wholesale, so the CLI tests are unaffected.

- [ ] **Step 7: Commit**

```bash
ruff check aaiclick/ && ruff format --check aaiclick/
git add aaiclick/cli_wait.py aaiclick/test_cli_wait.py
git commit -m "feat: wait_for_job blocks on change signals where available"
```

---

### Task 5: One shared e2e job waiter

**Files:**
- Create: `test_e2e/conftest.py`
- Create: `test_e2e/job_wait.py`
- Modify: `test_e2e/docker/test_runner_e2e.py` — delete local `_wait_for_job`, import the shared one
- Modify: `test_e2e/kubernetes/test_runner_e2e.py` — same

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `async def wait_for_job_by_name(job_name: str, timeout: float = 600.0) -> Job` in `test_e2e/job_wait.py`.

The two existing `_wait_for_job` bodies are byte-identical; only their docstrings differ. Both check `(JOB_COMPLETED, JOB_FAILED)` while `TERMINAL_JOB_STATUSES` also carries `JOB_CANCELLED`, so a cancelled job spins the full 600 s and then fails with a misleading "did not complete" dump.

These suites need real Docker or Kubernetes infrastructure and are excluded from the default pytest run, so the verification step below is an import check rather than a suite run.

- [ ] **Step 1: Create the shared helper**

Create `test_e2e/job_wait.py`:

```python
"""Shared job waiter for the runner e2e suites.

Queries the ORM directly rather than going through ``internal_api``: these
suites assert on the ``Job`` row itself, and a 600 s budget gains nothing from
the change signals ``cli_wait.wait_for_job`` uses.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlmodel import col, select

from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import TERMINAL_JOB_STATUSES, Job
from aaiclick.orchestration.orch_context import get_sql_session


async def wait_for_job_by_name(job_name: str, timeout: float = 600.0) -> Job:
    """Poll the most recent Job with this name until it reaches a terminal
    status, or fail. On timeout, dump per-task states so a stuck or failing
    task is diagnosable from the CI log (the worker writes its own output to
    per-task log files, not stdout).
    """
    deadline = utc_now() + timedelta(seconds=timeout)
    job = None
    while utc_now() < deadline:
        async with get_sql_session() as session:
            result = await session.execute(
                select(Job).where(Job.name == job_name).order_by(col(Job.id).desc()).limit(1)
            )
            job = result.scalar_one_or_none()
        if job is not None and job.status in TERMINAL_JOB_STATUSES:
            return job
        await asyncio.sleep(1.0)
    lines = [f"Job {job_name!r} did not complete within {timeout}s; job_status={getattr(job, 'status', None)}"]
    if job is not None:
        for t in await get_tasks_for_job(job.id):
            lines.append(f"  task entrypoint={t.entrypoint!r} status={t.status} attempt={t.attempt} error={t.error!r}")
    raise TimeoutError("\n".join(lines))
```

- [ ] **Step 2: Put `test_e2e/` on `sys.path`**

There is no `__init__.py` anywhere under `test_e2e/`, so pytest inserts each
test file's own directory — `test_e2e/docker/`, `test_e2e/kubernetes/` — and
not their parent. A `conftest.py` at `test_e2e/` makes pytest insert that
directory too, which is what lets both suites import `job_wait`.

Create `test_e2e/conftest.py`:

```python
"""Present so pytest puts ``test_e2e/`` on ``sys.path``.

The suites under it have no ``__init__.py``, so without this file each suite
directory is importable but their shared modules (``job_wait``) are not.
"""
```

- [ ] **Step 3: Use it from the docker suite**

In `test_e2e/docker/test_runner_e2e.py`, delete the whole `_wait_for_job` function and add to the imports:

```python
from job_wait import wait_for_job_by_name
```

Replace both call sites — `completed = await _wait_for_job(job_name)` becomes:

```python
    completed = await wait_for_job_by_name(job_name)
```

There are three such calls in this file. Then prune the imports that only the deleted function used: `JOB_COMPLETED`, `JOB_FAILED`, `get_tasks_for_job`, `get_sql_session`, `utc_now`, `col`, `select`, `timedelta`, and `asyncio`. Confirm each is genuinely unused before removing it:

```bash
grep -n "JOB_COMPLETED\|JOB_FAILED\|get_tasks_for_job\|get_sql_session\|utc_now\|col(\|select(\|timedelta\|asyncio\." test_e2e/docker/test_runner_e2e.py
```

Keep `TASK_COMPLETED` and `Job` — the test bodies still use them.

- [ ] **Step 4: Use it from the kubernetes suite**

Same edit in `test_e2e/kubernetes/test_runner_e2e.py`: delete `_wait_for_job`, add the `from job_wait import wait_for_job_by_name` import, replace the one `await _wait_for_job(job_name)` call, then run the same grep before pruning imports.

- [ ] **Step 5: Verify both suites still import and collect**

Run: `python -m pytest test_e2e/docker/ test_e2e/kubernetes/ --collect-only -q`
Expected: collection succeeds and lists the smoke tests. A `ModuleNotFoundError: No module named 'job_wait'` here means Step 2's conftest is missing or misplaced.

Run: `ruff check test_e2e/ && ruff format --check test_e2e/`
Expected: clean — this catches any import left behind by Steps 3-4.

- [ ] **Step 6: Commit**

```bash
git add test_e2e/conftest.py test_e2e/job_wait.py \
        test_e2e/docker/test_runner_e2e.py test_e2e/kubernetes/test_runner_e2e.py
git commit -m "fix: share one e2e job waiter and treat cancelled jobs as terminal"
```

---

### Task 6: Update the future-plans entry

**Files:**
- Modify: `docs/designs/future.md` — the "Change Signals — Consumers Beyond the UI" section
- Delete: `docs/superpowers/plans/2026-09-15-change-signal-consumers.md`
- Delete: `docs/superpowers/specs/2026-09-15-change-signal-consumers-design.md`

CLAUDE.md: remove the superpowers plan and spec once the feature lands, and remove `future.md` items when implemented. The `cli_wait` bullet is now done; what remains deferred is the local-mode gap, which `cross_process` names explicitly.

- [ ] **Step 1: Rewrite the section**

Replace the whole "Change Signals — Consumers Beyond the UI" section body (keep the heading) with:

```markdown
The signal (`aaiclick/orchestration/events`) is "a job, task or group row
committed", not a UI concept. The SSE stream and `cli_wait.wait_for_job` both
consume it; `SignalTransport.cross_process` marks which transports can carry
it between processes.

- **Local mode across processes** — `LocalTransport.cross_process` is `False`:
  chdb's file lock means jobs run only inside the `local start` server
  process, so a CLI waiting in another process falls back to polling. Closing
  that gap needs either the Postgres transport or an SSE client in the CLI.
- **MCP / SDK waiters** — `wait_for_job` is already the reusable
  subscribe-then-refetch loop; an in-process caller can await it directly.
  External tools in distributed mode can `LISTEN aaiclick_events` on Postgres
  instead.
```

- [ ] **Step 2: Delete the spec and plan**

```bash
git rm docs/superpowers/specs/2026-09-15-change-signal-consumers-design.md \
       docs/superpowers/plans/2026-09-15-change-signal-consumers.md
```

- [ ] **Step 3: Check nothing still references them**

Run: `grep -rn "change-signal-consumers" --include=*.md --include=*.py . | grep -v "^./.git/"`
Expected: no output.

- [ ] **Step 4: Run the full suite once more**

Run: `python -m pytest aaiclick/ -q`
Expected: PASS.

- [ ] **Step 5: Commit and push**

```bash
git add docs/designs/future.md
git commit -m "docs: record change-signal consumers as shipped"
git push -u origin claude/list-future-md-it-ttnuin
```
