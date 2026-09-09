"""Tests for the UI change-signal bus and the session listeners that feed it."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from aaiclick.backend import is_postgres

from .events import (
    STATE_LISTENING,
    EventBus,
    SignalTransport,
    event_bus,
    get_event_bus,
    get_transport,
    register_session_hooks,
    signal_transport,
    statement_touches_watched,
    unregister_session_hooks,
)
from .events.local import LocalTransport
from .events.state import TransportState
from .execution.claiming import cancel_job, update_task_status
from .execution.execution_worker import _set_pending_cleanup, register_execution_worker
from .factories import create_job
from .jobs import get_tasks_for_job
from .models import TASK_RUNNING
from .orch_context import get_sql_session

SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _drain(bus: EventBus, timeout: float = 5.0) -> int:
    """Close ``bus`` and count the signals a fresh subscriber still receives."""
    bus.close()

    async def count() -> int:
        return len([signal async for signal in bus.subscribe()])

    return await asyncio.wait_for(count(), timeout)


async def test_subscriber_receives_published_signal():
    bus = EventBus()
    received = []

    async def consume() -> None:
        async for _ in bus.subscribe():
            received.append(True)
            return

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    bus.publish()
    await asyncio.wait_for(consumer, timeout=5)
    assert received == [True]


async def test_burst_collapses_into_one_pending_signal():
    bus = EventBus()
    signals = []

    async def consume() -> None:
        async for _ in bus.subscribe():
            signals.append(True)

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    for _ in range(10):
        bus.publish()
    await asyncio.sleep(0.05)
    bus.close()
    await asyncio.wait_for(consumer, timeout=5)
    assert len(signals) == 1


async def test_close_ends_subscription_without_signal():
    bus = EventBus()
    assert await _drain(bus) == 0


async def test_publish_after_close_is_ignored():
    bus = EventBus()
    bus.close()
    bus.publish()
    assert await _drain(bus) == 0


def test_event_bus_context_swaps_and_restores():
    default = get_event_bus()
    scoped = EventBus()
    with event_bus(scoped):
        assert get_event_bus() is scoped
    assert get_event_bus() is default


@pytest.mark.parametrize(
    "sql, expected",
    [
        pytest.param("UPDATE tasks SET status = 'x'", True, id="update-tasks"),
        pytest.param("\n    UPDATE jobs SET status = :s WHERE id = :id", True, id="leading-whitespace"),
        pytest.param("insert into groups (id) values (1)", True, id="lowercase-insert"),
        pytest.param("DELETE FROM jobs WHERE id = :id", True, id="delete-jobs"),
        pytest.param("INSERT INTO table_run_refs (table_name) VALUES ('t')", False, id="unwatched-table"),
        pytest.param("SELECT id FROM tasks WHERE status = :s", False, id="select-only"),
        pytest.param("UPDATE tasks_archive SET x = 1", False, id="prefix-not-whole-word"),
    ],
)
def test_statement_touches_watched(sql, expected):
    assert statement_touches_watched(sql) is expected


async def _wait_listening(transport: SignalTransport, timeout: float = 10.0) -> None:
    """Poll until the transport reports it is listening."""

    async def poll() -> None:
        while transport.state != STATE_LISTENING:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


# Long enough for a Postgres NOTIFY to travel through the listener connection.
SETTLE = 0.3


@asynccontextmanager
async def recording(bus: EventBus) -> AsyncIterator[list[None]]:
    """Subscribe before the block runs; on exit settle, close the bus and
    hand back every signal the block's commits produced.

    A signal published with no subscriber is dropped, so the subscription
    must already exist when the write under test commits."""
    signals: list[None] = []

    async def consume() -> None:
        async for signal in bus.subscribe():
            signals.append(signal)

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)
    try:
        yield signals
        await asyncio.sleep(SETTLE)
    finally:
        bus.close()
        await asyncio.wait_for(consumer, 5)


def test_register_session_hooks_is_idempotent():
    register_session_hooks()
    register_session_hooks()
    unregister_session_hooks()
    unregister_session_hooks()
    register_session_hooks()


async def test_unregistered_hooks_publish_nothing(orch_ctx, live_bus):
    job = await create_job("events_unregistered", SAMPLE_TASK)
    await asyncio.sleep(SETTLE)
    unregister_session_hooks()
    try:
        async with recording(live_bus) as signals:
            await cancel_job(job.id)
    finally:
        register_session_hooks()
    assert signals == []


@pytest.mark.skipif(is_postgres(), reason="Postgres selection is covered in test_events_postgres.py")
def test_get_transport_is_local():
    assert isinstance(get_transport(), LocalTransport)


class RecordingTransport:
    """Stand-in that records which commit hooks fired, in order."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    @property
    def state(self) -> TransportState:
        return STATE_LISTENING

    def before_commit(self, session: Session) -> None:
        self.calls.append("before")

    def after_commit(self, session: Session) -> None:
        self.calls.append("after")

    async def feed(self, bus: EventBus, *, stop: asyncio.Event) -> None:
        await stop.wait()


def test_signal_transport_override_swaps_and_restores():
    default = type(get_transport())
    fake = RecordingTransport()
    with signal_transport(fake):
        assert get_transport() is fake
    assert isinstance(get_transport(), default)


async def test_hooks_call_transport_on_either_side_of_commit(orch_ctx):
    """One flagged commit reaches the transport exactly once before and once after."""
    job = await create_job("events_fake_transport", SAMPLE_TASK)
    fake = RecordingTransport()
    with signal_transport(fake):
        await cancel_job(job.id)
    assert fake.calls == ["before", "after"]


async def test_hooks_skip_transport_on_rollback(orch_ctx):
    job = await create_job("events_fake_rollback", SAMPLE_TASK)
    fake = RecordingTransport()
    with signal_transport(fake):
        async with get_sql_session() as session:
            await session.execute(
                text("UPDATE tasks SET status = :status WHERE job_id = :job_id"),
                {"status": TASK_RUNNING, "job_id": job.id},
            )
            await session.rollback()
    assert fake.calls == []


@pytest.fixture
async def live_bus() -> AsyncIterator[EventBus]:
    """A scoped bus fed by the active backend's transport, once it is listening."""
    bus = EventBus()
    stop = asyncio.Event()
    transport = get_transport()
    with event_bus(bus), signal_transport(transport):
        feed = asyncio.create_task(transport.feed(bus, stop=stop))
        await _wait_listening(transport)
        yield bus
    stop.set()
    await feed


async def test_task_status_write_publishes_one_signal(orch_ctx, live_bus):
    job = await create_job("events_status", SAMPLE_TASK)
    task = (await get_tasks_for_job(job.id))[0]
    await asyncio.sleep(SETTLE)
    async with recording(live_bus) as signals:
        await update_task_status(task.id, TASK_RUNNING)
    assert len(signals) == 1


async def test_raw_task_update_publishes(orch_ctx, live_bus):
    job = await create_job("events_raw", SAMPLE_TASK)
    await asyncio.sleep(SETTLE)
    async with recording(live_bus) as signals:
        async with get_sql_session() as session:
            await session.execute(
                text("UPDATE tasks SET status = :status WHERE job_id = :job_id"),
                {"status": TASK_RUNNING, "job_id": job.id},
            )
            await session.commit()
    assert len(signals) == 1


async def test_orm_update_statement_publishes(orch_ctx, live_bus):
    """``update(Task)`` builds a Core statement, not text — it must be caught too."""
    job = await create_job("events_orm_update", SAMPLE_TASK)
    task = (await get_tasks_for_job(job.id))[0]
    await asyncio.sleep(SETTLE)
    async with recording(live_bus) as signals:
        await _set_pending_cleanup(task.id, "boom")
    assert len(signals) == 1


async def test_cancel_job_publishes(orch_ctx, live_bus):
    job = await create_job("events_cancel", SAMPLE_TASK)
    await asyncio.sleep(SETTLE)
    async with recording(live_bus) as signals:
        await cancel_job(job.id)
    assert len(signals) == 1


async def test_unrelated_write_publishes_nothing(orch_ctx, live_bus):
    async with recording(live_bus) as signals:
        await register_execution_worker()
    assert signals == []


async def test_rolled_back_write_publishes_nothing(orch_ctx, live_bus):
    job = await create_job("events_rollback", SAMPLE_TASK)
    await asyncio.sleep(SETTLE)
    async with recording(live_bus) as signals:
        async with get_sql_session() as session:
            await session.execute(
                text("UPDATE tasks SET status = :status WHERE job_id = :job_id"),
                {"status": TASK_RUNNING, "job_id": job.id},
            )
            await session.rollback()
    assert signals == []
