"""Seed a job whose graph exercises every task status and topology shape.

The web e2e server runs as a subprocess against the default local SQLite
database, so this module writes to the same database in-process and the
server reads the result back through ``GET /jobs/{ref}/graph``.

Shape (9 tasks, 9 edges), chosen so one fixture proves layout, edge routing,
and every status colour at once::

    build_image ─▶ extract ─┬─▶ [group transforms] transform_a ─▶ transform_b ─┐
                            │                                                  ├─▶ report
                            ├─▶ validate ──────────────────────────────────────┘
                            │        └─▶ enrich ─▶ load
                            └─▶ notify

``transforms`` is a real ``Group`` with an internal edge, so its source is
``transform_a`` and its sink is ``transform_b``. ``extract >> group`` and
``group >> report`` therefore expand to exactly one edge each — the
source/sink logic in ``aaiclick.orchestration.graph``, exercised end to end.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlmodel import select

from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import (
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
    Group,
    Task,
)
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session, orch_context
from aaiclick.snowflake import get_snowflake_id

ENTRYPOINT = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"

# Anchored near "now" rather than a fixed date: a RUNNING task has no
# completion time, so the UI measures its duration against the current clock —
# a fixed past base would render an absurd "5096h 11m".
_BASE = datetime.now(timezone.utc) - timedelta(seconds=90)

# name -> (status, error, started_offset_s, completed_offset_s)
_STATES: dict[str, tuple[str, str | None, int | None, int | None]] = {
    "build_image": (TASK_COMPLETED, None, 0, 42),
    "extract": (TASK_COMPLETED, None, 42, 55),
    "transform_a": (TASK_COMPLETED, None, 55, 71),
    "transform_b": (TASK_RUNNING, None, 71, None),
    "validate": (TASK_CLAIMED, None, None, None),
    "enrich": (TASK_FAILED, "ValueError: unexpected null in column 'amount'", 55, 63),
    "load": (TASK_UPSTREAM_FAILED, "Upstream task 'enrich' failed", None, None),
    "notify": (TASK_CANCELLED, "Aborted: a sibling task in the group failed", None, None),
    "report": (TASK_PENDING, None, None, None),
}


async def _build(job_name: str) -> int:
    build_image = create_task(ENTRYPOINT, name="build_image")
    job = await create_job(job_name, build_image)

    extract = create_task(ENTRYPOINT, name="extract")
    transform_a = create_task(ENTRYPOINT, name="transform_a")
    transform_b = create_task(ENTRYPOINT, name="transform_b")
    validate = create_task(ENTRYPOINT, name="validate")
    enrich = create_task(ENTRYPOINT, name="enrich")
    load = create_task(ENTRYPOINT, name="load")
    notify = create_task(ENTRYPOINT, name="notify")
    report = create_task(ENTRYPOINT, name="report")

    transforms = Group(id=get_snowflake_id(), name="transforms")
    transform_a.group_id = transforms.id
    transform_b.group_id = transforms.id

    build_image >> extract
    transform_a >> transform_b
    extract >> transforms
    transforms >> report
    extract >> validate
    extract >> notify
    validate >> enrich
    enrich >> load
    validate >> report

    # Pass every item explicitly rather than relying on registry collection:
    # the group's members hang off ``group_id``, not off task-level edges.
    await commit_tasks(
        [extract, transform_a, transform_b, validate, enrich, load, notify, report, transforms],
        job_id=job.id,
    )

    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
        for task in rows:
            state = _STATES.get(task.name)
            if state is None:
                continue
            status, error, started, completed = state
            task.status = status
            task.error = error
            task.started_at = _BASE + timedelta(seconds=started) if started is not None else None
            task.completed_at = _BASE + timedelta(seconds=completed) if completed is not None else None
            task.is_image_build = task.name == "build_image"
            session.add(task)
        await session.commit()

    return job.id


async def seed_graph_job(job_name: str) -> int:
    """Create the demo graph job and return its id.

    ``with_ch=False`` is required: the e2e server subprocess holds the chdb
    file lock, and opening a second ClickHouse client here would deadlock.
    """
    async with orch_context(with_ch=False):
        return await _build(job_name)
