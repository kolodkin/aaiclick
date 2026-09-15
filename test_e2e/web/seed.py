"""Seed a job whose graph exercises every task status and topology shape.

The web e2e server runs as a subprocess against whichever backend
``AAICLICK_SQL_URL`` names, so this module writes to that same database
in-process and the server reads the result back through
``GET /jobs/{ref}/graph``.

Shape (9 tasks, 16 edges), chosen so one fixture proves layout, edge routing,
and every status colour at once::

    extract ─┬─▶ [group transforms] transform_a ─▶ transform_b ─┐
             │                                                   ├─▶ report
             ├─▶ validate ───────────────────────────────────────┘
             │        └─▶ enrich ─▶ load
             └─▶ notify

    build_image ─▶ (every task above)

The build task is a hub, not a link in the chain: ``inject_build_tasks`` wires
``build >> dependent`` for every task sharing the image, and a registry-mode
job normally shares one image job-wide. Modelling it as a chain would test a
shape that never occurs.

``transforms`` is a real ``Group`` with an internal edge, so its source is
``transform_a`` and its sink is ``transform_b``. ``extract >> group`` and
``group >> report`` therefore expand to exactly one edge each — the
source/sink logic in ``aaiclick.orchestration.graph``, exercised end to end.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import NamedTuple

from sqlmodel import select

from aaiclick import create_object_from_value
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import objects as objects_api
from aaiclick.internal_api import setup as setup_api
from aaiclick.internal_api import viewer as viewer_api
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import (
    JOB_RUNNING,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
    Group,
    Job,
    JobStatus,
    Task,
    TaskStatus,
)
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session, orch_context, task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.view_models import DashboardIn, ObjectQuery

ENTRYPOINT = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"

# Anchored near "now" rather than a fixed date: a RUNNING task has no
# completion time, so the UI measures its duration against the current clock —
# a fixed past base would render an absurd "5096h 11m". Naive UTC, as the app
# writes everywhere: the columns are TIMESTAMP WITHOUT TIME ZONE, and asyncpg
# rejects an aware datetime for them where SQLite quietly accepted one.
_BASE = utc_now() - timedelta(seconds=90)


def _offset(seconds: int | None) -> datetime | None:
    return _BASE + timedelta(seconds=seconds) if seconds is not None else None


class TaskState(NamedTuple):
    """Seeded state for one task in the fixture graph."""

    status: TaskStatus
    error: str | None = None
    started_offset_s: int | None = None
    completed_offset_s: int | None = None


#: Default states — every status the graph can render, exactly once each.
#: Pass a partial override to ``seed_graph_job`` to seed a different scenario;
#: task names not mentioned keep the default below.
DEFAULT_STATES: dict[str, TaskState] = {
    "build_image": TaskState(TASK_COMPLETED, None, 0, 42),
    "extract": TaskState(TASK_COMPLETED, None, 42, 55),
    "transform_a": TaskState(TASK_COMPLETED, None, 55, 71),
    "transform_b": TaskState(TASK_RUNNING, None, 71, None),
    "validate": TaskState(TASK_CLAIMED),
    "enrich": TaskState(TASK_FAILED, "ValueError: unexpected null in column 'amount'", 55, 63),
    "load": TaskState(TASK_UPSTREAM_FAILED, "Upstream task 'enrich' failed"),
    "notify": TaskState(TASK_CANCELLED, "Cancelled: job aborted before this task started"),
    "report": TaskState(TASK_PENDING),
}


async def _build(job_name: str, states: Mapping[str, TaskState], job_status: JobStatus) -> int:
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

    pipeline = [extract, transform_a, transform_b, validate, enrich, load, notify, report]

    # Mirrors `inject_build_tasks`, which wires `build >> dependent` for *every*
    # task sharing the built image — not just the entry task. In a registry-mode
    # docker/k8s job `stamp_inherited_image` spreads one image across the whole
    # job, so the build node's out-degree is N-1, a hub rather than a chain.
    for task in pipeline:
        build_image >> task

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
    await commit_tasks([*pipeline, transforms], job_id=job.id)

    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
        for task in rows:
            state = states.get(task.name)
            if state is None:
                continue
            task.status = state.status
            task.error = state.error
            task.started_at = _offset(state.started_offset_s)
            task.completed_at = _offset(state.completed_offset_s)
            task.is_image_build = task.name == "build_image"
            session.add(task)

        # Seed the job row too, or the header contradicts the tasks below it —
        # a PENDING job above a graph with RUNNING and FAILED nodes.
        job_row = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        job_row.status = job_status
        job_row.started_at = _BASE
        session.add(job_row)
        await session.commit()

    return job.id


async def seed_graph_job(
    job_name: str,
    states: Mapping[str, TaskState] | None = None,
    job_status: JobStatus = JOB_RUNNING,
) -> int:
    """Create the demo graph job and return its id.

    Args:
        job_name: Name for the seeded job.
        states: Per-task overrides merged over ``DEFAULT_STATES``; task names
            not mentioned keep their default. Pass this to seed a scenario
            other than the all-statuses default (e.g. an all-green graph).
        job_status: Status written to the job row.

    ``with_ch=False`` is required: the e2e server subprocess holds the chdb
    file lock, and opening a second ClickHouse client here would deadlock.
    """
    merged = {**DEFAULT_STATES, **(states or {})}
    async with orch_context(with_ch=False):
        return await _build(job_name, merged, job_status)


async def seed_viewer_objects() -> None:
    """A persistent ``orders`` object and a dashboard over it, for the viewer e2e.

    Persistent scopes need the orch lifecycle handler plus a task scope (as the
    ``orch_ctx`` test fixture provides). ``with_ch=True`` opens chdb, whose
    session is a per-process singleton that holds the data-directory lock for
    the life of the process — so this runs in its own short-lived process
    (``python seed.py viewer``) that exits before the e2e server starts.
    """
    seed_id = get_snowflake_id()
    async with orch_context(with_ch=True), task_scope(task_id=seed_id, job_id=seed_id, run_id=seed_id):
        # The local database survives between runs: drop a previous seed so the
        # row count the tests assert on stays exact.
        await objects_api.delete_object("orders")
        await create_object_from_value(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]}, name="orders", scope="global"
        )
        await viewer_api.save_dashboard(
            DashboardIn(
                name="sales",
                html=(
                    "<h1 id='title'>Sales</h1>"
                    "<script>document.getElementById('title').textContent = 'rows:' + window.queries.top.name.length</script>"
                ),
                queries={"top": ObjectQuery(object="orders", fields=["name", "amount"])},
            )
        )


if __name__ == "__main__":
    # `python seed.py viewer`: create the local schema, seed, exit (releasing chdb).
    setup_api.setup()
    asyncio.run(seed_viewer_objects())
