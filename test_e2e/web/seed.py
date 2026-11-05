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

``transforms`` is a real ``Group`` with an internal edge. ``extract >> group``
and ``group >> report`` are each drawn once, into and out of the container,
and laid out through every member — ``aaiclick.orchestration.graph``,
exercised end to end.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import NamedTuple

from sqlmodel import func, select

from aaiclick import create_object_from_value
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import objects as objects_api
from aaiclick.internal_api import setup as setup_api
from aaiclick.internal_api import viewer as viewer_api
from aaiclick.orchestration.factories import create_job, create_task, new_job_row
from aaiclick.orchestration.models import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_RUNNING,
    RUN_MANUAL,
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_UPSTREAM_FAILED,
    Group,
    Job,
    JobStatus,
    Task,
    TaskRun,
    TaskStatus,
)
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session, orch_context, task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.view_models import DashboardIn, ObjectQuery

#: The job ``seed_viewer_objects`` gives job-scoped objects, for the job page's
#: Objects dropdown.
OBJECTS_JOB = "viewer_objects_job"

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


#: Default states — every status the graph can render appears at least once.
#: Pass a partial override to ``seed_graph_job`` to seed a different scenario;
#: task names not mentioned keep the default below.
DEFAULT_STATES: dict[str, TaskState] = {
    "build_image": TaskState(TASK_COMPLETED, None, 0, 42),
    "extract": TaskState(TASK_COMPLETED, None, 42, 55),
    "transform_a": TaskState(TASK_COMPLETED, None, 55, 71),
    "transform_b": TaskState(TASK_RUNNING, None, 71, None),
    "validate": TaskState(TASK_COMPLETED, None, 42, 50),
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
            # One attempt per started task, so the Timings and Duration tabs have bars.
            if task.started_at is not None:
                session.add(
                    TaskRun(
                        run_id=get_snowflake_id(),
                        task_id=task.id,
                        status=state.status,
                        started_at=task.started_at,
                        completed_at=task.completed_at,
                    )
                )

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

    Must never query ClickHouse: the e2e server subprocess holds the chdb
    file lock, and opening a second client here would deadlock.
    """
    merged = {**DEFAULT_STATES, **(states or {})}
    async with orch_context():
        return await _build(job_name, merged, job_status)


async def seed_viewer_objects() -> None:
    """A persistent ``orders`` object and a dashboard over it, for the viewer e2e.

    Persistent scopes need the orch lifecycle handler plus a task scope (as the
    ``orch_ctx`` test fixture provides). It opens chdb, whose
    session is a per-process singleton that holds the data-directory lock for
    the life of the process — so this runs in its own short-lived process
    (``python seed.py viewer``) that exits before the e2e server starts.
    """
    seed_id = get_snowflake_id()
    async with orch_context(), task_scope(task_id=seed_id, job_id=seed_id, run_id=seed_id):
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
        # A fresh job per run: its name resolves to the latest, so earlier
        # runs' objects never leak into the dropdown.
        job_id = await _build(OBJECTS_JOB, DEFAULT_STATES, JOB_COMPLETED)
    async with orch_context(), task_scope(task_id=seed_id, job_id=job_id, run_id=seed_id):
        # Six: enough that the menu shows its filter box.
        for name in ("daily_totals", "raw_sales", "customers", "orders_clean", "returns", "weekly_totals"):
            await create_object_from_value({"id": [1, 2], "amount": [5, 7]}, name=name, scope="job")


#: Filler job names, cycled so lists look like a lived-in deployment, not a fresh one.
FILLER_NAMES = ("nightly_etl", "hourly_ingest", "basic_orchestration", "nyc_taxi_pipeline", "weekly_report")
FILLER_JOBS = 100
#: Tasks in ``WIDE_JOB`` — one entry fanning out, for task-heavy pages.
WIDE_JOB = "wide_pipeline"
WIDE_TASKS = 100


async def seed_filler_jobs() -> None:
    """``FILLER_JOBS`` finished jobs plus one ``WIDE_TASKS``-task job, never run.

    Terminal statuses only, so no worker ever picks them up. Seeded once per
    database: reruns against a surviving local root skip it.
    """
    async with orch_context():
        async with get_sql_session() as session:
            # Fillers commit last, so their presence means the whole seed landed.
            if (await session.execute(select(func.count()).where(Job.name == FILLER_NAMES[0]))).scalar_one():
                return
        done = (JOB_COMPLETED, TASK_COMPLETED)
        cycle = (done, done, done, (JOB_FAILED, TASK_FAILED), (JOB_CANCELLED, TASK_CANCELLED))

        def finished(job: Job, tasks: list[Task], statuses: tuple[JobStatus, TaskStatus]) -> None:
            job.status, task_status = statuses
            for row in (job, *tasks):
                row.started_at, row.completed_at = _BASE, _offset(30)
            for task in tasks:
                task.status = task_status

        # commit_tasks persists the fan-out edges; the job row rides in the same commit.
        wide = new_job_row(WIDE_JOB, run_type=RUN_MANUAL)
        entry = create_task(ENTRYPOINT, name="fan_out")
        branches = [create_task(ENTRYPOINT, name=f"part_{i:03d}") for i in range(WIDE_TASKS - 1)]
        for task in branches:
            entry >> task
        finished(wide, [entry, *branches], done)
        await commit_tasks(branches, job_id=wide.id, job=wide)

        # shortcut: fillers bypass commit_tasks (one commit, not 100) — fine for
        # edge-less plain module tasks; route through it if they gain images or edges.
        async with get_sql_session() as session:
            for i in range(FILLER_JOBS):
                job = new_job_row(FILLER_NAMES[i % len(FILLER_NAMES)], run_type=RUN_MANUAL)
                # Distinct, older-than-wide timestamps keep list order deterministic.
                job.created_at = wide.created_at - timedelta(seconds=i + 1)
                task = create_task(ENTRYPOINT, name="main")
                task.job_id = job.id
                finished(job, [task], cycle[i % len(cycle)])
                session.add_all([job, task])
            await session.commit()


if __name__ == "__main__":
    # `python seed.py viewer`: create the local schema, seed, exit (releasing chdb).
    setup_api.setup()

    async def _seed() -> None:
        await seed_filler_jobs()
        await seed_viewer_objects()

    asyncio.run(_seed())
