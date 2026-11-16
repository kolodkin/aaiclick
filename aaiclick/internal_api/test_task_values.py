"""Tests for ``aaiclick.internal_api.task_values``."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.data.object.refs import callable_ref, group_results_ref, native_ref, upstream_ref
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import Job, Task
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session

from . import errors, task_values

_SAMPLE_TASK = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"

pytestmark = pytest.mark.usefixtures("orch_ctx")


async def _pipeline() -> tuple[Job, Task, Task]:
    load = create_task(_SAMPLE_TASK, {"limit": 1000, "result": "kw"}, name="load")
    clean = create_task(
        _SAMPLE_TASK,
        {
            "df": upstream_ref(load.id),
            "rules": {"_aaiclick_type": "pydantic", "type": "m.Rules", "data": {"dedupe": True}},
            "fmt": callable_ref("m.f"),
            "parts": group_results_ref(7),
            "odd": {"_aaiclick_type": "mystery"},
        },
        name="clean",
    )
    idle = create_task(_SAMPLE_TASK, name="idle")
    job = await create_job("tv_job", load)
    await commit_tasks([clean, idle], job_id=job.id)
    async with get_sql_session() as session:
        for row in (await session.execute(select(Task).where(Task.job_id == job.id))).scalars():
            if row.name == "load":
                row.result = {"_aaiclick_type": "object", "table": f"j_{job.id}_orders", "persistent": True}
            elif row.name == "clean":
                row.result = native_ref({"dropped": 12})
            session.add(row)
        await session.commit()
    return job, load, clean


async def test_list_task_values_kinds_refs_and_targets():
    job, load, clean = await _pipeline()
    rows = await task_values.list_task_values(job)
    assert [(r.name, r.label, r.kind, r.direction, r.ref_type, r.preview, r.target) for r in rows] == [
        (f"{load.id}.limit", "load.limit", "value", "in", None, "1000", None),
        # a kwarg named "result" is not the task's result
        (f"{load.id}.result", "load.result", "value", "in", None, '"kw"', None),
        (f"{load.id}", "load.result", "ref", "out", "object", "orders", "orders"),
        (f"{clean.id}.df", "clean.df", "ref", "in", "task", "load.result", f"{load.id}"),
        (f"{clean.id}.fmt", "clean.fmt", "ref", "in", "function", "m.f", None),
        # an unknown _aaiclick_type lists under its raw type
        (f"{clean.id}.odd", "clean.odd", "ref", "in", "mystery", "mystery", None),
        (f"{clean.id}.parts", "clean.parts", "ref", "in", "group", "group 7 results (0 tasks)", None),
        (f"{clean.id}.rules", "clean.rules", "value", "in", None, 'm.Rules {"dedupe": true}', None),
        (f"{clean.id}", "clean.result", "value", "out", None, '{"dropped": 12}', None),
    ]
    assert {(r.scope, r.job_id, r.job_name, r.task_id is not None) for r in rows} == {("job", job.id, "tv_job", True)}


async def test_list_task_values_preview_is_cut():
    job = await create_job("tv_job", create_task(_SAMPLE_TASK, {"big": "x" * 500}, name="t"))
    (row,) = await task_values.list_task_values(job)
    assert len(row.preview) == task_values.PREVIEW_CHARS


async def test_read_task_value():
    job, load, clean = await _pipeline()
    assert await task_values.read_task_value(job.id, f"{clean.id}") == {"dropped": 12}
    assert await task_values.read_task_value(job.id, f"{load.id}.result") == "kw"
    assert await task_values.read_task_value(job.id, f"{clean.id}.df") == upstream_ref(load.id)


@pytest.mark.parametrize(
    "suffix",
    [
        pytest.param(".nope", id="missing_kwarg"),
        pytest.param(".2.3", id="malformed"),
    ],
)
async def test_read_task_value_not_found(suffix):
    job, load, _ = await _pipeline()
    with pytest.raises(errors.NotFound):
        await task_values.read_task_value(job.id, f"{load.id}{suffix}")


async def test_read_task_value_scoped_to_job_and_needs_a_result():
    job, load, _ = await _pipeline()
    other = await create_job("other_job", _SAMPLE_TASK)
    with pytest.raises(errors.NotFound):
        await task_values.read_task_value(other.id, f"{load.id}.limit")
    async with get_sql_session() as session:
        idle = (await session.execute(select(Task).where(Task.job_id == job.id, Task.name == "idle"))).scalar_one()
    with pytest.raises(errors.NotFound):
        await task_values.read_task_value(job.id, f"{idle.id}")
    with pytest.raises(errors.NotFound):
        await task_values.read_task_value(job.id, "orders")


async def test_read_task_value_over_cap_is_truncated():
    job = await create_job(
        "tv_job", create_task(_SAMPLE_TASK, {"big": "x" * (task_values.MAX_VALUE_BYTES + 1)}, name="t")
    )
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.job_id == job.id))).scalar_one()
    value = await task_values.read_task_value(job.id, f"{task.id}.big")
    assert value == {"truncated": True, "bytes": task_values.MAX_VALUE_BYTES + 3}
