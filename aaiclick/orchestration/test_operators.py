"""The committed group tree of a ``map()`` / ``reduce()`` run."""

from collections import Counter

import pytest
from sqlmodel import select

from aaiclick.data.data_context import data_context
from aaiclick.orchestration import get_job_result
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.fixtures.operator_pipelines import map_pipeline, reduce_pipeline
from aaiclick.orchestration.models import JOB_COMPLETED, Group, Task
from aaiclick.orchestration.orch_context import get_sql_session


@pytest.mark.parametrize(
    "pipeline, groups, members, expected",
    [
        # Five rows at partition=2: three parts in one nested group.
        pytest.param(
            map_pipeline,
            {("map", None), ("parts", "map")},
            {("_expand_map", "map"): 1, ("_finalize", "map"): 1, ("_map_part", "parts"): 3},
            [2, 4, 6, 8, 10],
            id="map",
        ),
        # Layers of 3, 2 and 1 parts, each nested under the call's frame.
        pytest.param(
            reduce_pipeline,
            {("reduce", None), ("layer_0", "reduce"), ("layer_1", "reduce"), ("layer_2", "reduce")},
            {
                ("_expand_reduce", "reduce"): 1,
                ("_finalize", "reduce"): 1,
                ("_reduce_part", "layer_0"): 3,
                ("_reduce_part", "layer_1"): 2,
                ("_reduce_part", "layer_2"): 1,
            },
            [15],
            id="reduce",
        ),
    ],
)
async def test_operator_call_is_one_group_frame(orch_ctx, pipeline, groups, members, expected):
    """The call's outer group holds the expander and ``_finalize`` directly and
    the runtime part group(s) as children; the consumer still reads the output."""
    j = await ajob_test(pipeline)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == expected

    async with get_sql_session() as session:
        rows = (await session.execute(select(Group).where(Group.job_id == j.id))).scalars().all()
        tasks = (await session.execute(select(Task).where(Task.job_id == j.id))).scalars().all()
    group_names: dict[int | None, str | None] = {None: None} | {g.id: g.name for g in rows}

    assert {(g.name, group_names[g.parent_group_id]) for g in rows} == groups
    grouped = Counter((t.name, group_names[t.group_id]) for t in tasks if t.group_id is not None)
    assert grouped == members
