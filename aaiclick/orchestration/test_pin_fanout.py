"""Tests for the PIN fan-out in ``OrchLifecycleHandler``.

A producer pins its result table for every downstream consumer. The
scheduler understands four dependency edge shapes (task→task, task→group,
group→task, group→group); the fan-out must resolve consumers through all of
them, expanding group targets to their member tasks.
"""

from sqlalchemy import text

from aaiclick.data.data_context.lifecycle import get_data_lifecycle
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import Group
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session, task_scope
from aaiclick.snowflake import get_snowflake_id

_ENTRY = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
_TABLE = "t_pinned"


def _member(group: Group):
    task = create_task(_ENTRY)
    task.group_id = group.id
    group.add_task(task)
    return task


async def _pin_from(producer_id: int, job_id: int) -> set[int]:
    """Pin a table as ``producer_id`` and return the consumer ids it fanned out to."""
    async with task_scope(task_id=producer_id, job_id=job_id, run_id=get_snowflake_id()):
        lifecycle = get_data_lifecycle()
        assert lifecycle is not None
        lifecycle.pin(_TABLE)
        await lifecycle.flush()
    async with get_sql_session() as session:
        rows = await session.execute(text("SELECT task_id FROM table_pin_refs"))
        return {row[0] for row in rows}


async def test_pin_task_to_group_reaches_members(orch_ctx):
    """A >> G: A's table is pinned once per member of G."""
    job = await create_job("pin_task_to_group", _ENTRY)
    producer = create_task(_ENTRY)
    group = Group(id=get_snowflake_id(), name="g")
    m1, m2 = _member(group), _member(group)
    producer >> group
    await commit_tasks([producer, group, m1, m2], job_id=job.id)

    assert await _pin_from(producer.id, job.id) == {m1.id, m2.id}


async def test_pin_group_to_task_reaches_consumer(orch_ctx):
    """G >> B: a member's table is pinned for B."""
    job = await create_job("pin_group_to_task", _ENTRY)
    group = Group(id=get_snowflake_id(), name="g")
    member = _member(group)
    consumer = create_task(_ENTRY)
    group >> consumer
    await commit_tasks([group, member, consumer], job_id=job.id)

    assert await _pin_from(member.id, job.id) == {consumer.id}


async def test_pin_group_to_group_reaches_members(orch_ctx):
    """G1 >> G2: a G1 member's table is pinned once per member of G2, never for siblings."""
    job = await create_job("pin_group_to_group", _ENTRY)
    g1 = Group(id=get_snowflake_id(), name="g1")
    g2 = Group(id=get_snowflake_id(), name="g2")
    m1, sibling = _member(g1), _member(g1)
    c1, c2 = _member(g2), _member(g2)
    g1 >> g2
    await commit_tasks([g1, g2, m1, sibling, c1, c2], job_id=job.id)

    assert await _pin_from(m1.id, job.id) == {c1.id, c2.id}
