"""Execution tests: consumers of task_result(data=..., tasks=[...]) wait for the tasks."""

from aaiclick.orchestration import get_job_result, task_result, tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED, Group
from aaiclick.snowflake import get_snowflake_id


@task
async def child_value() -> int:
    return 7


@task
async def parent_with_child_data():
    """Returns the child itself as data: a consumer must wait for it to resolve."""
    child = child_value()
    return task_result(data=child, tasks=[child])


@task
async def parent_without_data():
    return tasks_list(child_value())


@task
async def parent_with_plain_data():
    """Data independent of the children: consumers need not wait for them."""
    return task_result(data=3, tasks=[child_value()])


@task
async def consume(value) -> int:
    return value


@job("test_hold_child_data")
def hold_child_data():
    parent = parent_with_child_data()
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


@job("test_hold_group_successor")
def hold_group_successor():
    group = Group(id=get_snowflake_id(), name="producers")
    parent = parent_with_child_data()
    group.add_task(parent)
    seen = consume(value=parent)
    group >> seen
    return task_result(data=seen, tasks=[group, seen])


@job("test_no_hold_without_data")
def no_hold_without_data():
    parent = parent_without_data()
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


@job("test_no_hold_plain_data")
def no_hold_plain_data():
    parent = parent_with_plain_data()
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


async def test_consumer_waits_for_child_named_as_data(orch_ctx):
    """The parent's result is an upstream ref to a PENDING child; the consumer must not run yet."""
    j = await hold_child_data()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 7


async def test_consumer_of_parent_group_waits_for_children(orch_ctx):
    """G >> consumer with the parent a member of G: the returned child holds the consumer too."""
    j = await hold_group_successor()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 7


async def test_tasks_list_does_not_hold(orch_ctx):
    """tasks_list carries no data, so the consumer starts after the parent alone and sees None."""
    j = await no_hold_without_data()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) is None


async def test_plain_data_does_not_hold(orch_ctx):
    """A value that is not a returned task is readable as soon as the parent completes."""
    j = await no_hold_plain_data()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 3
