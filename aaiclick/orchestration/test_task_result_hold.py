"""Execution tests: consumers of task_result(data=..., tasks=[...]) wait for the tasks."""

import pytest

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


def _parent_then_consume(parent):
    """``consume`` reads ``parent``'s result; the job result is what it saw."""
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


@job("test_hold_child_data")
def hold_child_data():
    return _parent_then_consume(parent_with_child_data())


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
    return _parent_then_consume(parent_without_data())


@job("test_no_hold_plain_data")
def no_hold_plain_data():
    return _parent_then_consume(parent_with_plain_data())


@pytest.mark.parametrize(
    "pipeline, expected",
    [
        # The parent's result is an upstream ref to a PENDING child: the consumer must wait for it.
        pytest.param(hold_child_data, 7, id="data-task"),
        # G >> consumer with the parent a member of G: the data task holds the consumer too.
        pytest.param(hold_group_successor, 7, id="group-successor"),
        # tasks_list carries no data: the consumer starts after the parent alone and sees None.
        pytest.param(no_hold_without_data, None, id="tasks-list"),
        # Data that is not a returned task is readable as soon as the parent completes.
        pytest.param(no_hold_plain_data, 3, id="plain-data"),
    ],
)
async def test_consumer_reads_parent_result(orch_ctx, pipeline, expected):
    """A consumer of a task returning children reads the right value, held only when data is a returned task."""
    j = await ajob_test(pipeline)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == expected
