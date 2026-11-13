"""Local worker tests that each assert one job of a queue drained by one loop run."""

import pytest

from aaiclick.testing import set_task_image_source

from ..decorators import job, task
from ..factories import create_job, create_task
from ..jobs import get_tasks_for_job
from ..models import TASK_COMPLETED, TASK_PENDING_FAILURE_CLEANUP, Job, Task
from .execution_worker import execution_worker_main_loop

SAMPLE_TASKS = "aaiclick.orchestration.fixtures.sample_tasks"


@task
async def _child_step(label: str) -> str:
    """Child of a dynamic registration test — returns its label."""
    return label


@job("test_local_dynamic_list")
def dynamic_list_pipeline():
    """Entry task returning a plain list of Tasks (the documented @job shape)."""
    return [_child_step(label="a"), _child_step(label="b")]


def _shell(command: str) -> Task:
    return create_task(None, entry_type="shell", command=["sh", "-c", command])


@pytest.fixture(scope="module")
async def drained(orch_shared_ctx) -> dict[str, Job]:
    """Six jobs queued together and drained by one in-process worker loop."""
    jobs = {
        "simple": await create_job("test_local_job", f"{SAMPLE_TASKS}.simple_task"),
        "failing": await create_job("test_local_failing_job", f"{SAMPLE_TASKS}.failing_task"),
        "shell": await create_job("test_local_shell_job", _shell("echo from shell")),
        "shell_fail": await create_job("test_local_shell_fail", _shell("exit 7")),
        "dynamic": await dynamic_list_pipeline(),
        "container": await create_job("test_local_container_task", f"{SAMPLE_TASKS}.simple_task"),
    }
    await set_task_image_source(jobs["container"].id, {"type": "prebuilt", "image_tag": "python:3.12"})
    # Only successes count: simple, shell, and the dynamic entry with its two children.
    assert await execution_worker_main_loop(install_signal_handlers=False, max_empty_polls=1) == 5
    return jobs


async def _only_task(job: Job) -> Task:
    (found,) = await get_tasks_for_job(job.id)
    return found


async def test_local_worker_executes_task(drained):
    """In local mode the worker executes tasks in-process sharing the chdb session."""
    assert (await _only_task(drained["simple"])).status == TASK_COMPLETED


async def test_local_worker_handles_failure(drained):
    """In local mode task failures are handled without child process crashes."""
    failed = await _only_task(drained["failing"])
    assert failed.status == TASK_PENDING_FAILURE_CLEANUP
    assert failed.error is not None


async def test_local_worker_executes_shell_task(drained):
    """A shell task runs on the in-process worker (log content is pinned in
    test_execution.py — here only the dispatch through the worker loop)."""
    assert (await _only_task(drained["shell"])).status == TASK_COMPLETED


async def test_local_worker_shell_task_nonzero_exit(drained):
    """A failing shell task lands in PENDING_FAILURE_CLEANUP with its exit code."""
    failed = await _only_task(drained["shell_fail"])
    assert failed.status == TASK_PENDING_FAILURE_CLEANUP
    assert "exit 7" in (failed.error or "")


async def test_local_worker_registers_returned_task_list(drained):
    """An entry task returning a plain list of Tasks registers them as children."""
    tasks = await get_tasks_for_job(drained["dynamic"].id)
    assert len(tasks) == 3
    assert all(t.status == TASK_COMPLETED for t in tasks)


async def test_local_worker_refuses_container_task(drained):
    """In-process execution has no container runner: a task that declares an
    image fails naming AAICLICK_RUNNER instead of silently running on the host."""
    refused = await _only_task(drained["container"])
    assert refused.status == TASK_PENDING_FAILURE_CLEANUP
    assert "AAICLICK_RUNNER" in (refused.error or "")
