"""Tests for shell tasks on the subprocess runner's task child."""

from aaiclick.orchestration.execution.log_test_helpers import read_logs_via_child
from aaiclick.orchestration.execution.mp_worker import _run_task_in_child
from aaiclick.orchestration.execution.runner import ShellSpec
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_task
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import commit_tasks


async def _persisted_shell_task(command, command_env=None) -> Task:
    """A shell Task committed under a real job — the child fetches it from the DB."""
    job = await create_job("shell_host_job", simple_task)
    task = create_task(None, entry_type="shell", command=command, command_env=command_env)
    await commit_tasks(task, job.id)
    return task


async def test_shell_in_child_succeeds_on_exit_zero(orch_ctx_no_ch):
    task = await _persisted_shell_task(["true"])
    success, result_ref, error = await _run_task_in_child(task, 1, shell_spec=ShellSpec(["true"], None))
    assert success is True
    assert result_ref is None
    assert error is None


async def test_shell_in_child_fails_on_nonzero(orch_ctx_no_ch):
    task = await _persisted_shell_task(["false"])
    success, _, error = await _run_task_in_child(task, 1, shell_spec=ShellSpec(["false"], None))
    assert success is False
    assert "exit" in (error or "")


async def test_shell_in_child_command_env_overlaid(orch_ctx_no_ch):
    cmd = ["python", "-c", "import os,sys; sys.exit(0 if os.environ.get('K')=='v' else 3)"]
    task = await _persisted_shell_task(cmd, {"K": "v"})
    success, _, _ = await _run_task_in_child(task, 1, shell_spec=ShellSpec(cmd, {"K": "v"}))
    assert success is True


async def test_shell_in_child_streams_logs_to_clickhouse(orch_ctx_no_ch):
    """Shell output is captured to CH task_logs under a registered run_id."""
    cmd = ["sh", "-c", "echo first line; echo second line"]
    task = await _persisted_shell_task(cmd)

    success, _, _ = await _run_task_in_child(task, 1, shell_spec=ShellSpec(cmd, None))
    assert success is True

    refreshed = await get_task(task.id)
    assert refreshed is not None
    assert len(refreshed.run_ids) == 1
    assert read_logs_via_child(task.id, refreshed.run_ids[-1]) == ["first line", "second line"]
