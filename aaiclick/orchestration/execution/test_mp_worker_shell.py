"""Tests for the subprocess runner's host shell vehicle."""

from aaiclick.orchestration.execution.mp_worker import _run_shell_on_host
from aaiclick.orchestration.execution.worker import JobDispatch
from aaiclick.orchestration.models import Task


def _shell_task(command, command_env=None):
    return Task(id=1, job_id=1, name="t", entrypoint="", entry_type="shell",
                command=command, command_env=command_env)


async def test_shell_on_host_succeeds_on_exit_zero(orch_ctx_no_ch, tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))
    dispatch = JobDispatch("subprocess", None, None, "shell", ["true"], None)
    success, result_ref, log_path, error = await _run_shell_on_host(_shell_task(["true"]), 1, dispatch)
    assert success is True
    assert result_ref is None
    assert "shell-0.log" in log_path


async def test_shell_on_host_fails_on_nonzero(orch_ctx_no_ch, tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))
    dispatch = JobDispatch("subprocess", None, None, "shell", ["false"], None)
    success, _, _, error = await _run_shell_on_host(_shell_task(["false"]), 1, dispatch)
    assert success is False
    assert "exit" in (error or "")


async def test_shell_on_host_command_env_overlaid(orch_ctx_no_ch, tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOG_DIR", str(tmp_path))
    cmd = ["python", "-c", "import os,sys; sys.exit(0 if os.environ.get('K')=='v' else 3)"]
    dispatch = JobDispatch("subprocess", None, None, "shell", cmd, {"K": "v"})
    success, _, _, _ = await _run_shell_on_host(_shell_task(cmd, {"K": "v"}), 1, dispatch)
    assert success is True
