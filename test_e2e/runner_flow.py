"""The user-visible runner flows shared by the docker and kubernetes e2e suites.

Each suite's test is a marker plus one call here, so the two nightlies prove
the same path and differ only in the runner-specific assertions a suite adds
after the call (a kubernetes Secret being gone, say).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from job_wait import run_worker_until_done

from aaiclick.orchestration.docker_config import compute_image_tag
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, TASK_COMPLETED, Task
from aaiclick.orchestration.runner_config import ImageBuild, parse_image_source

# Exits 0 only when ``command_env`` reached the container (``K=v``), so the
# container runners' env delivery (docker ``--env-file``, kubernetes Secret) is
# proven end to end, not just the exit-code plumbing.
ASSERT_COMMAND_ENV = """python -c "import os, sys; sys.exit(0 if os.environ.get('K') == 'v' else 3)" """


def aaiclick_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    """Run a ``python -m aaiclick`` CLI invocation from ``cwd`` (the user repo,
    so the entrypoint module is importable), exactly as an external user would.
    Echoes the captured output (visible under ``pytest -s``) so a silent submit
    failure is diagnosable, then raises on a non-zero exit."""
    proc = subprocess.run(
        [sys.executable, "-m", "aaiclick", *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    print(
        f"$ aaiclick {' '.join(args)} [exit {proc.returncode}]\n--stdout--\n{proc.stdout}\n--stderr--\n{proc.stderr}",
        file=sys.stderr,
    )
    proc.check_returncode()
    return proc


async def run_smoke_flow(job_name: str, user_repo: tuple[str, str, Path]) -> list[Task]:
    """``register-job --build`` → ``run-job --git-sha`` → worker → every task
    completed and the chain's result read back through ClickHouse.

    ``user_repo`` is a published ``sample_job`` fixture ``(remote, sha, worktree)``.
    Returns the job's tasks for any runner-specific assertion."""
    remote, sha, worktree = user_repo

    aaiclick_cli(
        "register-job",
        "sample_jobs.entry_task",
        "--name",
        job_name,
        "--build",
        "--git-remote",
        remote,
        cwd=worktree,
    )
    aaiclick_cli("run-job", job_name, "--git-sha", sha, cwd=worktree)

    completed = await run_worker_until_done(job_name)
    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    # The image is a task property: the entry task carries the build source.
    entry = next(t for t in tasks if t.entrypoint == "sample_jobs.entry_task")
    assert entry.image_source is not None
    source = parse_image_source(entry.image_source)
    assert isinstance(source, ImageBuild)
    assert source.git_sha == sha
    assert compute_image_tag(source.git_sha).endswith(f":{sha}")
    entrypoints = {t.entrypoint for t in tasks}
    assert {"sample_jobs.entry_task", "sample_jobs.produce", "sample_jobs.double", "sample_jobs.compute_sum"} <= (
        entrypoints
    ), entrypoints
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]

    # produce([10, 20, 30]) → double → compute_sum = (10+20+30) * 2 = 120. Reading
    # it back confirms Objects passed between containers via ClickHouse. Native
    # return values are wrapped as ``{"native_value": ...}`` in Task.result.
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == {"native_value": {"total": 120}}, summed.result
    return tasks


async def run_shell_command_env_flow(job_name: str, cwd: Path) -> Task:
    """A shell command in a prebuilt image (no git repo, no build) that exits 0
    only if ``command_env`` arrived: the job completes with no auto-injected
    build task, so the single task is the shell one. Returned for any
    runner-specific follow-up assertion."""
    aaiclick_cli(
        "register-job",
        "shell.placeholder",
        "--name",
        job_name,
        "--image",
        "python:3.12",
        cwd=cwd,
    )
    aaiclick_cli(
        "run-job",
        job_name,
        "--entry-type",
        "shell",
        "--command",
        ASSERT_COMMAND_ENV,
        "--command-env",
        "K=v",
        cwd=cwd,
    )

    completed = await run_worker_until_done(job_name)
    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    assert len(tasks) == 1, [t.entrypoint for t in tasks]
    assert tasks[0].status == TASK_COMPLETED, tasks[0].error
    return tasks[0]
