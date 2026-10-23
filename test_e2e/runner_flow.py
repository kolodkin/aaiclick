"""The user-visible runner flows shared by the docker and kubernetes e2e suites.

A suite calls these helpers and adds only its runner-specific assertions
after them (a kubernetes Secret being gone, a jvm task's image), so both
nightlies prove the same path.
"""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

from job_wait import run_worker_until_done

from aaiclick.data.object.refs import native_ref
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, TASK_COMPLETED, Task
from aaiclick.orchestration.runner_config import ENTRY_JVM, ImageBuild, ImagePrebuilt, parse_image_source
from aaiclick.testing import UserRepo

# Exits 0 only when ``command_env`` reached the container (``K=v``), so the
# container runners' env delivery (docker ``--env-file``, kubernetes Secret) is
# proven end to end, not just the exit-code plumbing — and writes the value it
# saw to ``out.json``, so the file data plane's return leg is proven too.
ASSERT_COMMAND_ENV = (
    """python -c "import json, os, sys; d = os.environ['AAICLICK_IO_DIR']; """
    """json.dump({'_aaiclick_type': 'native', 'value': {'k': os.environ.get('K')}}, open(f'{d}/out.json', 'w')); """
    """sys.exit(0 if os.environ.get('K') == 'v' else 3)" """
)


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


async def run_build_job(job_name: str, entrypoint: str, user_repo: UserRepo, *run_args: str) -> list[Task]:
    """``register-job --build`` → ``run-job --git-sha`` → worker → job and every
    task ``COMPLETED``. ``run_args`` go to ``run-job``. Returns the job's tasks."""
    cwd = user_repo.worktree
    aaiclick_cli("register-job", entrypoint, "--name", job_name, "--build", "--git-remote", user_repo.remote, cwd=cwd)
    aaiclick_cli("run-job", job_name, "--git-sha", user_repo.sha, *run_args, cwd=cwd)

    completed = await run_worker_until_done(job_name)
    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    non_terminal = [t for t in tasks if t.status != TASK_COMPLETED]
    assert not non_terminal, [(t.entrypoint, t.status, t.error) for t in non_terminal]
    return tasks


async def run_smoke_flow(job_name: str, user_repo: UserRepo) -> None:
    """Build the ``sample_job`` fixture and run its chain; the result is read back
    through ClickHouse, proving Objects passed between containers."""
    tasks = await run_build_job(job_name, "sample_jobs.entry_task", user_repo)

    # The image is a task property: the entry task carries the build source.
    entry = next(t for t in tasks if t.entrypoint == "sample_jobs.entry_task")
    assert entry.image_source is not None
    source = parse_image_source(entry.image_source)
    assert isinstance(source, ImageBuild)
    assert source.git_sha == user_repo.sha
    entrypoints = {t.entrypoint for t in tasks}
    assert {"sample_jobs.produce", "sample_jobs.double", "sample_jobs.compute_sum"} <= entrypoints, entrypoints

    # produce([10, 20, 30]) → double → compute_sum = (10+20+30) * 2 = 120. Native
    # return values are wrapped as ``native`` refs in Task.result.
    summed = next(t for t in tasks if t.entrypoint == "sample_jobs.compute_sum")
    assert summed.result == native_ref({"total": 120}), summed.result


def submit_shell_job(job_name: str, command: str, cwd: Path, *run_args: str) -> None:
    """Register a job on the prebuilt ``python:3.12`` image (no git repo, no
    build) and submit ``command`` as its shell entry; ``run_args`` go to ``run-job``."""
    aaiclick_cli("register-job", "shell.placeholder", "--name", job_name, "--image", "python:3.12", cwd=cwd)
    aaiclick_cli("run-job", job_name, "--entry-type", "shell", "--command", command, *run_args, cwd=cwd)


async def run_shell_command_env_flow(job_name: str, cwd: Path) -> Task:
    """A shell command that exits 0 only if ``command_env`` arrived: the job
    completes with no auto-injected build task. Returns the single shell task."""
    submit_shell_job(job_name, ASSERT_COMMAND_ENV, cwd, "--command-env", "K=v")

    completed = await run_worker_until_done(job_name)
    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    assert len(tasks) == 1, [t.entrypoint for t in tasks]
    assert tasks[0].status == TASK_COMPLETED, tasks[0].error
    assert tasks[0].result == native_ref({"k": "v"}), tasks[0].result
    return tasks[0]


# Writes argv[1] to out.json: the parent returns it as its child DAG.
_WRITE_OUT_JSON = (
    "python -c \"import os, sys; open(os.environ['AAICLICK_IO_DIR'] + '/out.json', 'w').write(sys.argv[1])\""
)


async def run_child_build_flow(job_name: str, user_repo: UserRepo) -> None:
    """A shell task built from ``user_repo`` returns a child that declares its
    own build of the same commit from ``child.Dockerfile``: a second image, so
    the child's own build task runs and the child runs in that image."""
    child = {
        "_aaiclick_type": "task",
        "entry_type": "shell",
        "command": ["python", "-c", "import sample_jobs"],
        "git_remote": user_repo.remote,
        "git_sha": user_repo.sha,
        "dockerfile": "child.Dockerfile",
    }
    cwd = user_repo.worktree
    aaiclick_cli(
        "register-job", "shell.placeholder", "--name", job_name, "--build", "--git-remote", user_repo.remote, cwd=cwd
    )
    aaiclick_cli(
        "run-job",
        job_name,
        "--git-sha",
        user_repo.sha,
        "--entry-type",
        "shell",
        "--command",
        f"{_WRITE_OUT_JSON} {shlex.quote(json.dumps(child))}",
        cwd=cwd,
    )

    completed = await run_worker_until_done(job_name)
    assert completed.status == JOB_COMPLETED, completed.error

    tasks = await get_tasks_for_job(completed.id)
    assert all(t.status == TASK_COMPLETED for t in tasks), [(t.name, t.status, t.error) for t in tasks]
    assert sum(t.is_image_build for t in tasks) == 2, [t.name for t in tasks]
    spawned = next(t for t in tasks if t.command == child["command"])
    source = parse_image_source(spawned.image_source or {})
    assert isinstance(source, ImageBuild) and source.dockerfile == "child.Dockerfile", source


async def run_jvm_flow(job_name: str, user_repo: UserRepo, jvm_image: str) -> None:
    """``produce_values`` → ``SumTask`` (jvm, prebuilt ``jvm_image``) → ``report``.

    Covers what the SDK's unit suite cannot: the runner's ``in.json`` handoff
    into a JVM image, the host resolving an upstream Python result into it,
    ``out.json`` read back, and a Python task consuming the jvm return value."""
    tasks = await run_build_job(
        job_name,
        "sample_jobs.jvm_entry_task",
        user_repo,
        "--kwargs",
        json.dumps({"jvm_image": jvm_image}),
    )

    # The jvm task ran in the prebuilt fixture image, not the job's build image.
    summed = next(t for t in tasks if t.entry_type == ENTRY_JVM)
    assert summed.entrypoint == "io.github.kolodkin.aaiclick.e2e.SumTask"
    assert summed.image_source is not None
    source = parse_image_source(summed.image_source)
    assert isinstance(source, ImagePrebuilt)
    assert source.image_tag == jvm_image

    # The host resolved produce_values' [10, 20, 30] into in.json; the shim
    # wrote its map to out.json in the shared ``native`` shape.
    expected = native_ref({"total": 60.0, "count": 3})
    assert summed.result == expected, summed.result

    # And the Python consumer read that jvm result as a plain dict.
    reported = next(t for t in tasks if t.entrypoint == "sample_jobs.report")
    assert reported.result == expected, reported.result


async def run_jvm_fanout_flow(job_name: str, user_repo: UserRepo, jvm_image: str) -> None:
    """A jvm task returns a ``TaskResult`` built with the SDK's typed classes;
    the host registers its jvm children, swaps their ids for database ids,
    and hands the child ``data`` names to the downstream Python consumer."""
    tasks = await run_build_job(
        job_name,
        "sample_jobs.jvm_fanout_entry_task",
        user_repo,
        "--kwargs",
        json.dumps({"jvm_image": jvm_image}),
    )

    children = [t for t in tasks if t.entry_type == ENTRY_JVM and not t.entrypoint.endswith("#fanOut")]
    assert sorted(t.entrypoint for t in children) == [
        "io.github.kolodkin.aaiclick.e2e.FanOutTask#combine",
        "io.github.kolodkin.aaiclick.e2e.SumTask",
        "io.github.kolodkin.aaiclick.e2e.SumTask",
    ]
    # No image on the children: each inherited the jvm parent's prebuilt image.
    for child in children:
        assert child.image_source is not None
        source = parse_image_source(child.image_source)
        assert isinstance(source, ImagePrebuilt)
        assert source.image_tag == jvm_image

    expected = native_ref({"total": 60.0, "count": 3})
    combine = next(t for t in children if t.entrypoint.endswith("#combine"))
    assert combine.result == expected, combine.result

    # TaskResult data = fin.ref(): the consumer waited for combine and got its value.
    reported = next(t for t in tasks if t.entrypoint == "sample_jobs.report")
    assert reported.result == expected, reported.result
