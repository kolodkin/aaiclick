"""Sample job module installed into the docker-runner e2e test image.

The e2e container resolves entrypoints via ``importlib`` against this
module's dotted path (``sample_jobs.entry_task``). The Dockerfile installs
this directory as a real package so ``pip install /src`` puts the module
on ``sys.path`` — the same pattern a production user follows.

The entry task dynamically registers two computation tasks via
``tasks_list``. Each ends up running in its own container (since the
job is docker-mode), so the suite exercises the full multi-container
schedule path — IPC handoff, DB-driven dispatch, image reuse — instead
of just a single spawn."""

from __future__ import annotations

import os

from aaiclick.orchestration import task, tasks_list


@task
async def add(x: int, y: int) -> int:
    result = x + y
    print(f"add({x}, {y}) = {result}")
    return result


@task
async def square(x: int) -> int:
    result = x * x
    print(f"square({x}) = {result}")
    return result


@task
async def entry_task():
    """Job entry point. Logs the build-arg-emitted env vars (so a human
    reading the task log can see the framework forwarded them) and
    spawns two child tasks chained via an upstream ref."""
    print(f"git_remote={os.environ.get('GIT_REMOTE')}")
    print(f"git_sha={os.environ.get('GIT_SHA')}")
    print(f"git_branch={os.environ.get('GIT_BRANCH')}")
    print(f"build_context={os.environ.get('BUILD_CONTEXT')}")

    summed = add(x=2, y=3)
    squared = square(x=summed)
    return tasks_list(summed, squared)
