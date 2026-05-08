"""Sample job module installed into the docker-runner e2e test image.

The e2e container resolves entrypoints via ``importlib`` against this
module's dotted path (``sample_jobs.entry_task``). The Dockerfile installs
this directory as a real package so ``pip install /src`` puts the module
on ``sys.path`` — the same pattern a production user follows.

The entry task dynamically registers a three-step Object pipeline via
``tasks_list``. Each child runs in its own container and reads/writes
the host's ClickHouse, so the suite exercises the full multi-container
schedule path *plus* Object reference resolution across container
boundaries — not just a single spawn."""

from __future__ import annotations

import os

from aaiclick import Object, create_object_from_value
from aaiclick.orchestration import task, tasks_list


@task
async def produce() -> Object:
    """Create the seed dataset. Returned as a ClickHouse-backed Object so
    downstream tasks (in other containers) read it via the upstream ref
    rather than a Python value handoff."""
    return await create_object_from_value([10, 20, 30], aai_id=True)


@task
async def double(data: Object) -> Object:
    """Receive an Object from an upstream task in another container and
    produce a new Object derived from it."""
    return await (data * 2)


@task
async def compute_sum(data: Object) -> dict:
    """Read the chain's final values and reduce to a scalar so the test
    can assert the Objects flowed through ClickHouse correctly."""
    summed = await data.sum()
    return {"total": await summed.data()}


@task
async def entry_task():
    """Job entry point. Logs the build-arg-emitted env vars (so a human
    reading the task log can see the framework forwarded them) and
    spawns a produce → double → compute_sum chain. Each child receives
    the upstream Object via implicit dependency on the prior task's
    return value."""
    print(f"git_remote={os.environ.get('GIT_REMOTE')}")
    print(f"git_sha={os.environ.get('GIT_SHA')}")
    print(f"git_branch={os.environ.get('GIT_BRANCH')}")
    print(f"build_context={os.environ.get('BUILD_CONTEXT')}")

    raw = produce()
    doubled = double(data=raw)
    summed = compute_sum(data=doubled)
    return tasks_list(raw, doubled, summed)
