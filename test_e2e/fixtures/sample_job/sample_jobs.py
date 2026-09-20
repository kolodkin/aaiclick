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
from aaiclick.data.object.refs import upstream_ref
from aaiclick.orchestration import task, tasks_list
from aaiclick.orchestration.factories import create_task

# Class name of the @AaiTask method in the ``jvm_task`` fixture image.
JVM_SUM_TASK = "io.github.kolodkin.aaiclick.e2e.SumTask"


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

    raw = produce()
    doubled = double(data=raw)
    summed = compute_sum(data=doubled)
    return tasks_list(raw, doubled, summed)


@task
async def produce_values() -> list[int]:
    """Seed for the jvm chain: a plain list, since the JVM data plane is
    native values only (no Object refs)."""
    return [10, 20, 30]


@task
async def report(summary: dict) -> dict:
    """Consume the jvm task's return value on the Python side."""
    return {"total": summary["total"], "count": summary["count"]}


@task
async def jvm_entry_task(jvm_image: str):
    """Entry point for the jvm e2e: produce_values → SumTask (jvm) → report.

    The jvm task runs in the prebuilt ``jvm_image`` while the Python tasks
    inherit this task's build image, so one job mixes both image sources.
    ``create_task`` does not turn Task kwargs into upstream refs the way a
    ``@task`` factory does, so the ref and the dependency edge are explicit."""
    values = produce_values()
    summed = create_task(JVM_SUM_TASK, {"values": upstream_ref(values.id)}, entry_type="jvm", image=jvm_image)
    values >> summed
    reported = report(summary=summed)
    return tasks_list(values, summed, reported)
