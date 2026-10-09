"""The file the sandbox e2e submits: two jobs sharing one task. It is
committed into an empty remote by the sandbox and built on the default
Dockerfile, so it imports as ``YYYYMMDD.sb_<ts>_<name>``."""

from __future__ import annotations

from aaiclick.orchestration import TaskResult, job, task


@task
async def probe() -> dict:
    return {"n": 1}


@job
def first():
    return TaskResult(tasks=[probe()])


@job
def second():
    return TaskResult(tasks=[probe()])
