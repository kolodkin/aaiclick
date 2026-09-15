"""Shared helpers for the web e2e suite: browser navigation and in-process job setup."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, TypeVar

from aaiclick.internal_api.jobs import get_job_graph
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.jobs import get_tasks_for_job
from aaiclick.orchestration.orch_context import orch_context

T = TypeVar("T")

# Seeded admin credentials. In distributed mode the server enforces auth, so
# tests log in before the SPA shell renders; the server seeds this admin on
# startup from the same env vars. In local mode auth is off and login is skipped.
ADMIN_USER = os.getenv("AAICLICK_ADMIN_USERNAME", "admin")
ADMIN_PASS = os.getenv("AAICLICK_ADMIN_PASSWORD", "admin")


def login_if_needed(page) -> None:
    """Authenticate through the SPA login form when auth is enforced.

    Waits for either the login form (auth on) or the prompt input (auth off or
    already authenticated), then logs in only if the form is present.
    """
    page.wait_for_selector("#login-username, #prompt")
    if page.query_selector("#login-username"):
        page.fill("#login-username", ADMIN_USER)
        page.fill("#login-password", ADMIN_PASS)
        page.click("#login-submit")
        page.wait_for_selector("#prompt")


def open_page(page, url: str) -> None:
    """Navigate, wait for the SPA shell, and authenticate if the server asks."""
    page.goto(url)
    page.wait_for_selector("#root")
    login_if_needed(page)


# --- In-process job setup ---------------------------------------------------
#
# Tests create jobs the way seed.py does — straight into the database through
# the orchestration API, never through REST. That works identically against
# both backends and needs no credentials, which is what lets one test body run
# in local and distributed mode alike. Whichever worker the mode runs (in the
# server process locally, separate processes in distributed mode) then picks
# the job up, so status transitions arrive on their own.
#
# ``with_ch=False`` is required: in local mode the server subprocess holds the
# chdb file lock, and a second ClickHouse client here would deadlock.


def run_in_process(coro_fn: Callable[[], Awaitable[T]]) -> T:
    """Run an orchestration coroutine on its own thread with a fresh loop.

    ``pytest-asyncio`` is in auto mode so a loop is already running on the
    test thread, and Playwright's sync API cannot be driven from inside one
    either. A dedicated thread gives the coroutine a clean loop.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(coro_fn())).result()


class TaskRow(NamedTuple):
    """A task as the SPA sees it: ids are strings, since snowflakes exceed
    JS's safe-integer range and the ``@task`` routes carry them verbatim."""

    id: str
    name: str
    status: str


def submit_job(name: str, entrypoint: str, kwargs: dict | None = None) -> str:
    """Create a job in-process and return its id as a string."""

    async def go() -> int:
        async with orch_context(with_ch=False):
            job = await create_job(name, create_task(entrypoint, kwargs))
            return job.id

    return str(run_in_process(go))


def job_tasks(job_id: str) -> list[TaskRow]:
    """The job's tasks, in creation order."""

    async def go() -> list[TaskRow]:
        async with orch_context(with_ch=False):
            return [TaskRow(str(t.id), t.name, t.status) for t in await get_tasks_for_job(int(job_id))]

    return run_in_process(go)


def wait_for_task(job_id: str, status: str | None = None, timeout: float = 30.0) -> TaskRow:
    """The job's first task once it exists — and has reached ``status``, if given."""

    async def go() -> TaskRow:
        async with orch_context(with_ch=False):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                tasks = await get_tasks_for_job(int(job_id))
                if tasks and (status is None or tasks[0].status == status):
                    task = tasks[0]
                    return TaskRow(str(task.id), task.name, task.status)
                await asyncio.sleep(0.1)
            raise AssertionError(f"job {job_id}: task did not reach {status or 'existence'} within {timeout} s")

    return run_in_process(go)


def job_graph(job_id: str) -> dict:
    """The job's graph as ``GET /jobs/{id}/graph`` would return it, read in-process."""

    async def go() -> dict:
        async with orch_context(with_ch=False):
            return (await get_job_graph(int(job_id))).model_dump(mode="json")

    return run_in_process(go)
