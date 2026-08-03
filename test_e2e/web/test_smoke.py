"""Playwright golden-path smoke test for the operator UI.

Requires:
- SPA build present at ``aaiclick/server/static/index.html``
  (produced by ``npm run build``).
- ``playwright`` Python package installed
  (skipped automatically with ``pytest.importorskip`` if absent).

Run with::

    pytest test_e2e/web/test_smoke.py -v -p no:cov

The ``base_url`` and ``page`` fixtures are provided by
``test_e2e/web/conftest.py``, which launches a real uvicorn server
against the default chdb + SQLite backend on a free port.

This suite is excluded from the default ``pytest`` testpaths and only
runs when the path is passed explicitly or in a dedicated CI workflow."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from helpers import login_if_needed

from aaiclick.backend import is_local

# Guard 1: the SPA build must exist.
STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

# Guard 2: Playwright must be installed.  pytest.importorskip records a SKIP
# reason that appears in the pytest output — do not raise ImportError here.
pytest.importorskip("playwright.sync_api")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_home_loads(page, base_url: str) -> None:
    """Root URL renders the SPA shell (header + content area)."""
    page.goto(f"{base_url}/")
    page.wait_for_selector("#root")
    login_if_needed(page)
    # The header prompt input is present.
    page.wait_for_selector("#prompt")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_jobs_view_loads(page, base_url: str) -> None:
    """Navigating to /?p=@jobs shows the jobs view."""
    page.goto(f"{base_url}/?p=@jobs")
    page.wait_for_selector("#root")
    login_if_needed(page)
    # The prompt input is populated with the value from the URL.
    prompt_val = page.input_value("#prompt")
    assert prompt_val == "@jobs"


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
def test_prompt_updates_url(page, base_url: str) -> None:
    """Typing into the prompt input updates the URL query parameter."""
    page.goto(f"{base_url}/")
    login_if_needed(page)
    page.wait_for_selector("#prompt")
    page.fill("#prompt", "@registered")
    # After typing, the URL should contain ?p=@registered.
    page.wait_for_url(lambda url: "p=%40registered" in url or "p=@registered" in url)


def _run_task_and_wait(page, base_url: str, entrypoint: str) -> str:
    """Submit a job for ``entrypoint``, wait for it to complete, return task id.

    Uses Playwright's API request context (same origin, auth off in local mode).
    The id comes back as a JSON *string* — snowflakes exceed JS's safe-integer
    range — and is carried verbatim into the ``@task`` routes below.
    """
    api = f"{base_url}/api/v0"
    resp = page.request.post(f"{api}/jobs:run", data={"name": entrypoint})
    assert resp.ok, resp.text()
    job_id = resp.json()["id"]

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        detail = page.request.get(f"{api}/jobs/{job_id}").json()
        tasks = detail.get("tasks") or []
        if tasks and tasks[0]["status"] == "COMPLETED":
            return tasks[0]["id"]
        time.sleep(0.5)
    raise AssertionError("task did not reach COMPLETED within 30 s")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
@pytest.mark.skipif(
    not is_local(),
    reason="needs auth-off + an in-process worker (local_runtime), both local-mode only; "
    "the distributed e2e job enforces auth and runs no worker",
)
def test_task_view_shows_logs(page, base_url: str) -> None:
    """The task view renders captured logs (local mode only).

    Runs a job that prints to stdout/stderr, opens ``@task <id>``, and asserts
    the log viewer shows the printed lines — exercising the cross-host
    ``task_logs`` read path, the per-stream styling, and the 64-bit string-id
    round-trip end to end (a rounded id would 404 and show no logs).

    Local-mode only: it drives ``/jobs:run`` unauthenticated and relies on the
    in-process worker that ``local_runtime`` starts — the distributed e2e job
    enforces auth (401) and runs no worker, so the job would never execute."""
    task_id = _run_task_and_wait(page, base_url, "aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

    page.goto(f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector("#root")
    login_if_needed(page)

    logs = page.locator("div.logs")
    logs.get_by_text("This is stdout").wait_for(timeout=15000)
    logs.get_by_text("Error message").wait_for(timeout=15000)

    # Stream provenance: stderr lines carry the src-stderr marker, stdout lines don't.
    assert logs.locator(".src-stderr", has_text="Error message").count() == 1
    assert logs.locator(".src-stderr", has_text="This is stdout").count() == 0


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
@pytest.mark.skipif(
    not is_local(),
    reason="needs auth-off + an in-process worker (local_runtime), both local-mode only; "
    "the distributed e2e job enforces auth and runs no worker",
)
def test_task_view_colors_logs_by_level(page, base_url: str) -> None:
    """The task view colors lines by level and shows timestamps only when toggled."""
    task_id = _run_task_and_wait(page, base_url, "aaiclick.orchestration.fixtures.sample_tasks.task_with_log_levels")

    page.goto(f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector("#root")
    login_if_needed(page)

    logs = page.locator("div.logs")
    logs.get_by_test_id("log-line-ERROR").get_by_text("error line").wait_for(timeout=15000)
    logs.get_by_test_id("log-line-WARNING").get_by_text("warning line").wait_for(timeout=15000)

    error_color = logs.locator(".lvl-ERROR").first.evaluate("el => getComputedStyle(el).color")
    assert error_color

    assert logs.locator(".ts").count() == 0
    page.get_by_label("Show timestamps").check()
    logs.locator(".ts").first.wait_for(timeout=5000)


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
@pytest.mark.skipif(
    not is_local(),
    reason="needs auth-off + an in-process worker (local_runtime), both local-mode only; "
    "the distributed e2e job enforces auth and runs no worker",
)
def test_job_graph_view_renders_nodes(page, base_url: str) -> None:
    """`@job <ref> graph` renders a React Flow canvas with a node per task."""
    api = f"{base_url}/api/v0"
    entrypoint = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
    resp = page.request.post(f"{api}/jobs:run", data={"name": entrypoint})
    assert resp.ok, resp.text()
    job_id = resp.json()["id"]

    # The graph endpoint is authoritative — poll it before driving the browser
    # so a render failure is not confused with the job not having started.
    graph = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        graph = page.request.get(f"{api}/jobs/{job_id}/graph").json()
        if graph.get("nodes"):
            break
        time.sleep(0.5)
    assert graph and graph["nodes"], "graph endpoint returned no nodes"

    page.goto(f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("#root")
    login_if_needed(page)

    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.locator(".gnode").first.wait_for(timeout=15000)
    assert page.locator(".gnode").count() >= 1


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
@pytest.mark.skipif(
    not is_local(),
    reason="needs auth-off + an in-process worker (local_runtime), both local-mode only; "
    "the distributed e2e job enforces auth and runs no worker",
)
def test_task_view_meta_cells_do_not_overflow(page, base_url: str) -> None:
    """Long values wrap inside their grid cell instead of overlapping the next.

    Grid items default to ``min-width: auto`` and refuse to shrink below their
    content, so an unbreakable entrypoint or snowflake id used to spill across
    the neighbouring column and render two values on top of each other.
    """
    task_id = _run_task_and_wait(page, base_url, "aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

    page.goto(f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector("#root")
    login_if_needed(page)
    page.wait_for_selector(".meta div")

    overflowing = page.eval_on_selector_all(
        ".meta div",
        "els => els.filter(el => el.scrollWidth > el.clientWidth).map(el => el.textContent)",
    )

    assert overflowing == [], f"meta cells overflow their column: {overflowing}"
