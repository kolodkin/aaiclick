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

import re
import uuid
from pathlib import Path

import pytest
from helpers import TaskRow, job_tasks, login_if_needed, open_page, run_in_process, submit_job, wait_for_task

# Guard 1: the SPA build must exist.
STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

# Guard 2: Playwright must be installed.  pytest.importorskip records a SKIP
# reason that appears in the pytest output — do not raise ImportError here.
pytest.importorskip("playwright.sync_api")

from seed import seed_graph_job  # noqa: E402  (must follow the playwright guard)

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")

# The fallback poll interval the app uses while the stream is down, read from
# the source instead of duplicated: raising it there must not leave the
# live-update tests below passing against too short an idle window. A miss
# falls back to a deliberately generous window, so reformatting that line
# costs a slower test rather than a collection error for this whole file.
_MAIN_TSX = Path(__file__).resolve().parents[2] / "src" / "main.tsx"
_FALLBACK_MATCH = re.search(r"isLiveConnected\(\)\s*\?\s*false\s*:\s*(\d+)", _MAIN_TSX.read_text())
POLL_FALLBACK_MS = int(_FALLBACK_MATCH.group(1)) if _FALLBACK_MATCH else 5000


@_spa_built
def test_home_loads(page, base_url: str, shot) -> None:
    """Root URL renders the SPA shell (header + content area)."""
    open_page(page, f"{base_url}/")
    # The header prompt input is present.
    page.wait_for_selector("#prompt")
    shot("home")


@_spa_built
def test_jobs_view_loads(page, base_url: str, shot) -> None:
    """Navigating to /?p=@jobs shows the jobs view."""
    open_page(page, f"{base_url}/?p=@jobs")
    # The prompt input is populated with the value from the URL.
    prompt_val = page.input_value("#prompt")
    assert prompt_val == "@jobs"
    shot("jobs-view")


@_spa_built
def test_prompt_updates_url(page, base_url: str, shot) -> None:
    """Typing into the prompt input updates the URL query parameter."""
    page.goto(f"{base_url}/")
    login_if_needed(page)
    page.wait_for_selector("#prompt")
    page.fill("#prompt", "@registered")
    # After typing, the URL should contain ?p=@registered.
    page.wait_for_url(lambda url: "p=%40registered" in url or "p=@registered" in url)
    shot("prompt-registered")


def _run_task_and_wait(entrypoint: str) -> str:
    """Create a job for ``entrypoint``, wait for its task to complete, return the task id.

    In-process, like every job the suite creates: no REST call, so no auth in
    either mode, and whichever worker the mode runs executes it.
    """
    job_id = submit_job(entrypoint.rsplit(".", 1)[-1], entrypoint)
    return wait_for_task(job_id, "COMPLETED").id


@_spa_built
def test_task_view_shows_logs(page, base_url: str, shot) -> None:
    """The task view renders captured logs (local mode only).

    Runs a job that prints to stdout/stderr, opens ``@task <id>``, and asserts
    the log viewer shows the printed lines — exercising the cross-host
    ``task_logs`` read path, the per-stream styling, and the 64-bit string-id
    round-trip end to end (a rounded id would 404 and show no logs).

    Local-mode only: it drives ``/jobs:run`` unauthenticated and relies on the
    in-process worker that ``local_runtime`` starts — the distributed e2e job
    enforces auth (401) and runs no worker, so the job would never execute."""
    task_id = _run_task_and_wait("aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

    open_page(page, f"{base_url}/?p=@task {task_id}")

    logs = page.locator("div.logs")
    logs.get_by_text("This is stdout").wait_for(timeout=15000)
    logs.get_by_text("Error message").wait_for(timeout=15000)
    shot("task-logs")

    # Stream provenance: stderr lines carry the src-stderr marker, stdout lines don't.
    assert logs.locator(".src-stderr", has_text="Error message").count() == 1
    assert logs.locator(".src-stderr", has_text="This is stdout").count() == 0


@_spa_built
def test_task_view_colors_logs_by_level(page, base_url: str, shot) -> None:
    """The task view colors lines by level and shows timestamps only when toggled."""
    task_id = _run_task_and_wait("aaiclick.orchestration.fixtures.sample_tasks.task_with_log_levels")

    open_page(page, f"{base_url}/?p=@task {task_id}")

    logs = page.locator("div.logs")
    logs.get_by_test_id("log-line-ERROR").get_by_text("error line").wait_for(timeout=15000)
    logs.get_by_test_id("log-line-WARNING").get_by_text("warning line").wait_for(timeout=15000)

    error_color = logs.locator(".lvl-ERROR").first.evaluate("el => getComputedStyle(el).color")
    assert error_color

    assert logs.locator(".ts").count() == 0
    shot("task-logs-levels")
    page.get_by_label("Show timestamps").check()
    logs.locator(".ts").first.wait_for(timeout=5000)
    shot("task-logs-timestamps")


@_spa_built
def test_job_graph_view_renders_nodes(page, base_url: str, shot) -> None:
    """`@job <ref> graph` renders a React Flow canvas with a node per task."""
    entrypoint = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
    job_id = submit_job("simple_task", entrypoint)
    # The task row is authoritative — wait for it before driving the browser
    # so a render failure is not confused with the job not having started.
    wait_for_task(job_id)

    open_page(page, f"{base_url}/?p=@job {job_id} graph")

    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.locator(".gnode").first.wait_for(timeout=15000)
    assert page.locator(".gnode").count() >= 1
    shot("job-graph-simple")


@_spa_built
def test_task_view_meta_cells_do_not_overflow(page, base_url: str, shot) -> None:
    """Long values wrap inside their grid cell instead of overlapping the next.

    Grid items default to ``min-width: auto`` and refuse to shrink below their
    content, so an unbreakable entrypoint or snowflake id used to spill across
    the neighbouring column and render two values on top of each other.
    """
    task_id = _run_task_and_wait("aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

    open_page(page, f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector(".meta div")
    shot("task-meta")

    overflowing = page.eval_on_selector_all(
        ".meta div",
        "els => els.filter(el => el.scrollWidth > el.clientWidth).map(el => el.textContent)",
    )

    assert overflowing == [], f"meta cells overflow their column: {overflowing}"


@_spa_built
def test_task_view_truncates_long_entrypoint_from_the_start(page, base_url: str, shot) -> None:
    """An over-long entrypoint stays on one line, keeps its tail, and expands.

    The elision is done in CSS so it fits the column exactly; the assertions
    therefore check rendered geometry, not a character count.
    """
    entrypoint = "aaiclick.orchestration.fixtures.sample_tasks.task_with_output"
    task_id = _run_task_and_wait(entrypoint)

    open_page(page, f"{base_url}/?p=@task {task_id}")

    value = page.locator(".meta [data-testid='truncated']")
    toggle = page.locator(".meta [data-testid='truncated-toggle']")
    value.wait_for(timeout=15000)

    # Collapsed: one line, and clipped (so an ellipsis is actually showing).
    collapsed_height = value.bounding_box()["height"]
    assert value.evaluate("el => el.scrollWidth > el.clientWidth")
    assert value.evaluate("el => getComputedStyle(el).direction") == "rtl"

    # The toggle is a real, visible control — not just a dotted underline.
    assert toggle.is_visible()
    assert toggle.inner_text() == "show full"
    shot("task-entrypoint-collapsed")

    toggle.click()
    page.wait_for_selector(".meta .truncated.is-expanded")
    shot("task-entrypoint-expanded")

    assert toggle.inner_text() == "show less"
    assert value.bounding_box()["height"] > collapsed_height
    assert value.inner_text() == entrypoint


@_spa_built
def test_jobs_view_updates_live_without_polling(page, base_url: str, shot) -> None:
    """A status change reaches the jobs list over ``/events``, not a poll.

    The request log is the evidence: over an idle window longer than the 2 s
    polling fallback the page must fetch ``/jobs`` zero times, and it must
    hold exactly one ``/events`` stream. Only then is a job created under a
    name unique to this run — its row appearing at the top as ``COMPLETED``
    shows the stream delivered. Rows carry no id and the list is capped, so
    the name is the discriminator; unique rather than merely unused by other
    tests because only local mode's SQLite root is per-session — distributed
    mode's Postgres outlives the run, and a previous run's row would be there.

    The view's own liveness badge is asserted alongside: a streamed page looks
    exactly like a polled one, so the badge is what makes the ``sse-*``
    screenshots readable evidence.
    """
    requests: list[str] = []
    page.on("request", lambda req: requests.append(req.url))
    open_page(page, f"{base_url}/?p=@jobs")
    page.wait_for_selector("table")

    seen = len(requests)
    idle_window = POLL_FALLBACK_MS + 500
    page.wait_for_timeout(idle_window)
    idle = [u for u in requests[seen:] if "/api/v0/jobs" in u]
    assert idle == [], f"jobs list polled while the stream was up: {idle}"
    streams = [u for u in requests if u.endswith("/api/v0/events")]
    assert len(streams) == 1, f"expected one open /events stream, saw {streams}"
    # The view reports it too, so a silent fallback to polling is visible to an
    # operator instead of looking identical to a working stream.
    assert "live" in page.get_by_test_id("live-status").inner_text()
    shot("sse-idle-no-polling")

    job_name = f"async_task_{uuid.uuid4().hex[:8]}"
    submit_job(job_name, "aaiclick.orchestration.fixtures.sample_tasks.async_task")
    newest = page.locator("tbody tr").first
    newest.get_by_text(job_name, exact=True).wait_for(timeout=5000)
    shot("sse-row-arrived")
    # No timer-driven fetch of /jobs happened in the idle window above, so
    # /events is the only path the row and its status could have taken.
    newest.get_by_text("COMPLETED", exact=True).wait_for(timeout=5000)
    shot("sse-row-completed")


SLOW_TASK = "aaiclick.orchestration.fixtures.sample_tasks.slow_task"
# Mirrors the `steps` default in that fixture: the last line it logs.
SLOW_TASK_STEPS = 20


def _submit_slow_job() -> tuple[str, TaskRow]:
    """Start ``slow_task`` and return ``(job_id, task)`` once it has a task.

    The task runs for a few seconds, so the caller has a window in which the
    UI is showing a non-terminal state that must then change on its own. Long
    enough that page load plus the first assertions land well inside its
    lifetime; the tests wait for the real end, not this number.
    """
    job_id = submit_job("slow_task", SLOW_TASK, {"seconds": 4})
    return job_id, wait_for_task(job_id)


@_spa_built
def test_job_graph_updates_live_without_polling(page, base_url: str, shot) -> None:
    """A node's status changes on screen while the graph endpoint is not polled.

    The jobs-list test proves a *row* arrives over the stream; this proves the
    same for a view with its own query. The graph is opened while its only
    task is still running, the request log is watched for ``/graph`` fetches,
    and the node is required to reach ``COMPLETED`` anyway — which it can only
    do if the invalidation came from ``/events``.
    """
    job_id, _ = _submit_slow_job()

    requests: list[str] = []
    page.on("request", lambda req: requests.append(req.url))
    open_page(page, f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    node = page.locator(".gnode").first
    node.wait_for(timeout=15000)

    status = page.get_by_test_id("live-status")
    assert status.get_attribute("data-mode") == "live"
    before = node.inner_text()
    assert "COMPLETED" not in before, f"task already finished before the graph opened: {before}"
    shot("sse-graph-running")

    seen = len(requests)
    node.get_by_text("COMPLETED", exact=True).wait_for(timeout=30000)
    graph_fetches = [u for u in requests[seen:] if "/graph" in u]
    assert graph_fetches, "the graph never refetched, so nothing could have changed on screen"
    # One refetch per change signal is expected; a *timer* would keep firing
    # after the task finished, so the proof is the absence of polling in the
    # idle window below rather than the count here.
    seen = len(requests)
    page.wait_for_timeout(POLL_FALLBACK_MS + 500)
    idle = [u for u in requests[seen:] if "/graph" in u]
    assert idle == [], f"graph polled while the stream was up: {idle}"
    shot("sse-graph-completed")


@_spa_built
def test_task_view_separates_streamed_status_from_polled_logs(page, base_url: str, shot) -> None:
    """The task record streams; its logs poll — and the view labels each.

    Both halves update on screen while the task runs, by different means:
    ``/tasks/{id}`` is invalidated by ``/events`` and must not be polled,
    while ``/tasks/{id}/logs`` has no commit to hang a signal on and keeps its
    own 2 s timer. Asserting both in one test keeps the distinction from
    quietly regressing into "everything polls" or "everything streams".
    """
    _, task = _submit_slow_job()
    task_id = task.id

    requests: list[str] = []
    page.on("request", lambda req: requests.append(req.url))
    open_page(page, f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector(".logs-toolbar")

    # The record reading "live" is what makes the logs reading "polling"
    # meaningful: the stream is demonstrably up, so the logs are on a timer by
    # their own policy rather than by falling back.
    badges = page.get_by_test_id("live-status")
    assert badges.nth(0).get_attribute("data-mode") == "live", "task record should report the stream"
    assert badges.nth(1).get_attribute("data-mode") == "polling", "logs should report their own timer"
    lines_before = page.locator(".log-line").count()
    shot("sse-task-running")

    seen = len(requests)
    page.get_by_text("COMPLETED", exact=True).first.wait_for(timeout=30000)
    log_polls = [u for u in requests[seen:] if u.endswith("/logs")]
    assert log_polls, "logs never refetched, so no new lines could have appeared"
    assert page.locator(".log-line").count() > lines_before, "log lines did not accumulate while the task ran"
    # Going terminal stops the timer, so the lines written since the last poll
    # need one final fetch. Nothing else would collect them — this key is not
    # in LIVE_KEYS, so no `changed` frame touches it — and the panel would sit
    # a poll interval short of the truth for good.
    page.get_by_text(f"step {SLOW_TASK_STEPS} of {SLOW_TASK_STEPS}").wait_for(timeout=10000)
    shot("sse-task-completed")

    # Terminal now, so nothing commits and no signal fires — and neither half
    # may fetch anyway. A timer would keep going regardless, which is exactly
    # the difference under test. (This says nothing about the stream being
    # down: the fallback interval is inert here because the stream is up.)
    seen = len(requests)
    page.wait_for_timeout(POLL_FALLBACK_MS + 500)
    idle = [u for u in requests[seen:] if f"/tasks/{task_id}" in u]
    assert idle == [], f"task view kept fetching after the task finished: {idle}"


@_spa_built
def test_task_view_says_a_queued_task_has_not_started(page, base_url: str, shot) -> None:
    """A task that has not run yet says so instead of "no logs captured".

    The three empty log states mean different things — nothing yet, nothing
    flushed, nothing at all — and only the last is a final answer. A queued
    task also has nothing to poll for, so the panel shows no polling badge.

    Uses the seeded demo graph: ``report`` waits on a task seeded as RUNNING
    with no process behind it, so no worker in either mode ever claims it and
    there is no race to catch it queued.
    """
    job_id = str(run_in_process(lambda: seed_graph_job("smoke_queued")))
    report = next(t for t in job_tasks(job_id) if t.name == "report")

    open_page(page, f"{base_url}/?p=@task {report.id}")
    panel = page.locator(".logs")
    panel.wait_for(timeout=15000)
    assert "has not started" in panel.inner_text()
    assert page.get_by_test_id("live-status").count() == 1, "a queued task's logs must not claim to be polling"
    shot("task-logs-not-started")
