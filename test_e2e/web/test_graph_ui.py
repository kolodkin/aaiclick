"""Browser tests for the job graph view, driven by a seeded multi-state graph.

Run with::

    pytest test_e2e/web/test_graph_ui.py -v -p no:cov

Uses the ``base_url`` / ``page`` fixtures from ``conftest.py`` and the fixture
job from ``seed.py``. Excluded from the default ``pytest`` testpaths — this
suite only runs when its path is passed explicitly or in a dedicated workflow.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from helpers import login_if_needed

from aaiclick.backend import is_local
from aaiclick.orchestration.models import JOB_COMPLETED, TASK_COMPLETED, TASK_RUNNING

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

from seed import DEFAULT_STATES, TaskState, seed_graph_job  # noqa: E402  (must follow the playwright guard)

# Every status the graph can render, and the node class each one produces.
_EXPECTED_STATUSES = [
    "COMPLETED",
    "RUNNING",
    "CLAIMED",
    "FAILED",
    "UPSTREAM_FAILED",
    "CANCELLED",
    "PENDING",
]

_NODE_COUNT = 9
# `inject_build_tasks` fans a build out to everything sharing its image, so the
# seed has 8 pipeline edges plus 8 build dependencies. The build dependencies
# are collapsed into per-node badges, never drawn, so only the pipeline is.
_PIPELINE_EDGE_COUNT = 8
_BUILD_GATED_COUNT = 8

pytestmark = [
    pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`"),
    pytest.mark.skipif(
        not is_local(),
        reason="seeds the local SQLite database directly; the distributed e2e job "
        "runs against remote Postgres and enforces auth",
    ),
]


@pytest.fixture(scope="module")
def seeded_job_id() -> int:
    """Seed the demo graph once per module and return its job id.

    Runs on its own thread: ``pytest-asyncio`` is in auto mode so a loop is
    already running here, and Playwright's sync API cannot be driven from
    inside one either. A dedicated thread gives the seeding a clean loop and
    leaves the test thread loop-free.
    """
    return _seed("graph_ui_demo")


def _seed(name: str, **kwargs) -> int:
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(seed_graph_job(name, **kwargs))).result()


@pytest.fixture()
def graph_page(page, base_url: str, seeded_job_id: int):
    """Open the seeded job's graph view with every node rendered."""
    page.goto(f"{base_url}/?p=@job {seeded_job_id} graph")
    page.wait_for_selector("#root")
    login_if_needed(page)
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.locator(".gnode").first.wait_for(timeout=15000)
    page.wait_for_function(
        "count => document.querySelectorAll('.gnode').length === count",
        arg=_NODE_COUNT,
        timeout=15000,
    )
    return page


def test_graph_renders_every_task_and_edge(graph_page) -> None:
    """All 9 seeded tasks reach the canvas, with the pipeline edges drawn.

    The build's 8 dependencies are collapsed into badges, so the drawn edges
    are the pipeline alone.
    """
    assert graph_page.locator(".gnode").count() == _NODE_COUNT
    assert graph_page.locator(".react-flow__edge").count() == _PIPELINE_EDGE_COUNT


@pytest.mark.parametrize("status", _EXPECTED_STATUSES)
def test_graph_colors_each_status(graph_page, status: str) -> None:
    """Each task status renders its own node class, so colours are distinct."""
    assert graph_page.locator(f".gnode-{status}").count() >= 1


def test_build_dependencies_render_as_badges_not_edges(graph_page) -> None:
    """The build gates every other task, shown once per node rather than as
    N-1 edges crossing the canvas."""
    assert graph_page.locator(".gnode-build").count() == 1
    assert graph_page.locator("[data-testid='build-gate']").count() == _BUILD_GATED_COUNT

    # No build edge is ever drawn — the badge replaces them outright.
    assert graph_page.locator(".react-flow__edge").count() == _PIPELINE_EDGE_COUNT

    # The build task itself carries no badge; it is the build.
    build_node = graph_page.locator(".gnode-build")
    assert build_node.locator("[data-testid='build-gate']").count() == 0


def test_build_badge_reflects_build_status(page, base_url: str) -> None:
    """The badge is coloured by the build's own status, so a stalled or failed
    build is visible from any task it blocks."""
    job_id = _seed("graph_ui_building", states={"build_image": TaskState(TASK_RUNNING, None, 0, None)})

    page.goto(f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("#root")
    login_if_needed(page)
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.wait_for_function(
        "count => document.querySelectorAll('.gnode-buildgate-RUNNING').length === count",
        arg=_BUILD_GATED_COUNT,
        timeout=15000,
    )

    assert page.locator(".gnode-buildgate-RUNNING").count() == _BUILD_GATED_COUNT
    # Still no drawn build edges, even while the build is in flight.
    assert page.locator(".react-flow__edge").count() == _PIPELINE_EDGE_COUNT


def test_clicking_build_badge_opens_the_build_task(graph_page, seeded_job_id: int) -> None:
    """The badge is the way into the build's own detail and logs."""
    graph = graph_page.request.get(f"{graph_page.url.split('/?')[0]}/api/v0/jobs/{seeded_job_id}/graph").json()
    build_id = next(n["id"] for n in graph["nodes"] if n["is_image_build"])

    graph_page.locator("[data-testid='build-gate']").first.click()
    graph_page.wait_for_function("id => document.querySelector('#prompt').value === `@task ${id}`", arg=str(build_id))

    assert graph_page.input_value("#prompt") == f"@task {build_id}"


def test_graph_expands_group_to_source_and_sink_only(graph_page, seeded_job_id: int) -> None:
    """``extract >> group`` reaches only the group's source task, and
    ``group >> report`` leaves only from its sink — not from every member."""
    graph = graph_page.request.get(f"{graph_page.url.split('/?')[0]}/api/v0/jobs/{seeded_job_id}/graph").json()
    by_id = {n["id"]: n["name"] for n in graph["nodes"]}
    edges = {(by_id[e["source_id"]], by_id[e["target_id"]]) for e in graph["edges"]}

    assert ("extract", "transform_a") in edges
    assert ("extract", "transform_b") not in edges
    assert ("transform_b", "report") in edges
    assert ("transform_a", "report") not in edges


def test_clicking_a_node_navigates_to_the_task(graph_page) -> None:
    """Clicking a node sets the prompt to that task's route."""
    graph_page.locator(".gnode").first.click()
    graph_page.wait_for_function("() => document.querySelector('#prompt').value.startsWith('@task ')")

    assert graph_page.input_value("#prompt").startswith("@task ")


def test_toggle_switches_between_table_and_graph(page, base_url: str, seeded_job_id: int) -> None:
    """The Table/Graph chips move the prompt between the two views."""
    page.goto(f"{base_url}/?p=@job {seeded_job_id}")
    page.wait_for_selector("#root")
    login_if_needed(page)
    page.wait_for_selector("table")
    assert page.locator("[data-testid='job-graph']").count() == 0

    page.get_by_text("Graph", exact=True).click()
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)

    assert page.input_value("#prompt").endswith(" graph")


def test_seed_accepts_custom_states(page, base_url: str) -> None:
    """A caller-supplied state map overrides the defaults, so a UI test can
    stage any scenario without a second fixture graph."""
    all_green = {name: TaskState(TASK_COMPLETED, None, 0, 30) for name in DEFAULT_STATES}
    job_id = _seed("graph_ui_all_green", states=all_green, job_status=JOB_COMPLETED)

    page.goto(f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("#root")
    login_if_needed(page)
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.wait_for_function(
        "count => document.querySelectorAll('.gnode-COMPLETED').length === count",
        arg=_NODE_COUNT,
        timeout=15000,
    )

    assert page.locator(".gnode-COMPLETED").count() == _NODE_COUNT
    assert page.locator(".gnode-FAILED").count() == 0
