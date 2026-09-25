"""Browser tests for the graph of a real ``map()`` / ``reduce()`` run.

Each job runs on the mode's worker: the expander creates the partition tasks
at runtime, the finalize task joins them, and a consumer task reads the output.
The result proves the consumer waited for every partition; the graph view then
draws every node the run produced, including the runtime-created ones.

Run with::

    pytest test_e2e/web/test_operators_graph.py -v -p no:cov
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import NamedTuple

import pytest
from helpers import job_graph, job_result, open_page, submit_job, wait_for_job_completed

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

pytestmark = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")

_PIPELINES = "aaiclick.orchestration.fixtures.operator_pipelines"


class Scenario(NamedTuple):
    """One operator job and what its run must produce."""

    job: str
    part_name: str
    part_count: int
    frame: str
    part_groups: list[str]
    result: list


# Five rows at partition=2: map makes 3 parts in one group; reduce makes layers
# of 3, 2 and 1 parts. Either way the part groups nest in one frame per call.
MAP = Scenario("map_pipeline", "_map_part", 3, "map", ["parts"], [2, 4, 6, 8, 10])
REDUCE = Scenario("reduce_pipeline", "_reduce_part", 6, "reduce", ["layer_0", "layer_1", "layer_2"], [15])

# Task nodes besides the parts: the entry task, create_values, the expander,
# the finalize task and the consumer.
_FIXED_TASKS = 5


@pytest.mark.parametrize("scenario", [MAP, REDUCE], ids=["map", "reduce"])
def test_operator_graph_draws_the_run(page, base_url: str, shot, scenario: Scenario) -> None:
    """The consumer reads the filled output, and the graph shows every runtime
    node: the parts inside their group frames, the finalize join, and the hold
    edge from finalize to the consumer — all drawn inside one frame per call."""
    job_id = submit_job(scenario.job, f"{_PIPELINES}.{scenario.job}")
    wait_for_job_completed(job_id)

    assert job_result(job_id) == scenario.result

    graph = job_graph(job_id)
    names = {n["id"]: n["name"] for n in graph["nodes"]}
    parents = Counter((n["name"], names.get(n["parent_group_id"])) for n in graph["nodes"])
    edges = {(names[e["source_id"]], names[e["target_id"]]) for e in graph["edges"]}
    groups = [n["name"] for n in graph["nodes"] if n["kind"] == "group"]

    assert sorted(groups) == sorted([scenario.frame, *scenario.part_groups])
    assert parents[(scenario.frame, None)] == 1
    assert all(parents[(group, scenario.frame)] == 1 for group in scenario.part_groups)
    assert parents[(f"_expand_{scenario.frame}", scenario.frame)] == 1
    assert parents[("_finalize", scenario.frame)] == 1
    assert sum(parents[(scenario.part_name, group)] for group in scenario.part_groups) == scenario.part_count
    assert (scenario.part_groups[-1], "_finalize") in edges
    assert ("_finalize", "read_values") in edges

    open_page(page, f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.wait_for_function(
        "count => document.querySelectorAll('.gnode-COMPLETED').length === count",
        arg=_FIXED_TASKS + scenario.part_count,
        timeout=15000,
    )
    shot(f"graph-{scenario.job}")

    assert page.locator(".gnode").count() == _FIXED_TASKS + scenario.part_count
    assert page.locator("[data-testid='group-node']").count() == len(groups)
    assert page.locator(".ggroup-COMPLETED").count() == len(groups)

    frame = _group_box(page, scenario.frame)
    for group in scenario.part_groups:
        inner = _group_box(page, group)
        assert frame["x"] <= inner["x"] and inner["x"] + inner["width"] <= frame["x"] + frame["width"]
        assert frame["y"] <= inner["y"] and inner["y"] + inner["height"] <= frame["y"] + frame["height"]


def _group_box(page, name: str) -> dict:
    """The on-screen box of the group frame titled ``name``."""
    title = page.get_by_text(name, exact=True)
    return page.locator("[data-testid='group-node']").filter(has=title).bounding_box()
