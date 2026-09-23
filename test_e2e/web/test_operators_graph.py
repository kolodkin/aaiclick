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
from helpers import job_graph, job_result, open_page, submit_job, wait_for_job

from aaiclick.orchestration.models import JOB_COMPLETED

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

pytestmark = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")

_PIPELINES = "aaiclick.orchestration.fixtures.operator_pipelines"


class Scenario(NamedTuple):
    """One operator job and what its run must produce."""

    job: str
    part_name: str
    part_count: int
    groups: list[str]
    result: list


# Five rows at partition=2: map makes 3 parts in one group; reduce makes layers
# of 3, 2 and 1 parts. Task nodes are the entry task, create_values, the
# expander, the parts, the finalize task and the consumer.
MAP = Scenario("map_pipeline", "_map_part", 3, ["map"], [2, 4, 6, 8, 10])
REDUCE = Scenario("reduce_pipeline", "_reduce_part", 6, ["layer_0", "layer_1", "layer_2"], [15])


def _task_count(scenario: Scenario) -> int:
    return 5 + scenario.part_count


@pytest.mark.parametrize("scenario", [MAP, REDUCE], ids=["map", "reduce"])
def test_operator_graph_draws_the_run(page, base_url: str, shot, scenario: Scenario) -> None:
    """The consumer reads the filled output, and the graph shows every runtime
    node: the parts inside their group frames, the finalize join, and the hold
    edge from finalize to the consumer."""
    job_id = submit_job(scenario.job, f"{_PIPELINES}.{scenario.job}")
    wait_for_job(job_id, JOB_COMPLETED)

    assert job_result(job_id) == scenario.result

    graph = job_graph(job_id)
    names = {n["id"]: n["name"] for n in graph["nodes"]}
    kinds = Counter((n["kind"], n["name"]) for n in graph["nodes"])
    edges = {(names[e["source_id"]], names[e["target_id"]]) for e in graph["edges"]}

    assert kinds[("task", scenario.part_name)] == scenario.part_count
    assert [name for kind, name in kinds if kind == "group"] == scenario.groups
    assert (scenario.groups[-1], "_finalize") in edges
    assert ("_finalize", "read_values") in edges

    open_page(page, f"{base_url}/?p=@job {job_id} graph")
    page.wait_for_selector("[data-testid='job-graph']", timeout=15000)
    page.wait_for_function(
        "count => document.querySelectorAll('.gnode-COMPLETED').length === count",
        arg=_task_count(scenario),
        timeout=15000,
    )
    shot(f"graph-{scenario.job}")

    assert page.locator(".gnode").count() == _task_count(scenario)
    assert page.locator("[data-testid='group-node']").count() == len(scenario.groups)
    assert page.locator(".ggroup-COMPLETED").count() == len(scenario.groups)
