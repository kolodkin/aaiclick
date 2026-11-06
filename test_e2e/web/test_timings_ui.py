"""Browser tests for the job Timings and Duration tabs, on the seeded graph job.

Run with::

    pytest test_e2e/web/test_timings_ui.py -v -p no:cov
"""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers import open_page, run_in_process

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"

pytest.importorskip("playwright.sync_api")

from seed import DEFAULT_STATES, seed_graph_job  # noqa: E402  (must follow the playwright guard)

pytestmark = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")


@pytest.fixture(scope="module")
def seeded_job_id() -> int:
    return run_in_process(lambda: seed_graph_job("timings_ui_demo"))


def test_timings_tab_renders_a_row_per_task(page, base_url: str, seeded_job_id: int, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {seeded_job_id} timings")
    page.wait_for_selector("[data-testid='job-timings']", timeout=15000)
    shot("timings-seeded")

    assert page.locator("[data-testid='timings-row']").count() == len(DEFAULT_STATES)
    assert page.locator(".tl-bar").count() >= 1
    off_axis = page.evaluate(
        """() => [...document.querySelectorAll('.tl-bar')].filter((bar) => {
            const b = bar.getBoundingClientRect(), t = bar.parentElement.getBoundingClientRect();
            return b.left < t.left - 1 || b.right > t.right + 1;
        }).length"""
    )
    assert off_axis == 0


def test_duration_tab_ranks_longest_first(page, base_url: str, seeded_job_id: int, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {seeded_job_id} duration")
    page.wait_for_selector("[data-testid='duration-row']", timeout=15000)
    shot("duration-seeded")

    times = page.locator("[data-testid='duration-row'] .tl-num").all_inner_texts()
    seconds = [float(t.rstrip("s")) if t.endswith("s") and "m" not in t else 1e9 for t in times]
    assert seconds == sorted(seconds, reverse=True)
