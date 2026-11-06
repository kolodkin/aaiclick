"""Playwright coverage for the viewer modes (``@data``, ``@query``, ``@dashboard``).

The ``orders`` object and ``sales`` dashboard come from ``seed.py`` (``viewer``),
run by ``conftest.py`` before the server starts.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers import open_page

from aaiclick.backend import is_local

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"
# Playwright is installed only for the e2e job, so its API is reached through
# the guard's return value — importing the package would break the lint job.
playwright_api = pytest.importorskip("playwright.sync_api")

from seed import OBJECTS_JOB, WIDE_JOB  # noqa: E402  (must follow the playwright guard)

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
_local_only = pytest.mark.skipif(not is_local(), reason="the viewer seed runs through chdb (local mode)")


@_spa_built
@_local_only
def test_data_lists_and_previews_object(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data")
    row = page.get_by_test_id("objects-table").get_by_text("orders", exact=True)
    row.wait_for(timeout=15000)
    row.click()
    page.wait_for_url(lambda url: "orders" in url)
    rows = page.get_by_test_id("object-rows")
    rows.wait_for(timeout=15000)
    assert rows.locator("tbody tr").count() == 3


@_spa_built
@_local_only
def test_data_all_scopes_labels_and_opens_job_object(page, shot, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data")
    # Earlier seed runs may leave rows of the same name; any one will do.
    row = page.get_by_test_id("objects-table").locator("tr", has_text="raw_sales").first
    playwright_api.expect(row.locator("td").nth(1)).to_have_text("job")
    playwright_api.expect(row.locator("td").nth(2)).to_have_text(OBJECTS_JOB)
    shot("data-all-scopes")
    row.click()
    page.wait_for_url(lambda url: "job" in url and "raw_sales" in url)
    page.get_by_test_id("object-rows").wait_for(timeout=15000)
    shot("data-job-object")


@_spa_built
@_local_only
def test_query_all_scopes_picker_opens_job_object(page, shot, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@query")
    picker = page.get_by_test_id("query-object")
    option = picker.locator("option", has_text=f"raw_sales (job - {OBJECTS_JOB})").first
    option.wait_for(state="attached", timeout=15000)
    picker.select_option(option.get_attribute("value"))
    page.wait_for_url(lambda url: "job" in url and "raw_sales" in url)
    page.get_by_test_id("query-panel").wait_for(timeout=15000)
    shot("query-job-object")


@_spa_built
@_local_only
def test_query_runs_and_pages(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@query orders")
    page.get_by_test_id("query-panel").wait_for(timeout=15000)
    page.get_by_test_id("query-where").fill("amount >= 20")
    page.get_by_test_id("query-limit").fill("1")
    page.get_by_test_id("query-run").click()
    out = page.get_by_test_id("query-output")
    out.wait_for(timeout=15000)
    assert out.locator("tbody tr").count() == 1
    page.get_by_test_id("query-next").click()
    page.wait_for_function("() => document.querySelector('[data-testid=query-offset]').value === '1'")
    assert out.locator("tbody tr").count() == 1


@_spa_built
@_local_only
def test_dashboard_renders_saved_dashboard(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@dashboard sales")
    # The frame is sandboxed without allow-same-origin, so its document is not
    # reachable from the page; assert through Playwright's frame locator.
    title = page.frame_locator("[data-testid='dashboard-frame']").locator("#title")
    playwright_api.expect(title).to_have_text("rows:3", timeout=15000)


@_spa_built
@_local_only
def test_job_objects_menu_opens_object(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {OBJECTS_JOB}")
    menu = page.get_by_test_id("job-objects")
    menu.locator("summary").click()
    items = menu.locator("li a")
    assert items.all_inner_texts() == [
        "customers",
        "daily_totals",
        "orders_clean",
        "raw_sales",
        "returns",
        "weekly_totals",
    ]
    menu.get_by_test_id("objects-filter").fill("RAW")
    playwright_api.expect(items).to_have_text(["raw_sales"])
    # The toggle flips where a click opens the object; back off for the click below.
    toggle = menu.get_by_test_id("objects-new-tab")
    toggle.click()
    assert items.get_by_text("raw_sales").get_attribute("target") == "_blank"
    shot("job-objects-menu")
    toggle.click()
    assert items.get_by_text("raw_sales").get_attribute("target") is None
    items.get_by_text("raw_sales").click()
    page.wait_for_url(lambda url: "raw_sales" in url)
    page.get_by_test_id("object-rows").wait_for(timeout=15000)
    shot("job-objects-opened")


@_spa_built
@_local_only
def test_job_objects_menu_on_phone(page, base_url: str, shot) -> None:
    # The long job name wraps the button to the left edge; the popup must stay on screen.
    page.set_viewport_size({"width": 412, "height": 900})
    open_page(page, f"{base_url}/?p=@job {OBJECTS_JOB}")
    menu = page.get_by_test_id("job-objects")
    menu.locator("summary").click()
    box = menu.locator(".objects-popup").bounding_box()
    assert box["x"] >= 0 and box["x"] + box["width"] <= 412, box
    shot("job-objects-menu-phone")


@_spa_built
@_local_only
def test_task_objects_menu_on_phone(page, base_url: str, shot) -> None:
    page.set_viewport_size({"width": 412, "height": 900})
    open_page(page, f"{base_url}/?p=@job {OBJECTS_JOB}")
    page.get_by_text("extract", exact=True).click()
    page.wait_for_url(lambda url: "%40task" in url or "@task" in url)
    menu = page.get_by_test_id("job-objects")
    menu.locator("summary").click()
    playwright_api.expect(menu.locator("li a")).to_have_count(6)
    shot("task-objects-menu-phone")
    page.keyboard.press("Escape")
    playwright_api.expect(menu.locator("li a").first).to_be_hidden()


@_spa_built
@_local_only
def test_data_job_object_on_phone(page, base_url: str, shot) -> None:
    page.set_viewport_size({"width": 412, "height": 900})
    open_page(page, f"{base_url}/?p=@data")
    page.get_by_test_id("objects-table").locator("tr", has_text="raw_sales").first.click()
    page.get_by_test_id("object-rows").wait_for(timeout=15000)
    playwright_api.expect(page.locator(".chip", has_text="←").last).to_contain_text(f"{OBJECTS_JOB} #")
    jobs = page.locator(".scope-jobs")
    small = jobs.bounding_box()["height"]
    shot("data-job-object-phone")
    page.get_by_test_id("scope-jobs-toggle").click()
    playwright_api.expect(jobs).to_have_class("scope-jobs is-expanded")
    assert jobs.bounding_box()["height"] > small
    shot("data-job-list-expanded-phone")


@_spa_built
@_local_only
def test_wide_job_table_and_graph(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {WIDE_JOB}")
    page.get_by_text("part_000", exact=True).wait_for(timeout=15000)
    shot("job-table-100-tasks")
    open_page(page, f"{base_url}/?p=@job {WIDE_JOB} graph")
    # The canvas renders only on-screen nodes, so wait on the hub, not a count.
    page.locator(".gnode", has_text="fan_out").wait_for(timeout=15000)
    shot("job-graph-100-tasks")


@_spa_built
@_local_only
def test_data_scrolls_old_job_into_view(page, base_url: str) -> None:
    # Mid-page, not last: a job created meanwhile shifts the page by one.
    old = page.request.get(f"{base_url}/api/v0/jobs").json()["items"][30]
    open_page(page, f"{base_url}/?p=@data job {old['id']}")
    page.wait_for_function(
        """() => {
            const box = document.querySelector('.scope-jobs')?.getBoundingClientRect();
            const row = document.querySelector('.scope-jobs .is-active')?.getBoundingClientRect();
            return box && row && row.top >= box.top && row.bottom <= box.bottom;
        }""",
        timeout=15000,
    )
