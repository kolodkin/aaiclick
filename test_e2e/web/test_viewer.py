"""Playwright coverage for the viewer modes (``@data``, ``@dashboard``).

The ``orders`` object and ``sales`` dashboard come from ``seed.py`` (``viewer``),
run by ``conftest.py`` before the server starts.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote_plus

import pytest
from helpers import open_page

from aaiclick.backend import is_local

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"
# Playwright is installed only for the e2e job, so its API is reached through
# the guard's return value — importing the package would break the lint job.
playwright_api = pytest.importorskip("playwright.sync_api")

from seed import OBJECTS_JOB, WIDE_JOB  # noqa: E402  (must follow the playwright guard)


def _job_id(page, base_url: str, name: str) -> str:
    """The latest run of the seeded job ``name``: the UI opens jobs by id only."""
    return page.request.get(f"{base_url}/api/v0/jobs", params={"name": name}).json()["items"][0]["id"]


_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
_local_only = pytest.mark.skipif(not is_local(), reason="the viewer seed runs through chdb (local mode)")


@_spa_built
@_local_only
def test_data_lists_and_previews_object(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data")
    row = page.get_by_test_id("objects-table").get_by_text("orders", exact=True)
    row.wait_for(timeout=15000)
    # Persistent objects only: a job's objects are on its own page.
    playwright_api.expect(page.get_by_test_id("objects-table").locator("tr", has_text="raw_sales")).to_have_count(0)
    row.click()
    page.wait_for_url(lambda url: "orders" in url)
    rows = page.get_by_test_id("object-rows")
    rows.wait_for(timeout=15000)
    assert rows.locator("tbody tr").count() == 3


@_spa_built
@_local_only
def test_object_view_orders_and_pages(page, shot, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data orders")
    out = page.get_by_test_id("object-rows")
    out.wait_for(timeout=15000)
    page.get_by_test_id("orderby-menu").click()
    page.locator("[data-testid=orderby-add][data-col=amount]").click()
    page.get_by_test_id("orderby-dir").click()
    first = out.locator("tbody tr").first
    playwright_api.expect(first).to_contain_text("30")
    page.get_by_test_id("fields-menu").click()
    page.locator("[data-testid=field-toggle][data-col=name]").click()
    playwright_api.expect(out.locator("thead th")).to_have_count(2)
    shot("data-object-fields")
    limit = page.get_by_test_id("object-limit")
    limit.fill("1")
    limit.press("Enter")
    playwright_api.expect(out.locator("tbody tr")).to_have_count(1)
    page.get_by_test_id("object-next").click()
    playwright_api.expect(page.get_by_test_id("object-offset")).to_have_text("offset 1")
    playwright_api.expect(first).to_contain_text("20")


@_spa_built
@_local_only
def test_dashboard_renders_saved_dashboard(page, shot, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@dashboard sales")
    # The frame is sandboxed without allow-same-origin, so its document is not
    # reachable from the page; assert through Playwright's frame locator.
    frame = page.frame_locator("[data-testid='dashboard-frame']")
    title = frame.locator("#title")
    # `min` opens on its first option (15): two of the three orders pass.
    playwright_api.expect(title).to_have_text("rows:2", timeout=15000)
    # The page asks for a re-run through setParams; the frame is not reloaded.
    frame.locator("#raise").click()
    playwright_api.expect(title).to_have_text("rows:1", timeout=15000)
    shot("dashboard-params")


@_spa_built
@_local_only
def test_job_data_tab_opens_object(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, OBJECTS_JOB)}")
    page.get_by_test_id("job-data-tab").click()
    page.wait_for_url(lambda url: " data" in unquote_plus(url))
    table = page.get_by_test_id("objects-table")
    playwright_api.expect(table.locator("tr", has_text="extract.limit")).to_have_count(1)
    shot("job-data-tab")
    table.locator("tr", has_text="raw_sales").click()
    page.wait_for_url(lambda url: "raw_sales" in url)
    page.get_by_test_id("object-rows").wait_for(timeout=15000)
    # Still the job page: its header and tabs stay around the object.
    playwright_api.expect(page.locator("h2").first).to_contain_text(OBJECTS_JOB)
    shot("job-data-object")
    page.locator(".chip", has_text="←").last.click()
    page.wait_for_url(lambda url: unquote_plus(url).endswith(" data"))
    playwright_api.expect(table.locator("tr", has_text="raw_sales")).to_have_count(1)
    # Leaving the tab stays on this run: a name would resolve to the latest one.
    job_id = unquote_plus(page.url).split("@job ")[1].split(" ")[0]
    page.locator(".chip", has_text="Table").click()
    page.wait_for_url(lambda url: unquote_plus(url).endswith(f"@job {job_id}"), timeout=5000)


@_spa_built
@_local_only
def test_task_data_tab(page, base_url: str, shot) -> None:
    page.set_viewport_size({"width": 412, "height": 900})
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, OBJECTS_JOB)}")
    page.get_by_text("extract", exact=True).click()
    page.wait_for_url(lambda url: "@task" in unquote_plus(url))
    page.get_by_test_id("task-data-tab").click()
    rows = page.get_by_test_id("objects-table").locator("tbody tr")
    # Only this task's input and result: no objects, no other task's rows, no filter chips.
    playwright_api.expect(rows).to_have_count(2)
    playwright_api.expect(rows.filter(has_text="extract.limit")).to_contain_text("1000")
    playwright_api.expect(rows.filter(has_text="extract.result")).to_have_count(1)
    playwright_api.expect(page.get_by_test_id("filter-chip-all")).to_have_count(0)
    shot("task-data-tab-phone")
    rows.filter(has_text="extract.limit").click()
    page.get_by_test_id("value-view").get_by_test_id("object-rows").wait_for(timeout=15000)


@_spa_built
@_local_only
def test_typed_names_open_the_latest_job_and_task(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@job {OBJECTS_JOB} task extract")
    playwright_api.expect(page.locator("h2").first).to_contain_text("extract")
    # The task page itself links by id.
    page.get_by_test_id("task-data-tab").click()
    page.wait_for_url(lambda url: "@task " in unquote_plus(url))
    playwright_api.expect(page.get_by_test_id("objects-table").locator("tbody tr")).to_have_count(2)
    open_page(page, f"{base_url}/?p=@job {OBJECTS_JOB} task no_such_task")
    playwright_api.expect(page.get_by_text("Task not found")).to_be_visible()


@_spa_built
@_local_only
def test_task_data_tab_without_values(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, OBJECTS_JOB)}")
    page.get_by_text("validate", exact=True).click()
    page.wait_for_url(lambda url: "@task" in unquote_plus(url))
    page.get_by_test_id("task-data-tab").click()
    playwright_api.expect(page.get_by_text("No inputs or result")).to_be_visible()


@_spa_built
@_local_only
def test_wide_job_table_and_graph(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, WIDE_JOB)}")
    page.get_by_text("part_000", exact=True).wait_for(timeout=15000)
    shot("job-table-100-tasks")
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, WIDE_JOB)} graph")
    # The canvas renders only on-screen nodes, so wait on the hub, not a count.
    page.locator(".gnode", has_text="fan_out").wait_for(timeout=15000)
    shot("job-graph-100-tasks")


@_spa_built
@_local_only
def test_data_job_lists_and_opens_task_value(page, shot, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, OBJECTS_JOB)} data")
    table = page.get_by_test_id("objects-table")
    # The job's own kwargs lead the list.
    playwright_api.expect(table.locator("tbody tr").first).to_contain_text(f"{OBJECTS_JOB}.day")
    value_row = table.locator("tr", has_text="extract.limit")
    playwright_api.expect(value_row).to_contain_text("value")
    playwright_api.expect(value_row).to_contain_text("1000")
    ref_row = table.locator("tr", has_text="load.df")
    playwright_api.expect(ref_row).to_contain_text("task")
    playwright_api.expect(ref_row).to_contain_text("extract.result")
    shot("data-task-values")

    rows = table.locator("tbody tr")
    page.get_by_test_id("filter-chip-job").click()
    playwright_api.expect(rows).to_have_count(2)
    playwright_api.expect(rows.last).to_contain_text(f"{OBJECTS_JOB}.result")
    page.get_by_test_id("filter-chip-tasks").click()
    playwright_api.expect(rows).to_have_count(3)
    playwright_api.expect(value_row).to_have_count(1)
    page.get_by_test_id("filter-chip-objects").click()
    playwright_api.expect(value_row).to_have_count(0)
    playwright_api.expect(table.locator("tr", has_text="raw_sales")).to_have_count(1)
    page.get_by_test_id("filter-chip-all").click()

    # A ref's link follows it to the producing task's result.
    ref_row.get_by_role("button").click()
    view = page.get_by_test_id("value-view")
    playwright_api.expect(view).to_contain_text("extract.result")
    playwright_api.expect(view.get_by_test_id("object-rows")).to_contain_text("rows")
    shot("data-task-value")


@_spa_built
@_local_only
def test_data_task_values_on_phone(page, base_url: str, shot) -> None:
    page.set_viewport_size({"width": 412, "height": 900})
    open_page(page, f"{base_url}/?p=@job {_job_id(page, base_url, OBJECTS_JOB)} data")
    table = page.get_by_test_id("objects-table")
    table.locator("tr", has_text="extract.limit").wait_for(timeout=15000)
    # The wide table scrolls inside its own container; the page itself must not.
    assert page.evaluate("document.documentElement.scrollWidth") <= 412
    # No room for the Value / Target column: the value sits under the name.
    under = table.locator("tr", has_text="extract.limit").locator(".value-under")
    playwright_api.expect(under).to_be_visible()
    playwright_api.expect(under).to_have_text("1000")
    playwright_api.expect(table.locator("th.col-value")).to_be_hidden()
    table.scroll_into_view_if_needed()
    shot("data-task-values-phone")
    table.locator("tr", has_text="extract.limit").click()
    page.get_by_test_id("value-view").get_by_test_id("object-rows").wait_for(timeout=15000)
    assert page.evaluate("document.documentElement.scrollWidth") <= 412
    shot("data-task-value-phone")
