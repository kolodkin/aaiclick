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
pytest.importorskip("playwright.sync_api")
from playwright.sync_api import expect  # noqa: E402  (must follow the playwright guard)

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
_local_only = pytest.mark.skipif(not is_local(), reason="the viewer seed runs through chdb (local mode)")


@_spa_built
@_local_only
def test_data_lists_and_previews_object(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data")
    row = page.get_by_test_id("objects-table").get_by_text("orders")
    row.wait_for(timeout=15000)
    row.click()
    page.wait_for_url(lambda url: "orders" in url)
    rows = page.get_by_test_id("object-rows")
    rows.wait_for(timeout=15000)
    assert rows.locator("tbody tr").count() == 3


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
    expect(title).to_have_text("rows:3", timeout=15000)
