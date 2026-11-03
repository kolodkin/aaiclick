"""Prompt dropdown: argument values listed under the prompt, view waits for a pick.

Run with::

    pytest test_e2e/web/test_prompt_dropdown.py -v -p no:cov
"""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers import login_if_needed, open_page, run_in_process

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"
pytest.importorskip("playwright.sync_api")

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # noqa: E402  (must follow the playwright guard)

pytestmark = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")

from seed import DEFAULT_STATES, TaskState, seed_graph_job  # noqa: E402  (must follow the playwright guard)

ROWS = "#prompt-dropdown [role=option]"


# Already finished, so no worker picks them up: in distributed mode 50+ runnable
# jobs would starve other tests' tasks on the single execution worker.
_DONE = {name: TaskState("COMPLETED", None, 0, 1) for name in DEFAULT_STATES}


@pytest.fixture(scope="module")
def many_tasks() -> list[int]:
    """More tasks than one dropdown page (50), so scrolling has a second page to load."""
    return [run_in_process(lambda i=i: seed_graph_job(f"dropdown_{i}", _DONE, "COMPLETED")) for i in range(7)]


def test_typing_task_lists_tasks_without_switching_view(page, base_url: str, many_tasks, shot) -> None:
    open_page(page, f"{base_url}/?p=@jobs")
    page.wait_for_selector("text=Jobs")

    page.fill("#prompt", "@task ")

    page.wait_for_selector(ROWS)
    assert "p=%40jobs" in page.url or "p=@jobs" in page.url
    assert page.locator("h2", has_text="Jobs").is_visible()
    shot("dropdown-tasks")


def test_scrolling_loads_the_next_page(page, base_url: str, many_tasks) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task ")
    page.wait_for_selector(ROWS)
    assert page.locator(ROWS).count() == 50

    page.locator(ROWS).last.scroll_into_view_if_needed()

    page.wait_for_function(f"() => document.querySelectorAll('{ROWS}').length > 50")


def test_picking_a_row_commits_and_routes(page, base_url: str, many_tasks, shot) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task ")
    first = page.locator(ROWS).first
    first.wait_for()

    first.click()

    page.wait_for_url(lambda url: "%40task" in url or "@task" in url)
    assert page.input_value("#prompt").startswith("@task ")
    assert page.locator("#prompt-dropdown").count() == 0
    shot("dropdown-picked-task")


def test_no_matches(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task zz_no_such_task_zz")
    page.wait_for_selector("#prompt-dropdown >> text=no matches")
    shot("dropdown-no-matches")


def test_escape_resets_the_draft(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@jobs")
    page.fill("#prompt", "@task ")

    page.press("#prompt", "Escape")

    assert page.input_value("#prompt") == "@jobs"
    assert page.locator("#prompt-dropdown").count() == 0


def test_enter_commits_an_unknown_command(page, base_url: str) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@foo")

    page.press("#prompt", "Enter")

    page.wait_for_selector("text=Unknown command")


def test_back_restores_prompt_and_view(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@jobs")
    page.fill("#prompt", "@registered")
    page.wait_for_url(lambda url: "registered" in url)

    page.go_back()

    page.wait_for_url(lambda url: "jobs" in url)
    login_if_needed(page)
    assert page.input_value("#prompt") == "@jobs"


def test_arrow_keys_highlight_and_enter_picks(page, base_url: str, shot) -> None:
    open_page(page, f"{base_url}/?p=@jobs")
    page.fill("#prompt", "@re")
    page.wait_for_selector(ROWS)

    page.press("#prompt", "ArrowDown")

    selected = page.locator("#prompt-dropdown [aria-selected=true]")
    selected.wait_for()
    page.wait_for_timeout(300)
    assert selected.count() == 1
    shot("dropdown-commands-highlighted")
    page.press("#prompt", "Enter")
    page.wait_for_url(lambda url: "registered" in url)


def test_highlight_survives_the_next_page(page, base_url: str, many_tasks) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task ")
    page.wait_for_selector(ROWS)
    # A resting pointer over the list (where the login button was, in distributed
    # mode) must not steal the highlight as keyboard scrolling slides rows under it.
    box = page.locator("#prompt-dropdown").bounding_box()
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2 + 1)
    page.keyboard.press("Escape")
    page.fill("#prompt", "@task ")
    page.wait_for_selector(ROWS)

    for _ in range(50):
        page.press("#prompt", "ArrowDown")
    state = (
        f"() => {{ const r = [...document.querySelectorAll('{ROWS}')]; "
        "return {selected: r.findIndex((e) => e.getAttribute('aria-selected') === 'true'), rows: r.length}; }"
    )
    try:
        page.wait_for_function(f"() => document.querySelectorAll('{ROWS}').length > 50", timeout=15000)
    except PlaywrightTimeoutError as e:
        raise AssertionError(f"next page never loaded: {page.evaluate(state)}") from e

    assert page.locator(ROWS).nth(49).get_attribute("aria-selected") == "true"


def test_mousedown_on_the_list_keeps_it_open(page, base_url: str) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task zz_no_such_task_zz")
    empty = page.locator("#prompt-dropdown >> text=no matches")
    empty.wait_for()

    empty.click()

    assert page.locator("#prompt-dropdown").is_visible()


def test_recommitting_the_current_prompt_drops_the_draft(page, base_url: str) -> None:
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task half")

    page.click("#header-home")

    assert page.input_value("#prompt") == ""


def test_open_list_does_not_poll_while_live_updates_are_down(page, base_url: str, many_tasks) -> None:
    page.route("**/api/v0/events", lambda route: route.abort())
    open_page(page, f"{base_url}/")
    page.fill("#prompt", "@task ")
    page.wait_for_selector(ROWS)
    task_requests: list[str] = []
    page.on("request", lambda r: task_requests.append(r.url) if "/api/v0/tasks?" in r.url else None)

    page.wait_for_timeout(5000)

    assert task_requests == []
