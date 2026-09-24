"""CLI rendering of the viewer verbs."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest

from aaiclick.__main__ import main
from aaiclick.data.data_context import create_object_from_value

pytestmark = pytest.mark.usefixtures("orch_ctx")


async def _run_cli(*argv: str) -> None:
    """Run ``main()`` with ``argv`` against the test database.

    ``main()`` calls ``asyncio.run``, so it cannot share the test's running
    loop. ``run_in_executor`` gives it a thread with an empty context, so the
    CLI opens its own ``orch_context`` on the database ``orch_ctx`` just reset.
    """
    with patch("sys.argv", ["aaiclick", *argv]):
        await asyncio.get_running_loop().run_in_executor(None, main)


async def test_data_query_text_and_json(capsys):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="cli_orders", scope="global")
    query = ("data", "query", "cli_orders", "--fields", "name", "--order-by", "id:desc")

    await _run_cli(*query)
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].strip() == "name" and [ln.strip() for ln in lines[2:4]] == ["b", "a"]

    await _run_cli(*query, "--json")
    doc = json.loads(capsys.readouterr().out)
    assert doc["data"] == [["b"], ["a"]]


async def test_view_queries_save_list_delete(capsys):
    await _run_cli("view", "queries", "save", "q1", "cli_orders", "--where", "id > 0", "--json")
    assert json.loads(capsys.readouterr().out)["name"] == "q1"
    await _run_cli("view", "queries", "list")
    assert "q1" in capsys.readouterr().out
    await _run_cli("view", "queries", "delete", "q1")
    assert capsys.readouterr().out.strip() == "Deleted saved query 'q1'"
