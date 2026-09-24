"""CLI rendering of the viewer verbs."""

from __future__ import annotations

import json

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.testing import run_cli

pytestmark = pytest.mark.usefixtures("orch_ctx")


async def test_data_query_text_and_json(capsys):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="cli_orders", scope="global")
    query = ("data", "query", "cli_orders", "--fields", "name", "--order-by", "id:desc")

    await run_cli(*query)
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].strip() == "name" and [ln.strip() for ln in lines[2:4]] == ["b", "a"]

    await run_cli(*query, "--json")
    doc = json.loads(capsys.readouterr().out)
    assert doc["data"] == [["b"], ["a"]]


async def test_view_queries_save_list_delete(capsys):
    await run_cli("view", "queries", "save", "q1", "cli_orders", "--where", "id > 0", "--json")
    assert json.loads(capsys.readouterr().out)["name"] == "q1"
    await run_cli("view", "queries", "list")
    assert "q1" in capsys.readouterr().out
    await run_cli("view", "queries", "delete", "q1")
    assert capsys.readouterr().out.strip() == "Deleted saved query 'q1'"
