"""CLI rendering of the viewer verbs."""

from __future__ import annotations

import argparse
import json

import pytest

from aaiclick import __main__ as cli
from aaiclick.data.data_context import create_object_from_value

pytestmark = pytest.mark.usefixtures("orch_ctx")


async def test_data_query_text_and_json(capsys):
    await create_object_from_value({"id": [1, 2], "name": ["a", "b"]}, name="cli_orders", scope="global")
    ns = argparse.Namespace(
        object="cli_orders",
        scope="persistent",
        where=None,
        fields="name",
        order_by="id:desc",
        limit=100,
        offset=0,
        csv=False,
        json=False,
    )
    await cli._run_data_query(ns)
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].strip() == "name" and [ln.strip() for ln in lines[2:4]] == ["b", "a"]

    ns.json = True
    await cli._run_data_query(ns)
    doc = json.loads(capsys.readouterr().out)
    assert doc["data"] == [["b"], ["a"]]


async def test_view_queries_save_list_delete(capsys):
    await cli._run_view_queries_save(
        argparse.Namespace(
            name="q1",
            object="cli_orders",
            scope="persistent",
            where="id > 0",
            fields=None,
            order_by=None,
            cell_view=None,
            json=True,
        )
    )
    assert json.loads(capsys.readouterr().out)["name"] == "q1"
    await cli._run_view_queries_list(argparse.Namespace(scope=None, object=None, json=False))
    assert "q1" in capsys.readouterr().out
    await cli._run_view_queries_delete(argparse.Namespace(name="q1", json=False))
    assert capsys.readouterr().out.strip() == "Deleted saved query 'q1'"
