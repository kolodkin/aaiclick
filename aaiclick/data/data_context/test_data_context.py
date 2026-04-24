"""Tests for create_object DDL and registry persistence."""

from __future__ import annotations

import json

from sqlmodel import select

from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.data_context.lifecycle import get_data_lifecycle
from aaiclick.data.models import FIELDTYPE_ARRAY
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session


async def test_create_object_emits_no_aai_id_column(ctx):
    obj = await create_object_from_value([1.0, 2.0, 3.0])
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}' ORDER BY position"
    )
    names = [r[0] for r in result.result_rows]
    assert "aai_id" not in names
    assert names == ["value"]


async def test_create_object_emits_no_comment_clauses(ctx):
    obj = await create_object_from_value([1, 2, 3])
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name, comment FROM system.columns WHERE table = '{obj.table}'"
    )
    for name, comment in result.result_rows:
        assert comment == "", f"column {name} has unexpected comment {comment!r}"


async def test_create_object_writes_schema_doc(ctx):
    obj = await create_object_from_value([1, 2, 3])
    # Registry write goes through the DBLifecycleHandler queue; flush so the
    # INSERT has committed before we read.
    await get_data_lifecycle().flush()
    async with get_sql_session() as sess:
        result = await sess.execute(
            select(TableRegistry.schema_doc).where(TableRegistry.table_name == obj.table)
        )
        raw = result.scalar_one()
    assert raw is not None
    parsed = json.loads(raw)
    assert parsed["fieldtype"] == FIELDTYPE_ARRAY
    assert [c["name"] for c in parsed["columns"]] == ["value"]
    assert parsed["columns"][0]["fieldtype"] == FIELDTYPE_ARRAY


async def test_create_object_allows_user_column_named_aai_id(ctx):
    """aai_id is no longer reserved — users can define a column with that name."""
    obj = await create_object_from_value({"aai_id": [1, 2], "label": ["a", "b"]})
    ch_client = get_ch_client()
    result = await ch_client.query(
        f"SELECT name FROM system.columns WHERE table = '{obj.table}'"
    )
    names = {r[0] for r in result.result_rows}
    assert "aai_id" in names
    assert "label" in names
