"""``Object.select_sql`` — the SELECT an object reads itself with."""

from __future__ import annotations

import pytest

from aaiclick.data.data_context import create_object_from_value


@pytest.mark.parametrize(
    "aai_id, order_clause",
    [
        pytest.param(False, "", id="no-aai-id"),
        pytest.param(True, " ORDER BY aai_id", id="aai-id-orders-by-default"),
    ],
)
async def test_select_sql_base_object_is_plain_select(ctx, aai_id, order_clause):
    obj = await create_object_from_value({"a": [1, 2], "b": ["x", "y"]}, aai_id=aai_id)
    assert obj.select_sql() == f"SELECT * FROM {obj.table}{order_clause}"


async def test_select_sql_view_applies_where_order_limit_offset(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    view = obj.view(where="a > 1", order_by="a DESC", limit=5, offset=1)
    assert view.select_sql(columns="`a`, `b`") == (
        f"SELECT `a`, `b` FROM {obj.table} WHERE (a > 1) ORDER BY a DESC LIMIT 5 OFFSET 1"
    )


async def test_select_sql_per_call_overrides_win(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3]})
    view = obj.view(limit=5)
    assert view.select_sql(limit=2, offset=1).endswith("LIMIT 2 OFFSET 1")


async def test_view_result_honors_constraints(ctx):
    obj = await create_object_from_value({"a": [1, 2, 3]})
    result = await obj.view(where="a >= 2").result()
    assert sorted(row[0] for row in result.result_rows) == [2, 3]
