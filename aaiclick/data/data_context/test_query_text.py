"""``query_text``: a SELECT in one of ClickHouse's named output formats."""

from __future__ import annotations

import json

import pytest

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import JSON_COMPACT_SETTINGS, query_text


async def test_query_text_json_compact_quotes_64bit_and_keeps_structure(ctx):
    obj = await create_object_from_value({"n": [1, 2], "tags": [["a"], ["b", "c"]]})
    text = await query_text(
        f"SELECT toUInt64(n) AS n, tags FROM {obj.table} ORDER BY n", "JSONCompact", JSON_COMPACT_SETTINGS
    )
    doc = json.loads(text)
    assert [c["name"] for c in doc["meta"]] == ["n", "tags"]
    assert doc["data"] == [["1", ["a"]], ["2", ["b", "c"]]]


async def test_query_text_csv_with_names(ctx):
    obj = await create_object_from_value({"n": [1, 2]})
    text = await query_text(f"SELECT n FROM {obj.table} ORDER BY n", "CSVWithNames")
    assert text.splitlines() == ['"n"', "1", "2"]


async def test_settings_apply_after_a_trailing_line_comment(ctx):
    """Settings sent with a query hold even when its text ends in a ``--`` comment.

    On chdb the settings travel as a ``SETTINGS`` clause appended to the text;
    appended on the comment's line they would be commented out, and the
    ceiling below would silently not apply.
    """
    with pytest.raises(Exception, match="[Rr]esult"):
        await query_text(
            "SELECT number FROM numbers(50) -- newest first",
            settings={"max_result_rows": 3, "result_overflow_mode": "throw"},
        )
