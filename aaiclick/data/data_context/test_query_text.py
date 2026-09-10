"""``query_text``: a SELECT in one of ClickHouse's named output formats."""

from __future__ import annotations

import json

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import query_text

JSON_SETTINGS = {
    "output_format_json_quote_64bit_integers": 1,
    "output_format_json_quote_decimals": 1,
    "output_format_json_quote_denormals": 1,
    "output_format_json_named_tuples_as_objects": 1,
}


async def test_query_text_json_compact_quotes_64bit_and_keeps_structure(ctx):
    obj = await create_object_from_value({"n": [1, 2], "tags": [["a"], ["b", "c"]]})
    text = await query_text(f"SELECT toUInt64(n) AS n, tags FROM {obj.table} ORDER BY n", "JSONCompact", JSON_SETTINGS)
    doc = json.loads(text)
    assert [c["name"] for c in doc["meta"]] == ["n", "tags"]
    assert doc["data"] == [["1", ["a"]], ["2", ["b", "c"]]]


async def test_query_text_csv_with_names(ctx):
    obj = await create_object_from_value({"n": [1, 2]})
    text = await query_text(f"SELECT n FROM {obj.table} ORDER BY n", "CSVWithNames")
    assert text.splitlines() == ['"n"', "1", "2"]
