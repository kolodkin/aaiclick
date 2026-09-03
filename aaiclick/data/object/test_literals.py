"""Tests for inline literal operands — ``literals.py``.

A Python scalar or short list operand becomes a constant subquery instead of
a table. Sync tests pin the SQL / type produced; the DB tests cover the
values that only round-trip through ClickHouse.
"""

from datetime import datetime, timezone

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_SCALAR
from aaiclick.data.object.literals import LITERAL_BASE_TABLE, LITERAL_LIST_MAX, literal_query_info

DT_UTC = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
DT_NAIVE = datetime(2024, 1, 15, 10, 30, 0)
DT_LITERAL = "'2024-01-15 10:30:00.000000'"


@pytest.mark.parametrize(
    "value, expected_source, expected_type",
    [
        pytest.param(3, "(SELECT CAST(3 AS Int64) AS value)", "Int64", id="int"),
        pytest.param(2.5, "(SELECT CAST(2.5 AS Float64) AS value)", "Float64", id="float"),
        pytest.param(True, "(SELECT CAST(true AS Bool) AS value)", "Bool", id="bool"),
        pytest.param("it's", "(SELECT CAST('it\\'s' AS String) AS value)", "String", id="str-escaped"),
        # tz-aware and naive datetimes both render as naive UTC
        pytest.param(
            DT_UTC,
            f"(SELECT CAST({DT_LITERAL} AS DateTime64(3, 'UTC')) AS value)",
            "DateTime64(3, 'UTC')",
            id="datetime-utc",
        ),
        pytest.param(
            DT_NAIVE,
            f"(SELECT CAST({DT_LITERAL} AS DateTime64(3, 'UTC')) AS value)",
            "DateTime64(3, 'UTC')",
            id="datetime-naive",
        ),
    ],
)
def test_literal_query_info_scalar(value, expected_source, expected_type):
    info = literal_query_info(value)
    assert info is not None
    assert info.source == expected_source
    assert info.fieldtype == FIELDTYPE_SCALAR
    assert info.value_type == expected_type
    assert info.base_table == LITERAL_BASE_TABLE


@pytest.mark.parametrize(
    "value, expected_source, expected_type",
    [
        pytest.param([1, 2], "(SELECT arrayJoin(CAST([1, 2] AS Array(Int64))) AS value)", "Int64", id="ints"),
        pytest.param(
            [1, 2.5], "(SELECT arrayJoin(CAST([1, 2.5] AS Array(Float64))) AS value)", "Float64", id="mixed-numbers"
        ),
        pytest.param(
            ["a", "b"], "(SELECT arrayJoin(CAST(['a', 'b'] AS Array(String))) AS value)", "String", id="strings"
        ),
        pytest.param([], "(SELECT arrayJoin(CAST([] AS Array(String))) AS value)", "String", id="empty"),
    ],
)
def test_literal_query_info_list(value, expected_source, expected_type):
    info = literal_query_info(value)
    assert info is not None
    assert info.source == expected_source
    assert info.fieldtype == FIELDTYPE_ARRAY
    assert info.value_type == expected_type


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(list(range(LITERAL_LIST_MAX + 1)), id="over-threshold"),
        pytest.param([1, None], id="contains-none"),
    ],
)
def test_literal_query_info_falls_back_to_table(value):
    """Lists that can't be inlined return None so the caller materializes a table."""
    assert literal_query_info(value) is None


async def test_isin_list_over_threshold_uses_table(ctx):
    """A list longer than LITERAL_LIST_MAX still works — through the table path."""
    obj = await create_object_from_value([0, 5, LITERAL_LIST_MAX + 5])
    result = await obj.isin(list(range(LITERAL_LIST_MAX + 1)))
    assert await result.data() == [1, 1, 0]


async def test_datetime_list_operand(ctx):
    """Datetime literals round-trip through ClickHouse on the naive-UTC convention."""
    obj = await create_object_from_value([DT_UTC, datetime(2025, 6, 20, tzinfo=timezone.utc)])
    result = await obj.isin([DT_NAIVE])
    assert await result.data() == [1, 0]


async def test_bool_scalar_operand(ctx):
    obj = await create_object_from_value([True, False, True])
    result = await (obj == True)  # noqa: E712 — operator overload under test
    assert await result.data() == [1, 0, 1]
