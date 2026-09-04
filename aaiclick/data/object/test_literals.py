"""Tests for inline literal operands — ``literals.py``.

A Python scalar or short list operand becomes a constant subquery instead of
a table. Sync tests pin the SQL / type produced; the DB tests cover the
values that only round-trip through ClickHouse.
"""

from datetime import datetime, timezone

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_SCALAR
from aaiclick.data.object.literals import LITERAL_LIST_MAX, literal_query_info

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
    assert info.base_table == ""


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


@pytest.mark.parametrize(
    "rows, allowed, expected",
    [
        pytest.param([True, False, True], [True], [1, 0, 1], id="bool"),
        # tz-aware rows match a naive literal: both sit on the naive-UTC convention
        pytest.param([DT_UTC, datetime(2025, 6, 20, tzinfo=timezone.utc)], [DT_NAIVE], [1, 0], id="datetime"),
    ],
)
async def test_literal_operand_round_trip(ctx, rows, allowed, expected):
    """Literal types the sync tests can only spell out are accepted by ClickHouse."""
    obj = await create_object_from_value(rows)
    result = await obj.isin(allowed)
    assert await result.data() == expected
