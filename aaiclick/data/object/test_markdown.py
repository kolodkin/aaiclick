"""
Tests for Object.markdown() — plain-text markdown table rendering.

Columns are auto-sized to the widest cell, floats render with 2 decimals,
None renders as ``N/A``, and ``truncate={col: n}`` caps a column's width.
"""

import pytest

from aaiclick import create_object_from_value


@pytest.mark.parametrize(
    "value, truncate, expected",
    [
        pytest.param(
            {"name": ["Alice", "Bob"], "score": [1, 22]},
            None,
            "| name  | score |\n|-------|-------|\n| Alice | 1     |\n| Bob   | 22    |",
            id="dict",
        ),
        # Scalar and array data render as a single ``value`` column.
        pytest.param(42, None, "| value |\n|-------|\n| 42    |", id="scalar"),
        pytest.param([1, 22, 333], None, "| value |\n|-------|\n| 1     |\n| 22    |\n| 333   |", id="array"),
        # Pipes are escaped and newlines collapse so each row stays on one line.
        pytest.param(["a|b", "x\ny"], None, "| value |\n|-------|\n| a\\|b  |\n| x y   |", id="sanitizes-cells"),
        # Only the named column is truncated, with a trailing ellipsis.
        pytest.param(
            {"id": ["first-row", "second-row"], "text": ["short", "a very long sentence"]},
            {"text": 8},
            "| id         | text     |\n"
            "|------------|----------|\n"
            "| first-row  | short    |\n"
            "| second-row | a very … |",
            id="truncate-named-column",
        ),
    ],
)
async def test_markdown(ctx, value, truncate, expected):
    obj = await create_object_from_value(value)
    assert await obj.markdown(truncate=truncate) == expected


async def test_markdown_formats_floats_and_nulls(ctx):
    """Floats render with 2 decimals; NULLs (here from a failed nullable cast) render as N/A."""
    obj = await create_object_from_value([{"n": "1.5"}, {"n": "bad"}, {"n": "2"}])
    view = obj.with_cast("n", "Float64", nullable=True, alias="f")
    expected = "| n   | f    |\n|-----|------|\n| 1.5 | 1.50 |\n| bad | N/A  |\n| 2   | 2.00 |"
    assert await view.markdown() == expected


async def test_markdown_view_renders_only_its_rows(ctx):
    obj = await create_object_from_value({"k": ["a", "b", "c"], "v": [1, 2, 3]})
    view = obj.where("v >= 2")
    assert await view.markdown() == "| k | v |\n|---|---|\n| b | 2 |\n| c | 3 |"


async def test_markdown_lazy_operator_result(ctx):
    """An operator result is a lazy plan; markdown() materializes it first."""
    obj = await create_object_from_value([1, 2, 3])
    assert await (obj * 10).markdown() == "| value |\n|-------|\n| 10    |\n| 20    |\n| 30    |"
