"""SQL text guards in ``aaiclick.data.sql_utils``."""

from __future__ import annotations

import pytest

from aaiclick.data.sql_utils import validate_where_expression


@pytest.mark.parametrize(
    "expr",
    [
        pytest.param("amount > 100 AND name = 'x'", id="plain"),
        pytest.param("event = 'DROP TABLE'", id="keyword-inside-literal"),
        pytest.param("p_value > 1 AND t_stamp < now()", id="prefixed-column-names"),
        pytest.param("replaceAll(name, 'a', 'b') = 'x'", id="function-call"),
    ],
)
def test_validate_where_expression_accepts(expr):
    assert validate_where_expression(expr) is None


@pytest.mark.parametrize(
    "expr",
    [
        pytest.param("1 = 1; DROP TABLE x", id="statement-separator"),
        pytest.param("id IN (SELECT id FROM p_secret)", id="subquery"),
        pytest.param("EXISTS (SELECT 1)", id="exists-subquery"),
        pytest.param("a = 1 UNION ALL SELECT 1", id="union"),
        pytest.param("DELETE FROM x", id="dml"),
    ],
)
def test_validate_where_expression_rejects(expr):
    assert validate_where_expression(expr) is not None
