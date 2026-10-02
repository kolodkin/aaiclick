"""
Tests for aaiclick.oplog.query_sandbox — scope enforcement, read-only
query validation, row-limit truncation, and table liveness.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aaiclick.data.data_context import get_ch_client
from aaiclick.data.scope import (
    SCOPE_GLOBAL,
    SCOPE_JOB,
    SCOPE_TEMP_NAMED,
    make_scoped_table_name,
)
from aaiclick.data.view_models import ColumnSchema
from aaiclick.oplog.query_sandbox import (
    DEFAULT_ROW_LIMIT,
    ROW_LIMIT_CEILING,
    QueryResult,
    TableSchema,
    ToolError,
    describe_table,
    liveness,
    run_select,
    sandboxed_select,
)

INTERMEDIATE_TABLE = "t_11111111111111111111"
TARGET_TABLE = "t_22222222222222222222"
PERSISTENT_INPUT = "p_raw_sales"
SCOPE = {PERSISTENT_INPUT, INTERMEDIATE_TABLE, TARGET_TABLE}


def _mock_query_result(rows, column_names=None):
    result = MagicMock()
    result.result_rows = rows
    result.column_names = column_names or []
    return result


def _ch_client_with_real_parser(rows, column_names=None):
    """Client double whose ``EXPLAIN`` goes to real ClickHouse; data queries return ``rows``.

    ``validate_scope`` asks ClickHouse to parse the SQL, so a fully faked
    client would turn the scope check into a no-op and the test would be
    asserting on the fake. Only the data round trip is canned.
    """
    client = MagicMock()
    client.raw_query = get_ch_client().raw_query
    client.query = AsyncMock(return_value=_mock_query_result(rows, column_names))
    return client


# The four scope shapes, built through their real producers. The guard reads
# table position from the parse tree rather than matching names, so these are
# regression cases for the shape-blind hole rather than the mechanism itself.
OUT_OF_SCOPE_ID = 7502577539063427072
OUT_OF_SCOPE_TABLES: dict[str, str] = {
    # The unnamed-temp form has no factory; aaiclick/data/object/object.py builds it.
    "temp": f"t_{OUT_OF_SCOPE_ID}",
    "temp-named": make_scoped_table_name(SCOPE_TEMP_NAMED, "orders", snowid=OUT_OF_SCOPE_ID),
    "job": make_scoped_table_name(SCOPE_JOB, "payroll", job_id=OUT_OF_SCOPE_ID),
    "global": make_scoped_table_name(SCOPE_GLOBAL, "sales"),
}


@pytest.mark.parametrize("table", OUT_OF_SCOPE_TABLES.values(), ids=list(OUT_OF_SCOPE_TABLES))
async def test_sandboxed_select_rejects_out_of_scope_table(orch_ctx, table):
    """Every scoped-table shape aaiclick creates is rejected when out of graph.

    ClickHouse keeps every persistent table in one database, so reaching one
    of these is a read outside the graph.
    """
    err = await sandboxed_select(f"SELECT * FROM {table}", SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"
    assert table in err.message


@pytest.mark.parametrize(
    "sql, function",
    [
        # Reads every persistent table in one call.
        pytest.param("SELECT * FROM merge(currentDatabase(), '^p_')", "merge", id="merge"),
        pytest.param("SELECT * FROM remote('h:9000', 'default', 'p_sales')", "remote", id="remote"),
        pytest.param("SELECT * FROM url('http://x/y', CSV, 'a String')", "url", id="url"),
        pytest.param("SELECT * FROM file('/etc/passwd', 'LineAsString')", "file", id="file"),
        pytest.param("SELECT * FROM cluster('c', currentDatabase(), 'p_sales')", "cluster", id="cluster"),
    ],
)
async def test_sandboxed_select_rejects_table_functions(orch_ctx, sql, function):
    """A table function names its target in a string literal, not as an identifier.

    Nothing in table position is an identifier at all, so a guard that looks
    for table names sees an empty query. ``merge`` and ``cluster`` reach every
    table in the database; ``url`` and ``file`` reach outside it entirely.
    """
    err = await sandboxed_select(sql, SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"
    assert function in err.message


@pytest.mark.parametrize(
    "sql, expected",
    [
        # Right table, but a database qualifier we cannot prove refers to ours.
        pytest.param(f"SELECT * FROM default.{PERSISTENT_INPUT}", f"default.{PERSISTENT_INPUT}", id="qualified"),
        # A CTE name is a table identifier that no graph contains.
        pytest.param(f"WITH c AS (SELECT 1) SELECT * FROM c JOIN {TARGET_TABLE} USING (x)", "c", id="cte"),
    ],
)
async def test_sandboxed_select_rejects_non_graph_table_identifiers(orch_ctx, sql, expected):
    """Anything in table position must be a table of this graph, whatever it is."""
    err = await sandboxed_select(sql, SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"
    assert expected in err.message


async def test_sandboxed_select_rejects_system_tables(orch_ctx):
    """``system.*`` never reaches the scope check — SYSTEM is a forbidden keyword.

    Recorded because the scope check would also reject it: the two guards
    overlap here, and only the outer one reports the reason.
    """
    err = await sandboxed_select("SELECT * FROM system.tables", SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "not_select"


async def test_sandboxed_select_never_evaluates_sql_while_validating_scope(orch_ctx):
    """Scope validation parses the SQL; nothing in it runs first.

    The payload escapes a ``SELECT * FROM (EXPLAIN AST …)`` wrapper: close the
    parenthesis, append a statement, reopen one so the text parses. ``throwIf``
    builds its message at run time, so it reaches the error text only if that
    statement executed — a parse error just echoes the source.
    """
    err = await sandboxed_select("SELECT 1) UNION ALL SELECT throwIf(1, concat('exec', 'uted')) FROM (SELECT 1", SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "invalid_argument"
    assert "executed" not in err.message


async def test_sandboxed_select_reports_unparseable_sql(orch_ctx):
    """SQL ClickHouse cannot parse is rejected, not passed along unchecked."""
    err = await sandboxed_select(f"SELECT * FROM {TARGET_TABLE} WHERE (", SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "invalid_argument"


async def test_sandboxed_select_happy_path_caps_rows(orch_ctx):
    mock_client = _ch_client_with_real_parser([(1, "a"), (2, "b")], ["id", "name"])

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await sandboxed_select(f"SELECT id, name FROM {TARGET_TABLE}", SCOPE)

    assert isinstance(result, QueryResult)
    assert result.columns == ["id", "name"]
    assert result.rows == [[1, "a"], [2, "b"]]
    assert not result.truncated
    settings = mock_client.query.call_args.kwargs["settings"]
    assert settings["limit"] == DEFAULT_ROW_LIMIT + 1  # one past, so truncation is detectable


async def test_sandboxed_select_truncation_flag(orch_ctx):
    """More rows than row_limit returns truncated=True and trims to row_limit."""
    rows = [(i,) for i in range(6)]  # 6 rows returned
    mock_client = _ch_client_with_real_parser(rows, ["id"])

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await sandboxed_select(f"SELECT id FROM {TARGET_TABLE}", SCOPE, row_limit=5)

    assert isinstance(result, QueryResult)
    assert result.truncated
    assert len(result.rows) == 5


async def test_sandboxed_select_sends_sql_unchanged(orch_ctx):
    """The cap travels as a setting; the query text, comments and all, is sent as written."""
    mock_client = _ch_client_with_real_parser([(1,)], ["id"])
    sql = f"SELECT id FROM {TARGET_TABLE} LIMIT 3 -- newest first"

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        await sandboxed_select(sql, SCOPE)

    assert mock_client.query.call_args.args[0] == sql


async def test_sandboxed_select_pins_execution_settings(orch_ctx):
    """Every query carries max_execution_time and max_result_rows to prevent runaway scans."""
    mock_client = _ch_client_with_real_parser([], [])

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        await sandboxed_select(f"SELECT 1 FROM {TARGET_TABLE}", SCOPE)

    settings = mock_client.query.call_args.kwargs["settings"]
    assert "max_execution_time" in settings
    assert "max_result_rows" in settings
    assert "limit" in settings


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"SELECT id FROM {TARGET_TABLE} WHERE id IN {OUT_OF_SCOPE_TABLES['global']}", id="in"),
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE id GLOBAL NOT IN {OUT_OF_SCOPE_TABLES['global']}", id="global-not-in"
        ),
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE in(id, {OUT_OF_SCOPE_TABLES['global']})", id="function-form"
        ),
        pytest.param(f"SELECT id FROM {TARGET_TABLE} WHERE id IN default.{PERSISTENT_INPUT}", id="qualified"),
    ],
)
async def test_sandboxed_select_rejects_in_with_out_of_scope_table(orch_ctx, sql):
    """``expr IN table`` reads the table without a table position.

    The bare-identifier form is an ``Identifier`` under the ``in`` function,
    never a ``TableExpression``, so a guard that only walks table positions
    lets it read any table in the database. Every IN-family function —
    ``nullIn``, ``globalNotNullIn``, the ``IgnoreSet`` variants — does the same.
    """
    err = await sandboxed_select(sql, SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"SELECT id FROM {TARGET_TABLE} SETTINGS max_execution_time = 0", id="top-level"),
        pytest.param(f"SELECT id FROM (SELECT id FROM {TARGET_TABLE} SETTINGS max_result_rows = 0)", id="subquery"),
    ],
)
async def test_sandboxed_select_rejects_settings_clause(orch_ctx, sql):
    """A query-level SETTINGS clause outranks the caps ``run_select`` sets.

    ``readonly=2`` permits settings changes, and ClickHouse applies a clause in
    the text over settings sent beside the query, so the caller could lift its
    own execution-time and result-row limits.
    """
    err = await sandboxed_select(sql, SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "invalid_argument"
    assert "SETTINGS" in err.message


async def test_run_select_truncates_past_the_ceiling_instead_of_failing(orch_ctx):
    """A result larger than the ceiling is cut, not turned into an error.

    ``max_result_rows`` alone makes ClickHouse throw once the ceiling is
    crossed; with ``result_overflow_mode='break'`` it stops reading instead,
    and the sandbox reports the truncation. The query's own LIMIT is above the
    ceiling so the ``limit`` setting is what wins.
    """
    result = await run_select(f"SELECT number FROM numbers({ROW_LIMIT_CEILING * 2}) LIMIT {ROW_LIMIT_CEILING * 2}")

    assert result.truncated
    assert len(result.rows) == DEFAULT_ROW_LIMIT


async def test_run_select_is_refused_write_access_by_clickhouse(orch_ctx):
    """ClickHouse refuses the write itself, so the keyword guard is not the only gate.

    ``run_select`` is reached only after ``validate_select_safety``; this pins
    the layer beneath it, so a bypass of that regex still cannot write. The
    statement carries its own LIMIT so no ``LIMIT`` injection masks the result.
    """
    with pytest.raises(Exception, match="[Rr]eadonly"):
        await run_select("CREATE TABLE t_written ENGINE=Memory AS SELECT 1 LIMIT 1")


async def test_describe_table_returns_columns():
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([("id", "UInt64"), ("name", "String")]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        schema = await describe_table(TARGET_TABLE)

    assert isinstance(schema, TableSchema)
    assert schema.table == TARGET_TABLE
    assert schema.columns == [
        ColumnSchema(name="id", type="UInt64"),
        ColumnSchema(name="name", type="String"),
    ]


async def test_liveness_reports_missing_tables():
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        alive = await liveness({TARGET_TABLE, PERSISTENT_INPUT})

    assert alive == {TARGET_TABLE: True, PERSISTENT_INPUT: False}


@pytest.mark.parametrize(
    "sql, rows, columns",
    [
        # A SELECT against a p_* persistent input must pass the scope check.
        pytest.param(f"SELECT id FROM {PERSISTENT_INPUT}", [(1,)], ["id"], id="persistent-input"),
        # A keyword inside a single-quoted literal (e.g. 'INSERT') must not be rejected.
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE event_type = 'INSERT'",
            [],
            [],
            id="keyword-in-string-literal",
        ),
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE name = 'a;b'",
            [],
            [],
            id="semicolon-in-string-literal",
        ),
        # A table-id-shaped token inside a literal must not trigger out_of_scope.
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE ref = 't_99999999999999999999'",
            [],
            [],
            id="table-id-in-string-literal",
        ),
        # Columns are not table positions, so a scope-prefixed column name is
        # simply a column — p_value included, which no name pattern could allow.
        pytest.param(
            f"SELECT t_start, j_id, p_value FROM {TARGET_TABLE}",
            [(1, 2, 3)],
            ["t_start", "j_id", "p_value"],
            id="columns-named-like-scoped-tables",
        ),
        # A derived table parses to a Subquery in table position, so it is the
        # rewrite the out-of-scope message points a rejected CTE at.
        pytest.param(
            f"SELECT * FROM (SELECT id FROM {TARGET_TABLE}) AS s",
            [(1,)],
            ["id"],
            id="derived-table-subquery",
        ),
        # The IN-with-table form against a table of the graph is an ordinary in-scope read.
        pytest.param(
            f"SELECT id FROM {TARGET_TABLE} WHERE id IN {INTERMEDIATE_TABLE}",
            [(1,)],
            ["id"],
            id="in-with-graph-table",
        ),
    ],
)
async def test_sandboxed_select_accepts_valid_select(orch_ctx, sql, rows, columns):
    mock_client = _ch_client_with_real_parser(rows, columns)

    with patch("aaiclick.oplog.query_sandbox.get_ch_client", return_value=mock_client):
        result = await sandboxed_select(sql, SCOPE)

    assert isinstance(result, QueryResult)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(f"INSERT INTO {TARGET_TABLE} VALUES (1)", id="insert"),
        pytest.param("UPDATE foo SET x=1", id="update"),
        pytest.param("DELETE FROM foo", id="delete"),
        pytest.param("DROP TABLE foo", id="drop"),
        pytest.param("ALTER TABLE foo ADD COLUMN x Int32", id="alter"),
        pytest.param("TRUNCATE TABLE foo", id="truncate"),
        pytest.param("CREATE TABLE foo (x Int32)", id="create"),
        pytest.param("SYSTEM FLUSH LOGS", id="system"),
        # DROP hidden inside a SELECT string still trips the forbidden-keyword guard.
        pytest.param(f"SELECT 1 FROM {TARGET_TABLE}; DROP TABLE {TARGET_TABLE}", id="ddl-keyword-inside-select"),
    ],
)
async def test_sandboxed_select_rejects_write_statements(sql):
    err = await sandboxed_select(sql, SCOPE)
    assert isinstance(err, ToolError)
    assert err.kind == "not_select"
