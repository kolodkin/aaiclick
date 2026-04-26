"""
Tests for aaiclick.ai.agents.lineage_tools — scope enforcement, read-only
query validation, row-limit truncation, and graph classification.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aaiclick.ai.agents.lineage_tools import (
    LINEAGE_TOOL_DEFINITIONS,
    ColumnSchema,
    LineageToolbox,
    QueryResult,
    TableSchema,
    ToolError,
)
from aaiclick.oplog.lineage import OplogEdge, OplogGraph
from aaiclick.testing import make_oplog_node

INTERMEDIATE_TABLE = "t_11111111111111111111"
TARGET_TABLE = "t_22222222222222222222"
PERSISTENT_INPUT = "p_raw_sales"


def _sample_graph() -> OplogGraph:
    """p_raw_sales -> t_1 (intermediate, filter) -> t_2 (target, aggregate)."""
    nodes = [
        make_oplog_node(INTERMEDIATE_TABLE, "filter", {"input": PERSISTENT_INPUT}),
        make_oplog_node(TARGET_TABLE, "aggregate", {"input": INTERMEDIATE_TABLE}),
    ]
    nodes[0].sql_template = f"SELECT * FROM {PERSISTENT_INPUT} WHERE active"
    nodes[1].sql_template = f"SELECT sum(x) FROM {INTERMEDIATE_TABLE}"
    edges = [
        OplogEdge(source=PERSISTENT_INPUT, target=INTERMEDIATE_TABLE, operation="filter"),
        OplogEdge(source=INTERMEDIATE_TABLE, target=TARGET_TABLE, operation="aggregate"),
    ]
    return OplogGraph(nodes=nodes, edges=edges)


def _mock_query_result(rows, column_names=None):
    result = MagicMock()
    result.result_rows = rows
    result.column_names = column_names or []
    return result


async def test_query_table_rejects_non_select():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table(f"INSERT INTO {TARGET_TABLE} VALUES (1)")
    assert isinstance(err, ToolError)
    assert err.kind == "not_select"


async def test_query_table_rejects_ddl_keywords_inside_select():
    """DROP hidden inside a SELECT string still trips the forbidden-keyword guard."""
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table(f"SELECT 1 FROM {TARGET_TABLE}; DROP TABLE {TARGET_TABLE}")
    assert isinstance(err, ToolError)
    assert err.kind == "not_select"


async def test_query_table_rejects_out_of_scope_table():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table("SELECT * FROM t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"
    assert "t_99999999999999999999" in err.message


async def test_query_table_happy_path_wraps_limit():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(1, "a"), (2, "b")], ["id", "name"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id, name FROM {TARGET_TABLE}")

    assert isinstance(result, QueryResult)
    assert result.columns == ["id", "name"]
    assert result.rows == [[1, "a"], [2, "b"]]
    assert not result.truncated
    called_sql = mock_client.query.call_args.args[0]
    assert "LIMIT 101" in called_sql  # default row_limit=100 → LIMIT 101


async def test_query_table_truncation_flag():
    """More rows than row_limit returns truncated=True and trims to row_limit."""
    toolbox = LineageToolbox(_sample_graph())
    rows = [(i,) for i in range(6)]  # 6 rows returned
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result(rows, ["id"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE}", row_limit=5)

    assert isinstance(result, QueryResult)
    assert result.truncated
    assert len(result.rows) == 5


async def test_query_table_respects_existing_limit():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(1,)], ["id"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE} LIMIT 3")

    called_sql = mock_client.query.call_args.args[0]
    # Only the user-supplied LIMIT 3 should be present — no injected LIMIT
    assert called_sql.count("LIMIT") == 1
    assert "LIMIT 3" in called_sql


async def test_query_table_pins_execution_settings():
    """Every query carries max_execution_time and max_result_rows to prevent runaway scans."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([], []))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        await toolbox.query_table(f"SELECT 1 FROM {TARGET_TABLE}")

    settings = mock_client.query.call_args.kwargs["settings"]
    assert "max_execution_time" in settings
    assert "max_result_rows" in settings


async def test_get_op_sql_returns_template():
    toolbox = LineageToolbox(_sample_graph())
    sql = await toolbox.get_op_sql(TARGET_TABLE)
    assert sql == f"SELECT sum(x) FROM {INTERMEDIATE_TABLE}"


async def test_get_op_sql_unknown_table_not_found():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.get_op_sql("t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "not_found"


async def test_list_graph_nodes_classifies_kinds_and_liveness():
    toolbox = LineageToolbox(_sample_graph())
    # Only intermediate and target exist; persistent input is gone.
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(INTERMEDIATE_TABLE,), (TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        nodes = await toolbox.list_graph_nodes()

    by_table = {n.table: n for n in nodes}
    assert by_table[PERSISTENT_INPUT].kind == "input"
    assert by_table[INTERMEDIATE_TABLE].kind == "intermediate"
    assert by_table[TARGET_TABLE].kind == "target"
    assert by_table[PERSISTENT_INPUT].live is False
    assert by_table[INTERMEDIATE_TABLE].live is True
    assert by_table[TARGET_TABLE].live is True


async def test_list_graph_nodes_caches_liveness_within_session():
    """Repeat calls reuse the cached liveness map — no second system.tables round-trip."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        await toolbox.list_graph_nodes()
        await toolbox.list_graph_nodes()

    assert mock_client.query.await_count == 1


async def test_get_schema_returns_columns():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([("id", "UInt64"), ("name", "String")]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        schema = await toolbox.get_schema(TARGET_TABLE)

    assert isinstance(schema, TableSchema)
    assert schema.table == TARGET_TABLE
    assert schema.columns == [
        ColumnSchema(name="id", type="UInt64"),
        ColumnSchema(name="name", type="String"),
    ]


async def test_get_schema_rejects_out_of_scope():
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.get_schema("t_99999999999999999999")
    assert isinstance(err, ToolError)
    assert err.kind == "out_of_scope"


async def test_get_schema_not_live_when_describe_fails():
    """DESCRIBE TABLE raising (e.g., table dropped mid-session) → ToolError('not_live')."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(side_effect=RuntimeError("UNKNOWN_TABLE"))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        err = await toolbox.get_schema(TARGET_TABLE)

    assert isinstance(err, ToolError)
    assert err.kind == "not_live"
    assert TARGET_TABLE in err.message


async def test_query_table_accepts_persistent_input():
    """A SELECT against a p_* persistent input must pass the scope check."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(1,)], ["id"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {PERSISTENT_INPUT}")

    assert isinstance(result, QueryResult)


async def test_query_table_allows_keyword_in_string_literal():
    """A keyword inside a single-quoted literal (e.g. 'INSERT') must not be rejected."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([], []))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE} WHERE event_type = 'INSERT'")

    assert isinstance(result, QueryResult)


async def test_query_table_allows_semicolon_in_string_literal():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([], []))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE} WHERE name = 'a;b'")

    assert isinstance(result, QueryResult)


async def test_query_table_scope_check_ignores_table_id_in_string_literal():
    """A table-id-shaped token inside a literal must not trigger out_of_scope."""
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([], []))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.query_table(f"SELECT id FROM {TARGET_TABLE} WHERE ref = 't_99999999999999999999'")

    assert isinstance(result, QueryResult)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE foo SET x=1",
        "DELETE FROM foo",
        "DROP TABLE foo",
        "ALTER TABLE foo ADD COLUMN x Int32",
        "TRUNCATE TABLE foo",
        "CREATE TABLE foo (x Int32)",
        "SYSTEM FLUSH LOGS",
    ],
)
async def test_query_table_rejects_write_statements(sql):
    toolbox = LineageToolbox(_sample_graph())
    err = await toolbox.query_table(sql)
    assert isinstance(err, ToolError)
    assert err.kind == "not_select"


def test_lineage_tool_definitions_cover_every_toolbox_method():
    """LINEAGE_TOOL_DEFINITIONS must mention exactly the tools dispatch_tool handles."""
    declared = {t["function"]["name"] for t in LINEAGE_TOOL_DEFINITIONS}
    assert declared == {"list_graph_nodes", "get_op_sql", "get_schema", "query_table"}


async def test_dispatch_tool_unknown_returns_error_string():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("does_not_exist", {})
    assert "unknown tool" in result


async def test_dispatch_tool_missing_required_arg_returns_error_string():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("query_table", {})
    assert "missing required argument" in result


async def test_dispatch_tool_formats_query_result_as_table():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(1, "a"), (2, "b")], ["id", "name"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("query_table", {"sql": f"SELECT id, name FROM {TARGET_TABLE}"})

    assert "id | name" in result
    assert "1 | a" in result
    assert "2 | b" in result


async def test_dispatch_tool_formats_tool_error_with_kind():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("query_table", {"sql": "SELECT * FROM t_99999999999999999999"})
    assert '"kind": "out_of_scope"' in result


async def test_dispatch_tool_formats_list_graph_nodes():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([(TARGET_TABLE,)], ["name"]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("list_graph_nodes", {})

    assert TARGET_TABLE in result
    assert "[target]" in result
    assert "live=True" in result


async def test_dispatch_tool_formats_get_op_sql():
    toolbox = LineageToolbox(_sample_graph())
    result = await toolbox.dispatch_tool("get_op_sql", {"table": TARGET_TABLE})
    assert f"SELECT sum(x) FROM {INTERMEDIATE_TABLE}" == result


async def test_dispatch_tool_formats_get_schema():
    toolbox = LineageToolbox(_sample_graph())
    mock_client = MagicMock()
    mock_client.query = AsyncMock(return_value=_mock_query_result([("id", "UInt64"), ("val", "Float64")]))

    with patch("aaiclick.ai.agents.lineage_tools.get_ch_client", return_value=mock_client):
        result = await toolbox.dispatch_tool("get_schema", {"table": TARGET_TABLE})

    assert "id: UInt64" in result
    assert "val: Float64" in result
