"""
aaiclick.ai.agents.tools - Tools exposed to AI agents for table inspection.
"""

from __future__ import annotations

from typing import Any

from aaiclick.data.ch_client import get_ch_client
from aaiclick.oplog.lineage import backward_oplog

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "sample_table",
            "description": "Sample rows from a ClickHouse table and return them as text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "limit": {"type": "integer", "description": "Max rows to return (default 10)"},
                    "where": {"type": "string", "description": "Optional WHERE clause predicate"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Get the column names and types of a ClickHouse table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Get count, non-null count, min, and max for a column.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "column": {"type": "string", "description": "Column name"},
                },
                "required": ["table", "column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_upstream",
            "description": "Trace all upstream operations that produced a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "depth": {"type": "integer", "description": "Max traversal depth (default 10)"},
                },
                "required": ["table"],
            },
        },
    },
]


async def sample_table(table: str, limit: int = 10, where: str | None = None) -> str:
    """Sample rows from a table and return formatted text."""
    ch_client = get_ch_client()
    table_escaped = table.replace("'", "\\'")
    where_clause = f" WHERE {where}" if where else ""
    result = await ch_client.query(f"SELECT * FROM {table_escaped}{where_clause} LIMIT {limit}")
    if not result.result_rows:
        return f"(empty table: {table})"
    header = " | ".join(str(col) for col in result.column_names)
    rows = [" | ".join(str(v) for v in row) for row in result.result_rows]
    return f"{header}\n" + "\n".join(rows)


async def get_schema(table: str) -> str:
    """Return column names and types for a table."""
    ch_client = get_ch_client()
    table_escaped = table.replace("'", "\\'")
    result = await ch_client.query(f"DESCRIBE TABLE {table_escaped}")
    lines = [f"{row[0]}: {row[1]}" for row in result.result_rows]
    return "\n".join(lines) if lines else f"(table {table} not found)"


async def get_stats(table: str, column: str) -> str:
    """Return count, non-null count, min, and max for a column."""
    ch_client = get_ch_client()
    table_escaped = table.replace("'", "\\'")
    column_escaped = column.replace("`", "\\`")
    try:
        result = await ch_client.query(f"""
            SELECT
                count() AS count,
                countIf(`{column_escaped}` IS NOT NULL) AS non_null,
                min(`{column_escaped}`) AS min_val,
                max(`{column_escaped}`) AS max_val
            FROM {table_escaped}
        """)
    except Exception as exc:
        return f"(error querying {column} in {table}: {exc})"
    if not result.result_rows:
        return f"(no stats for {column} in {table})"
    row = result.result_rows[0]
    return f"count={row[0]}, non_null={row[1]}, min={row[2]}, max={row[3]}"


async def trace_upstream(table: str, depth: int = 10) -> str:
    """Trace upstream operations and return formatted text."""
    nodes = await backward_oplog(table, max_depth=depth)
    if not nodes:
        return f"(no upstream operations found for {table})"
    lines = []
    for node in nodes:
        inputs = ", ".join(node.args + list(node.kwargs.values()))
        lines.append(f"{node.table} <- {node.operation}({inputs})")
    return "\n".join(lines)


async def dispatch_tool(name: str, arguments: dict[str, Any]) -> str:
    """Dispatch a tool call by name to the appropriate function."""
    if name == "sample_table":
        return await sample_table(
            arguments["table"],
            limit=arguments.get("limit", 10),
            where=arguments.get("where"),
        )
    elif name == "get_schema":
        return await get_schema(arguments["table"])
    elif name == "get_stats":
        return await get_stats(arguments["table"], arguments["column"])
    elif name == "trace_upstream":
        return await trace_upstream(arguments["table"], depth=arguments.get("depth", 10))
    else:
        return f"(unknown tool: {name})"
