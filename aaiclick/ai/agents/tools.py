"""
aaiclick.ai.agents.tools - Tools exposed to AI agents for table inspection.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.lineage import OplogNode, backward_oplog, backward_oplog_row

logger = logging.getLogger(__name__)

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
            "name": "get_column_stats",
            "description": (
                "Get count, non-null count, min, and max for every column in a table. "
                "No need to know column names upfront — the tool discovers them automatically."
            ),
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
    {
        "type": "function",
        "function": {
            "name": "trace_row",
            "description": (
                "Trace one row backward through the oplog, returning the "
                "operation and positionally-aligned source aai_id at each "
                "step. Only returns data when the job ran under "
                "PreservationMode.STRATEGY (otherwise the lineage id "
                "arrays are empty and the trace is empty). Start from "
                "the target table with a specific aai_id you care about."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table the row currently lives in"},
                    "aai_id": {"type": "integer", "description": "The aai_id of the row to trace"},
                    "depth": {"type": "integer", "description": "Max traversal depth (default 10)"},
                },
                "required": ["table", "aai_id"],
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


async def get_column_stats(table: str) -> str:
    """Return count, non-null count, min, and max for every column in a table.

    Discovers columns via DESCRIBE TABLE, then queries stats for all of them
    in a single round-trip — the LLM never needs to guess column names.
    """
    ch_client = get_ch_client()
    table_escaped = table.replace("'", "\\'")

    desc_result = await ch_client.query(f"DESCRIBE TABLE {table_escaped}")
    if not desc_result.result_rows:
        return f"(table {table} not found or has no columns)"

    columns = [row[0] for row in desc_result.result_rows]

    select_parts = []
    for col in columns:
        col_escaped = col.replace("`", "\\`")
        select_parts.append(
            f"count() AS `{col_escaped}__count`, "
            f"countIf(`{col_escaped}` IS NOT NULL) AS `{col_escaped}__non_null`, "
            f"min(`{col_escaped}`) AS `{col_escaped}__min`, "
            f"max(`{col_escaped}`) AS `{col_escaped}__max`"
        )

    try:
        result = await ch_client.query(
            f"SELECT {', '.join(select_parts)} FROM {table_escaped}"
        )
    except Exception as exc:
        return f"(error querying stats for {table}: {exc})"

    if not result.result_rows:
        return f"(no data in {table})"

    row = result.result_rows[0]
    lines = []
    for i, col in enumerate(columns):
        base = i * 4
        lines.append(
            f"{col}: count={row[base]}, non_null={row[base + 1]}, "
            f"min={row[base + 2]}, max={row[base + 3]}"
        )

    return "\n".join(lines)


async def get_schemas_for_nodes(nodes: list[OplogNode]) -> str:
    """Fetch DESCRIBE TABLE for every table in a lineage graph.

    Returns a formatted string with schemas for all tables, suitable for
    injection into the initial LLM context so the model never needs to
    guess column names. Queries run in parallel via asyncio.gather.
    """
    if not nodes:
        return ""

    seen: set[str] = set()
    tables: list[str] = []
    for node in nodes:
        for tbl in [node.table] + list(node.kwargs.values()):
            if tbl not in seen:
                seen.add(tbl)
                tables.append(tbl)

    async def _describe(tbl: str) -> str:
        try:
            schema = await get_schema(tbl)
            if "not found" in schema:
                return f"`{tbl}`: (schema unavailable)"
            indented = "\n".join(f"  {line}" for line in schema.split("\n"))
            return f"`{tbl}`:\n{indented}"
        except Exception:
            return f"`{tbl}`: (schema unavailable)"

    sections = await asyncio.gather(*(_describe(tbl) for tbl in tables))
    if not sections:
        return ""
    return "# Table Schemas\n\n" + "\n\n".join(sections)


async def trace_upstream(table: str, depth: int = 10) -> str:
    """Trace upstream operations and return formatted text."""
    nodes = await backward_oplog(table, max_depth=depth)
    if not nodes:
        return f"(no upstream operations found for {table})"
    lines = []
    for node in nodes:
        inputs = ", ".join(node.kwargs.values())
        lines.append(f"{node.table} <- {node.operation}({inputs})")
    return "\n".join(lines)


async def trace_row(table: str, aai_id: int, depth: int = 10) -> str:
    """Trace one aai_id backward through the oplog, returning formatted text."""
    steps = await backward_oplog_row(table, aai_id, max_depth=depth)
    if not steps:
        return (
            f"(no row-level lineage for {table}.aai_id={aai_id} — "
            f"run the job under PreservationMode.STRATEGY to populate the trace)"
        )
    lines = []
    for step in steps:
        sources = ", ".join(
            f"{role}={src_id}" for role, src_id in step.source_aai_ids.items()
        )
        lines.append(
            f"{step.table}.aai_id={step.aai_id}  <- {step.operation}({sources})"
        )
    return "\n".join(lines)


async def dispatch_tool(name: str, arguments: dict[str, Any]) -> str:
    """Dispatch a tool call by name to the appropriate function.

    Missing required arguments and unknown tool names produce a
    retryable error string fed back to the agent loop, so one bad
    model-emitted tool call doesn't abort the whole debug session.
    Unexpected exceptions are logged with ``exc_info`` so real bugs
    stay visible.
    """
    try:
        if name == "sample_table":
            return await sample_table(
                arguments["table"],
                limit=arguments.get("limit", 10),
                where=arguments.get("where"),
            )
        elif name == "get_schema":
            return await get_schema(arguments["table"])
        elif name == "get_column_stats":
            return await get_column_stats(arguments["table"])
        elif name == "trace_upstream":
            return await trace_upstream(
                arguments["table"], depth=arguments.get("depth", 10)
            )
        elif name == "trace_row":
            return await trace_row(
                arguments["table"],
                arguments["aai_id"],
                depth=arguments.get("depth", 10),
            )
        else:
            return f"(unknown tool: {name})"
    except KeyError as exc:
        return (
            f"(error calling {name}: missing required argument {exc}. "
            f"provided arguments: {sorted(arguments.keys())})"
        )
    except Exception as exc:
        logger.exception("tool %s raised an unexpected exception", name)
        return f"(error calling {name}: {exc})"
