"""
aaiclick.ai.agents.lineage_tools - Tier 1 agent tools scoped to a lineage graph.

All tools operate on a single ``OplogGraph`` — the backward lineage of the
target table being debugged. The read-only SQL sandbox and graph
classification live in ``aaiclick.oplog``; this module adds the
LLM-facing tool loop: argument coercion, text formatting, and the
OpenAI-style tool definitions.

See ``docs/designs/lineage.md`` for the design.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from aaiclick.oplog.lineage import GraphNode, OplogGraph, classify_nodes
from aaiclick.oplog.query_sandbox import (
    DEFAULT_ROW_LIMIT,
    ROW_LIMIT_CEILING,
    QueryResult,
    TableSchema,
    ToolError,
    describe_table,
    liveness,
    sandboxed_select,
)

logger = logging.getLogger(__name__)


class LineageToolbox:
    """Scoped Tier 1 tool surface.

    Instantiate once per debug session with the backward lineage graph of
    the target table. A query is rejected unless every table it reads is in
    ``graph.tables``; table functions are rejected outright.
    """

    def __init__(self, graph: OplogGraph):
        self.graph = graph
        self._tables = graph.tables
        self._kinds = classify_nodes(graph)
        self._node_by_table = {n.table: n for n in graph.nodes}
        self._liveness_cache: dict[str, bool] | None = None

    async def query_table(self, sql: str, row_limit: int = DEFAULT_ROW_LIMIT) -> QueryResult | ToolError:
        """Execute a read-only SELECT against tables in the current graph.

        The out-of-scope error gets a graph-flavored suffix so the LLM knows
        which tool to call to inspect the scope.
        """
        if not isinstance(row_limit, int) or isinstance(row_limit, bool):
            try:
                row_limit = int(row_limit)
            except (TypeError, ValueError):
                return ToolError("invalid_argument", f"row_limit must be an integer, got {row_limit!r}.")
        result = await sandboxed_select(sql, self._tables, row_limit)
        if isinstance(result, ToolError) and result.kind == "out_of_scope":
            return result._replace(message=result.message + " Use list_graph_nodes() to see what's in scope.")
        return result

    async def get_op_sql(self, table: str) -> str | ToolError:
        """Rendered SQL template for the operation that produced ``table``."""
        node = self._node_by_table.get(table)
        if node is None:
            return ToolError("not_found", f"No operation in the graph produced {table}.")
        return node.sql_template or ""

    async def list_graph_nodes(self) -> list[GraphNode]:
        """Every table in the graph with kind + liveness.

        The liveness lookup is cached for the lifetime of the toolbox so the
        agent can re-call the tool (or both seed-context and tool call) without
        a second round-trip to ``system.tables``.
        """
        if self._liveness_cache is None:
            self._liveness_cache = await liveness(self._tables)
        alive = self._liveness_cache
        nodes: list[GraphNode] = []
        for table in sorted(self._tables):
            node = self._node_by_table.get(table)
            operation = node.operation if node else "(input)"
            task_id = node.task_id if node else None
            job_id = node.job_id if node else None
            nodes.append(
                GraphNode(
                    table=table,
                    kind=self._kinds[table],
                    operation=operation,
                    live=alive.get(table, False),
                    task_id=task_id,
                    job_id=job_id,
                )
            )
        return nodes

    async def get_schema(self, table: str) -> TableSchema | ToolError:
        """Columns and types for a table in the graph."""
        if table not in self._tables:
            return ToolError("out_of_scope", f"{table} is not in the lineage graph.")
        try:
            return await describe_table(table)
        except Exception as exc:
            logger.exception("DESCRIBE TABLE %s failed", table)
            return ToolError("not_live", f"Could not describe {table}: {exc}")

    async def dispatch_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """Invoke a tool by name and format the result as LLM-readable text.

        Missing arguments, unknown tools, and in-tool ``ToolError`` results are
        all returned as strings so one bad call doesn't abort the loop — the
        model can read the error and retry.
        """
        handler = _TOOL_HANDLERS.get(name)
        if handler is None:
            return f"(unknown tool: {name})"
        try:
            result = await handler(self, arguments)
        except KeyError as exc:
            return f"(error calling {name}: missing required argument {exc})"
        except Exception as exc:
            logger.exception("tool %s raised an unexpected exception", name)
            return f"(error calling {name}: {exc})"
        return _format_tool_result(result)


_TOOL_HANDLERS: dict[str, Any] = {
    "query_table": lambda tb, a: tb.query_table(a["sql"], row_limit=a.get("row_limit", DEFAULT_ROW_LIMIT)),
    "get_op_sql": lambda tb, a: tb.get_op_sql(a["table"]),
    "list_graph_nodes": lambda tb, _a: tb.list_graph_nodes(),
    "get_schema": lambda tb, a: tb.get_schema(a["table"]),
}


def _format_tool_result(result: Any) -> str:
    """Serialize a tool's typed result as compact text for the LLM."""
    if isinstance(result, ToolError):
        return json.dumps({"error": {"kind": result.kind, "message": result.message}})
    if isinstance(result, QueryResult):
        if not result.rows and not result.columns:
            return "(empty result)"
        header = " | ".join(result.columns)
        body = "\n".join(" | ".join(str(v) for v in row) for row in result.rows)
        suffix = f"\n(truncated to {len(result.rows)} rows)" if result.truncated else ""
        return f"{header}\n{body}{suffix}" if body else f"{header}\n(no rows){suffix}"
    if isinstance(result, TableSchema):
        lines = [f"{c.name}: {c.type}" for c in result.columns]
        return f"`{result.table}`:\n" + "\n".join(lines) if lines else f"`{result.table}`: (no columns)"
    if isinstance(result, list):
        lines = []
        for n in result:
            lines.append(
                f"- {n.table} [{n.kind}] operation={n.operation} live={n.live} task_id={n.task_id} job_id={n.job_id}"
            )
        return "\n".join(lines) if lines else "(no nodes)"
    if isinstance(result, str):
        return result or "(empty)"
    return str(result)


LINEAGE_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_graph_nodes",
            "description": (
                "List every table in the current lineage graph with its kind "
                "(input / intermediate / target), the operation that produced it, "
                "and whether it currently exists in ClickHouse."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_op_sql",
            "description": (
                "Return the rendered SQL template for the operation that produced "
                "`table`. Use this first to form a hypothesis before running queries."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table in the graph"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": (
                "Return column names and types for a table in the graph. Rejects tables outside the graph."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table in the graph"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_table",
            "description": (
                "Execute a read-only SELECT against tables in the current lineage "
                "graph. Rejects non-SELECT, out-of-scope tables, table functions, and "
                "SETTINGS clauses; write a CTE as a subquery in FROM instead, and use "
                "has(column, value) rather than IN for an array column. Results are "
                "capped at row_limit."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SELECT statement"},
                    "row_limit": {
                        "type": "integer",
                        "description": f"Max rows returned (default {DEFAULT_ROW_LIMIT}, ceiling {ROW_LIMIT_CEILING})",
                    },
                },
                "required": ["sql"],
            },
        },
    },
]
