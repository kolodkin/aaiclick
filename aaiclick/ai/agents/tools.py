"""
aaiclick.ai.agents.tools - Schema helpers shared by the lineage agents.
"""

from __future__ import annotations

import asyncio

from aaiclick.ai.agents.lineage_tools import describe_table
from aaiclick.oplog.lineage import OplogNode


async def get_schema(table: str) -> str:
    """Return column names and types for a table."""
    schema = await describe_table(table)
    lines = [f"{c.name}: {c.type}" for c in schema.columns]
    return "\n".join(lines) if lines else f"(table {table} not found)"


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
