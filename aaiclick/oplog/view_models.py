"""Lineage / oplog domain view models.

Domain-specific pydantic models for the lineage AI agents. The
``OplogNode`` / ``OplogEdge`` / ``OplogGraph`` types in
``aaiclick.oplog.lineage`` are themselves pydantic ``BaseModel``s and
serialize natively across MCP and REST — no adapter layer needed.
"""

from __future__ import annotations

from pydantic import BaseModel


class LineageAnswer(BaseModel):
    """Plain-text answer from a lineage AI agent (``explain_lineage`` / ``debug_result``)."""

    text: str
