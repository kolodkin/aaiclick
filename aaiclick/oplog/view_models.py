"""Lineage / oplog domain view models."""

from __future__ import annotations

from pydantic import BaseModel


class LineageAnswer(BaseModel):
    """Plain-text answer from a lineage AI agent (``explain_lineage`` / ``debug_result``)."""

    text: str
