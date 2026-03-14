"""
aaiclick.lineage.models - Data models for operation lineage tracking.

OperationLog records each data operation (create, add, concat, etc.)
with source and result table references, enabling lineage graph construction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class OperationLog:
    """Record of a single data operation.

    Captures which tables were consumed (source_tables) and produced
    (result_table) by an operation, along with metadata for debugging.
    """

    id: int
    result_table: str
    operation: str
    source_tables: list[str] = field(default_factory=list)
    sql_template: str | None = None
    task_id: int | None = None
    job_id: int | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
