"""
aaiclick.lineage.collector - Lightweight event sink for operation lineage.

Collects OperationLog entries in memory during a data_context session.
Activated via data_context(lineage=True) and stored in a ContextVar.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

from ..snowflake_id import get_snowflake_id
from .models import OperationLog


# ContextVar holding the active collector (None when lineage is disabled)
_lineage_collector: ContextVar[Optional[LineageCollector]] = ContextVar(
    "lineage_collector", default=None
)


def get_lineage_collector() -> LineageCollector | None:
    """Return the active LineageCollector, or None if lineage is disabled."""
    return _lineage_collector.get()


class LineageCollector:
    """Collects operation events during a data_context session.

    Events are buffered in memory. The buffer can be accessed via
    the `operations` property for graph queries and testing.
    """

    def __init__(
        self,
        task_id: int | None = None,
        job_id: int | None = None,
    ) -> None:
        self._buffer: list[OperationLog] = []
        self.task_id = task_id
        self.job_id = job_id

    def record(
        self,
        result_table: str,
        operation: str,
        source_tables: list[str],
        sql: str | None = None,
    ) -> None:
        """Buffer an operation event."""
        entry = OperationLog(
            id=get_snowflake_id(),
            result_table=result_table,
            operation=operation,
            source_tables=list(source_tables),
            sql_template=sql,
            task_id=self.task_id,
            job_id=self.job_id,
        )
        self._buffer.append(entry)

    @property
    def operations(self) -> list[OperationLog]:
        """Return all buffered operation logs."""
        return list(self._buffer)

    def clear(self) -> None:
        """Clear the operation buffer."""
        self._buffer.clear()
