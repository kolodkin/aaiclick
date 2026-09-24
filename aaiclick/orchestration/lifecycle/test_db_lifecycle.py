"""Tests for TableRegistry.schema_doc persistence."""

from __future__ import annotations

from sqlmodel import select

from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.orch_context import get_sql_session


async def test_table_registry_accepts_schema_doc(orch_ctx):
    async with get_sql_session() as session:
        session.add(
            TableRegistry(
                table_name="t_test_1",
                job_id=None,
                task_id=None,
                run_id=None,
                schema_doc='{"columns":[],"fieldtype":"s"}',
            )
        )
        await session.commit()

    async with get_sql_session() as session:
        result = await session.execute(select(TableRegistry).where(TableRegistry.table_name == "t_test_1"))
        assert result.scalar_one().schema_doc == '{"columns":[],"fieldtype":"s"}'


async def test_table_registry_schema_doc_is_optional(orch_ctx):
    async with get_sql_session() as session:
        session.add(TableRegistry(table_name="t_test_2"))
        await session.commit()

    async with get_sql_session() as session:
        result = await session.execute(select(TableRegistry).where(TableRegistry.table_name == "t_test_2"))
        assert result.scalar_one().schema_doc is None
