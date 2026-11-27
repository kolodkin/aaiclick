"""Tests for the orchestration SQL engine factory."""

import pytest
from sqlalchemy.exc import IntegrityError

from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


async def test_engine_rejects_child_row_without_parent(orch_ctx_no_ch):
    """A task referencing a job that does not exist is a FK violation on every backend."""
    async with get_sql_session() as session:
        session.add(Task(id=get_snowflake_id(), job_id=get_snowflake_id(), entrypoint="mod.fn", name="orphan"))
        with pytest.raises(IntegrityError):
            await session.commit()
