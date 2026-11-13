"""Round-trip the viewer table through the SQL session."""

from __future__ import annotations

from sqlmodel import select

from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.viewer.models import DashboardRow


async def test_viewer_rows_round_trip(orch_ctx_no_ch):
    async with get_sql_session() as session:
        session.add(DashboardRow(id=get_snowflake_id(), name="d", scope="persistent", html="<p/>", queries="{}"))
        await session.commit()
    async with get_sql_session() as session:
        d = (await session.execute(select(DashboardRow).where(DashboardRow.name == "d"))).scalar_one()
    assert (d.html, d.params) == ("<p/>", "[]")
