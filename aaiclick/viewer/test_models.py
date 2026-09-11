"""Round-trip the viewer tables through the SQL session."""

from __future__ import annotations

from sqlmodel import select

from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import DEFAULT_TENANT_ID
from aaiclick.viewer.models import DashboardRow, SavedQueryRow


async def test_viewer_rows_round_trip(orch_ctx_no_ch):
    async with get_sql_session() as session:
        session.add(SavedQueryRow(id=get_snowflake_id(), name="q", object="orders", scope="persistent", where="a > 1"))
        session.add(DashboardRow(id=get_snowflake_id(), name="d", scope="persistent", html="<p/>", queries="{}"))
        await session.commit()
    async with get_sql_session() as session:
        q = (await session.execute(select(SavedQueryRow).where(SavedQueryRow.name == "q"))).scalar_one()
        d = (await session.execute(select(DashboardRow).where(DashboardRow.name == "d"))).scalar_one()
    assert (q.object, q.where, q.tenant_id) == ("orders", "a > 1", DEFAULT_TENANT_ID)
    assert d.html == "<p/>"
