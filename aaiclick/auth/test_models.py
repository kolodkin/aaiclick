from sqlmodel import select

from aaiclick.auth.models import ROLE_ADMIN, RefreshToken, User
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


async def test_user_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="alice", password_hash="x", role=ROLE_ADMIN))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(User).where(User.username == "alice"))).scalar_one()
        assert row.id == uid
        assert row.role == ROLE_ADMIN
        assert row.disabled is False


async def test_refresh_token_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="bob", password_hash="x", role=ROLE_ADMIN))
        session.add(RefreshToken(id=get_snowflake_id(), user_id=uid, token_hash="h", expires_at=utc_now()))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(RefreshToken).where(RefreshToken.user_id == uid))).scalar_one()
        assert row.token_hash == "h"
        assert row.rotated_at is None
