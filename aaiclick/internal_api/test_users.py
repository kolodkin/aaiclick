import pytest

from aaiclick.auth.view_models import CreateUserRequest, UserView
from aaiclick.internal_api import users
from aaiclick.internal_api.errors import Conflict, NotFound
from aaiclick.view_models import Page


async def test_create_user_returns_view(orch_ctx):
    view = await users.create_user(
        CreateUserRequest(username="alice", password="pw", role="admin")
    )
    assert isinstance(view, UserView)
    assert view.username == "alice" and view.role == "admin"


async def test_create_duplicate_raises_conflict(orch_ctx):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    with pytest.raises(Conflict):
        await users.create_user(CreateUserRequest(username="alice", password="pw"))


async def test_list_users_paginated(orch_ctx):
    await users.create_user(CreateUserRequest(username="a", password="pw"))
    await users.create_user(CreateUserRequest(username="b", password="pw"))
    page = await users.list_users()
    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2


async def test_set_role_missing_raises_not_found(orch_ctx):
    with pytest.raises(NotFound):
        await users.set_role(12345, "admin")
