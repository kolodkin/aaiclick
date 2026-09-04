from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

from ..conftest import bearer

ADMIN = bearer(1, superadmin=True)
VIEWER = bearer(2, tenants={9: "admin"})


async def test_admin_can_create_user(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "newbie", "password": "pw"},
        headers=ADMIN,
    )
    assert res.status_code == 201 and res.json()["username"] == "newbie"


async def test_viewer_forbidden(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "x", "password": "pw"},
        headers=VIEWER,
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_list_users(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.get(f"{API_PREFIX}/users", headers=ADMIN)
    assert res.status_code == 200 and res.json()["total"] >= 1


async def test_get_enable_and_email_routes(orch_ctx, app_client, enabled):
    created = await users.create_user(CreateUserRequest(username="carol", password="pw"))
    got = await app_client.get(f"{API_PREFIX}/users/{created.id}", headers=ADMIN)
    assert got.status_code == 200 and got.json()["username"] == "carol"

    await app_client.post(f"{API_PREFIX}/users/{created.id}/disable", headers=ADMIN)
    back = await app_client.post(f"{API_PREFIX}/users/{created.id}/enable", headers=ADMIN)
    assert back.status_code == 200 and back.json()["disabled"] is False

    mail = await app_client.put(
        f"{API_PREFIX}/users/{created.id}/email", json={"email": "c@example.com"}, headers=ADMIN
    )
    assert mail.status_code == 200 and mail.json()["email"] == "c@example.com"
    assert (await app_client.get(f"{API_PREFIX}/users/0", headers=ADMIN)).status_code == 404
