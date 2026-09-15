from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.server.app import API_PREFIX

from ..conftest import bearer


def admin() -> dict[str, str]:
    """Minted per call, never at import: ``bearer`` signs a 60-second JWT, and a
    module-level constant expires before a full-suite run reaches this file."""
    return bearer(1, role="admin")


def viewer() -> dict[str, str]:
    return bearer(2, role="viewer")


async def test_admin_can_create_user(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "newbie", "password": "pw"},
        headers=admin(),
    )
    assert res.status_code == 201 and res.json()["username"] == "newbie"


async def test_viewer_forbidden(orch_ctx, app_client, enabled):
    res = await app_client.post(
        f"{API_PREFIX}/users",
        json={"username": "x", "password": "pw"},
        headers=viewer(),
    )
    assert res.status_code == 403 and res.json()["code"] == "forbidden"


async def test_list_users(orch_ctx, app_client, enabled):
    await users.create_user(CreateUserRequest(username="alice", password="pw"))
    res = await app_client.get(f"{API_PREFIX}/users", headers=admin())
    assert res.status_code == 200 and res.json()["total"] >= 1


async def test_get_enable_and_email_routes(orch_ctx, app_client, enabled):
    created = await users.create_user(CreateUserRequest(username="carol", password="pw"))
    got = await app_client.get(f"{API_PREFIX}/users/{created.id}", headers=admin())
    assert got.status_code == 200 and got.json()["username"] == "carol"

    await app_client.post(f"{API_PREFIX}/users/{created.id}/disable", headers=admin())
    back = await app_client.post(f"{API_PREFIX}/users/{created.id}/enable", headers=admin())
    assert back.status_code == 200 and back.json()["disabled"] is False

    mail = await app_client.put(
        f"{API_PREFIX}/users/{created.id}/email", json={"email": "c@example.com"}, headers=admin()
    )
    assert mail.status_code == 200 and mail.json()["email"] == "c@example.com"
    assert (await app_client.get(f"{API_PREFIX}/users/0", headers=admin())).status_code == 404


async def test_admin_sets_a_role(orch_ctx, app_client, enabled):
    created = await users.create_user(CreateUserRequest(username="dan", password="pw"))
    res = await app_client.put(f"{API_PREFIX}/users/{created.id}/role", json={"role": "admin"}, headers=admin())
    assert res.status_code == 200 and res.json()["role"] == "admin"
    bad = await app_client.put(f"{API_PREFIX}/users/{created.id}/role", json={"role": "superadmin"}, headers=admin())
    assert bad.status_code == 422
