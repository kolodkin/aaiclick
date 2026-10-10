from collections.abc import Iterator
from pathlib import Path

import pytest

from aaiclick.auth.models import ROLE_VIEWER
from aaiclick.auth.view_models import CreateUserRequest
from aaiclick.internal_api import users
from aaiclick.sandbox.repo import SandboxRepo, sandbox_repo_override
from aaiclick.testing import init_bare_repo

from ..app import API_PREFIX
from ..conftest import bearer

SOURCE = """\
from aaiclick.orchestration import TaskResult, job, task


@task
async def hello():
    return "hello"


@job
def hello_job():
    return TaskResult(tasks=[hello()])
"""


@pytest.fixture
def sandbox_remote(tmp_path: Path) -> Iterator[Path]:
    remote = init_bare_repo(tmp_path / "remote.git")
    with sandbox_repo_override(SandboxRepo(str(remote), tmp_path / "clone")):
        yield remote


@pytest.fixture
async def viewer_headers(orch_ctx, enabled) -> dict[str, str]:
    user = await users.create_user(CreateUserRequest(username="sb_viewer", password="pw", role=ROLE_VIEWER))
    return bearer(user.id, role=ROLE_VIEWER)


async def test_config_hides_remote_from_non_admin(app_client, viewer_headers, sandbox_remote):
    body = (await app_client.get(f"{API_PREFIX}/sandbox/config", headers=viewer_headers)).json()
    assert body == {"enabled": True, "remote": None}


async def test_config_shows_remote_to_admin(orch_ctx, app_client, sandbox_remote):
    body = (await app_client.get(f"{API_PREFIX}/sandbox/config")).json()
    assert body == {"enabled": True, "remote": str(sandbox_remote)}


async def test_config_when_disabled(orch_ctx, app_client, monkeypatch):
    monkeypatch.delenv("AAICLICK_SANDBOX", raising=False)
    body = (await app_client.get(f"{API_PREFIX}/sandbox/config")).json()
    assert body == {"enabled": False, "remote": None}


async def test_submit_when_disabled_is_404(orch_ctx, app_client, monkeypatch):
    monkeypatch.delenv("AAICLICK_SANDBOX", raising=False)
    resp = await app_client.post(f"{API_PREFIX}/sandbox", json={"name": "hello", "source": SOURCE})
    assert resp.status_code == 404


async def test_viewer_can_submit_and_row_is_pending(app_client, viewer_headers, sandbox_remote):
    resp = await app_client.post(
        f"{API_PREFIX}/sandbox", json={"name": "hello", "source": SOURCE}, headers=viewer_headers
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["job_names"] == ["hello_job"]
    assert body["submitted_by"] == "sb_viewer"
    assert body["path"].startswith("2") and body["path"].endswith("_hello.py")
    listed = (await app_client.get(f"{API_PREFIX}/sandbox", headers=viewer_headers)).json()
    assert [r["id"] for r in listed["items"]] == [body["id"]]
    detail = (await app_client.get(f"{API_PREFIX}/sandbox/{body['id']}", headers=viewer_headers)).json()
    assert detail["source"] == SOURCE


@pytest.mark.parametrize(
    "payload, detail",
    [
        ({"name": "9bad", "source": "x"}, "name"),
        ({"name": "ok", "source": "def a(:"}, "line 1"),
        ({"name": "ok", "source": "x = 1\n"}, "no @job function found"),
    ],
)
async def test_submit_rejects(app_client, viewer_headers, sandbox_remote, payload, detail):
    resp = await app_client.post(f"{API_PREFIX}/sandbox", json=payload, headers=viewer_headers)
    assert resp.status_code == 422
    assert detail in resp.text
