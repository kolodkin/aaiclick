from datetime import timedelta

from aaiclick.audit import store
from aaiclick.audit.view_models import AuditListFilter
from aaiclick.datetime_utils import utc_now
from aaiclick.internal_api import audit


async def _row(**overrides):
    values = dict(
        at=utc_now(),
        user_id=1,
        username="alice",
        auth_kind="session",
        tenant_id=1,
        method="POST",
        path="/api/v0/jobs:run",
        action=None,
        status=200,
        duration_ms=12,
        client_ip="127.0.0.1",
    )
    values.update(overrides)
    return await store.insert(**values)


async def test_list_filters_and_orders_newest_first(orch_ctx):
    old = await _row(at=utc_now() - timedelta(days=2), method="GET", path="/api/v0/jobs")
    await _row(user_id=2, username="bob", path="/mcp/", action="run_job")
    newest = await _row()

    page = await audit.list_audit()
    assert page.total == 3 and page.items[0].id == newest.id and page.items[-1].id == old.id

    assert [r.username for r in (await audit.list_audit(AuditListFilter(user_id=2))).items] == ["bob"]
    assert (await audit.list_audit(AuditListFilter(username="alice"))).total == 2
    assert (await audit.list_audit(AuditListFilter(method="get"))).total == 1
    assert (await audit.list_audit(AuditListFilter(path="/mcp"))).items[0].action == "run_job"
    assert (await audit.list_audit(AuditListFilter(since=utc_now() - timedelta(hours=1)))).total == 2
    assert (await audit.list_audit(AuditListFilter(limit=1, offset=1))).items[0].username == "bob"
