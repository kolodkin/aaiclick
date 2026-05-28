from __future__ import annotations

from aaiclick.server.app import STATIC_DIR, app


def test_static_dir_constant_points_into_server_package():
    assert STATIC_DIR.name == "static"
    assert STATIC_DIR.parent.name == "server"


def test_api_routes_still_registered():
    paths = {r.path for r in app.routes if hasattr(r, "path")}
    assert "/health" in paths
    assert any(p.startswith("/api/v0/jobs") for p in paths)
