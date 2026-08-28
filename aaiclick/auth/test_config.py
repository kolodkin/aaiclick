import pytest

from aaiclick.auth import config


@pytest.mark.parametrize(
    "is_local, expected",
    [
        pytest.param(True, False, id="local-mode-disables-auth"),
        pytest.param(False, True, id="distributed-mode-enables-auth"),
    ],
)
def test_auth_enabled(monkeypatch, is_local, expected):
    monkeypatch.setattr(config, "is_local", lambda: is_local)
    assert config.auth_enabled() is expected


def test_ttls_have_defaults(monkeypatch):
    monkeypatch.delenv("AAICLICK_JWT_ACCESS_TTL", raising=False)
    monkeypatch.delenv("AAICLICK_JWT_REFRESH_TTL", raising=False)
    assert config.access_ttl() == 1800
    assert config.refresh_ttl() == 1209600


def test_require_jwt_secret_raises_when_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_JWT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_JWT_SECRET"):
        config.require_jwt_secret()


def test_admin_seed_none_when_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_ADMIN_USERNAME", raising=False)
    monkeypatch.delenv("AAICLICK_ADMIN_PASSWORD", raising=False)
    assert config.admin_seed() is None


def test_admin_seed_username_defaults_to_superadmin(monkeypatch):
    monkeypatch.delenv("AAICLICK_ADMIN_USERNAME", raising=False)
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", "pw")
    seed = config.admin_seed()
    assert seed is not None and seed.username == "superadmin" and seed.password == "pw"


def test_admin_seed_env_username_wins(monkeypatch):
    monkeypatch.setenv("AAICLICK_ADMIN_USERNAME", "ops")
    monkeypatch.setenv("AAICLICK_ADMIN_PASSWORD", "pw")
    seed = config.admin_seed()
    assert seed is not None and seed.username == "ops"
