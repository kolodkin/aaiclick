import pytest

from aaiclick.auth import config


def test_auth_disabled_by_default(monkeypatch):
    monkeypatch.delenv("AAICLICK_AUTH_ENABLED", raising=False)
    assert config.auth_enabled() is False


def test_auth_enabled_truthy(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    assert config.auth_enabled() is True


def test_ttls_have_defaults(monkeypatch):
    monkeypatch.delenv("AAICLICK_JWT_ACCESS_TTL", raising=False)
    monkeypatch.delenv("AAICLICK_JWT_REFRESH_TTL", raising=False)
    assert config.access_ttl() == 1800
    assert config.refresh_ttl() == 1209600


def test_require_jwt_secret_raises_when_enabled_and_unset(monkeypatch):
    monkeypatch.setenv("AAICLICK_AUTH_ENABLED", "true")
    monkeypatch.delenv("AAICLICK_JWT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_JWT_SECRET"):
        config.require_jwt_secret()


def test_admin_seed_none_when_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_ADMIN_USERNAME", raising=False)
    monkeypatch.delenv("AAICLICK_ADMIN_PASSWORD", raising=False)
    assert config.admin_seed() is None
