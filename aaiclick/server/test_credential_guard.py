import pytest

from .credential_guard import CREDENTIAL_ENV_VARS, require_no_placeholder_credentials


def test_placeholder_credential_refuses_start(monkeypatch):
    monkeypatch.setenv("AAICLICK_JWT_SECRET", "change-me-jwt")

    with pytest.raises(RuntimeError, match=r"AAICLICK_JWT_SECRET.*env\.init\.sh"):
        require_no_placeholder_credentials()


def test_real_credentials_pass(monkeypatch):
    for name in CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    require_no_placeholder_credentials()
