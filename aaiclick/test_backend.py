import pytest

from aaiclick.backend import parse_ch_url, redact_url, s3_client_settings


def test_parse_ch_url_percent_decodes_userinfo(monkeypatch):
    """A password holding ``@`` or ``:`` can only be written percent-encoded,
    exactly as in the SQL URL — so it must reach ClickHouse decoded."""
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://ana%40corp:p%40ss%3Aw0rd@ch.local:9000/metrics")
    assert parse_ch_url() == {
        "host": "ch.local",
        "port": 9000,
        "username": "ana@corp",
        "password": "p@ss:w0rd",
        "database": "metrics",
    }


def test_parse_ch_url_defaults(monkeypatch):
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://ch.local")
    assert parse_ch_url() == {
        "host": "ch.local",
        "port": 8123,
        "username": "default",
        "password": "",
        "database": "default",
    }


@pytest.mark.parametrize(
    "url, expected",
    [
        pytest.param("postgresql+asyncpg://u:pw@h:5432/db", "postgresql+asyncpg://u:***@h:5432/db", id="password"),
        pytest.param("clickhouse://u:p%40ss@h/db", "clickhouse://u:***@h/db", id="encoded_password"),
        pytest.param("clickhouse://u@h/db", "clickhouse://u@h/db", id="user_only"),
        pytest.param("sqlite+aiosqlite:////tmp/local.db", "sqlite+aiosqlite:////tmp/local.db", id="no_userinfo"),
    ],
)
def test_redact_url(url, expected):
    assert redact_url(url) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, {}, id="unset-public-buckets-only"),
        pytest.param("", {}, id="empty"),
        pytest.param("server", {"s3_allow_server_credentials_in_user_queries": 1}, id="server-identity"),
    ],
)
def test_s3_client_settings(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("AAICLICK_S3_CREDENTIALS", raising=False)
    else:
        monkeypatch.setenv("AAICLICK_S3_CREDENTIALS", value)
    assert s3_client_settings() == expected


def test_s3_credentials_rejects_unknown_mode(monkeypatch):
    monkeypatch.setenv("AAICLICK_S3_CREDENTIALS", "bogus")
    with pytest.raises(ValueError, match="'server'"):
        s3_client_settings()
