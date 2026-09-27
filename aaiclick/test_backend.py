import pytest

from aaiclick.backend import parse_ch_url, redact_url


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
