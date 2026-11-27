import pytest

from aaiclick.backend import get_ch_url, get_sql_url, internal_db, internal_table, parse_ch_url, redact_url


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


# req: user_guide/deployment/local-config
def test_local_urls_derive_from_local_root(monkeypatch, tmp_path):
    monkeypatch.setenv("AAICLICK_LOCAL_ROOT", str(tmp_path))
    monkeypatch.delenv("AAICLICK_SQL_URL", raising=False)
    monkeypatch.delenv("AAICLICK_CH_URL", raising=False)
    assert get_sql_url() == f"sqlite+aiosqlite:///{tmp_path}/local.db"
    assert get_ch_url() == f"chdb://{tmp_path}/chdb_data"


# req: user_guide/deployment/internal-database
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("chdb:///tmp/x/chdb_data", "default_internal"),
        ("clickhouse://u:p@h:8123/warehouse", "warehouse_internal"),
        ("clickhouse://h:8123", "default_internal"),
    ],
)
def test_internal_db(monkeypatch, url, expected):
    monkeypatch.setenv("AAICLICK_CH_URL", url)
    assert internal_db() == expected
    assert internal_table("task_logs") == f"`{expected}`.`task_logs`"


def test_internal_table_escapes_backticks(monkeypatch):
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://h:8123/we`ird")
    assert internal_table("task_logs") == "`we``ird_internal`.`task_logs`"


def test_internal_db_rejects_internal_suffix(monkeypatch):
    """A data database named ``X_internal`` would be deployment ``X``'s internal one."""
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://h:8123/warehouse_internal")
    with pytest.raises(ValueError, match="_internal"):
        internal_db()
