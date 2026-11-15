"""Tests for Azure Blob Storage: ``az://container/path`` URLs on
``Object.export()`` and ``open_object_from_url()``.

Azurite, Microsoft's Azure Storage emulator, stands in for Azure. Locally the
module starts it with ``npx``; set AAICLICK_TEST_AZURITE_ENDPOINT to use one
already running (the ``Blob storage dist`` CI job, where the ClickHouse server
defines the ``azurite_test`` collection in ``config.d``). On chdb the
connection string comes from AZURE_STORAGE_CONNECTION_STRING. Under xdist, set
the endpoint so workers share one Azurite.
"""

from __future__ import annotations

import io
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from typing import NamedTuple

import pyarrow.parquet as pq
import pytest
from azure.storage.blob import BlobServiceClient
from sqlalchemy import text

from aaiclick import create_object_from_value, open_object_from_url
from aaiclick.backend import internal_table, is_chdb
from aaiclick.data.azure import AZURE_CONNECTION_ENV
from aaiclick.data.object_context import ch_client, get_ch_client
from aaiclick.data.url_utils import AzUrl, parse_az_url
from aaiclick.orchestration.execution.runner import deserialize_task_params
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.testing import SAMPLE_TABLE

_CONTAINER = "aaiclick-test"
_CONNECTION = "azurite_test"
# Azurite's published development account — not a secret.
_DEV_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


def _connection_string(endpoint: str) -> str:
    return (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={_DEV_KEY};"
        f"BlobEndpoint={endpoint}/devstoreaccount1;"
    )


class Container:
    """The test container on Azurite: ``az://`` URLs for aaiclick, reads for asserts."""

    def __init__(self, endpoint: str):
        service = BlobServiceClient.from_connection_string(_connection_string(endpoint))
        self._client = service.get_container_client(_CONTAINER)
        if not self._client.exists():
            self._client.create_container()

    def url(self, path: str) -> str:
        return f"az://{_CONTAINER}/{path}"

    def read_parquet(self, path: str) -> dict[str, list]:
        body = self._client.download_blob(path).readall()
        return pq.read_table(io.BytesIO(body)).to_pydict()


class Azurite(NamedTuple):
    proc: subprocess.Popen
    endpoint: str
    workdir: str


def _start_azurite() -> Azurite:
    npx = shutil.which("npx")
    if npx is None:
        pytest.skip("npx not found; set AAICLICK_TEST_AZURITE_ENDPOINT to use a running Azurite")
    with socket.socket() as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    workdir = tempfile.mkdtemp(prefix="azurite-")
    args = ["-y", "-p", "azurite@3.34.0", "azurite-blob", "--silent", "--location", workdir]
    args += ["--blobHost", "127.0.0.1", "--blobPort", str(port), "--skipApiVersionCheck", "--loose"]
    proc = subprocess.Popen([npx, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and proc.poll() is None:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return Azurite(proc, f"http://127.0.0.1:{port}", workdir)
        except OSError:
            time.sleep(0.5)
    proc.terminate()
    proc.wait()
    shutil.rmtree(workdir, ignore_errors=True)
    raise RuntimeError(f"Azurite did not start (exit code {proc.returncode})")


@pytest.fixture(scope="module")
def container() -> Iterator[Container]:
    """Azurite plus the test container; on chdb, the connection string in our env."""
    endpoint = os.getenv("AAICLICK_TEST_AZURITE_ENDPOINT") or None
    azurite = None
    if endpoint is None:
        if not is_chdb():
            pytest.skip("ClickHouse server tests need AAICLICK_TEST_AZURITE_ENDPOINT and the azurite_test collection")
        azurite = _start_azurite()
        endpoint = azurite.endpoint
    try:
        with pytest.MonkeyPatch.context() as env:
            env.setenv(AZURE_CONNECTION_ENV, _connection_string(endpoint))
            yield Container(endpoint)
    finally:
        if azurite is not None:
            azurite.proc.terminate()
            azurite.proc.wait()
            shutil.rmtree(azurite.workdir, ignore_errors=True)


# =============================================================================
# Validation
# =============================================================================


@pytest.mark.parametrize(
    "url, connection, match",
    [
        ("az://container", "lake", "blob path"),
        ("az:///path.parquet", "lake", "container"),
        ("az://c/p.parquet?sig=abc", "lake", "query parameters"),
        ("s3://b/k.parquet", "lake", "connection"),
        ("/tmp/x.parquet", "lake", "connection|chdb backend"),
        ("az://c/p.parquet", "bad name", "connection"),
    ],
)
async def test_open_rejects_bad_azure_arguments(ctx, url, connection, match):
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url, connection=connection)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("az://reports/2026/daily.parquet", AzUrl("reports", "2026/daily.parquet")),
        ("az://reports/part-?.parquet", AzUrl("reports", "part-?.parquet")),
        ("az://reports/a#b.parquet", AzUrl("reports", "a#b.parquet")),
        ("AZ://reports/x.parquet", AzUrl("reports", "x.parquet")),
    ],
)
def test_parse_az_url(url, expected):
    assert parse_az_url(url) == expected


async def test_chdb_missing_env_var(ctx, monkeypatch):
    monkeypatch.setattr(ch_client, "is_chdb", lambda: True)
    monkeypatch.delenv(AZURE_CONNECTION_ENV, raising=False)
    with pytest.raises(ValueError, match=AZURE_CONNECTION_ENV):
        await open_object_from_url("az://c/p.parquet", connection=_CONNECTION)


async def test_non_ascii_connection_name_is_rejected_before_sql(ctx, monkeypatch):
    """ClickHouse cannot parse it, and its syntax error would echo the key."""
    monkeypatch.setenv(AZURE_CONNECTION_ENV, "AccountName=a;AccountKey=SECRETKEY")
    with pytest.raises(ValueError, match="Invalid connection name") as raised:
        await open_object_from_url("az://c/p.parquet", connection="café")
    assert "SECRETKEY" not in str(raised.value)


async def test_failed_collection_ddl_keeps_key_out_of_exception(ctx, monkeypatch):
    """ClickHouse's error echoes the statement; neither message nor chain may carry it."""
    monkeypatch.setattr(ch_client, "is_chdb", lambda: True)
    monkeypatch.setenv(AZURE_CONNECTION_ENV, "AccountName=a;AccountKey=SECRETKEY")

    class EchoingClient:
        async def command(self, sql, **_):
            raise RuntimeError(f"Syntax error: {sql}")

    monkeypatch.setattr(ch_client, "get_ch_client", EchoingClient)
    with pytest.raises(ValueError, match="Could not define named collection") as raised:
        await open_object_from_url("az://c/p.parquet", connection="lake")
    assert raised.value.__context__ is None
    assert "SECRETKEY" not in str(raised.value)


async def test_server_needs_connection(ctx, monkeypatch):
    monkeypatch.setattr(ch_client, "is_chdb", lambda: False)
    with pytest.raises(ValueError, match="needs connection"):
        await open_object_from_url("az://c/p.parquet")


# =============================================================================
# Export and open
# =============================================================================


@pytest.mark.parametrize("extension", [".parquet", ".arrow", ".jsonl"])
async def test_export_open_round_trip(ctx, container, extension):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(container.url(f"roundtrip/sample{extension}"), overwrite=True, connection=_CONNECTION)
    opened = await open_object_from_url(url, connection=_CONNECTION)
    assert await opened.data() == SAMPLE_TABLE


async def test_export_honors_view_constraints(ctx, container):
    obj = await create_object_from_value(SAMPLE_TABLE)
    view = obj.view(where="score > 15", order_by="id", limit=1)
    await view.export(container.url("export/view.parquet"), overwrite=True, connection=_CONNECTION)
    assert container.read_parquet("export/view.parquet") == {"id": [2], "name": ["bob"], "score": [20.0]}


async def test_export_to_existing_key_needs_overwrite(ctx, container):
    url = container.url("export/twice.parquet")
    await (await create_object_from_value([1, 2])).export(url, overwrite=True, connection=_CONNECTION)
    replacement = await create_object_from_value([7, 8, 9])
    with pytest.raises(Exception, match="already exists"):
        await replacement.export(url, connection=_CONNECTION)
    assert container.read_parquet("export/twice.parquet") == {"value": [1, 2]}
    await replacement.export(url, overwrite=True, connection=_CONNECTION)
    assert container.read_parquet("export/twice.parquet") == {"value": [7, 8, 9]}


@pytest.mark.parametrize("setting", ["azure_truncate_on_insert", "azure_create_new_file_on_insert"])
async def test_export_rejects_reserved_azure_settings(ctx, setting):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="cannot set"):
        await obj.export("az://c/data.parquet", connection=_CONNECTION, ch_settings={setting: 1})


@pytest.mark.parametrize(
    "prefix, glob",
    [("glob", "*"), pytest.param("qglob", "?", id="question-mark-is-a-glob-not-a-query")],
)
async def test_open_glob(ctx, container, prefix, glob):
    for part in (1, 2):
        obj = await create_object_from_value([part])
        await obj.export(container.url(f"{prefix}/part-{part}.parquet"), overwrite=True, connection=_CONNECTION)
    opened = await open_object_from_url(container.url(f"{prefix}/part-{glob}.parquet"), connection=_CONNECTION)
    assert sorted(await opened.data()) == [1, 2]


async def test_open_url_with_quote_in_key(ctx, container):
    obj = await create_object_from_value([1, 2, 3])
    url = await obj.export(container.url("open/it's.parquet"), overwrite=True, connection=_CONNECTION)
    assert await (await open_object_from_url(url, connection=_CONNECTION)).data() == [1, 2, 3]


async def test_located_azure_object_survives_task_boundary(ctx, container):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(container.url("open/boundary.parquet"), overwrite=True, connection=_CONNECTION)
    opened = await open_object_from_url(url, connection=_CONNECTION)
    params = await deserialize_task_params({"data": opened._serialize_ref()}, job_id=0)
    rehydrated = params["data"]
    assert rehydrated.schema.connection == _CONNECTION
    assert await rehydrated.data() == SAMPLE_TABLE
    with pytest.raises(ValueError, match="read-only"):
        await rehydrated.insert({"id": [4], "name": ["dave"], "score": [1.0]})


async def test_oplog_and_registry_carry_no_account_key(orch_ctx, container):
    async with task_scope(task_id=7, job_id=70, run_id=700):
        obj = await create_object_from_value(SAMPLE_TABLE)
        url = await obj.export(container.url("open/oplog.parquet"), overwrite=True, connection=_CONNECTION)
        opened = await open_object_from_url(url, connection=_CONNECTION)
        assert await opened.view(where="score > 15")["score"].sum().data() == 50.25

    oplog = await get_ch_client().query(
        "SELECT count(), countIf(positionCaseInsensitive(toString(tuple(*)), 'AccountKey') > 0)"
        f" FROM {internal_table('operation_log')} WHERE job_id = 70"
    )
    recorded, leaked = oplog.result_rows[0]
    assert recorded > 0
    assert leaked == 0
    async with get_sql_session() as session:
        leaked = await session.execute(text("SELECT count(*) FROM table_registry WHERE schema_doc LIKE '%AccountKey%'"))
        assert leaked.scalar_one() == 0
