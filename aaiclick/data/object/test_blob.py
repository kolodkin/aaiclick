"""Tests for blob storage: ``Object.export()`` to a bucket URL and
``open_object_from_url()`` over it, on S3 and Azure.

Shared behavior runs once per backend through the ``store`` fixture:
- S3: an in-process moto server. Set AAICLICK_TEST_FILESERVER_HOST=host.docker.internal
  when ClickHouse runs in a container and must reach back to the test process.
- Azure: Azurite, Microsoft's Azure Storage emulator, started with ``npx``; set
  AAICLICK_TEST_AZURITE_ENDPOINT to share a running one (CI, or under xdist). A
  ClickHouse server must define the ``azurite_test`` collection
  (docker/clickhouse/test/); chdb reads AZURE_STORAGE_CONNECTION_STRING.
"""

from __future__ import annotations

import copy
import io
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from typing import NamedTuple

import boto3
import pyarrow.parquet as pq
import pytest
from azure.storage.blob import BlobServiceClient
from moto.server import ThreadedMotoServer
from sqlalchemy import text

from aaiclick import ColumnInfo, Schema, create_object, create_object_from_value, open_object_from_url
from aaiclick.backend import internal_table, is_chdb
from aaiclick.data.blob_sql import AZURE_CONNECTION_ENV
from aaiclick.data.models import FIELDTYPE_ARRAY
from aaiclick.data.object_context import ch_client, get_ch_client
from aaiclick.data.url_utils import AzUrl, parse_az_url
from aaiclick.orchestration.execution.runner import deserialize_task_params
from aaiclick.orchestration.orch_context import task_scope
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.testing import SAMPLE_TABLE

_BUCKET = "aaiclick-test"
_HOST = os.getenv("AAICLICK_TEST_FILESERVER_HOST", "localhost")
_KEYS = {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing"}
_CONTAINER = "aaiclick-test"
_CONNECTION = "azurite_test"
_S3_CONNECTION = "s3_test"
# Azurite's published development account — not a secret.
_DEV_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class Bucket:
    """A bucket on the module's moto server: URLs for ClickHouse, reads for asserts."""

    connection = None

    def __init__(self, port: int):
        self._base = f"http://{_HOST}:{port}/{_BUCKET}"
        self._s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{port}",
            region_name="us-east-1",
            aws_access_key_id=_KEYS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=_KEYS["AWS_SECRET_ACCESS_KEY"],
        )
        self._s3.create_bucket(Bucket=_BUCKET)

    def url(self, key: str) -> str:
        return f"{self._base}/{key}"

    def copy(self, key: str, new_key: str) -> str:
        self._s3.copy_object(Bucket=_BUCKET, Key=new_key, CopySource={"Bucket": _BUCKET, "Key": key})
        return self.url(new_key)

    def parquet_compression(self, key: str) -> str:
        body = self._s3.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
        return pq.ParquetFile(io.BytesIO(body)).metadata.row_group(0).column(0).compression

    def read_parquet(self, key: str) -> dict[str, list]:
        body = self._s3.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
        return pq.read_table(io.BytesIO(body)).to_pydict()


def _connection_string(endpoint: str) -> str:
    return (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey={_DEV_KEY};"
        f"BlobEndpoint={endpoint}/devstoreaccount1;"
    )


class Container:
    """The test container on Azurite: ``az://`` URLs for aaiclick, reads for asserts."""

    connection = _CONNECTION

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
    port = _free_port()
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


@pytest.fixture(scope="module")
def bucket() -> Iterator[Bucket]:
    """Start a throwaway S3 server on a free port; chdb reads the AWS pair from our env."""
    port = _free_port()
    server = ThreadedMotoServer(ip_address="0.0.0.0", port=port, verbose=False)
    server.start()
    try:
        with pytest.MonkeyPatch.context() as env:
            for name, value in _KEYS.items():
                env.setenv(name, value)
            yield Bucket(port)
    finally:
        server.stop()


@pytest.fixture
def s3_collection(bucket) -> Bucket:
    """The same bucket, read and written through the ``s3_test`` named collection."""
    via = copy.copy(bucket)
    via.connection = _S3_CONNECTION
    return via


@pytest.fixture(params=["s3", "s3_collection", "azure"])
def store(request) -> Bucket | Container:
    """The behavior every backend shares runs once against each."""
    return request.getfixturevalue(
        {"s3": "bucket", "s3_collection": "s3_collection", "azure": "container"}[request.param]
    )


@pytest.fixture(autouse=True)
async def server_lends_s3_identity(request, ctx) -> None:
    """Skip S3 tests on a remote ClickHouse that cannot reach the bucket: one
    whose profile does not lend its S3 identity to queries
    (docker/clickhouse/users.d/s3.xml), or, through a collection, one that does
    not define ``s3_test`` (docker/clickhouse/test/). chdb uses our env either
    way, and tests that never reach a bucket run everywhere."""
    callspec = getattr(request.node, "callspec", None)
    store = callspec.params.get("store") if callspec is not None else None
    if is_chdb():
        return
    if store == "s3_collection":
        result = await get_ch_client().query(
            f"SELECT count() FROM system.named_collections WHERE name = '{_S3_CONNECTION}'"
        )
        if result.result_rows[0][0] == 0:
            pytest.skip(f"ClickHouse defines no {_S3_CONNECTION} named collection")
        return
    if "bucket" not in request.fixturenames and store != "s3":
        return
    result = await get_ch_client().query(
        "SELECT value FROM system.settings WHERE name = 's3_allow_server_credentials_in_user_queries'"
    )
    if [tuple(row) for row in result.result_rows] != [("1",)]:
        pytest.skip("ClickHouse profile lacks s3_allow_server_credentials_in_user_queries")


# =============================================================================
# Export
# =============================================================================


async def test_export_writes_the_file(ctx, store):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = store.url("export/sample.parquet")
    assert await obj.export(url, overwrite=True, connection=store.connection) == url
    assert store.read_parquet("export/sample.parquet") == SAMPLE_TABLE


async def test_export_honors_view_constraints(ctx, store):
    obj = await create_object_from_value(SAMPLE_TABLE)
    view = obj.view(where="score > 15", order_by="id", limit=1)
    await view.export(store.url("export/view.parquet"), overwrite=True, connection=store.connection)
    assert store.read_parquet("export/view.parquet") == {"id": [2], "name": ["bob"], "score": [20.0]}


async def test_export_to_existing_key_needs_overwrite(ctx, store):
    url = store.url("export/twice.parquet")
    await (await create_object_from_value([1, 2])).export(url, overwrite=True, connection=store.connection)
    replacement = await create_object_from_value([7, 8, 9])
    with pytest.raises(Exception, match="already exists"):
        await replacement.export(url, connection=store.connection)
    assert store.read_parquet("export/twice.parquet") == {"value": [1, 2]}
    await replacement.export(url, overwrite=True, connection=store.connection)
    assert store.read_parquet("export/twice.parquet") == {"value": [7, 8, 9]}


async def test_export_to_bucket_applies_ch_settings(ctx, bucket):
    obj = await create_object_from_value(SAMPLE_TABLE)
    await obj.export(
        bucket.url("export/settings.parquet"),
        overwrite=True,
        ch_settings={"output_format_parquet_compression_method": "snappy"},
    )
    assert bucket.parquet_compression("export/settings.parquet") == "SNAPPY"


@pytest.mark.parametrize(
    "url, connection, setting",
    [
        ("s3://bucket/data.parquet", None, "s3_allow_server_credentials_in_user_queries"),
        ("s3://bucket/data.parquet", None, "s3_create_new_file_on_insert"),
        ("s3://bucket/data.parquet", None, "s3_truncate_on_insert"),
        ("az://c/data.parquet", _CONNECTION, "azure_truncate_on_insert"),
        ("az://c/data.parquet", _CONNECTION, "azure_create_new_file_on_insert"),
    ],
)
async def test_export_rejects_reserved_ch_settings(ctx, url, connection, setting):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="cannot set"):
        await obj.export(url, connection=connection, ch_settings={setting: 1})


async def test_export_rejects_unsupported_scheme(ctx):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="http, https, s3 or az"):
        await obj.export("ftp://example.com/data.parquet")


# =============================================================================
# Open a file in place
# =============================================================================


@pytest.fixture
async def sample_url(ctx, store) -> str:
    """SAMPLE_TABLE exported as parquet; the key is replaced on every test."""
    obj = await create_object_from_value(SAMPLE_TABLE)
    return await obj.export(store.url("open/sample.parquet"), overwrite=True, connection=store.connection)


@pytest.mark.parametrize("extension", [".parquet", ".arrow", ".json", ".jsonl", ".orc", ".avro"])
async def test_export_open_round_trip(ctx, store, extension):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(store.url(f"roundtrip/sample{extension}"), overwrite=True, connection=store.connection)
    opened = await open_object_from_url(url, connection=store.connection)
    assert await opened.data() == SAMPLE_TABLE


@pytest.mark.parametrize(
    "format, expected",
    [
        pytest.param("CSVWithNames", SAMPLE_TABLE, id="header-detected"),
        # ClickHouse's own default for ``.csv``: no header, positional names.
        pytest.param(None, dict(zip(("c1", "c2", "c3"), SAMPLE_TABLE.values(), strict=True)), id="headerless"),
    ],
)
async def test_csv_round_trip(ctx, store, format, expected):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(
        store.url("roundtrip/header.csv"), overwrite=True, format=format, connection=store.connection
    )
    assert await (await open_object_from_url(url, connection=store.connection)).data() == expected


async def test_located_object_filters_and_aggregates_in_place(ctx, store, sample_url):
    opened = await open_object_from_url(sample_url, connection=store.connection)
    assert await opened.view(where="score > 15")["score"].sum().data() == 50.25


async def test_open_selected_columns(ctx, store, sample_url):
    opened = await open_object_from_url(sample_url, columns=["id", "name"], connection=store.connection)
    assert await opened.data() == {"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]}


@pytest.mark.parametrize("glob", ["*", pytest.param("?", id="question-mark-is-a-glob-not-a-query")])
async def test_open_glob(ctx, store, glob):
    for part in (1, 2):
        obj = await create_object_from_value([part])
        await obj.export(store.url(f"glob/part-{part}.parquet"), overwrite=True, connection=store.connection)
    opened = await open_object_from_url(store.url(f"glob/part-{glob}.parquet"), connection=store.connection)
    assert sorted(await opened.data()) == [1, 2]


async def test_open_without_extension_detects_format(ctx, bucket):
    """ClickHouse detects the format from the content when no extension names it."""
    obj = await create_object_from_value(SAMPLE_TABLE)
    await obj.export(bucket.url("open/detect.parquet"), overwrite=True)
    opened = await open_object_from_url(bucket.copy("open/detect.parquet", "open/detect"))
    assert await opened.data() == SAMPLE_TABLE


async def test_open_unknown_format_is_clickhouse_error(ctx):
    """Format names pass through; ClickHouse rejects one it does not know, before any request."""
    with pytest.raises(Exception, match="Unknown format"):
        await open_object_from_url("s3://bucket/data", format="Nope")


async def test_open_url_with_quote_in_key(ctx, store):
    obj = await create_object_from_value([1, 2, 3])
    url = await obj.export(store.url("open/it's.parquet"), overwrite=True, connection=store.connection)
    assert await (await open_object_from_url(url, connection=store.connection)).data() == [1, 2, 3]


async def test_located_object_rejects_insert(ctx, store, sample_url):
    opened = await open_object_from_url(sample_url, connection=store.connection)
    with pytest.raises(ValueError, match="read-only"):
        await opened.insert([4])


async def test_located_object_rejects_insert_from_url(ctx, store, sample_url):
    opened = await open_object_from_url(sample_url, connection=store.connection)
    with pytest.raises(ValueError, match="read-only"):
        await opened.insert_from_url(sample_url)


@pytest.mark.parametrize(
    "url, kwargs, match",
    [
        pytest.param("ftp://example.com/data.parquet", {}, "http, https, s3 or az", id="bad-scheme"),
        pytest.param(
            "s3://bucket/data.parquet",
            {"columns": ["id"], "column_types": {}},
            "column_types",
            id="column-missing-from-types",
        ),
        pytest.param("s3://bucket/data", {"format": "Parquet') --"}, "Invalid format name", id="format-injection"),
        pytest.param("s3://bucket/data.parquet", {"columns": []}, "non-empty", id="empty-columns"),
    ],
)
async def test_open_rejects_bad_arguments(ctx, url, kwargs, match):
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url, **kwargs)


def _located_schema(**overrides) -> Schema:
    fields = {
        "fieldtype": FIELDTYPE_ARRAY,
        "columns": {"value": ColumnInfo("Int64", fieldtype=FIELDTYPE_ARRAY)},
        "location": "s3://bucket/data.parquet",
        "format": "Parquet",
    }
    return Schema(**(fields | overrides))


async def test_located_object_cannot_be_named(ctx):
    with pytest.raises(ValueError, match="cannot be named"):
        await create_object(_located_schema(), name="lake_sample")


@pytest.mark.parametrize(
    "overrides, match",
    [
        pytest.param({"format": "Parquet') COMMENT 'injected' --"}, "Invalid format name", id="format-injection"),
        pytest.param({"format": None}, "Invalid format name", id="format-missing"),
        pytest.param({"location": "relative/data.parquet"}, "absolute path|chdb backend", id="location-relative"),
        pytest.param({"location": "ftp://example.com/data.parquet"}, "http, https, s3 or az", id="location-bad-scheme"),
    ],
)
async def test_create_object_validates_location_and_format(ctx, overrides, match):
    with pytest.raises(ValueError, match=match):
        await create_object(_located_schema(**overrides))


@pytest.mark.parametrize(
    "url, match",
    [
        pytest.param("https://KEY:SECRET@example.com/bucket/data.parquet", "credentials", id="userinfo"),
        pytest.param(
            "https://example.com/bucket/data.parquet?X-Amz-Signature=abc", "query parameters", id="signed-url"
        ),
    ],
)
async def test_urls_carrying_credentials_are_rejected(ctx, url, match):
    """No SQL aaiclick renders may carry a secret, so such URLs never reach ClickHouse."""
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match=match):
        await obj.export(url)
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url)


async def test_located_object_survives_a_task_boundary(ctx, store, sample_url):
    opened = await open_object_from_url(sample_url, connection=store.connection)
    params = await deserialize_task_params({"data": opened._serialize_ref()}, job_id=0)
    rehydrated = params["data"]
    assert rehydrated.schema.connection == store.connection
    assert await rehydrated.data() == SAMPLE_TABLE
    with pytest.raises(ValueError, match="read-only"):
        await rehydrated.insert({"id": [4], "name": ["dave"], "score": [1.0]})


# =============================================================================
# Azure connections
# =============================================================================


@pytest.mark.parametrize(
    "url, connection, match",
    [
        ("az://container", "lake", "blob path"),
        ("az:///path.parquet", "lake", "container"),
        ("az://c/p.parquet?sig=abc", "lake", "query parameters"),
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


async def test_chdb_defines_collection_once_per_client(ctx, monkeypatch):
    """Efficiency contract: one CREATE + ALTER per client and key, not per call.
    A rotated key is redefined."""
    monkeypatch.setattr(ch_client, "is_chdb", lambda: True)
    monkeypatch.setenv(AZURE_CONNECTION_ENV, "AccountName=a;AccountKey=ONE")
    statements: list[str] = []

    class RecordingClient:
        async def command(self, sql, **_):
            statements.append(sql)

    client = RecordingClient()
    monkeypatch.setattr(ch_client, "get_ch_client", lambda: client)
    for _ in range(3):
        assert await ch_client.ensure_azure_connection("lake") == "lake"
    assert len(statements) == 2
    monkeypatch.setenv(AZURE_CONNECTION_ENV, "AccountName=a;AccountKey=TWO")
    await ch_client.ensure_azure_connection("lake")
    assert len(statements) == 4


async def test_server_needs_connection(ctx, monkeypatch):
    monkeypatch.setattr(ch_client, "is_chdb", lambda: False)
    with pytest.raises(ValueError, match="needs connection"):
        await open_object_from_url("az://c/p.parquet")


async def test_oplog_and_registry_carry_no_account_key(ctx, container):
    async with task_scope(task_id=7, job_id=70, run_id=700):
        obj = await create_object_from_value(SAMPLE_TABLE)
        url = await obj.export(container.url("open/oplog.parquet"), overwrite=True, connection=container.connection)
        opened = await open_object_from_url(url, connection=container.connection)
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
