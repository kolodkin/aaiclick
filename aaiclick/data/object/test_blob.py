"""Tests for blob storage: ``Object.export()`` to a bucket URL and
``open_object_from_url()`` over it.

An in-process moto server stands in for S3. Set
AAICLICK_TEST_FILESERVER_HOST=host.docker.internal when ClickHouse runs in a
container and must reach back to the test process.
"""

from __future__ import annotations

import io
import os
import socket
from collections.abc import Iterator

import boto3
import pyarrow.parquet as pq
import pytest
from moto.server import ThreadedMotoServer

from aaiclick import ColumnInfo, Schema, create_object, create_object_from_value, open_object_from_url
from aaiclick.backend import is_chdb
from aaiclick.data.models import FIELDTYPE_ARRAY
from aaiclick.data.object_context import get_ch_client
from aaiclick.orchestration.execution.runner import deserialize_task_params
from aaiclick.testing import SAMPLE_TABLE

_BUCKET = "aaiclick-test"
_HOST = os.getenv("AAICLICK_TEST_FILESERVER_HOST", "localhost")
_KEYS = {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing"}


class Bucket:
    """A bucket on the module's moto server: URLs for ClickHouse, reads for asserts."""

    def __init__(self, port: int):
        self.url = f"http://{_HOST}:{port}/{_BUCKET}"
        self._s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{port}",
            region_name="us-east-1",
            aws_access_key_id=_KEYS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=_KEYS["AWS_SECRET_ACCESS_KEY"],
        )
        self._s3.create_bucket(Bucket=_BUCKET)

    def copy(self, key: str, new_key: str) -> str:
        self._s3.copy_object(Bucket=_BUCKET, Key=new_key, CopySource={"Bucket": _BUCKET, "Key": key})
        return f"{self.url}/{new_key}"

    def parquet_compression(self, key: str) -> str:
        body = self._s3.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
        return pq.ParquetFile(io.BytesIO(body)).metadata.row_group(0).column(0).compression

    def read_parquet(self, key: str) -> dict[str, list]:
        body = self._s3.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
        return pq.read_table(io.BytesIO(body)).to_pydict()


@pytest.fixture(scope="module")
def bucket() -> Iterator[Bucket]:
    """Start a throwaway S3 server on a free port; chdb reads the AWS pair from our env."""
    with socket.socket() as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server = ThreadedMotoServer(ip_address="0.0.0.0", port=port, verbose=False)
    server.start()
    try:
        with pytest.MonkeyPatch.context() as env:
            for name, value in _KEYS.items():
                env.setenv(name, value)
            yield Bucket(port)
    finally:
        server.stop()


@pytest.fixture(autouse=True)
async def server_lends_s3_identity(request, ctx) -> None:
    """Skip bucket tests on a remote ClickHouse whose profile does not lend its
    S3 identity to queries (docker/clickhouse/users.d/s3.xml); chdb has no such
    guard, and tests that never reach a bucket run everywhere."""
    if "bucket" not in request.fixturenames:
        return
    if is_chdb():
        return
    result = await get_ch_client().query(
        "SELECT value FROM system.settings WHERE name = 's3_allow_server_credentials_in_user_queries'"
    )
    if [tuple(row) for row in result.result_rows] != [("1",)]:
        pytest.skip("ClickHouse profile lacks s3_allow_server_credentials_in_user_queries")


# =============================================================================
# Export to a bucket
# =============================================================================


async def test_export_to_bucket(ctx, bucket):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = f"{bucket.url}/export/sample.parquet"
    assert await obj.export(url) == url
    assert bucket.read_parquet("export/sample.parquet") == SAMPLE_TABLE


async def test_export_to_bucket_honors_view_constraints(ctx, bucket):
    obj = await create_object_from_value(SAMPLE_TABLE)
    await obj.view(where="score > 15", order_by="id", limit=1).export(f"{bucket.url}/export/view.parquet")
    assert bucket.read_parquet("export/view.parquet") == {"id": [2], "name": ["bob"], "score": [20.0]}


async def test_export_to_existing_key_needs_overwrite(ctx, bucket):
    url = f"{bucket.url}/export/twice.parquet"
    await (await create_object_from_value([1, 2])).export(url)
    replacement = await create_object_from_value([7, 8, 9])
    with pytest.raises(Exception, match="already exists"):
        await replacement.export(url)
    assert bucket.read_parquet("export/twice.parquet") == {"value": [1, 2]}
    await replacement.export(url, overwrite=True)
    assert bucket.read_parquet("export/twice.parquet") == {"value": [7, 8, 9]}


async def test_export_to_bucket_applies_ch_settings(ctx, bucket):
    obj = await create_object_from_value(SAMPLE_TABLE)
    await obj.export(
        f"{bucket.url}/export/settings.parquet",
        overwrite=True,
        ch_settings={"output_format_parquet_compression_method": "snappy"},
    )
    assert bucket.parquet_compression("export/settings.parquet") == "SNAPPY"


@pytest.mark.parametrize(
    "setting",
    ["s3_allow_server_credentials_in_user_queries", "s3_create_new_file_on_insert", "s3_truncate_on_insert"],
)
async def test_export_rejects_reserved_ch_settings(ctx, setting):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="cannot set"):
        await obj.export("s3://bucket/data.parquet", ch_settings={setting: 1})


async def test_export_rejects_unsupported_scheme(ctx):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="http, https or s3"):
        await obj.export("ftp://example.com/data.parquet")


# =============================================================================
# Open a bucket file in place
# =============================================================================


@pytest.fixture
async def sample_url(ctx, bucket) -> str:
    """SAMPLE_TABLE exported as parquet; the key is replaced on every test."""
    obj = await create_object_from_value(SAMPLE_TABLE)
    return await obj.export(f"{bucket.url}/open/sample.parquet", overwrite=True)


@pytest.mark.parametrize("extension", [".parquet", ".arrow", ".json", ".jsonl", ".orc", ".avro"])
async def test_export_open_round_trip(ctx, bucket, extension):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(f"{bucket.url}/roundtrip/sample{extension}", overwrite=True)
    opened = await open_object_from_url(url)
    assert await opened.data() == SAMPLE_TABLE


@pytest.mark.parametrize(
    "format, expected",
    [
        pytest.param("CSVWithNames", SAMPLE_TABLE, id="header-detected"),
        # ClickHouse's own default for ``.csv``: no header, positional names.
        pytest.param(None, dict(zip(("c1", "c2", "c3"), SAMPLE_TABLE.values(), strict=True)), id="headerless"),
    ],
)
async def test_csv_round_trip(ctx, bucket, format, expected):
    obj = await create_object_from_value(SAMPLE_TABLE)
    url = await obj.export(f"{bucket.url}/roundtrip/header.csv", overwrite=True, format=format)
    assert await (await open_object_from_url(url)).data() == expected


async def test_located_object_filters_and_aggregates_in_place(ctx, sample_url):
    opened = await open_object_from_url(sample_url)
    assert await opened.view(where="score > 15")["score"].sum().data() == 50.25


async def test_open_selected_columns(ctx, sample_url):
    opened = await open_object_from_url(sample_url, columns=["id", "name"])
    assert await opened.data() == {"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]}


async def test_open_single_character_glob(ctx, bucket):
    """``?`` is a ClickHouse glob, not the start of a query string."""
    for part in (1, 2):
        await (await create_object_from_value([part])).export(f"{bucket.url}/glob/part-{part}.parquet", overwrite=True)
    opened = await open_object_from_url(f"{bucket.url}/glob/part-?.parquet")
    assert sorted(await opened.data()) == [1, 2]


async def test_open_without_extension_detects_format(ctx, bucket, sample_url):
    """ClickHouse detects the format from the content when no extension names it."""
    opened = await open_object_from_url(bucket.copy("open/sample.parquet", "open/sample"))
    assert await opened.data() == SAMPLE_TABLE


async def test_open_unknown_format_is_clickhouse_error(ctx):
    """Format names pass through; ClickHouse rejects one it does not know, before any request."""
    with pytest.raises(Exception, match="Unknown format"):
        await open_object_from_url("s3://bucket/data", format="Nope")


async def test_open_url_with_quote_in_key(ctx, bucket):
    obj = await create_object_from_value([1, 2, 3])
    url = await obj.export(f"{bucket.url}/open/it's.parquet", overwrite=True)
    assert await (await open_object_from_url(url)).data() == [1, 2, 3]


async def test_located_object_rejects_insert(ctx, sample_url):
    opened = await open_object_from_url(sample_url)
    with pytest.raises(ValueError, match="read-only"):
        await opened.insert([4])


async def test_located_object_rejects_insert_from_url(ctx, sample_url):
    opened = await open_object_from_url(sample_url)
    with pytest.raises(ValueError, match="read-only"):
        await opened.insert_from_url(sample_url)


@pytest.mark.parametrize(
    "url, kwargs, match",
    [
        pytest.param("ftp://example.com/data.parquet", {}, "http, https or s3", id="bad-scheme"),
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
        pytest.param({"location": "ftp://example.com/data.parquet"}, "http, https or s3", id="location-bad-scheme"),
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


async def test_located_object_stays_read_only_across_a_task_boundary(ctx, sample_url):
    opened = await open_object_from_url(sample_url)
    params = await deserialize_task_params({"data": opened._serialize_ref()}, job_id=0)
    with pytest.raises(ValueError, match="read-only"):
        await params["data"].insert({"id": [4], "name": ["dave"], "score": [1.0]})
