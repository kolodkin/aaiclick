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
from aaiclick.data.models import FIELDTYPE_ARRAY

SAMPLE = {
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "score": [10.5, 20.0, 30.25],
}
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
    saved = {k: os.environ.get(k) for k in _KEYS}
    os.environ.update(_KEYS)
    yield Bucket(port)
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    server.stop()


# =============================================================================
# Export to a bucket
# =============================================================================


async def test_export_to_bucket(ctx, bucket):
    obj = await create_object_from_value(SAMPLE)
    url = f"{bucket.url}/export/sample.parquet"
    assert await obj.export(url) == url
    assert bucket.read_parquet("export/sample.parquet") == SAMPLE


async def test_export_to_bucket_honors_view_constraints(ctx, bucket):
    obj = await create_object_from_value(SAMPLE)
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


async def test_export_rejects_unsupported_scheme(ctx):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="http, https or s3"):
        await obj.export("ftp://example.com/data.parquet")


# =============================================================================
# Open a bucket file in place
# =============================================================================


@pytest.fixture
async def sample_url(ctx, bucket) -> str:
    """SAMPLE exported as parquet; the key is replaced on every test."""
    obj = await create_object_from_value(SAMPLE)
    return await obj.export(f"{bucket.url}/open/sample.parquet", overwrite=True)


@pytest.mark.parametrize("extension", [".parquet", ".csv", ".tsv", ".jsonl", ".orc", ".avro"])
async def test_export_open_round_trip(ctx, bucket, extension):
    obj = await create_object_from_value(SAMPLE)
    url = await obj.export(f"{bucket.url}/roundtrip/sample{extension}", overwrite=True)
    opened = await open_object_from_url(url)
    assert await opened.data() == SAMPLE


async def test_located_object_filters_and_aggregates_in_place(ctx, sample_url):
    opened = await open_object_from_url(sample_url)
    assert await opened.view(where="score > 15")["score"].sum().data() == 50.25


async def test_open_selected_columns(ctx, sample_url):
    opened = await open_object_from_url(sample_url, columns=["id", "name"])
    assert await opened.data() == {"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]}


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
        pytest.param("s3://bucket/data", {}, "extension", id="no-extension-no-format"),
        pytest.param("s3://bucket/data", {"format": "Nope"}, "Unsupported format", id="unknown-format"),
        pytest.param("s3://bucket/data.parquet", {"columns": []}, "non-empty", id="empty-columns"),
    ],
)
async def test_open_rejects_bad_arguments(ctx, url, kwargs, match):
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url, **kwargs)


async def test_located_object_cannot_be_named(ctx, sample_url):
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"value": ColumnInfo("Int64", fieldtype=FIELDTYPE_ARRAY)},
        location=sample_url,
        format="Parquet",
    )
    with pytest.raises(ValueError, match="cannot be named"):
        await create_object(schema, name="lake_sample")
