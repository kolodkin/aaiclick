"""
aaiclick.data.blob_sql - ClickHouse sources over bucket URLs.

See docs/designs/blob_storage.md — "Credentials".

A ``connection`` names a ClickHouse named collection holding the credentials,
so no secret enters the SQL aaiclick sends: ``azureBlobStorage(<collection>,
container = ..., blob_path = ..., format = ...)`` for ``az://``,
``s3(<collection>, url = ..., format = ...)`` for S3. Without one, S3 uses
ClickHouse's own identity. On chdb, S3 always does (the process env), and the
Azure collection comes from AZURE_STORAGE_CONNECTION_STRING
(``ensure_azure_connection`` in ``object_context/ch_client.py``).
"""

from aaiclick.backend import is_chdb

from .sql_utils import quote_sql_literal
from .url_utils import is_az_url, parse_az_url

AZURE_CONNECTION_ENV = "AZURE_STORAGE_CONNECTION_STRING"
DEFAULT_AZURE_CONNECTION = "aaiclick_azure"


def blob_args(url: str, fmt: str, connection: str | None) -> str:
    """Arguments for the ``s3`` / ``azureBlobStorage`` function and the
    ``S3`` / ``AzureBlobStorage`` engine over *url*. An ``az://`` URL needs the
    resolved *connection*."""
    if is_az_url(url):
        assert connection is not None
        az = parse_az_url(url)
        return (
            f"{connection}, container = {quote_sql_literal(az.container)}, "
            f"blob_path = {quote_sql_literal(az.blob_path)}, format = '{fmt}'"
        )
    # chdb has no server config to hold a collection; its S3 reads our env.
    if connection is not None and not is_chdb():
        return f"{connection}, url = {quote_sql_literal(url)}, format = '{fmt}'"
    return f"{quote_sql_literal(url)}, '{fmt}'"


def blob_function(url: str, fmt: str, connection: str | None) -> str:
    """The ClickHouse table function over a bucket URL."""
    return f"{'azureBlobStorage' if is_az_url(url) else 's3'}({blob_args(url, fmt, connection)})"
