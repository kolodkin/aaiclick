"""
aaiclick.data.azure - Azure Blob Storage sources for ClickHouse.

Every ``az://container/path`` URL renders as ``azureBlobStorage(<collection>,
container=..., blob_path=..., format=...)``: the endpoint and key live in a
ClickHouse named collection, never in the SQL aaiclick sends. On a ClickHouse
server the operator defines the collection; on chdb aaiclick creates it from
AZURE_STORAGE_CONNECTION_STRING (``ensure_azure_connection`` in
``object_context/ch_client.py``). See docs/designs/blob_storage.md — "Azure".
"""

from .sql_utils import quote_sql_literal
from .url_utils import is_az_url, parse_az_url

AZURE_CONNECTION_ENV = "AZURE_STORAGE_CONNECTION_STRING"
DEFAULT_AZURE_CONNECTION = "aaiclick_azure"


def azure_args(url: str, fmt: str, connection: str) -> str:
    """Arguments for ``azureBlobStorage(...)`` / ``ENGINE = AzureBlobStorage(...)``."""
    az = parse_az_url(url)
    return (
        f"{connection}, container = {quote_sql_literal(az.container)}, "
        f"blob_path = {quote_sql_literal(az.blob_path)}, format = '{fmt}'"
    )


def blob_function(url: str, fmt: str, connection: str | None) -> str:
    """The ClickHouse table function over a bucket URL: ``azureBlobStorage`` for
    ``az://`` (through the resolved *connection*), ``s3`` otherwise."""
    if is_az_url(url):
        assert connection is not None
        return f"azureBlobStorage({azure_args(url, fmt, connection)})"
    return f"s3({quote_sql_literal(url)}, '{fmt}')"
