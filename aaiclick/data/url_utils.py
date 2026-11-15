"""
aaiclick.data.url_utils - Checks on the URLs aaiclick hands to ClickHouse.

See docs/designs/blob_storage.md — "Credentials".
"""

from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlparse

from aaiclick.backend import is_chdb

AZ_SCHEME = "az"
BLOB_URL_SCHEMES = ("http", "https", "s3", AZ_SCHEME)


class AzUrl(NamedTuple):
    """An ``az://container/blob_path`` URL, split as ClickHouse's Azure engine takes it."""

    container: str
    blob_path: str


def is_url(path: str) -> bool:
    """True when *path* names a URL rather than a local file."""
    return "://" in path


def validate_blob_url(url: str) -> None:
    """Reject a URL the ClickHouse ``s3`` function / ``S3`` engine cannot address."""
    parsed = urlparse(url)
    if parsed.scheme not in BLOB_URL_SCHEMES:
        raise ValueError(f"Blob URL must use http, https, s3 or az scheme, got '{parsed.scheme}'")
    if not parsed.netloc:
        noun = "container" if parsed.scheme == AZ_SCHEME else "host or bucket"
        raise ValueError(f"Blob URL must have a valid {noun}")
    if parsed.scheme == AZ_SCHEME and not parse_az_url(url).blob_path:
        raise ValueError("az:// URL must name a blob path after the container")
    # The URL is rendered into SQL and stored in the registry, so it must not
    # be able to carry a secret: no userinfo, no query parameters.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Blob URL must not carry credentials; ClickHouse resolves them itself")
    # A bare ``?`` is a ClickHouse glob; ``key=value`` is a (possibly signed) query.
    if "=" in parsed.query:
        raise ValueError("Blob URL must not carry query parameters")


def is_az_url(url: str) -> bool:
    """True when *url* addresses Azure Blob Storage."""
    return urlparse(url).scheme == AZ_SCHEME


def parse_az_url(url: str) -> AzUrl:
    """Split ``az://container/path`` into container and blob path.

    Split on the raw string: ``urlparse`` would cut the path at a ``?`` glob
    or a ``#``, both legal in blob names.
    """
    container, _, blob_path = url.removeprefix(f"{AZ_SCHEME}://").partition("/")
    return AzUrl(container, blob_path)


def validate_connection(url: str, connection: str | None) -> None:
    """Reject a ``connection`` that is not a named-collection name, or is given
    for anything but an ``az://`` URL. The name is inlined into SQL."""
    if connection is None:
        return
    if not is_az_url(url):
        raise ValueError("connection applies only to az:// URLs")
    # ASCII only: ClickHouse rejects other bare identifiers, and its syntax
    # error would echo the secret-bearing CREATE NAMED COLLECTION.
    if not (connection.isascii() and connection.isidentifier()):
        raise ValueError(f"Invalid connection name {connection!r}: expected a ClickHouse named collection")


def require_local_backend() -> None:
    """Reject a local path on a remote ClickHouse, which cannot reach our disk."""
    if not is_chdb():
        raise ValueError("Local file paths need the chdb backend; use an http, https, s3 or az URL")


def validate_location(location: str) -> None:
    """Reject a location a located Object cannot point at: a bad blob URL, or a
    local path that is relative or used without the embedded chdb backend."""
    if is_url(location):
        validate_blob_url(location)
        return
    require_local_backend()
    if not Path(location).is_absolute():
        raise ValueError(f"Local file location must be an absolute path, got {location!r}")
