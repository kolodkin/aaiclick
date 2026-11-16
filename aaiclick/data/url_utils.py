"""
aaiclick.data.url_utils - Checks on the URLs aaiclick hands to ClickHouse.

See docs/designs/blob_storage.md — "Credentials".
"""

from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlparse

from aaiclick.backend import is_chdb
from aaiclick.names import name_error

AZ_SCHEME = "az"


class SchemeRule(NamedTuple):
    """What a blob URL scheme requires: what its netloc names, and whether a
    path must follow it (Azure has no default blob)."""

    netloc: str
    needs_path: bool


_SCHEME_RULES = {
    "http": SchemeRule("host", needs_path=False),
    "https": SchemeRule("host", needs_path=False),
    "s3": SchemeRule("bucket", needs_path=False),
    AZ_SCHEME: SchemeRule("container", needs_path=True),
}


class AzUrl(NamedTuple):
    """An ``az://container/blob_path`` URL, split as ClickHouse's Azure engine takes it."""

    container: str
    blob_path: str


def is_url(path: str) -> bool:
    """True when *path* names a URL rather than a local file."""
    return "://" in path


def validate_blob_url(url: str) -> None:
    """Reject a URL the ClickHouse ``s3`` / ``azureBlobStorage`` sources cannot address."""
    parsed = urlparse(url)
    rule = _SCHEME_RULES.get(parsed.scheme)
    if rule is None:
        raise ValueError(f"Blob URL must use http, https, s3 or az scheme, got '{parsed.scheme}'")
    if not parsed.netloc:
        raise ValueError(f"Blob URL must have a valid {rule.netloc}")
    # Split the raw string: urlparse cuts the path at a ``?`` glob or ``#``.
    if rule.needs_path and not url.split("://", 1)[1].partition("/")[2]:
        raise ValueError(f"{parsed.scheme}:// URL must name a blob path after the {rule.netloc}")
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
    container, _, blob_path = url.split("://", 1)[1].partition("/")
    return AzUrl(container, blob_path)


def validate_connection(url: str, connection: str | None) -> None:
    """Reject a ``connection`` that is not a named-collection name, or is given
    for a local path. The name is inlined into SQL."""
    if connection is None:
        return
    if not is_url(url):
        raise ValueError("connection applies only to bucket URLs")
    # ASCII only: ClickHouse rejects other bare identifiers, and its syntax
    # error would echo the secret-bearing CREATE NAMED COLLECTION.
    if err := name_error("connection", connection):
        raise ValueError(err)


def require_local_backend() -> None:
    """Reject a local path on a remote ClickHouse, which cannot reach our disk."""
    if not is_chdb():
        raise ValueError("Local file paths need the chdb backend; use an http, https, s3 or az URL")


def validate_location(location: str, connection: str | None = None) -> None:
    """Reject a location a located Object cannot point at: a bad blob URL, a
    local path that is relative or used without the embedded chdb backend, or
    a ``connection`` the location cannot take."""
    validate_connection(location, connection)
    if is_url(location):
        validate_blob_url(location)
        return
    require_local_backend()
    if not Path(location).is_absolute():
        raise ValueError(f"Local file location must be an absolute path, got {location!r}")
