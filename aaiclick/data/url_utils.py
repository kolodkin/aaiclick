"""
aaiclick.data.url_utils - Checks on the URLs aaiclick hands to ClickHouse.

See docs/designs/blob_storage.md — "Credentials".
"""

from pathlib import Path
from urllib.parse import urlparse

from aaiclick.backend import is_chdb

BLOB_URL_SCHEMES = ("http", "https", "s3")


def is_url(path: str) -> bool:
    """True when *path* names a URL rather than a local file."""
    return "://" in path


def validate_blob_url(url: str) -> None:
    """Reject a URL the ClickHouse ``s3`` function / ``S3`` engine cannot address."""
    parsed = urlparse(url)
    if parsed.scheme not in BLOB_URL_SCHEMES:
        raise ValueError(f"Blob URL must use http, https or s3 scheme, got '{parsed.scheme}'")
    if not parsed.netloc:
        raise ValueError("Blob URL must have a valid host or bucket")
    # The URL is rendered into SQL and stored in the registry, so it must not
    # be able to carry a secret: no userinfo, no query parameters.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Blob URL must not carry credentials; ClickHouse resolves them itself")
    # A bare ``?`` is a ClickHouse glob; ``key=value`` is a (possibly signed) query.
    if "=" in parsed.query:
        raise ValueError("Blob URL must not carry query parameters")


def require_local_backend() -> None:
    """Reject a local path on a remote ClickHouse, which cannot reach our disk."""
    if not is_chdb():
        raise ValueError("Local file paths need the chdb backend; use an http, https or s3 URL")


def validate_location(location: str) -> None:
    """Reject a location a located Object cannot point at: a bad blob URL, or a
    local path that is relative or used without the embedded chdb backend."""
    if is_url(location):
        validate_blob_url(location)
        return
    require_local_backend()
    if not Path(location).is_absolute():
        raise ValueError(f"Local file location must be an absolute path, got {location!r}")
