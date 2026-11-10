"""
aaiclick.data.url_utils - Checks on the URLs aaiclick hands to ClickHouse.

See docs/designs/blob_storage.md — "Credentials".
"""

from urllib.parse import urlparse

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
    # be able to carry a secret: no userinfo, no (possibly signed) query string.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Blob URL must not carry credentials; ClickHouse resolves them itself")
    if parsed.query:
        raise ValueError("Blob URL must not carry a query string")
