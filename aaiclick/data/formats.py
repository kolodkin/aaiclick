"""
aaiclick.data.formats - File-extension and format-name handling for ClickHouse data formats.

``Object.export()`` and ``open_object_from_url()`` pick a format from a file
extension through this registry. Explicit ``format=`` names are passed to
ClickHouse as-is: any format it knows works, and it rejects the rest.

Adding an extension is a single edit to ``FORMATS``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple


class FormatSpec(NamedTuple):
    """A ClickHouse data format aaiclick maps a file extension to, or treats specially."""

    name: str
    """ClickHouse format identifier — passed verbatim to ``FORMAT`` / ``file()`` / ``url()``."""

    extensions: tuple[str, ...]
    """Natural file extensions, lowercase, including the leading dot."""

    blob: bool = False
    """Format reads the whole document as a single string (JSON-blob mode)."""


# fmt: off
FORMATS: tuple[FormatSpec, ...] = (
    # Columnar / binary
    FormatSpec("Parquet",      (".parquet",)),
    FormatSpec("Arrow",        (".arrow",)),
    FormatSpec("ORC",          (".orc",)),
    FormatSpec("Avro",         (".avro",)),
    # Text — the formats ClickHouse itself picks for these extensions
    FormatSpec("CSV",          (".csv",)),
    FormatSpec("TSV",          (".tsv",)),
    FormatSpec("JSON",         (".json",)),
    FormatSpec("JSONEachRow",  (".jsonl", ".ndjson")),
    # Text reports — ClickHouse writes these but cannot read them back
    FormatSpec("Markdown",     (".md",)),
    FormatSpec("XML",          (".xml",)),
    FormatSpec("SQLInsert",    (".sql",)),
    # JSON blob mode — read whole document, extract via JSONExtract
    FormatSpec("RawBLOB",      (), blob=True),
    FormatSpec("JSONAsString", (), blob=True),
)
# fmt: on


# Compression suffixes stripped before the extension lookup; ClickHouse picks
# the codec from the suffix itself, and rejects one its build lacks.
COMPRESSION_SUFFIXES: frozenset[str] = frozenset({".gz", ".xz", ".zst", ".br", ".lz4", ".bz2", ".snappy"})

# ClickHouse resolves the format itself: from the extension, else the content.
FORMAT_AUTO = "auto"

_FORMAT_NAME = re.compile(r"[A-Za-z0-9_]+")


# Derived lookups — built once at import.
JSON_BLOB_FORMATS: frozenset[str] = frozenset(f.name for f in FORMATS if f.blob)
EXTENSION_TO_FORMAT: dict[str, str] = {ext: f.name for f in FORMATS for ext in f.extensions}


def validate_format_name(fmt: str | None) -> None:
    """Reject a format name that is not a bare identifier.

    Callers inline the name into SQL, so this keeps it injection-safe. Whether
    the format exists and can be read is ClickHouse's call.
    """
    if fmt is None or not _FORMAT_NAME.fullmatch(fmt):
        raise ValueError(f"Invalid format name {fmt!r}: expected a ClickHouse format such as 'Parquet'")


def format_for_extension(path: str) -> str:
    """Return the ClickHouse format matching the file extension of *path*.

    A trailing compression suffix from :data:`COMPRESSION_SUFFIXES` is stripped
    before lookup, so ``data.csv.gz`` resolves to ``CSV``.

    Raises:
        ValueError: If no supported extension is present.
    """
    suffixes = [s.lower() for s in Path(path).suffixes]
    if suffixes and suffixes[-1] in COMPRESSION_SUFFIXES:
        suffixes = suffixes[:-1]
    suffix = suffixes[-1] if suffixes else ""
    fmt = EXTENSION_TO_FORMAT.get(suffix)
    if fmt is None:
        supported = sorted(EXTENSION_TO_FORMAT.keys())
        raise ValueError(
            f"Unsupported export extension {suffix!r}. "
            f"Supported: {', '.join(supported)} "
            f"(optionally with {', '.join(sorted(COMPRESSION_SUFFIXES))} compression), or pass format="
        )
    return fmt
