"""
aaiclick.data.formats - File-extension and format-name handling for ClickHouse data formats.

``Object.export()`` and ``open_object_from_url()`` pick a format from a file
extension through ``EXTENSION_TO_FORMAT``. Explicit ``format=`` names are passed to
ClickHouse as-is: any format it knows works, and it rejects the rest.

Adding an extension is a single entry there.
"""

from __future__ import annotations

from pathlib import Path

# Extension → the format ClickHouse itself picks for it; ``.sql`` is ours.
EXTENSION_TO_FORMAT: dict[str, str] = {
    ".parquet": "Parquet",
    ".arrow": "Arrow",
    ".orc": "ORC",
    ".avro": "Avro",
    ".csv": "CSV",
    ".tsv": "TSV",
    ".json": "JSON",
    ".jsonl": "JSONEachRow",
    ".ndjson": "JSONEachRow",
    # Text reports — ClickHouse writes these but cannot read them back
    ".md": "Markdown",
    ".xml": "XML",
    ".sql": "SQLInsert",
}

# Formats that read the whole document as one string, for JSONExtract.
JSON_BLOB_FORMATS: frozenset[str] = frozenset({"RawBLOB", "JSONAsString"})

# Compression suffixes stripped before the extension lookup; ClickHouse picks
# the codec from the suffix itself, and rejects one its build lacks.
COMPRESSION_SUFFIXES: frozenset[str] = frozenset({".gz", ".xz", ".zst", ".br", ".lz4", ".bz2", ".snappy"})

# ClickHouse resolves the format itself: from the extension, else the content.
FORMAT_AUTO = "auto"


def validate_format_name(fmt: str | None) -> None:
    """Reject a format name that is not a bare identifier.

    Callers inline the name into SQL, so this keeps it injection-safe. Whether
    the format exists and can be read is ClickHouse's call.
    """
    if fmt is None or not fmt.isidentifier():
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
