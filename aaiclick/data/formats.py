"""
aaiclick.data.formats - Single source of truth for ClickHouse data formats.

Both ``Object.export()`` (output side) and ``create_object_from_url()`` (input
side) consume this registry. Each ``FormatSpec`` records:

- the ClickHouse format identifier (passed verbatim to ``FORMAT`` / ``file()`` /
  ``url()``),
- the natural file extensions for the format,
- whether ClickHouse supports the format as input and/or output.

Adding a new format is a single edit to ``FORMATS``.
"""

from __future__ import annotations

import gzip
import lzma
from collections.abc import Callable
from pathlib import Path
from typing import IO, NamedTuple, cast


class FormatSpec(NamedTuple):
    """A ClickHouse data format known to aaiclick."""

    name: str
    """ClickHouse format identifier — passed verbatim to ``FORMAT`` / ``file()`` / ``url()``."""

    extensions: tuple[str, ...]
    """Natural file extensions, lowercase, including the leading dot."""

    input: bool
    """Format can be read by ``create_object_from_url`` / file ingestion."""

    output: bool
    """Format can be written by ``Object.export``."""

    blob: bool = False
    """Format reads the whole document as a single string (JSON-blob mode)."""


# fmt: off
FORMATS: tuple[FormatSpec, ...] = (
    # Columnar / binary
    FormatSpec("Parquet",              (".parquet",),                  input=True,  output=True),
    FormatSpec("Arrow",                (".arrow",),                    input=False, output=True),
    FormatSpec("ORC",                  (".orc",),                      input=True,  output=True),
    FormatSpec("Avro",                 (".avro",),                     input=True,  output=True),
    # CSV family
    FormatSpec("CSV",                  (),                             input=True,  output=False),
    FormatSpec("CSVWithNames",         (".csv",),                      input=True,  output=True),
    FormatSpec("CSVWithNamesAndTypes", (),                             input=True,  output=False),
    # TSV family
    FormatSpec("TSV",                  (),                             input=True,  output=False),
    FormatSpec("TSVWithNames",         (".tsv",),                      input=True,  output=True),
    FormatSpec("TSVWithNamesAndTypes", (),                             input=True,  output=False),
    # JSON family
    FormatSpec("JSON",                 (),                             input=True,  output=False),
    FormatSpec("JSONEachRow",          (".json", ".jsonl", ".ndjson"), input=True,  output=True),
    FormatSpec("JSONCompactEachRow",   (),                             input=True,  output=False),
    # Text reports
    FormatSpec("Markdown",             (".md",),                       input=False, output=True),
    FormatSpec("XML",                  (".xml",),                      input=False, output=True),
    FormatSpec("SQLInsert",            (".sql",),                      input=False, output=True),
    # JSON blob mode — read whole document, extract via JSONExtract
    FormatSpec("RawBLOB",              (),                             input=True,  output=False, blob=True),
    FormatSpec("JSONAsString",         (),                             input=True,  output=False, blob=True),
)
# fmt: on


# Compression codecs supported on both backends. chdb delegates to
# ClickHouse's ``file()`` auto-detection; remote compresses client-side via
# the matching stdlib opener below. Only stdlib codecs are advertised so
# both backends behave identically with zero extra dependencies.
def _gzip_open(path: str) -> IO[bytes]:
    return cast(IO[bytes], gzip.open(path, "wb"))


def _lzma_open(path: str) -> IO[bytes]:
    return cast(IO[bytes], lzma.open(path, "wb"))


_COMPRESSION_OPENERS: dict[str, Callable[[str], IO[bytes]]] = {
    ".gz": _gzip_open,
    ".xz": _lzma_open,
}
COMPRESSION_SUFFIXES: frozenset[str] = frozenset(_COMPRESSION_OPENERS)


# Derived lookups — built once at import.
INPUT_FORMATS: frozenset[str] = frozenset(f.name for f in FORMATS if f.input)
OUTPUT_FORMATS: frozenset[str] = frozenset(f.name for f in FORMATS if f.output)
JSON_BLOB_FORMATS: frozenset[str] = frozenset(f.name for f in FORMATS if f.blob)
EXTENSION_TO_FORMAT: dict[str, str] = {ext: f.name for f in FORMATS for ext in f.extensions if f.output}


def format_for_extension(path: str) -> str:
    """Return the ClickHouse output format matching the file extension of *path*.

    A trailing compression suffix from :data:`COMPRESSION_SUFFIXES` is stripped
    before lookup, so ``data.csv.gz`` resolves to ``CSVWithNames``.

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
            f"(optionally with {', '.join(sorted(COMPRESSION_SUFFIXES))} compression)"
        )
    return fmt


def open_export_writer(path: str) -> IO[bytes]:
    """Return a binary writer for *path*, transparently wrapping compressed suffixes.

    Matches the set in :data:`COMPRESSION_SUFFIXES`. Unknown suffixes fall
    through to a plain binary ``open`` — the caller is expected to have
    already validated the extension via :func:`format_for_extension`.
    """
    opener = _COMPRESSION_OPENERS.get(Path(path).suffix.lower())
    return opener(path) if opener else open(path, "wb")
