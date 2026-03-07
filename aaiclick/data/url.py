"""
aaiclick.data.url - Load data from external URLs into ClickHouse Objects.

Uses ClickHouse's native url() table function for zero Python memory footprint.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .data_context import create_object, get_ch_client
from .models import FIELDTYPE_ARRAY, Schema
from .sql_utils import quote_identifier

SUPPORTED_URL_FORMATS = frozenset({
    "Parquet", "CSV", "CSVWithNames", "CSVWithNamesAndTypes",
    "TSV", "TSVWithNames", "TSVWithNamesAndTypes",
    "JSON", "JSONEachRow", "JSONCompactEachRow",
    "ORC", "Avro",
})


def _validate_url(url: str) -> None:
    """Validate URL is a proper HTTP(S) URL."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"URL must use http or https scheme, got '{parsed.scheme}'"
        )
    if not parsed.netloc:
        raise ValueError("URL must have a valid host")


def _validate_url_columns(columns: list[str]) -> None:
    """Validate column list is non-empty and has no reserved names."""
    if not columns:
        raise ValueError("columns must be a non-empty list")
    for col in columns:
        if col == "aai_id":
            raise ValueError("'aai_id' is a reserved column name and cannot be used")


def _validate_url_format(fmt: str) -> None:
    """Validate format is a supported ClickHouse URL format."""
    if fmt not in SUPPORTED_URL_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. "
            f"Supported formats: {sorted(SUPPORTED_URL_FORMATS)}"
        )


async def create_object_from_url(
    url: str,
    columns: list[str],
    format: str = "Parquet",
    where: str | None = None,
    limit: int | None = None,
) -> Object:
    """
    Create a new Object by loading data from an external URL using ClickHouse's url() table function.

    All data flows directly from the URL into ClickHouse - zero Python memory footprint.
    ClickHouse handles the HTTP request, parsing, and type inference natively.

    Args:
        url: HTTP(S) URL to load data from (e.g., Parquet file on S3, CSV on web server)
        columns: List of column names to select from the URL source
        format: ClickHouse format name. Default "Parquet".
            Supported: Parquet, CSV, CSVWithNames, TSV, TSVWithNames,
            JSON, JSONEachRow, ORC, Avro, etc.
        where: Optional SQL WHERE clause for filtering rows at load time
        limit: Optional row limit applied at load time

    Returns:
        Object: New Object with loaded data.
            - 1 column: column named "value"
            - Multiple columns: columns keep original names

    Raises:
        ValueError: If URL, columns, format, or limit are invalid
        RuntimeError: If no active DataContext
    """
    _validate_url(url)
    _validate_url_columns(columns)
    _validate_url_format(format)
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError(f"limit must be a positive integer, got {limit}")
    if where is not None and ";" in where:
        raise ValueError("WHERE clause must not contain ';'")

    ch = get_ch_client()

    # Escape single quotes in URL for safe SQL embedding
    safe_url = url.replace("'", "\\'")

    # Infer column types via DESCRIBE on the url() table function
    quoted_columns = [quote_identifier(c) for c in columns]
    columns_str = ", ".join(quoted_columns)
    describe_query = (
        f"DESCRIBE (SELECT {columns_str} FROM url('{safe_url}', '{format}') LIMIT 0)"
    )
    describe_result = await ch.query(describe_query)

    ch_types: dict[str, str] = {}
    for row in describe_result.result_rows:
        ch_types[row[0]] = row[1]

    # Build schema
    if len(columns) == 1:
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": ch_types[columns[0]]},
        )
        select_cols = f"{quoted_columns[0]} AS value"
    else:
        schema_columns: dict[str, str] = {"aai_id": "UInt64"}
        for col_name in columns:
            schema_columns[col_name] = ch_types[col_name]
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=schema_columns,
        )
        select_cols = columns_str

    # Create target table
    obj = await create_object(schema)

    # Insert data from URL (aai_id uses DEFAULT generateSnowflakeID())
    where_clause = f" WHERE {where}" if where else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    # Build column list excluding aai_id for INSERT
    insert_col_names = [k for k in schema.columns if k != "aai_id"]
    insert_cols_str = ", ".join(insert_col_names)

    insert_query = (
        f"INSERT INTO {obj.table} ({insert_cols_str}) "
        f"SELECT {select_cols} "
        f"FROM url('{safe_url}', '{format}')"
        f"{where_clause}"
        f"{limit_clause}"
    )
    await ch.command(insert_query)

    return obj
