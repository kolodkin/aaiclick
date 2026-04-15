"""
aaiclick.data.url - Load data from external URLs into ClickHouse Objects.

Uses ClickHouse's native url() table function for zero Python memory footprint.
Supports both tabular formats (Parquet, CSV, JSONEachRow) and nested JSON APIs
via RawBLOB/JSONAsString with JSONExtract-based column extraction.
"""

from __future__ import annotations

from urllib.parse import urlparse

from ..data_context import create_object, get_ch_client
from ..formats import INPUT_FORMATS
from ..models import ColumnInfo, FIELDTYPE_ARRAY, FIELDTYPE_DICT, FLOAT_TYPES, INT_TYPES, Schema, parse_ch_type
from ..sql_utils import escape_sql_string, quote_identifier

JSON_BLOB_FORMATS = frozenset({"RawBLOB", "JSONAsString"})

_FORMAT_SOURCE_COLUMN = {
    "RawBLOB": "raw_blob",
    "JSONAsString": "json",
}


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
    if fmt not in INPUT_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. "
            f"Supported formats: {sorted(INPUT_FORMATS)}"
        )


def _json_extract_expr(field_name: str, col_info: ColumnInfo) -> str:
    """Build a JSONExtract expression for a single JSON field.

    Selects the most specific JSONExtract variant based on the ColumnInfo type:
    - Array or Nullable types use generic JSONExtract with explicit CH type
    - String/Int/Float/Bool use specialized JSONExtractString/Int/Float/Bool

    Args:
        field_name: JSON field name to extract
        col_info: ColumnInfo describing the target ClickHouse type

    Returns:
        SQL expression like "JSONExtractString(elem, 'cveID')"
    """
    safe_field = escape_sql_string(field_name)

    if col_info.array or col_info.nullable:
        return f"JSONExtract(elem, '{safe_field}', '{col_info.ch_type()}')"

    base = col_info.type
    if base == "String":
        return f"JSONExtractString(elem, '{safe_field}')"
    if base == "Bool":
        return f"JSONExtractBool(elem, '{safe_field}')"
    if base in INT_TYPES:
        return f"JSONExtractInt(elem, '{safe_field}')"
    if base in FLOAT_TYPES:
        return f"JSONExtractFloat(elem, '{safe_field}')"

    return f"JSONExtract(elem, '{safe_field}', '{col_info.ch_type()}')"


def _build_json_select(
    json_columns: dict[str, ColumnInfo],
    json_path: str,
    format: str,
    safe_url: str,
) -> tuple[str, str]:
    """Build SELECT expressions and FROM subquery for JSON extraction.

    Args:
        json_columns: Mapping of JSON field name to target ColumnInfo
        json_path: Dot-path to the JSON array (e.g., "vulnerabilities")
        format: RawBLOB or JSONAsString
        safe_url: SQL-safe URL string (single quotes escaped)

    Returns:
        (select_exprs, from_subquery) tuple for use in INSERT...SELECT
    """
    source_col = _FORMAT_SOURCE_COLUMN[format]
    safe_path = escape_sql_string(json_path)

    select_parts = []
    for field_name, col_info in json_columns.items():
        expr = _json_extract_expr(field_name, col_info)
        select_parts.append(f"{expr} AS {quote_identifier(field_name)}")

    select_exprs = ", ".join(select_parts)
    from_subquery = (
        f"(SELECT arrayJoin(JSONExtractArrayRaw("
        f"{quote_identifier(source_col)}, '{safe_path}')) AS elem "
        f"FROM url('{safe_url}', '{format}')) AS _json_src"
    )

    return select_exprs, from_subquery


async def create_object_from_url(
    url: str,
    columns: list[str] | None = None,
    format: str = "Parquet",
    where: str | None = None,
    limit: int | None = None,
    json_path: str | None = None,
    json_columns: dict[str, ColumnInfo] | None = None,
    ch_settings: dict[str, str | int] | None = None,
    column_types: dict[str, ColumnInfo] | None = None,
) -> Object:
    """
    Create a new Object by loading data from an external URL using ClickHouse's url() table function.

    All data flows directly from the URL into ClickHouse - zero Python memory footprint.
    ClickHouse handles the HTTP request, parsing, and type inference natively.

    Supports two modes:

    **Tabular mode** (default): For formats where each row maps to an Object row.
        Requires `columns` parameter. Types are inferred via DESCRIBE unless
        ``column_types`` is provided.

    **JSON mode**: For nested JSON APIs that return an array inside an envelope.
        Requires `json_path` + `json_columns`. Uses JSONExtract to parse fields.
        Format must be RawBLOB or JSONAsString.

    Args:
        url: HTTP(S) URL to load data from
        columns: Column names to select (tabular mode). Mutually exclusive with json_columns.
        format: ClickHouse format name. Default "Parquet".
        where: Optional SQL WHERE clause for filtering rows at load time
        limit: Optional row limit applied at load time
        json_path: Path to JSON array in response (e.g., "vulnerabilities"). Requires json_columns.
        json_columns: Mapping of JSON field names to ColumnInfo types. Requires json_path.
        ch_settings: Optional ClickHouse query settings passed to the read operation.
            Useful for format-specific options, e.g.
            ``{"input_format_csv_skip_first_lines": 1}`` to skip a comment header line.
        column_types: Optional explicit column types for tabular mode. When provided,
            skips the DESCRIBE query and uses these types directly. Useful for formats
            like CSV where ClickHouse may fail to infer numeric types (e.g. for large
            remote files with LIMIT 0 type sampling). Keys must match entries in ``columns``.

    Returns:
        Object: New Object with loaded data.

    Raises:
        ValueError: If parameters are invalid or incompatible
        RuntimeError: If no active DataContext
    """
    _validate_url(url)
    _validate_url_format(format)
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError(f"limit must be a positive integer, got {limit}")
    if where is not None and ";" in where:
        raise ValueError("WHERE clause must not contain ';'")

    json_mode = json_path is not None or json_columns is not None
    tabular_mode = columns is not None

    if json_mode and tabular_mode:
        raise ValueError("columns and json_columns/json_path are mutually exclusive")

    if json_mode:
        if json_path is None or json_columns is None:
            raise ValueError("json_path and json_columns must both be provided")
        if not json_columns:
            raise ValueError("json_columns must be a non-empty dict")
        if format not in JSON_BLOB_FORMATS:
            raise ValueError(
                f"JSON mode requires format to be one of {sorted(JSON_BLOB_FORMATS)}, "
                f"got '{format}'"
            )
        for col_name in json_columns:
            if col_name == "aai_id":
                raise ValueError("'aai_id' is a reserved column name and cannot be used")
        return await _create_from_json(url, format, json_path, json_columns, where, limit, ch_settings)

    if columns is None:
        raise ValueError("Either columns or json_path/json_columns must be provided")
    _validate_url_columns(columns)
    return await _create_from_tabular(url, format, columns, where, limit, ch_settings, column_types)


async def _create_from_tabular(
    url: str,
    format: str,
    columns: list[str],
    where: str | None,
    limit: int | None,
    ch_settings: dict[str, str | int] | None,
    column_types: dict[str, ColumnInfo] | None = None,
) -> Object:
    """Load data from a tabular URL source (Parquet, CSV, JSONEachRow, etc.)."""
    ch = get_ch_client()
    settings = ch_settings or {}
    safe_url = escape_sql_string(url)
    safe_source = f"url('{safe_url}', '{format}')"

    quoted_columns = [quote_identifier(c) for c in columns]
    columns_str = ", ".join(quoted_columns)

    if column_types is not None:
        ch_types: dict[str, ColumnInfo] = column_types
    else:
        describe_query = (
            f"DESCRIBE (SELECT {columns_str} FROM {safe_source} LIMIT 0)"
        )
        describe_result = await ch.query(describe_query, settings=settings)
        ch_types = {row[0]: parse_ch_type(row[1]) for row in describe_result.result_rows}

    if len(columns) == 1:
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": ColumnInfo("UInt64"), "value": ch_types[columns[0]]},
        )
        select_cols = f"{quoted_columns[0]} AS value"
    else:
        schema_columns: dict[str, ColumnInfo] = {"aai_id": ColumnInfo("UInt64")}
        for col_name in columns:
            schema_columns[col_name] = ch_types[col_name]
        schema = Schema(
            fieldtype=FIELDTYPE_DICT,
            columns=schema_columns,
        )
        select_cols = columns_str

    obj = await create_object(schema)

    where_clause = f" WHERE {where}" if where else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    insert_col_names = [k for k in schema.columns if k != "aai_id"]
    insert_cols_str = ", ".join(insert_col_names)

    insert_query = (
        f"INSERT INTO {obj.table} ({insert_cols_str}) "
        f"SELECT {select_cols} "
        f"FROM {safe_source}"
        f"{where_clause}"
        f"{limit_clause}"
    )
    await ch.command(insert_query, settings=settings)

    return obj


async def _create_from_json(
    url: str,
    format: str,
    json_path: str,
    json_columns: dict[str, ColumnInfo],
    where: str | None,
    limit: int | None,
    ch_settings: dict[str, str | int] | None,
) -> Object:
    """Load data from a nested JSON API via RawBLOB/JSONAsString + JSONExtract."""
    ch = get_ch_client()
    settings = ch_settings or {}
    safe_url = escape_sql_string(url)

    schema_columns: dict[str, ColumnInfo] = {"aai_id": ColumnInfo("UInt64")}
    for col_name, col_info in json_columns.items():
        schema_columns[col_name] = col_info

    schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=schema_columns)
    obj = await create_object(schema)

    select_exprs, from_subquery = _build_json_select(
        json_columns, json_path, format, safe_url,
    )

    insert_col_names = [k for k in schema_columns if k != "aai_id"]
    insert_cols_str = ", ".join(quote_identifier(c) for c in insert_col_names)

    where_clause = f" WHERE {where}" if where else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    insert_query = (
        f"INSERT INTO {obj.table} ({insert_cols_str}) "
        f"SELECT {select_exprs} "
        f"FROM {from_subquery}"
        f"{where_clause}"
        f"{limit_clause}"
    )
    await ch.command(insert_query, settings=settings)

    return obj
