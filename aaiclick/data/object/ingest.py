"""
aaiclick.data.ingest - Database-level functions for ingesting and concatenating data.

This module provides database-level functions for copying, concatenating, and
inserting data. Functions take table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from typing import Callable, Awaitable

from aaiclick.oplog.collector import oplog_record
from ..data_context import create_object
from ..models import ColumnInfo, ColumnMeta, CopyInfo, Schema, QueryInfo, IngestQueryInfo, FIELDTYPE_ARRAY, FIELDTYPE_DICT, FIELDTYPE_SCALAR, ValueType, parse_ch_type, INT_TYPES, FLOAT_TYPES, NUMERIC_TYPES
from ..sql_utils import quote_identifier


def _are_types_compatible(target_type: str, source_type: str) -> bool:
    """
    Check if source_type is directly compatible with target_type (no CAST).

    Used for UNION ALL in concat where ClickHouse requires exact type matches.
    Only allows same-type or same-category integer/float matches within the same
    category (int↔int, float↔float), but NOT across categories (int↔float).
    """
    if target_type == source_type:
        return True

    if target_type in INT_TYPES and source_type in INT_TYPES:
        return True

    if target_type in FLOAT_TYPES and source_type in FLOAT_TYPES:
        return True

    return False


def _are_types_castable(target_type: str, source_type: str) -> bool:
    """
    Check if source_type can be CAST to target_type.

    Used for INSERT with explicit CAST where ClickHouse allows casting between
    all numeric types (Int*, UInt*, Float*), but not between numeric and string.
    """
    if _are_types_compatible(target_type, source_type):
        return True

    if target_type in NUMERIC_TYPES and source_type in NUMERIC_TYPES:
        return True

    return False


async def _get_table_schema(table: str, ch_client) -> tuple[str, dict[str, ColumnInfo]]:
    """
    Get fieldtype and columns from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        Tuple of (fieldtype, columns dict mapping names to ColumnInfo)
    """
    columns_query = f"""
    SELECT name, type, comment
    FROM system.columns
    WHERE table = '{table}'
    ORDER BY position
    """
    columns_result = await ch_client.query(columns_query)

    columns = {}
    aai_id_fieldtype = None
    col_fieldtype = FIELDTYPE_SCALAR
    for name, col_type, comment in columns_result.result_rows:
        columns[name] = parse_ch_type(col_type)
        if not comment:
            continue
        meta = ColumnMeta.from_yaml(comment)
        if name == "aai_id":
            # New tables store the object-level fieldtype on aai_id.
            aai_id_fieldtype = meta.fieldtype if meta.fieldtype else None
        elif meta.fieldtype and col_fieldtype == FIELDTYPE_SCALAR:
            col_fieldtype = meta.fieldtype

    # Prefer the explicit object-level fieldtype stored on aai_id (new tables).
    # For old tables aai_id carries FIELDTYPE_SCALAR (its former placeholder), so
    # fall back to structural inference: tables with columns beyond aai_id/value
    # are DICT; otherwise use col_fieldtype (ARRAY or SCALAR from column comments).
    if aai_id_fieldtype and aai_id_fieldtype != FIELDTYPE_SCALAR:
        fieldtype = aai_id_fieldtype
    else:
        is_dict = bool(set(columns.keys()) - {"aai_id", "value"})
        fieldtype = FIELDTYPE_DICT if is_dict else col_fieldtype

    return fieldtype, columns


async def _get_value_column_type(table: str, ch_client) -> ColumnInfo:
    """
    Get the value column type from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        ColumnInfo for the value column
    """
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{table}' AND name = 'value'
    """
    type_result = await ch_client.query(type_query)
    if type_result.result_rows:
        return parse_ch_type(type_result.result_rows[0][0])
    raise RuntimeError(f"Table '{table}' has no 'value' column")


async def _get_fieldtype(table: str, ch_client) -> str:
    """
    Get the fieldtype of the value column from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        Fieldtype string (FIELDTYPE_SCALAR or FIELDTYPE_ARRAY)
    """
    columns_query = f"""
    SELECT comment FROM system.columns
    WHERE table = '{table}' AND name = 'value'
    """
    result = await ch_client.query(columns_query)
    if result.result_rows:
        meta = ColumnMeta.from_yaml(result.result_rows[0][0])
        if meta.fieldtype:
            return meta.fieldtype
    return FIELDTYPE_SCALAR


async def copy_db(copy_info: CopyInfo, ch_client):
    """Copy data into a new Object using a database-internal INSERT...SELECT.

    Creates a new Object with the schema from `copy_info`, then inserts all
    rows from the source query directly inside ClickHouse — no Python round-trip.

    Args:
        copy_info: CopyInfo with source query, column definitions, and fieldtype.
        ch_client: Active ClickHouse client instance.

    Returns:
        Object: New Object containing the copied data.
    """
    schema = Schema(
        fieldtype=copy_info.fieldtype,
        columns=copy_info.columns,
        col_fieldtype=copy_info.col_fieldtype,
    )
    result = await create_object(schema)

    alias = " AS s" if copy_info.source_query.startswith('(') else ""
    insert_query = f"INSERT INTO {result.table} SELECT * FROM {copy_info.source_query}{alias}"
    await ch_client.command(insert_query)

    return result


async def copy_db_selected_fields(copy_info: CopyInfo, ch_client):
    """
    Copy selected fields from a dict Object to a new Object at database level.

    For single-field selection: Creates an array Object with the selected column as 'value'.
    For multi-field selection: Creates a dict Object with the selected columns.

    Args:
        copy_info: CopyInfo with source query, columns, and selected fields info
        ch_client: ClickHouse client instance

    Returns:
        Object: New Object instance with copied data
    """
    alias = " AS v" if copy_info.source_query.startswith('(') else ""

    if copy_info.is_single_field:
        field = copy_info.selected_fields[0]
        new_schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": ColumnInfo("UInt64"), "value": copy_info.columns[field]}
        )
        result = await create_object(new_schema)
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT aai_id, value FROM {copy_info.source_query}{alias}
        """
    else:
        columns = {"aai_id": ColumnInfo("UInt64")}
        for field in copy_info.selected_fields:
            columns[field] = copy_info.columns[field]

        new_schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=columns)
        result = await create_object(new_schema)
        fields_str = ", ".join(quote_identifier(f) for f in copy_info.selected_fields)
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT aai_id, {fields_str} FROM {copy_info.source_query}{alias}
        """

    await ch_client.command(insert_query)
    return result


async def _insert_source(
    target_table: str,
    info: IngestQueryInfo,
    target_types: dict[str, ColumnInfo],
    alias_index: int,
    ch_client,
) -> None:
    """Insert a single source into a target table.

    Args:
        target_table: Destination table name.
        info: Source query info.
        target_types: Mapping of data column name to target ColumnInfo
            (excludes ``aai_id``).  Column names are derived from the keys.
        alias_index: Index used to alias subquery sources.
        ch_client: ClickHouse client instance.
    """
    col_names = sorted(target_types)
    cast_exprs = ", ".join(
        f"CAST({col} AS {target_types[col].ch_type()}) AS {col}"
        for col in col_names
    )
    insert_cols = ", ".join(["aai_id"] + col_names)
    if info.source.startswith('('):
        select = f"SELECT aai_id, {cast_exprs} FROM {info.source} AS s{alias_index}"
    else:
        select = f"SELECT aai_id, {cast_exprs} FROM {info.source}"

    await ch_client.command(f"""
    INSERT INTO {target_table} ({insert_cols})
    {select}
    """)


async def concat_objects_db(
    query_infos: list[IngestQueryInfo],
    ch_client,
):
    """
    Concatenate multiple sources into a new Object, one INSERT per source.

    Preserves existing Snowflake IDs from all sources.
    Order is maintained via existing Snowflake IDs when data is retrieved.

    Args:
        query_infos: List of QueryInfo (source and base_table pairs, minimum 2)
        ch_client: ClickHouse client instance

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If less than 2 query_infos provided
        ValueError: If first source does not have array fieldtype
    """
    if len(query_infos) < 2:
        raise ValueError("concat requires at least 2 sources")

    first_info = query_infos[0]
    if first_info.fieldtype not in (FIELDTYPE_ARRAY, FIELDTYPE_DICT):
        raise ValueError("concat requires first source to have array fieldtype")

    # Validate all sources have compatible schemas and promote nullable
    first_columns = first_info.columns
    result_columns = dict(first_columns)
    data_col_names = sorted(k for k in first_columns if k != "aai_id")

    for i, info in enumerate(query_infos[1:], start=1):
        other_columns = info.columns
        other_data_cols = sorted(k for k in other_columns if k != "aai_id")

        if other_data_cols != data_col_names:
            raise ValueError(
                f"concat source {i} has columns {other_data_cols}, "
                f"expected {data_col_names}"
            )

        for col_name in data_col_names:
            target_def = result_columns[col_name]
            source_def = other_columns[col_name]
            if not _are_types_compatible(target_def.type, source_def.type):
                raise ValueError(
                    f"concat source {i} column '{col_name}' has incompatible type "
                    f"{source_def.type} (target: {target_def.type})"
                )
            # Promote to nullable if any source is nullable
            if source_def.nullable and not target_def.nullable:
                result_columns[col_name] = ColumnInfo(
                    target_def.type,
                    nullable=True,
                    array=target_def.array,
                    low_cardinality=target_def.low_cardinality,
                )

    schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=result_columns)
    result = await create_object(schema)

    data_columns = {k: v for k, v in result_columns.items() if k != "aai_id"}
    for i, info in enumerate(query_infos):
        await _insert_source(
            result.table, info, data_columns, i, ch_client,
        )

    oplog_record(result.table, "concat", args=[info.base_table for info in query_infos])
    return result


async def insert_objects_db(
    target_info: IngestQueryInfo,
    source_infos: list[IngestQueryInfo],
    ch_client,
) -> None:
    """
    Insert data from multiple sources into target, one INSERT per source.

    Preserves existing Snowflake IDs. Sources may have a subset of target
    columns — missing columns get their ClickHouse default values. Sources
    may also include computed columns from Views (via with_columns).

    Args:
        target_info: QueryInfo for the target (must have array fieldtype)
        source_infos: List of QueryInfo for sources
        ch_client: ClickHouse client instance

    Raises:
        ValueError: If target does not have array fieldtype
        ValueError: If any source has columns not in target
        ValueError: If any source value types are incompatible with target
    """
    if not source_infos:
        return

    if target_info.fieldtype not in (FIELDTYPE_ARRAY, FIELDTYPE_DICT):
        raise ValueError("insert requires target table to have array fieldtype")

    target_columns = target_info.columns
    target_data_cols = set(k for k in target_columns if k != "aai_id")

    for i, info in enumerate(source_infos):
        source_columns = info.columns
        all_source_cols = sorted(k for k in source_columns if k != "aai_id")

        # Skip extra source columns not in target (intersection semantics)
        col_names = [c for c in all_source_cols if c in target_data_cols]

        # Validate types for matched columns
        for col_name in col_names:
            target_def = target_columns[col_name]
            source_def = source_columns[col_name]
            if not _are_types_castable(target_def.type, source_def.type):
                raise ValueError(
                    f"Cannot insert {source_def.type} into {target_def.type} "
                    f"for column '{col_name}': types are incompatible"
                )

        source_target_types = {col: target_columns[col] for col in col_names}
        await _insert_source(
            target_info.base_table, info, source_target_types, i, ch_client,
        )

    for info in source_infos:
        oplog_record(
            target_info.base_table, "insert",
            kwargs={"source": info.base_table, "target": target_info.base_table},
        )


