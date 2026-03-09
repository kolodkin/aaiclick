"""
aaiclick.data.ingest - Database-level functions for ingesting and concatenating data.

This module provides database-level functions for copying, concatenating, and
inserting data. Functions take table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from typing import Callable, Awaitable

from .data_context import create_object
from .models import ColumnDef, ColumnMeta, CopyInfo, Schema, QueryInfo, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, ValueType, parse_ch_type
from .sql_utils import quote_identifier


def _are_types_compatible(target_type: str, source_type: str) -> bool:
    """
    Check if source_type can be inserted into target_type.

    ClickHouse allows casting between numeric types (Int*, UInt*, Float*),
    but not between numeric and string types.

    Args:
        target_type: ClickHouse type of target column
        source_type: ClickHouse type of source column

    Returns:
        bool: True if types are compatible for insertion
    """
    if target_type == source_type:
        return True

    int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
    float_types = {"Float32", "Float64"}
    numeric_types = int_types | float_types

    if target_type in numeric_types and source_type in numeric_types:
        return True

    string_types = {"String", "FixedString"}

    if (target_type in numeric_types and source_type in string_types) or \
       (target_type in string_types and source_type in numeric_types):
        return False

    return False


async def _get_table_schema(table: str, ch_client) -> tuple[str, dict[str, ColumnDef]]:
    """
    Get fieldtype and columns from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        Tuple of (fieldtype, columns dict mapping names to ColumnDef)
    """
    columns_query = f"""
    SELECT name, type, comment
    FROM system.columns
    WHERE table = '{table}'
    ORDER BY position
    """
    columns_result = await ch_client.query(columns_query)

    columns = {}
    fieldtype = FIELDTYPE_SCALAR
    for name, col_type, comment in columns_result.result_rows:
        columns[name] = parse_ch_type(col_type)
        if name == "value" and comment:
            meta = ColumnMeta.from_yaml(comment)
            if meta.fieldtype:
                fieldtype = meta.fieldtype

    return fieldtype, columns


async def _get_value_column_type(table: str, ch_client) -> ColumnDef:
    """
    Get the value column type from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        ColumnDef for the value column
    """
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{table}' AND name = 'value'
    """
    type_result = await ch_client.query(type_query)
    if type_result.result_rows:
        return parse_ch_type(type_result.result_rows[0][0])
    return ColumnDef("Float64")


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
    """
    Copy data to a new Object at database level.

    Args:
        copy_info: CopyInfo with source query and schema metadata
        ch_client: ClickHouse client instance

    Returns:
        Object: New Object instance with copied data
    """
    schema = Schema(fieldtype=copy_info.fieldtype, columns=copy_info.columns)
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
            columns={"aai_id": ColumnDef("UInt64"), "value": copy_info.columns[field]}
        )
        result = await create_object(new_schema)
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT aai_id, value FROM {copy_info.source_query}{alias}
        """
    else:
        columns = {"aai_id": ColumnDef("UInt64")}
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


async def concat_objects_db(
    query_infos: list[QueryInfo],
    ch_client,
):
    """
    Concatenate multiple sources at database level via single UNION ALL.

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

    # Get full schema from first source
    first_fieldtype, first_columns = await _get_table_schema(
        query_infos[0].base_table, ch_client
    )
    if first_fieldtype != FIELDTYPE_ARRAY:
        raise ValueError("concat requires first source to have array fieldtype")

    # Validate all sources have compatible schemas and promote nullable
    result_columns = dict(first_columns)
    data_col_names = sorted(k for k in first_columns if k != "aai_id")

    for i, info in enumerate(query_infos[1:], start=1):
        _, other_columns = await _get_table_schema(info.base_table, ch_client)
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
                result_columns[col_name] = ColumnDef(target_def.type, nullable=True)

    schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=result_columns)
    result = await create_object(schema)

    # Build UNION ALL with all data columns
    select_cols = ", ".join(["aai_id"] + data_col_names)
    union_parts = []
    for i, info in enumerate(query_infos):
        if info.source.startswith('('):
            union_parts.append(f"SELECT {select_cols} FROM {info.source} AS s{i}")
        else:
            union_parts.append(f"SELECT {select_cols} FROM {info.source}")

    insert_query = f"""
    INSERT INTO {result.table}
    {' UNION ALL '.join(union_parts)}
    """
    await ch_client.command(insert_query)

    return result


async def insert_objects_db(
    target_table: str,
    source_tables: list[str],
    ch_client,
) -> None:
    """
    Insert data from multiple source tables into target via single operation.

    Preserves existing Snowflake IDs. Order is maintained via existing
    Snowflake IDs when data is retrieved.

    Args:
        target_table: Target table name (must have array fieldtype)
        source_tables: List of source table names
        ch_client: ClickHouse client instance

    Raises:
        ValueError: If target table does not have array fieldtype
        ValueError: If any source value types are incompatible with target
    """
    if not source_tables:
        return

    target_fieldtype, target_columns = await _get_table_schema(target_table, ch_client)
    if target_fieldtype != FIELDTYPE_ARRAY:
        raise ValueError("insert requires target table to have array fieldtype")

    data_col_names = sorted(k for k in target_columns if k != "aai_id")

    # Validate all source schemas are compatible
    for source_table in source_tables:
        _, source_columns = await _get_table_schema(source_table, ch_client)
        source_data_cols = sorted(k for k in source_columns if k != "aai_id")

        if source_data_cols != data_col_names:
            raise ValueError(
                f"Source table {source_table} has columns {source_data_cols}, "
                f"expected {data_col_names}"
            )

        for col_name in data_col_names:
            target_def = target_columns[col_name]
            source_def = source_columns[col_name]
            if not _are_types_compatible(target_def.type, source_def.type):
                raise ValueError(
                    f"Cannot insert {source_def.type} into {target_def.type} "
                    f"for column '{col_name}': types are incompatible"
                )

    # Build CAST expressions for each data column
    cast_exprs = ", ".join(
        f"CAST({col} AS {target_columns[col].ch_type()}) AS {col}"
        for col in data_col_names
    )

    union_parts = [
        f"SELECT aai_id, {cast_exprs} FROM {table}"
        for table in source_tables
    ]
    insert_query = f"""
    INSERT INTO {target_table}
    {' UNION ALL '.join(union_parts)}
    """
    await ch_client.command(insert_query)


