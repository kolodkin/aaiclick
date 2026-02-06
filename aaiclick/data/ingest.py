"""
aaiclick.data.ingest - Database-level functions for ingesting and concatenating data.

This module provides database-level functions for copying, concatenating, and
inserting data. Functions take table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from typing import Callable, Awaitable

from .data_context import create_object
from .models import ColumnMeta, Schema, QueryInfo, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, ValueType


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


async def _get_table_schema(table: str, ch_client) -> tuple[str, dict[str, str]]:
    """
    Get fieldtype and columns from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        Tuple of (fieldtype, columns dict)
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
        columns[name] = col_type
        if name == "value" and comment:
            meta = ColumnMeta.from_yaml(comment)
            if meta.fieldtype:
                fieldtype = meta.fieldtype

    return fieldtype, columns


async def _get_value_column_type(table: str, ch_client) -> str:
    """
    Get the value column type from a table.

    Args:
        table: Table name
        ch_client: ClickHouse client instance

    Returns:
        ClickHouse type string for value column
    """
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{table}' AND name = 'value'
    """
    type_result = await ch_client.query(type_query)
    return type_result.result_rows[0][0] if type_result.result_rows else "Float64"


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


async def copy_db(obj):
    """
    Copy an Object or View to a new Object at database level.

    For base Objects: Creates a new Object with a copy of all data.
    For Views with selected_field (single): Creates an array Object.
    For Views with selected_fields (multiple): Creates a dict Object.

    Args:
        obj: Object or View instance to copy

    Returns:
        Object: New Object instance with copied data
    """
    ch_client = obj.ch_client

    # Check if this is a View with field selection
    selected_field = getattr(obj, 'selected_field', None)
    selected_fields = getattr(obj, 'selected_fields', None)
    source = getattr(obj, '_source', None)

    # Handle multi-field selection (dict output)
    if selected_fields and source:
        source_schema = source._schema
        # Build schema with selected columns
        columns = {"aai_id": "UInt64"}
        for field in selected_fields:
            columns[field] = source_schema.columns[field]

        new_schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,  # Each column is an array
            columns=columns
        )
        result = await create_object(new_schema)

        # Build column list for INSERT
        fields_str = ", ".join(selected_fields)
        query_info = obj._get_query_info()
        if query_info.source.startswith('('):
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT aai_id, {fields_str} FROM {query_info.source} AS v
            """
        else:
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT aai_id, {fields_str} FROM {query_info.source}
            """
        await ch_client.command(insert_query)
        return result

    # Handle single-field selection (array output)
    if selected_field and source:
        source_schema = source._schema
        value_type = source_schema.columns[selected_field]

        new_schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": value_type}
        )
        result = await create_object(new_schema)

        query_info = obj._get_query_info()
        if query_info.source.startswith('('):
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT aai_id, value FROM {query_info.source} AS v
            """
        else:
            insert_query = f"""
            INSERT INTO {result.table}
            SELECT aai_id, value FROM {query_info.source}
            """
        await ch_client.command(insert_query)
        return result

    # Handle base Object or View without field selection
    fieldtype, columns = await _get_table_schema(obj.table, ch_client)
    schema = Schema(fieldtype=fieldtype, columns=columns)

    result = await create_object(schema)

    # Use _build_select to handle any view constraints (where, limit, etc.)
    if obj.has_constraints:
        insert_query = f"INSERT INTO {result.table} SELECT * FROM ({obj._build_select()}) AS s"
    else:
        insert_query = f"INSERT INTO {result.table} SELECT * FROM {obj.table}"
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

    # Use base_table for metadata queries
    fieldtype = await _get_fieldtype(query_infos[0].base_table, ch_client)
    if fieldtype != FIELDTYPE_ARRAY:
        raise ValueError("concat requires first source to have array fieldtype")

    value_type = await _get_value_column_type(query_infos[0].base_table, ch_client)

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    result = await create_object(schema)

    # Single multi-table UNION ALL operation using sources (can be subqueries)
    # Add alias for subqueries (sources starting with '(')
    union_parts = []
    for i, info in enumerate(query_infos):
        if info.source.startswith('('):
            union_parts.append(f"SELECT aai_id, value FROM {info.source} AS s{i}")
        else:
            union_parts.append(f"SELECT aai_id, value FROM {info.source}")

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

    fieldtype = await _get_fieldtype(target_table, ch_client)
    if fieldtype != FIELDTYPE_ARRAY:
        raise ValueError("insert requires target table to have array fieldtype")

    target_value_type = await _get_value_column_type(target_table, ch_client)

    # Validate all source types are compatible
    for source_table in source_tables:
        source_value_type = await _get_value_column_type(source_table, ch_client)
        if not _are_types_compatible(target_value_type, source_value_type):
            raise ValueError(
                f"Cannot insert {source_value_type} into {target_value_type}: "
                f"types are incompatible"
            )

    # Single multi-source INSERT with UNION ALL
    union_parts = [
        f"SELECT aai_id, CAST(value AS {target_value_type}) as value FROM {table}"
        for table in source_tables
    ]
    insert_query = f"""
    INSERT INTO {target_table}
    {' UNION ALL '.join(union_parts)}
    """
    await ch_client.command(insert_query)


