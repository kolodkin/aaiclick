"""
aaiclick.ingest - Database-level functions for ingesting and concatenating data.

This module provides database-level functions for copying, concatenating, and
inserting data. Functions take table names and ch_client instead of Object instances.
"""

from __future__ import annotations

from typing import Callable, Awaitable

from .models import ColumnMeta, Schema, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, ValueType


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


async def copy_db(table: str, ch_client, create_object: Callable[[Schema], Awaitable]):
    """
    Copy a table to a new object at database level.

    Creates a new Object with a copy of all data from the source table.
    Preserves all column metadata including fieldtype.

    Args:
        table: Source table name
        ch_client: ClickHouse client instance
        create_object: Async callable to create a new Object from Schema

    Returns:
        Object: New Object instance with copied data
    """
    fieldtype, columns = await _get_table_schema(table, ch_client)
    schema = Schema(fieldtype=fieldtype, columns=columns)

    result = await create_object(schema)

    insert_query = f"INSERT INTO {result.table} SELECT * FROM {table}"
    await ch_client.command(insert_query)

    return result


async def concat_objects_db(
    table_a: str,
    table_b: str,
    ch_client,
    create_object: Callable[[Schema], Awaitable],
):
    """
    Concatenate two tables at database level.

    Preserves existing Snowflake IDs from both tables.
    Order is maintained via existing Snowflake IDs when data is retrieved.

    Args:
        table_a: First table name (must have array fieldtype)
        table_b: Second table name
        ch_client: ClickHouse client instance
        create_object: Async callable to create a new Object from Schema

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If table_a does not have array fieldtype
    """
    fieldtype_a = await _get_fieldtype(table_a, ch_client)
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("concat requires first table to have array fieldtype")

    value_type = await _get_value_column_type(table_a, ch_client)

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    result = await create_object(schema)

    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, value FROM {table_a}
    UNION ALL
    SELECT aai_id, value FROM {table_b}
    """
    await ch_client.command(insert_query)

    return result


async def concat_value_db(
    table_a: str,
    value: ValueType,
    ch_client,
    create_object: Callable[[Schema], Awaitable],
    create_object_from_value: Callable[[ValueType], Awaitable],
):
    """
    Concatenate a value to a table at database level.

    First copies table_a, then inserts the value.

    Args:
        table_a: Source table name (must have array fieldtype)
        value: Value to append (scalar or list)
        ch_client: ClickHouse client instance
        create_object: Async callable to create a new Object from Schema
        create_object_from_value: Async callable to create Object from value

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If table_a does not have array fieldtype
    """
    fieldtype_a = await _get_fieldtype(table_a, ch_client)
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("concat requires first table to have array fieldtype")

    result = await copy_db(table_a, ch_client, create_object)

    temp_obj = await create_object_from_value(value)

    col_type = await _get_value_column_type(result.table, ch_client)

    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, CAST(value AS {col_type}) as value
    FROM {temp_obj.table}
    """
    await ch_client.command(insert_query)

    await ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")

    return result


async def insert_object_db(table_a: str, table_b: str, ch_client) -> None:
    """
    Insert data from one table into another at database level.

    Preserves existing Snowflake IDs. Order is maintained via existing
    Snowflake IDs when data is retrieved.

    Args:
        table_a: Target table name (must have array fieldtype)
        table_b: Source table name
        ch_client: ClickHouse client instance

    Raises:
        ValueError: If table_a does not have array fieldtype
        ValueError: If value types are incompatible
    """
    fieldtype_a = await _get_fieldtype(table_a, ch_client)
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("insert requires target table to have array fieldtype")

    target_value_type = await _get_value_column_type(table_a, ch_client)
    source_value_type = await _get_value_column_type(table_b, ch_client)

    if not _are_types_compatible(target_value_type, source_value_type):
        raise ValueError(
            f"Cannot insert {source_value_type} into {target_value_type}: types are incompatible"
        )

    insert_query = f"""
    INSERT INTO {table_a}
    SELECT aai_id, CAST(value AS {target_value_type}) as value
    FROM {table_b}
    """
    await ch_client.command(insert_query)


async def insert_value_db(
    table_a: str,
    value: ValueType,
    ch_client,
    create_object_from_value: Callable[[ValueType], Awaitable],
) -> None:
    """
    Insert a value into a table at database level.

    Args:
        table_a: Target table name (must have array fieldtype)
        value: Value to insert (scalar or list)
        ch_client: ClickHouse client instance
        create_object_from_value: Async callable to create Object from value

    Raises:
        ValueError: If table_a does not have array fieldtype
        ValueError: If value types are incompatible
    """
    fieldtype_a = await _get_fieldtype(table_a, ch_client)
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("insert requires target table to have array fieldtype")

    if isinstance(value, list) and len(value) == 0:
        return

    temp_obj = await create_object_from_value(value)

    target_value_type = await _get_value_column_type(table_a, ch_client)
    temp_value_type = await _get_value_column_type(temp_obj.table, ch_client)

    if not _are_types_compatible(target_value_type, temp_value_type):
        await ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")
        raise ValueError(
            f"Cannot insert {temp_value_type} into {target_value_type}: types are incompatible"
        )

    insert_query = f"""
    INSERT INTO {table_a}
    SELECT aai_id, CAST(value AS {target_value_type}) as value
    FROM {temp_obj.table}
    """
    await ch_client.command(insert_query)

    await ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")
