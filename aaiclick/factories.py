"""
aaiclick.factories - Factory functions for creating Object instances.

This module provides factory functions to create Object instances with ClickHouse tables,
automatically inferring schemas from Python values using numpy for type detection.
"""

from typing import Union, Dict, List
import numpy as np
from .object import Object
from .client import get_client
from .aai_dtypes import ColumnMeta, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY


# Type aliases
ValueScalarType = Union[int, float, bool, str]
ValueListType = Union[List[int], List[float], List[bool], List[str]]
ValueType = Union[ValueScalarType, ValueListType, Dict[str, Union[ValueScalarType, ValueListType]]]
Schema = Union[str, List[str]]


def _infer_clickhouse_type(value: Union[ValueScalarType, ValueListType]) -> str:
    """
    Infer ClickHouse column type from Python value using numpy.

    Args:
        value: Python value (scalar or list)

    Returns:
        str: ClickHouse type string
    """
    if isinstance(value, list):
        if not value:
            return "String"  # Default for empty list

        # Use numpy to infer the dtype
        arr = np.array(value)
        dtype = arr.dtype

        # Map numpy dtype to ClickHouse type
        if np.issubdtype(dtype, np.bool_):
            return "UInt8"
        elif np.issubdtype(dtype, np.integer):
            return "Int64"
        elif np.issubdtype(dtype, np.floating):
            return "Float64"
        else:
            return "String"

    # Scalar value type inference
    if isinstance(value, bool):
        return "UInt8"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif isinstance(value, str):
        return "String"
    else:
        return "String"  # Default fallback


def _build_column_comment(fieldtype: str) -> str:
    """
    Build a YAML column comment with fieldtype.

    Args:
        fieldtype: 's' for scalar, 'a' for array

    Returns:
        str: YAML comment string
    """
    meta = ColumnMeta(fieldtype=fieldtype)
    return meta.to_yaml()


async def create_object(schema: Schema) -> Object:
    """
    Create a new Object with a ClickHouse table using the specified schema.

    Args:
        schema: Column definition(s). Can be:
            - str: Single column definition (e.g., "value Float64")
            - list[str]: Multiple column definitions (e.g., ["id Int64", "value Float64"])

    Returns:
        Object: New Object instance with created table

    Examples:
        >>> # Single column
        >>> obj = await create_object("value Float64")
        >>>
        >>> # Multiple columns
        >>> obj = await create_object(["id Int64", "name String", "age UInt8"])
    """
    obj = Object()
    client = await get_client()

    # Convert schema to column definitions
    if isinstance(schema, str):
        columns = schema
    else:
        columns = ", ".join(schema)

    create_query = f"""
    CREATE TABLE {obj.table} (
        {columns}
    ) ENGINE = MergeTree ORDER BY tuple()
    """
    await client.command(create_query)
    return obj


async def create_object_from_value(val: ValueType) -> Object:
    """
    Create a new Object from Python values with automatic schema inference.

    Args:
        val: Value to create object from. Can be:
            - Scalar (int, float, bool, str): Creates single column "value"
            - List of scalars: Creates single column "value" with multiple rows
            - Dict: Creates one column per key, single row

    Returns:
        Object: New Object instance with data

    Examples:
        >>> # From scalar
        >>> obj = await create_object_from_value(42)
        >>> # Creates table with column: value Int64
        >>>
        >>> # From list
        >>> obj = await create_object_from_value([1.5, 2.5, 3.5])
        >>> # Creates table with column: value Float64 and 3 rows
        >>>
        >>> # From dict
        >>> obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})
        >>> # Creates table with columns: id Int64, name String, age Int64
    """
    obj = Object()
    client = await get_client()

    if isinstance(val, dict):
        # Dict: one column per key
        columns = []
        values = []

        for key, value in val.items():
            col_type = _infer_clickhouse_type(value)
            # Determine fieldtype: 'a' for list/array, 's' for scalar
            fieldtype = FIELDTYPE_ARRAY if isinstance(value, list) else FIELDTYPE_SCALAR
            comment = _build_column_comment(fieldtype)
            columns.append(f"{key} {col_type} COMMENT '{comment}'")

            # Format value for SQL
            if isinstance(value, str):
                values.append(f"'{value}'")
            elif isinstance(value, bool):
                values.append("1" if value else "0")
            else:
                values.append(str(value))

        create_query = f"""
        CREATE TABLE {obj.table} (
            {", ".join(columns)}
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await client.command(create_query)

        # Insert single row
        insert_query = f"INSERT INTO {obj.table} VALUES ({', '.join(values)})"
        await client.command(insert_query)

    elif isinstance(val, list):
        # List: single column "value" with multiple rows
        # Add row_id column to ensure stable ordering for element-wise operations
        col_type = _infer_clickhouse_type(val)
        row_id_comment = _build_column_comment(FIELDTYPE_SCALAR)
        value_comment = _build_column_comment(FIELDTYPE_ARRAY)

        create_query = f"""
        CREATE TABLE {obj.table} (
            row_id UInt64 COMMENT '{row_id_comment}',
            value {col_type} COMMENT '{value_comment}'
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await client.command(create_query)

        # Insert multiple rows with explicit row IDs
        for idx, item in enumerate(val):
            if isinstance(item, str):
                value_str = f"'{item}'"
            elif isinstance(item, bool):
                value_str = "1" if item else "0"
            else:
                value_str = str(item)

            insert_query = f"INSERT INTO {obj.table} VALUES ({idx}, {value_str})"
            await client.command(insert_query)

    else:
        # Scalar: single column "value" with single row
        col_type = _infer_clickhouse_type(val)
        value_comment = _build_column_comment(FIELDTYPE_SCALAR)

        create_query = f"""
        CREATE TABLE {obj.table} (
            value {col_type} COMMENT '{value_comment}'
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await client.command(create_query)

        # Insert single row
        if isinstance(val, str):
            value_str = f"'{val}'"
        elif isinstance(val, bool):
            value_str = "1" if val else "0"
        else:
            value_str = str(val)

        insert_query = f"INSERT INTO {obj.table} VALUES ({value_str})"
        await client.command(insert_query)

    return obj
