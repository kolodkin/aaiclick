"""
aaiclick.factories - Factory functions for creating Object instances.

This module provides factory functions to create Object instances with ClickHouse tables,
automatically inferring schemas from Python values using numpy for type detection.
"""

from typing import Union, Dict, List
import numpy as np
from .object import Object, ColumnMeta, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY
from .client import get_client
from .snowflake import generate_snowflake_ids


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
            - Scalar (int, float, bool, str): Creates single column "value" (no aai_id)
            - List of scalars: Creates "aai_id" and "value" columns with multiple rows
            - Dict of scalars: Creates one column per key, single row (no aai_id)
            - Dict of arrays: Creates "aai_id" plus one column per key, multiple rows

    Returns:
        Object: New Object instance with data

    Table Schema Details:
        - Scalars (single value): No aai_id column - single row tables don't need ordering
        - Arrays (lists): Includes aai_id column with snowflake IDs for guaranteed insertion order
        - Dict of scalars: No aai_id column - single row tables don't need ordering
        - Dict of arrays: Includes aai_id column with snowflake IDs for guaranteed insertion order

    Examples:
        >>> # From scalar (no aai_id)
        >>> obj = await create_object_from_value(42)
        >>> # Creates table with column: value Int64
        >>>
        >>> # From list (with aai_id)
        >>> obj = await create_object_from_value([1.5, 2.5, 3.5])
        >>> # Creates table with columns: aai_id UInt64, value Float64
        >>>
        >>> # From dict of scalars (no aai_id)
        >>> obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})
        >>> # Creates table with columns: id Int64, name String, age Int64
        >>>
        >>> # From dict of arrays (with aai_id)
        >>> obj = await create_object_from_value({"x": [1, 2], "y": [3, 4]})
        >>> # Creates table with columns: aai_id UInt64, x Int64, y Int64
    """
    obj = Object()
    client = await get_client()

    if isinstance(val, dict):
        # Check if any values are lists (dict of arrays)
        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays:
            # Dict of arrays: one column per key, one row per array element
            # All arrays must have the same length
            columns = []
            array_len = None

            # First pass: build columns and validate array lengths
            for key, value in val.items():
                if isinstance(value, list):
                    if array_len is None:
                        array_len = len(value)
                    elif len(value) != array_len:
                        raise ValueError(
                            f"All arrays must have same length. "
                            f"Expected {array_len}, got {len(value)} for key '{key}'"
                        )
                    col_type = _infer_clickhouse_type(value)
                else:
                    raise ValueError(
                        f"Dict of arrays requires all values to be lists. "
                        f"Key '{key}' has type {type(value).__name__}"
                    )
                comment = _build_column_comment(FIELDTYPE_ARRAY)
                columns.append(f"{key} {col_type} COMMENT '{comment}'")

            # Add aai_id for ordering
            aai_id_comment = _build_column_comment(FIELDTYPE_SCALAR)
            columns.insert(0, f"aai_id UInt64 COMMENT '{aai_id_comment}'")

            create_query = f"""
            CREATE TABLE {obj.table} (
                {", ".join(columns)}
            ) ENGINE = MergeTree ORDER BY tuple()
            """
            await client.command(create_query)

            # Generate snowflake IDs for all rows
            aai_ids = generate_snowflake_ids(array_len or 0)

            # Insert rows
            keys = list(val.keys())
            for idx in range(array_len or 0):
                row_values = [str(aai_ids[idx])]
                for key in keys:
                    item = val[key][idx]
                    if isinstance(item, str):
                        row_values.append(f"'{item}'")
                    elif isinstance(item, bool):
                        row_values.append("1" if item else "0")
                    else:
                        row_values.append(str(item))

                insert_query = f"INSERT INTO {obj.table} VALUES ({', '.join(row_values)})"
                await client.command(insert_query)

        else:
            # Dict of scalars: one column per key, single row
            columns = []
            values = []

            for key, value in val.items():
                col_type = _infer_clickhouse_type(value)
                comment = _build_column_comment(FIELDTYPE_SCALAR)
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
        # Add aai_id column to ensure stable ordering for element-wise operations
        col_type = _infer_clickhouse_type(val)
        aai_id_comment = _build_column_comment(FIELDTYPE_SCALAR)
        value_comment = _build_column_comment(FIELDTYPE_ARRAY)

        create_query = f"""
        CREATE TABLE {obj.table} (
            aai_id UInt64 COMMENT '{aai_id_comment}',
            value {col_type} COMMENT '{value_comment}'
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await client.command(create_query)

        # Generate snowflake IDs for all rows
        aai_ids = generate_snowflake_ids(len(val))

        # Insert multiple rows with snowflake row IDs
        for idx, item in enumerate(val):
            if isinstance(item, str):
                value_str = f"'{item}'"
            elif isinstance(item, bool):
                value_str = "1" if item else "0"
            else:
                value_str = str(item)

            insert_query = f"INSERT INTO {obj.table} VALUES ({aai_ids[idx]}, {value_str})"
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
