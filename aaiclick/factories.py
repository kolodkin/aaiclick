"""
aaiclick.factories - Factory functions for creating Object instances.

This module provides factory functions to create Object instances with ClickHouse tables,
automatically inferring schemas from Python values using numpy for type detection.
"""

from typing import Union, Dict, List, Optional, TYPE_CHECKING
import numpy as np
from .object import Object, ColumnMeta, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY
from .ch_client import get_ch_client
from .snowflake import get_snowflake_ids

if TYPE_CHECKING:
    from .context import Context


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


async def create_object(schema: Schema, context: Optional["Context"] = None) -> Object:
    """
    Create a new Object with a ClickHouse table using the specified schema.

    Args:
        schema: Column definition(s). Can be:
            - str: Single column definition (e.g., "value Float64")
            - list[str]: Multiple column definitions (e.g., ["id Int64", "value Float64"])
        context: Optional Context to register the object with for lifecycle management

    Returns:
        Object: New Object instance with created table

    Examples:
        >>> # Single column
        >>> obj = await create_object("value Float64")
        >>>
        >>> # Multiple columns
        >>> obj = await create_object(["id Int64", "name String", "age UInt8"])
        >>>
        >>> # With context
        >>> async with Context() as ctx:
        ...     obj = await create_object("value Float64", context=ctx)
    """
    obj = Object()
    ch_client = context.ch_client if context else await get_ch_client()

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
    await ch_client.command(create_query)

    # Register with context if provided
    if context:
        context._register_object(obj)

    return obj


async def create_object_from_value(val: ValueType, context: Optional["Context"] = None) -> Object:
    """
    Create a new Object from Python values with automatic schema inference.

    Args:
        val: Value to create object from. Can be:
            - Scalar (int, float, bool, str): Creates "aai_id" and "value" columns, single row
            - List of scalars: Creates "aai_id" and "value" columns with multiple rows
            - Dict of scalars: Creates "aai_id" plus one column per key, single row
            - Dict of arrays: Creates "aai_id" plus one column per key, multiple rows
        context: Optional Context to register the object with for lifecycle management

    Returns:
        Object: New Object instance with data

    Table Schema Details:
        - All tables include aai_id column with snowflake IDs
        - Scalars (single value): Single row with aai_id and value
        - Arrays (lists): Multiple rows with aai_id and value, ordered by aai_id
        - Dict of scalars: Single row with aai_id plus columns for each key
        - Dict of arrays: Multiple rows with aai_id plus columns for each key, ordered by aai_id

    Examples:
        >>> # From scalar (with aai_id)
        >>> obj = await create_object_from_value(42)
        >>> # Creates table with columns: aai_id UInt64, value Int64
        >>>
        >>> # From list (with aai_id)
        >>> obj = await create_object_from_value([1.5, 2.5, 3.5])
        >>> # Creates table with columns: aai_id UInt64, value Float64
        >>>
        >>> # From dict of scalars (with aai_id)
        >>> obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})
        >>> # Creates table with columns: aai_id UInt64, id Int64, name String, age Int64
        >>>
        >>> # From dict of arrays (with aai_id)
        >>> obj = await create_object_from_value({"x": [1, 2], "y": [3, 4]})
        >>> # Creates table with columns: aai_id UInt64, x Int64, y Int64
        >>>
        >>> # With context
        >>> async with Context() as ctx:
        ...     obj = await create_object_from_value([1, 2, 3], context=ctx)
    """
    obj = Object()
    ch_client = context.ch_client if context else await get_ch_client()

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
            await ch_client.command(create_query)

            # Generate snowflake IDs for all rows
            aai_ids = get_snowflake_ids(array_len or 0)

            # Build data rows for bulk insert
            if array_len and array_len > 0:
                keys = list(val.keys())
                # Zip aai_ids with all column arrays to create rows
                data = [list(row) for row in zip(aai_ids, *[val[key] for key in keys])]

                # Use clickhouse-connect's built-in insert
                await ch_client.insert(obj.table, data)

        else:
            # Dict of scalars: one column per key, single row with aai_id
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

            # Add aai_id for consistency
            aai_id_comment = _build_column_comment(FIELDTYPE_SCALAR)
            columns.insert(0, f"aai_id UInt64 COMMENT '{aai_id_comment}'")

            create_query = f"""
            CREATE TABLE {obj.table} (
                {", ".join(columns)}
            ) ENGINE = MergeTree ORDER BY tuple()
            """
            await ch_client.command(create_query)

            # Generate single aai_id for scalar dict
            aai_id = get_snowflake_ids(1)[0]
            values.insert(0, str(aai_id))

            # Insert single row
            insert_query = f"INSERT INTO {obj.table} VALUES ({', '.join(values)})"
            await ch_client.command(insert_query)

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
        await ch_client.command(create_query)

        # Generate snowflake IDs for all rows
        aai_ids = get_snowflake_ids(len(val))

        # Build data rows for bulk insert
        if val:
            # Zip aai_ids with values to create rows
            data = [list(row) for row in zip(aai_ids, val)]
            # Use clickhouse-connect's built-in insert
            await ch_client.insert(obj.table, data)

    else:
        # Scalar: single row with aai_id and value
        col_type = _infer_clickhouse_type(val)
        aai_id_comment = _build_column_comment(FIELDTYPE_SCALAR)
        value_comment = _build_column_comment(FIELDTYPE_SCALAR)

        create_query = f"""
        CREATE TABLE {obj.table} (
            aai_id UInt64 COMMENT '{aai_id_comment}',
            value {col_type} COMMENT '{value_comment}'
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await ch_client.command(create_query)

        # Generate single aai_id for scalar
        aai_id = get_snowflake_ids(1)[0]

        # Insert single row
        if isinstance(val, str):
            value_str = f"'{val}'"
        elif isinstance(val, bool):
            value_str = "1" if val else "0"
        else:
            value_str = str(val)

        insert_query = f"INSERT INTO {obj.table} VALUES ({aai_id}, {value_str})"
        await ch_client.command(insert_query)

    # Register with context if provided
    if context:
        context._register_object(obj)

    return obj
