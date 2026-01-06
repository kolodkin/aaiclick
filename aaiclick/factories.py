"""
aaiclick.factories - Factory functions for creating Object instances.

This module provides database-level factory functions to create Object instances with ClickHouse tables,
automatically inferring schemas from Python values using numpy for type detection.

Note: These functions are internal and should only be called via Context methods.
Each factory function takes ch_client and ctx parameters instead of using Object instances.
"""

from __future__ import annotations

from typing import Union, Dict, List

import numpy as np

from .models import Schema, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY
from .snowflake import get_snowflake_ids


# Type aliases
ValueScalarType = Union[int, float, bool, str]
ValueListType = Union[List[int], List[float], List[bool], List[str]]
ValueType = Union[ValueScalarType, ValueListType, Dict[str, Union[ValueScalarType, ValueListType]]]


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


async def create_object_from_value(val, ch_client, ctx):
    """
    Create a new Object from Python values with automatic schema inference at database level.

    Internal function - use Context.create_object_from_value() instead.

    Args:
        val: Value to create object from. Can be:
            - Scalar (int, float, bool, str): Creates "aai_id" and "value" columns, single row
            - List of scalars: Creates "aai_id" and "value" columns with multiple rows
            - Dict of scalars: Creates "aai_id" plus one column per key, single row
            - Dict of arrays: Creates "aai_id" plus one column per key, multiple rows
        ch_client: ClickHouse client instance
        ctx: Context instance for creating result object

    Returns:
        New Object instance with data

    Table Schema Details:
        - All tables include aai_id column with snowflake IDs
        - Scalars (single value): Single row with aai_id and value
        - Arrays (lists): Multiple rows with aai_id and value, ordered by aai_id
        - Dict of scalars: Single row with aai_id plus columns for each key
        - Dict of arrays: Multiple rows with aai_id plus columns for each key, ordered by aai_id
    """
    if isinstance(val, dict):
        # Check if any values are lists (dict of arrays)
        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays:
            # Dict of arrays: one column per key, one row per array element
            # All arrays must have the same length
            columns = {"aai_id": "UInt64"}
            array_len = None

            # First pass: build schema and validate array lengths
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
                columns[key] = col_type

            schema = Schema(
                fieldtype=FIELDTYPE_ARRAY,
                columns=columns
            )

            # Create object with schema
            obj = await ctx.create_object(schema)

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
            columns = {"aai_id": "UInt64"}
            values = []

            for key, value in val.items():
                col_type = _infer_clickhouse_type(value)
                columns[key] = col_type

                # Format value for SQL
                if isinstance(value, str):
                    values.append(f"'{value}'")
                elif isinstance(value, bool):
                    values.append("1" if value else "0")
                else:
                    values.append(str(value))

            schema = Schema(
                fieldtype=FIELDTYPE_SCALAR,
                columns=columns
            )

            # Create object with schema
            obj = await ctx.create_object(schema)

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

        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": col_type}
        )

        # Create object with schema
        obj = await ctx.create_object(schema)

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

        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": "UInt64", "value": col_type}
        )

        # Create object with schema
        obj = await ctx.create_object(schema)

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

    return obj
