"""
aaiclick.ingest - Functions for ingesting and concatenating data.

This module provides functions for copying and concatenating Object instances.
"""

from __future__ import annotations

from typing import Union

from .object import Object, ColumnMeta, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR
from .sql_template_loader import load_sql_template
from .factories import ValueType


async def copy(obj: Object) -> Object:
    """
    Copy an object to a new object and table.

    Creates a new Object with a copy of all data from the source object.
    Preserves all column metadata including fieldtype.

    Args:
        obj: Object to copy

    Returns:
        Object: New Object instance with copied data

    Examples:
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_copy = await copy(obj_a)
        >>> await obj_copy.data()  # Returns [1, 2, 3]
    """
    result = Object(obj._ctx)

    # Use copy template
    template = load_sql_template("copy")
    create_query = template.format(
        result_table=result.table,
        source_table=obj.table
    )
    await obj._ctx.ch_client.command(create_query)

    # Query column metadata from source table
    columns_query = f"""
    SELECT name, comment
    FROM system.columns
    WHERE table = '{obj.table}'
    ORDER BY position
    """
    columns_result = await obj._ctx.ch_client.query(columns_query)

    # Copy comments to preserve fieldtype metadata
    for name, comment in columns_result.result_rows:
        if comment:  # Only add comment if it exists
            await obj._ctx.ch_client.command(
                f"ALTER TABLE {result.table} COMMENT COLUMN {name} '{comment}'"
            )

    return result


async def _concat_object_to_object(obj_a: Object, obj_b: Object) -> Object:
    """
    Concatenate two objects together.

    Helper function for concat when both arguments are Objects.

    Args:
        obj_a: First Object (must have array fieldtype)
        obj_b: Second Object (array or scalar)

    Returns:
        Object: New Object instance with concatenated data
    """
    result = Object(obj_a._ctx)

    # Both scalars and arrays have aai_id now, so use same template
    template = load_sql_template("concat_array")
    create_query = template.format(
        result_table=result.table,
        left_table=obj_a.table,
        right_table=obj_b.table
    )
    await obj_a._ctx.ch_client.command(create_query)

    # Add comments to preserve fieldtype metadata
    aai_id_comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
    value_comment = ColumnMeta(fieldtype=FIELDTYPE_ARRAY).to_yaml()
    await obj_a._ctx.ch_client.command(f"ALTER TABLE {result.table} COMMENT COLUMN aai_id '{aai_id_comment}'")
    await obj_a._ctx.ch_client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{value_comment}'")

    return result


async def _concat_value_to_object(obj_a: Object, value: ValueType) -> Object:
    """
    Concatenate a value to an object.

    Helper function for concat when second argument is a ValueType.
    First copies obj_a, then inserts the value.

    Args:
        obj_a: First Object (must have array fieldtype)
        value: Value to append (scalar or list)

    Returns:
        Object: New Object instance with concatenated data
    """
    from .factories import create_object_from_value

    # First copy obj_a
    result = await copy(obj_a)

    # Create a temporary object from the value
    temp_obj = await create_object_from_value(value, obj_a._ctx)

    # Get the data from temp object
    temp_data_query = f"SELECT value FROM {temp_obj.table}"
    temp_result = await obj_a._ctx.ch_client.query(temp_data_query)

    # Insert the values into result table
    if temp_result.result_rows:
        # Get column type from result table
        type_query = f"""
        SELECT type FROM system.columns
        WHERE table = '{result.table}' AND name = 'value'
        """
        type_result = await obj_a._ctx.ch_client.query(type_query)
        col_type = type_result.result_rows[0][0] if type_result.result_rows else "String"

        # Get next aai_id
        max_id_query = f"SELECT max(aai_id) FROM {result.table}"
        max_id_result = await obj_a._ctx.ch_client.query(max_id_query)
        next_id = (max_id_result.result_rows[0][0] or 0) + 1

        # Build insert data
        data = []
        for row in temp_result.result_rows:
            data.append([next_id, row[0]])
            next_id += 1

        # Use clickhouse-connect's built-in insert
        await obj_a._ctx.ch_client.insert(result.table, data)

    # Delete the temporary object
    await obj_a._ctx.ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")

    return result


async def concat(obj_a: Object, obj_b: Union[Object, ValueType]) -> Object:
    """
    Concatenate an object with another object or value.

    Creates a new Object with rows from obj_a followed by rows/values from obj_b.
    obj_a must have array fieldtype. obj_b can be:
    - An Object (array or scalar)
    - A ValueType (scalar or list)

    When obj_b is a ValueType, the function first copies obj_a, then inserts the value(s).

    Args:
        obj_a: First Object (must have array fieldtype)
        obj_b: Second Object or value to concatenate

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If obj_a does not have array fieldtype

    Examples:
        >>> # Concatenate two Object arrays
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value([4, 5, 6])
        >>> result = await concat(obj_a, obj_b)
        >>> await result.data()  # Returns [1, 2, 3, 4, 5, 6]
        >>>
        >>> # Append Object scalar to array
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value(42)
        >>> result = await concat(obj_a, obj_b)
        >>> await result.data()  # Returns [1, 2, 3, 42]
        >>>
        >>> # Append scalar value to array
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> result = await concat(obj_a, 42)
        >>> await result.data()  # Returns [1, 2, 3, 42]
        >>>
        >>> # Append list values to array
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> result = await concat(obj_a, [4, 5])
        >>> await result.data()  # Returns [1, 2, 3, 4, 5]
    """
    # Check that obj_a has array fieldtype
    fieldtype_a = await obj_a._get_fieldtype()
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("concat requires obj_a to have array fieldtype")

    # Dispatch to appropriate helper function based on obj_b type
    if isinstance(obj_b, Object):
        return await _concat_object_to_object(obj_a, obj_b)
    else:
        return await _concat_value_to_object(obj_a, obj_b)
