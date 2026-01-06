"""
aaiclick.ingest - Functions for ingesting and concatenating data.

This module provides functions for copying and concatenating Object instances.
"""

from __future__ import annotations

from typing import Union

from .object import Object
from .models import ColumnMeta, Schema, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR
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
    # Query column metadata from source table
    columns_query = f"""
    SELECT name, type, comment
    FROM system.columns
    WHERE table = '{obj.table}'
    ORDER BY position
    """
    columns_result = await obj.ch_client.query(columns_query)

    # Build schema from source table metadata
    columns = {}
    fieldtype = FIELDTYPE_SCALAR  # Default
    for name, col_type, comment in columns_result.result_rows:
        columns[name] = col_type
        # Extract fieldtype from value column comment
        if name == "value" and comment:
            meta = ColumnMeta.from_yaml(comment)
            if meta.fieldtype:
                fieldtype = meta.fieldtype

    schema = Schema(fieldtype=fieldtype, columns=columns)

    # Create result object with schema (registered for automatic cleanup)
    result = await obj.ctx.create_object(schema)

    # Insert data from source table
    insert_query = f"INSERT INTO {result.table} SELECT * FROM {obj.table}"
    await obj.ch_client.command(insert_query)

    return result


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
    # Exact match is always compatible
    if target_type == source_type:
        return True

    # Define numeric type families
    int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
    float_types = {"Float32", "Float64"}
    numeric_types = int_types | float_types

    # Check if both are numeric types (compatible)
    if target_type in numeric_types and source_type in numeric_types:
        return True

    # String types
    string_types = {"String", "FixedString"}

    # Numeric and string types are incompatible
    if (target_type in numeric_types and source_type in string_types) or \
       (target_type in string_types and source_type in numeric_types):
        return False

    # For other types, be conservative and reject
    return False


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
    # Get value column type from first object
    type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{obj_a.table}' AND name = 'value'
    """
    type_result = await obj_a.ch_client.query(type_query)
    value_type = type_result.result_rows[0][0] if type_result.result_rows else "Float64"

    # Build schema for result (concat always produces array)
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    # Create result object with schema (registered for automatic cleanup)
    result = await obj_a.ctx.create_object(schema)

    # Insert concatenated data preserving original Snowflake IDs
    # Snowflake IDs have timestamps, so obj_b's IDs will naturally be >= obj_a's IDs
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, value FROM {obj_a.table}
    UNION ALL
    SELECT aai_id, value FROM {obj_b.table}
    ORDER BY aai_id
    """
    await obj_a.ch_client.command(insert_query)

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
    temp_obj = await create_object_from_value(value, obj_a.ctx)

    # Get the data from temp object
    temp_data_query = f"SELECT value FROM {temp_obj.table}"
    temp_result = await obj_a.ch_client.query(temp_data_query)

    # Insert the values into result table
    if temp_result.result_rows:
        # Get column type from result table
        type_query = f"""
        SELECT type FROM system.columns
        WHERE table = '{result.table}' AND name = 'value'
        """
        type_result = await obj_a.ch_client.query(type_query)
        col_type = type_result.result_rows[0][0] if type_result.result_rows else "String"

        # Get next aai_id
        max_id_query = f"SELECT max(aai_id) FROM {result.table}"
        max_id_result = await obj_a.ch_client.query(max_id_query)
        next_id = (max_id_result.result_rows[0][0] or 0) + 1

        # Build insert data
        data = []
        for row in temp_result.result_rows:
            data.append([next_id, row[0]])
            next_id += 1

        # Use clickhouse-connect's built-in insert
        await obj_a.ch_client.insert(result.table, data)

    # Delete the temporary object
    await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")

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


async def _insert_object_to_object(obj_a: Object, obj_b: Object) -> None:
    """
    Insert data from obj_b into obj_a in place.

    Helper function for insert when both arguments are Objects.

    Args:
        obj_a: Target Object (must have array fieldtype)
        obj_b: Source Object (array or scalar)

    Raises:
        ValueError: If value types are incompatible
    """
    # Get the value type from target object
    target_type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{obj_a.table}' AND name = 'value'
    """
    target_type_result = await obj_a.ch_client.query(target_type_query)
    target_value_type = target_type_result.result_rows[0][0] if target_type_result.result_rows else "String"

    # Get the value type from source object
    source_type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{obj_b.table}' AND name = 'value'
    """
    source_type_result = await obj_a.ch_client.query(source_type_query)
    source_value_type = source_type_result.result_rows[0][0] if source_type_result.result_rows else "String"

    # Validate type compatibility
    if not _are_types_compatible(target_value_type, source_value_type):
        raise ValueError(
            f"Cannot insert {source_value_type} into {target_value_type}: types are incompatible"
        )

    # Get max aai_id from target table
    max_id_query = f"SELECT max(aai_id) FROM {obj_a.table}"
    max_id_result = await obj_a.ch_client.query(max_id_query)
    next_id = (max_id_result.result_rows[0][0] or 0) + 1

    # Insert data from obj_b with renumbered aai_ids
    insert_query = f"""
    INSERT INTO {obj_a.table}
    SELECT row_number() OVER (ORDER BY aai_id) + {next_id - 1} as aai_id,
           CAST(value AS {target_value_type}) as value
    FROM {obj_b.table}
    """
    await obj_a.ch_client.command(insert_query)


async def _insert_value_to_object(obj_a: Object, value: ValueType) -> None:
    """
    Insert a value into obj_a in place.

    Helper function for insert when second argument is a ValueType.

    Args:
        obj_a: Target Object (must have array fieldtype)
        value: Value to insert (scalar or list)

    Raises:
        ValueError: If value types are incompatible
    """
    from .factories import create_object_from_value

    # Handle empty list as no-op
    if isinstance(value, list) and len(value) == 0:
        return

    # Create a temporary object from the value
    temp_obj = await create_object_from_value(value, obj_a.ctx)

    # Get the value type from temp object
    temp_type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{temp_obj.table}' AND name = 'value'
    """
    temp_type_result = await obj_a.ch_client.query(temp_type_query)
    temp_value_type = temp_type_result.result_rows[0][0] if temp_type_result.result_rows else "String"

    # Get the value type from target object
    target_type_query = f"""
    SELECT type FROM system.columns
    WHERE table = '{obj_a.table}' AND name = 'value'
    """
    target_type_result = await obj_a.ch_client.query(target_type_query)
    target_value_type = target_type_result.result_rows[0][0] if target_type_result.result_rows else "String"

    # Validate type compatibility
    if not _are_types_compatible(target_value_type, temp_value_type):
        # Delete the temporary object before raising error
        await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")
        raise ValueError(
            f"Cannot insert {temp_value_type} into {target_value_type}: types are incompatible"
        )

    # Get the data from temp object
    temp_data_query = f"SELECT value FROM {temp_obj.table}"
    temp_result = await obj_a.ch_client.query(temp_data_query)

    # Insert the values into obj_a table
    if temp_result.result_rows:
        # Get next aai_id
        max_id_query = f"SELECT max(aai_id) FROM {obj_a.table}"
        max_id_result = await obj_a.ch_client.query(max_id_query)
        next_id = (max_id_result.result_rows[0][0] or 0) + 1

        # Insert with type casting
        insert_select_query = f"""
        INSERT INTO {obj_a.table}
        SELECT {next_id} + row_number() OVER (ORDER BY aai_id) - 1 as aai_id,
               CAST(value AS {target_value_type}) as value
        FROM {temp_obj.table}
        """
        await obj_a.ch_client.command(insert_select_query)

    # Delete the temporary object
    await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_obj.table}")


async def insert(obj_a: Object, obj_b: Union[Object, ValueType]) -> None:
    """
    Insert data from obj_b into obj_a in place (modifying obj_a's table).

    obj_a must have array fieldtype. obj_b can be:
    - An Object (array or scalar)
    - A ValueType (scalar or list)

    Unlike concat, this function modifies obj_a's table directly without creating a new object.

    Args:
        obj_a: Target Object to insert into (must have array fieldtype)
        obj_b: Source Object or value to insert

    Raises:
        ValueError: If obj_a does not have array fieldtype

    Examples:
        >>> # Insert Object into another Object
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value([4, 5, 6])
        >>> await insert(obj_a, obj_b)
        >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5, 6]
        >>>
        >>> # Insert scalar value
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> await insert(obj_a, 42)
        >>> await obj_a.data()  # Returns [1, 2, 3, 42]
        >>>
        >>> # Insert list of values
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> await insert(obj_a, [4, 5])
        >>> await obj_a.data()  # Returns [1, 2, 3, 4, 5]
    """
    # Check that obj_a has array fieldtype
    fieldtype_a = await obj_a._get_fieldtype()
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("insert requires obj_a to have array fieldtype")

    # Dispatch to appropriate helper function based on obj_b type
    if isinstance(obj_b, Object):
        await _insert_object_to_object(obj_a, obj_b)
    else:
        await _insert_value_to_object(obj_a, obj_b)
