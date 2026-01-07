"""
aaiclick.ingest - Functions for ingesting and concatenating data.

This module provides functions for copying and concatenating Object instances.
"""

from __future__ import annotations

from typing import Union

from .object import Object
from .models import ColumnMeta, Schema, FIELDTYPE_ARRAY, FIELDTYPE_SCALAR, ValueType
from .sql_template_loader import load_sql_template
from .context import create_object_from_value
from .snowflake import get_snowflake_ids


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


async def _concat_preserve_ids(result: Object, obj_a: Object, obj_b: Object) -> None:
    """
    Concat by preserving original aai_ids from both objects.

    Used when min(obj_b) > max(obj_a), so no ID conflicts exist.
    """
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, value FROM {obj_a.table}
    UNION ALL
    SELECT aai_id, value FROM {obj_b.table}
    ORDER BY aai_id
    """
    await obj_a.ch_client.command(insert_query)


async def _concat_renumber_ids(result: Object, obj_a: Object, obj_b: Object, max_a_id: int) -> None:
    """
    Concat by preserving obj_a IDs but renumbering obj_b IDs using Snowflake IDs.

    Used when there's an ID conflict (min(obj_b) <= max(obj_a)).
    """
    # Get count of rows in obj_b
    count_query = f"SELECT count(*) FROM {obj_b.table}"
    count_result = await obj_a.ch_client.query(count_query)
    count_b = count_result.result_rows[0][0]

    # Generate Snowflake IDs for obj_b rows
    new_ids = get_snowflake_ids(count_b)

    # Create temporary table for new ID mappings
    temp_table = f"temp_ids_{result.table}"
    create_temp = f"""
    CREATE TABLE {temp_table} (
        row_num UInt64,
        new_id UInt64
    ) ENGINE = Memory
    """
    await obj_a.ch_client.command(create_temp)

    # Insert ID mappings (row_num -> new_snowflake_id)
    if new_ids:
        data = [[i+1, id_val] for i, id_val in enumerate(new_ids)]
        await obj_a.ch_client.insert(temp_table, data)

    # Insert data with preserved obj_a IDs and renumbered obj_b IDs
    insert_query = f"""
    INSERT INTO {result.table}
    SELECT aai_id, value FROM {obj_a.table}
    UNION ALL
    SELECT t.new_id as aai_id, b.value
    FROM (
        SELECT row_number() OVER (ORDER BY aai_id) as row_num, value
        FROM {obj_b.table}
    ) b
    JOIN {temp_table} t ON b.row_num = t.row_num
    ORDER BY aai_id
    """
    await obj_a.ch_client.command(insert_query)

    # Clean up temporary table
    await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")


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

    # Get max aai_id from obj_a to check for ID conflicts
    max_a_query = f"SELECT max(aai_id) FROM {obj_a.table}"
    max_a_result = await obj_a.ch_client.query(max_a_query)
    max_a_id = max_a_result.result_rows[0][0] or 0

    # Get min aai_id from obj_b
    min_b_query = f"SELECT min(aai_id) FROM {obj_b.table}"
    min_b_result = await obj_a.ch_client.query(min_b_query)
    min_b_id = min_b_result.result_rows[0][0]

    # Dispatch based on ID conflict check
    if min_b_id is not None and min_b_id <= max_a_id:
        # ID conflict: renumber obj_b
        await _concat_renumber_ids(result, obj_a, obj_b, max_a_id)
    else:
        # No conflict: preserve IDs
        await _concat_preserve_ids(result, obj_a, obj_b)

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


async def _insert_renumber_ids(obj_a: Object, obj_b: Object, target_value_type: str) -> None:
    """
    Insert obj_b into obj_a with Snowflake ID renumbering.

    Always renumbers obj_b's IDs using Snowflake ID generation.
    """
    # Get count of rows in obj_b
    count_query = f"SELECT count(*) FROM {obj_b.table}"
    count_result = await obj_a.ch_client.query(count_query)
    count_b = count_result.result_rows[0][0]

    # Generate Snowflake IDs for obj_b rows
    new_ids = get_snowflake_ids(count_b)

    # Create temporary table for new ID mappings
    temp_table = f"temp_ids_{obj_a.table}_{id(obj_b)}"
    create_temp = f"""
    CREATE TABLE {temp_table} (
        row_num UInt64,
        new_id UInt64
    ) ENGINE = Memory
    """
    await obj_a.ch_client.command(create_temp)

    # Insert ID mappings (row_num -> new_snowflake_id)
    if new_ids:
        data = [[i+1, id_val] for i, id_val in enumerate(new_ids)]
        await obj_a.ch_client.insert(temp_table, data)

    # Insert data with new Snowflake IDs
    insert_query = f"""
    INSERT INTO {obj_a.table}
    SELECT t.new_id as aai_id, CAST(b.value AS {target_value_type}) as value
    FROM (
        SELECT row_number() OVER (ORDER BY aai_id) as row_num, value
        FROM {obj_b.table}
    ) b
    JOIN {temp_table} t ON b.row_num = t.row_num
    """
    await obj_a.ch_client.command(insert_query)

    # Clean up temporary table
    await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")


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

    # Always renumber for insert using Snowflake IDs
    await _insert_renumber_ids(obj_a, obj_b, target_value_type)


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

    # Get count of rows in temp object
    count_query = f"SELECT count(*) FROM {temp_obj.table}"
    count_result = await obj_a.ch_client.query(count_query)
    count_temp = count_result.result_rows[0][0]

    # Insert the values into obj_a table using Snowflake IDs
    if count_temp > 0:
        # Generate Snowflake IDs for temp rows
        new_ids = get_snowflake_ids(count_temp)

        # Create temporary table for new ID mappings
        temp_id_table = f"temp_ids_{obj_a.table}_{id(temp_obj)}"
        create_temp = f"""
        CREATE TABLE {temp_id_table} (
            row_num UInt64,
            new_id UInt64
        ) ENGINE = Memory
        """
        await obj_a.ch_client.command(create_temp)

        # Insert ID mappings (row_num -> new_snowflake_id)
        data = [[i+1, id_val] for i, id_val in enumerate(new_ids)]
        await obj_a.ch_client.insert(temp_id_table, data)

        # Insert with type casting and Snowflake IDs
        insert_select_query = f"""
        INSERT INTO {obj_a.table}
        SELECT t.new_id as aai_id, CAST(v.value AS {target_value_type}) as value
        FROM (
            SELECT row_number() OVER (ORDER BY aai_id) as row_num, value
            FROM {temp_obj.table}
        ) v
        JOIN {temp_id_table} t ON v.row_num = t.row_num
        """
        await obj_a.ch_client.command(insert_select_query)

        # Clean up ID mapping table
        await obj_a.ch_client.command(f"DROP TABLE IF EXISTS {temp_id_table}")

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
