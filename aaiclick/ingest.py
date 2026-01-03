"""
aaiclick.ingest - Functions for ingesting and concatenating data.

This module provides functions for concatenating Object instances.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .object import Object

from .object import Object, ColumnMeta, FIELDTYPE_ARRAY
from .client import get_client
from .sql_template_loader import load_sql_template


async def concat(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Concatenate two objects together.

    Creates a new Object with rows from obj_a followed by rows from obj_b.
    obj_a must be an array. obj_b can be an array or scalar.

    Args:
        obj_a: First Object (must have array fieldtype)
        obj_b: Second Object to concatenate (array or scalar)

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If obj_a does not have array fieldtype

    Examples:
        >>> # Concatenate two arrays
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value([4, 5, 6])
        >>> result = await concat(obj_a, obj_b)
        >>> await result.data()  # Returns [1, 2, 3, 4, 5, 6]
        >>>
        >>> # Append scalar to array
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value(42)
        >>> result = await concat(obj_a, obj_b)
        >>> await result.data()  # Returns [1, 2, 3, 42]
    """
    # Check that obj_a has array fieldtype
    fieldtype_a = await obj_a._get_fieldtype()
    if fieldtype_a != FIELDTYPE_ARRAY:
        raise ValueError("concat requires obj_a to have array fieldtype")

    # Create result object
    client = await get_client()
    result = Object()

    # Both scalars and arrays have aai_id now, so use same template
    template = load_sql_template("concat_array")
    create_query = template.format(
        result_table=result.table,
        left_table=obj_a.table,
        right_table=obj_b.table
    )
    await client.command(create_query)

    # Add comments to preserve fieldtype metadata
    from .object import FIELDTYPE_SCALAR
    aai_id_comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
    value_comment = ColumnMeta(fieldtype=FIELDTYPE_ARRAY).to_yaml()
    await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN aai_id '{aai_id_comment}'")
    await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{value_comment}'")

    return result
