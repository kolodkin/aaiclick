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
    Only works with array fieldtype objects (objects with aai_id column).

    Args:
        obj_a: First Object (must have array fieldtype)
        obj_b: Second Object to concatenate

    Returns:
        Object: New Object instance with concatenated data

    Raises:
        ValueError: If obj_a does not have array fieldtype

    Examples:
        >>> obj_a = await create_object_from_value([1, 2, 3])
        >>> obj_b = await create_object_from_value([4, 5, 6])
        >>> result = await concat(obj_a, obj_b)
        >>> await result.data()  # Returns [1, 2, 3, 4, 5, 6]
    """
    # Check that obj_a has array fieldtype
    has_aai_id = await obj_a._has_aai_id()
    if not has_aai_id:
        raise ValueError("concat requires obj_a to have array fieldtype (aai_id column)")

    fieldtype = await obj_a._get_fieldtype()
    if fieldtype != FIELDTYPE_ARRAY:
        raise ValueError("concat requires obj_a to have array fieldtype")

    # Create result object
    result = Object()
    client = await get_client()

    # Use concat_array template
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
