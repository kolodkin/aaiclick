"""
aaiclick.operators - Operator implementations for Object class.

This module contains static functions that implement all operators for Object instances.
Each operator function takes two Object parameters and returns a new Object with the result.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .object import Object

from .models import Schema, ColumnMeta, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY


# Operator to SQL expression mapping
OPERATOR_EXPRESSIONS = {
    # Arithmetic
    "+": "a.value + b.value",
    "-": "a.value - b.value",
    "*": "a.value * b.value",
    "/": "a.value / b.value",
    "//": "intDiv(a.value, b.value)",
    "%": "a.value % b.value",
    "**": "power(a.value, b.value)",
    # Comparison
    "==": "a.value = b.value",
    "!=": "a.value != b.value",
    "<": "a.value < b.value",
    "<=": "a.value <= b.value",
    ">": "a.value > b.value",
    ">=": "a.value >= b.value",
    # Bitwise
    "&": "bitAnd(a.value, b.value)",
    "|": "bitOr(a.value, b.value)",
    "^": "bitXor(a.value, b.value)",
}


async def _apply_operator_db(table_a: str, table_b: str, operator: str, ch_client, ctx) -> Object:
    """
    Apply an operator on two tables at the database level.

    Args:
        table_a: First table name
        table_b: Second table name
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance
        ctx: Context instance for creating result object

    Returns:
        Object: New Object instance pointing to result table
    """
    from .object import Object

    # Get SQL expression from operator mapping
    expression = OPERATOR_EXPRESSIONS[operator]

    # Get fieldtype from first table's value column
    fieldtype_query = f"""
    SELECT comment FROM system.columns
    WHERE table = '{table_a}' AND name = 'value'
    """
    result = await ch_client.query(fieldtype_query)
    fieldtype = FIELDTYPE_SCALAR
    if result.result_rows:
        meta = ColumnMeta.from_yaml(result.result_rows[0][0])
        if meta.fieldtype:
            fieldtype = meta.fieldtype

    # Get value column types from both tables
    type_query_a = f"""
    SELECT type FROM system.columns
    WHERE table = '{table_a}' AND name = 'value'
    """
    type_result_a = await ch_client.query(type_query_a)
    type_a = type_result_a.result_rows[0][0] if type_result_a.result_rows else "Float64"

    type_query_b = f"""
    SELECT type FROM system.columns
    WHERE table = '{table_b}' AND name = 'value'
    """
    type_result_b = await ch_client.query(type_query_b)
    type_b = type_result_b.result_rows[0][0] if type_result_b.result_rows else "Float64"

    # Determine result type: promote to Float64 if mixing integer and float types
    int_types = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
    float_types = {"Float32", "Float64"}

    if (type_a in int_types and type_b in float_types) or (type_a in float_types and type_b in int_types):
        value_type = "Float64"
    elif type_a in float_types or type_b in float_types:
        value_type = "Float64"
    else:
        value_type = type_a

    # Build schema for result table
    schema = Schema(
        fieldtype=fieldtype,
        columns={"aai_id": "UInt64", "value": value_type}
    )

    # Create result object with schema
    result = await ctx.create_object(schema)

    # Insert data based on fieldtype
    if fieldtype == FIELDTYPE_ARRAY:
        # Array operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT a.rn as aai_id, {expression} AS value
        FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {table_a}) AS a
        INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {table_b}) AS b
        ON a.rn = b.rn
        """
    else:
        # Scalar operation
        insert_query = f"""
        INSERT INTO {result.table}
        SELECT 1 AS aai_id, {expression} AS value
        FROM {table_a} AS a, {table_b} AS b
        """

    await ch_client.command(insert_query)

    return result


# Arithmetic Operators

async def add(obj_a: Object, obj_b: Object) -> Object:
    """
    Add two objects together.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a + obj_b
    """
    obj_a.checkstale()
    obj_b.checkstale()
    return await _apply_operator_db(obj_a.table, obj_b.table, "+", obj_a.ch_client, obj_a.ctx)


async def sub(obj_a: Object, obj_b: Object) -> Object:
    """
    Subtract one object from another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a - obj_b
    """
    return await obj_a._apply_operator(obj_b, "-")


async def mul(obj_a: Object, obj_b: Object) -> Object:
    """
    Multiply two objects together.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a * obj_b
    """
    return await obj_a._apply_operator(obj_b, "*")


async def truediv(obj_a: Object, obj_b: Object) -> Object:
    """
    Divide one object by another.

    Args:
        obj_a: First Object (numerator)
        obj_b: Second Object (denominator)

    Returns:
        Object: New Object with result of obj_a / obj_b
    """
    return await obj_a._apply_operator(obj_b, "/")


async def floordiv(obj_a: Object, obj_b: Object) -> Object:
    """
    Floor divide one object by another.

    Args:
        obj_a: First Object (numerator)
        obj_b: Second Object (denominator)

    Returns:
        Object: New Object with result of obj_a // obj_b
    """
    return await obj_a._apply_operator(obj_b, "//")


async def mod(obj_a: Object, obj_b: Object) -> Object:
    """
    Modulo operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a % obj_b
    """
    return await obj_a._apply_operator(obj_b, "%")


async def pow(obj_a: Object, obj_b: Object) -> Object:
    """
    Raise one object to the power of another.

    Args:
        obj_a: First Object (base)
        obj_b: Second Object (exponent)

    Returns:
        Object: New Object with result of obj_a ** obj_b
    """
    return await obj_a._apply_operator(obj_b, "**")


# Comparison Operators

async def eq(obj_a: Object, obj_b: Object) -> Object:
    """
    Check equality between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a == obj_b
    """
    return await obj_a._apply_operator(obj_b, "==")


async def ne(obj_a: Object, obj_b: Object) -> Object:
    """
    Check inequality between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a != obj_b
    """
    return await obj_a._apply_operator(obj_b, "!=")


async def lt(obj_a: Object, obj_b: Object) -> Object:
    """
    Check if one object is less than another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a < obj_b
    """
    return await obj_a._apply_operator(obj_b, "<")


async def le(obj_a: Object, obj_b: Object) -> Object:
    """
    Check if one object is less than or equal to another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a <= obj_b
    """
    return await obj_a._apply_operator(obj_b, "<=")


async def gt(obj_a: Object, obj_b: Object) -> Object:
    """
    Check if one object is greater than another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a > obj_b
    """
    return await obj_a._apply_operator(obj_b, ">")


async def ge(obj_a: Object, obj_b: Object) -> Object:
    """
    Check if one object is greater than or equal to another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a >= obj_b
    """
    return await obj_a._apply_operator(obj_b, ">=")


# Bitwise Operators

async def and_(obj_a: Object, obj_b: Object) -> Object:
    """
    Bitwise AND operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a & obj_b
    """
    return await obj_a._apply_operator(obj_b, "&")


async def or_(obj_a: Object, obj_b: Object) -> Object:
    """
    Bitwise OR operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a | obj_b
    """
    return await obj_a._apply_operator(obj_b, "|")


async def xor(obj_a: Object, obj_b: Object) -> Object:
    """
    Bitwise XOR operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a ^ obj_b
    """
    return await obj_a._apply_operator(obj_b, "^")
