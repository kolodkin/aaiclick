"""
aaiclick.operators - Operator implementations for Object class.

This module contains static functions that implement all operators for Object instances.
Each operator function takes two Object parameters and returns a new Object with the result.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .object import Object


# Arithmetic Operators

async def add(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Add two objects together.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a + obj_b
    """
    return await obj_a._apply_operator(obj_b, "+")


async def sub(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Subtract one object from another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a - obj_b
    """
    return await obj_a._apply_operator(obj_b, "-")


async def mul(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Multiply two objects together.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a * obj_b
    """
    return await obj_a._apply_operator(obj_b, "*")


async def truediv(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Divide one object by another.

    Args:
        obj_a: First Object (numerator)
        obj_b: Second Object (denominator)

    Returns:
        Object: New Object with result of obj_a / obj_b
    """
    return await obj_a._apply_operator(obj_b, "/")


async def floordiv(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Floor divide one object by another.

    Args:
        obj_a: First Object (numerator)
        obj_b: Second Object (denominator)

    Returns:
        Object: New Object with result of obj_a // obj_b
    """
    return await obj_a._apply_operator(obj_b, "DIV")


async def mod(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Modulo operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a % obj_b
    """
    return await obj_a._apply_operator(obj_b, "%")


async def pow(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Raise one object to the power of another.

    Args:
        obj_a: First Object (base)
        obj_b: Second Object (exponent)

    Returns:
        Object: New Object with result of obj_a ** obj_b
    """
    return await obj_a._apply_operator(obj_b, "power", is_function=True)


# Comparison Operators

async def eq(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Check equality between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a == obj_b
    """
    return await obj_a._apply_operator(obj_b, "=")


async def ne(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Check inequality between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a != obj_b
    """
    return await obj_a._apply_operator(obj_b, "!=")


async def lt(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Check if one object is less than another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a < obj_b
    """
    return await obj_a._apply_operator(obj_b, "<")


async def le(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Check if one object is less than or equal to another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a <= obj_b
    """
    return await obj_a._apply_operator(obj_b, "<=")


async def gt(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Check if one object is greater than another.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with boolean result of obj_a > obj_b
    """
    return await obj_a._apply_operator(obj_b, ">")


async def ge(obj_a: "Object", obj_b: "Object") -> "Object":
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

async def and_(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Bitwise AND operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a & obj_b
    """
    return await obj_a._apply_operator(obj_b, "bitAnd", is_function=True)


async def or_(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Bitwise OR operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a | obj_b
    """
    return await obj_a._apply_operator(obj_b, "bitOr", is_function=True)


async def xor(obj_a: "Object", obj_b: "Object") -> "Object":
    """
    Bitwise XOR operation between two objects.

    Args:
        obj_a: First Object
        obj_b: Second Object

    Returns:
        Object: New Object with result of obj_a ^ obj_b
    """
    return await obj_a._apply_operator(obj_b, "bitXor", is_function=True)
