"""
aaiclick.operators - Operator implementations for Object class.

This module contains static functions that implement all operators for Object instances.
Each operator function takes two Object parameters and returns a new Object with the result.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .object import Object


# Arithmetic Operators

async def add(a: "Object", b: "Object") -> "Object":
    """
    Add two objects together.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a + b
    """
    return await a._binary_operation(b, "+")


async def sub(a: "Object", b: "Object") -> "Object":
    """
    Subtract one object from another.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a - b
    """
    return await a._binary_operation(b, "-")


async def mul(a: "Object", b: "Object") -> "Object":
    """
    Multiply two objects together.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a * b
    """
    return await a._binary_operation(b, "*")


async def truediv(a: "Object", b: "Object") -> "Object":
    """
    Divide one object by another.

    Args:
        a: First Object (numerator)
        b: Second Object (denominator)

    Returns:
        Object: New Object with result of a / b
    """
    return await a._binary_operation(b, "/")


async def floordiv(a: "Object", b: "Object") -> "Object":
    """
    Floor divide one object by another.

    Args:
        a: First Object (numerator)
        b: Second Object (denominator)

    Returns:
        Object: New Object with result of a // b
    """
    return await a._binary_operation(b, "DIV")


async def mod(a: "Object", b: "Object") -> "Object":
    """
    Modulo operation between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a % b
    """
    return await a._binary_operation(b, "%")


async def pow(a: "Object", b: "Object") -> "Object":
    """
    Raise one object to the power of another.

    Args:
        a: First Object (base)
        b: Second Object (exponent)

    Returns:
        Object: New Object with result of a ** b
    """
    return await a._binary_operation(b, "power")


# Comparison Operators

async def eq(a: "Object", b: "Object") -> "Object":
    """
    Check equality between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a == b
    """
    return await a._binary_operation(b, "=")


async def ne(a: "Object", b: "Object") -> "Object":
    """
    Check inequality between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a != b
    """
    return await a._binary_operation(b, "!=")


async def lt(a: "Object", b: "Object") -> "Object":
    """
    Check if one object is less than another.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a < b
    """
    return await a._binary_operation(b, "<")


async def le(a: "Object", b: "Object") -> "Object":
    """
    Check if one object is less than or equal to another.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a <= b
    """
    return await a._binary_operation(b, "<=")


async def gt(a: "Object", b: "Object") -> "Object":
    """
    Check if one object is greater than another.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a > b
    """
    return await a._binary_operation(b, ">")


async def ge(a: "Object", b: "Object") -> "Object":
    """
    Check if one object is greater than or equal to another.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with boolean result of a >= b
    """
    return await a._binary_operation(b, ">=")


# Bitwise Operators

async def and_(a: "Object", b: "Object") -> "Object":
    """
    Bitwise AND operation between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a & b
    """
    return await a._binary_operation(b, "bitAnd")


async def or_(a: "Object", b: "Object") -> "Object":
    """
    Bitwise OR operation between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a | b
    """
    return await a._binary_operation(b, "bitOr")


async def xor(a: "Object", b: "Object") -> "Object":
    """
    Bitwise XOR operation between two objects.

    Args:
        a: First Object
        b: Second Object

    Returns:
        Object: New Object with result of a ^ b
    """
    return await a._binary_operation(b, "bitXor")
