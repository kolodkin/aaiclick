"""
Parametrized tests for binary operators across different data types.

Tests scalar and array operations with various operator combinations,
using pytest parametrization for comprehensive coverage.
"""

import pytest
from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Helper Function for Operator Application
# =============================================================================


async def apply_operator(obj_a, obj_b, operator: str):
    """
    Apply a binary operator to two objects using match/case.

    Args:
        obj_a: First Object operand
        obj_b: Second Object operand
        operator: Operator string ('+', '-', etc.)

    Returns:
        Object: Result of the operation

    Raises:
        ValueError: If operator is not supported
    """
    match operator:
        case "+":
            return await (obj_a + obj_b)
        case "-":
            return await (obj_a - obj_b)
        case _:
            raise ValueError(f"Unsupported operator: {operator}")


# =============================================================================
# Integer Scalar Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Addition tests
        (100, 50, "+", 150),
        (0, 0, "+", 0),
        (-10, 5, "+", -5),
        (1000, 2000, "+", 3000),
        # Subtraction tests
        (100, 30, "-", 70),
        (0, 0, "-", 0),
        (50, 100, "-", -50),
        (1000, 1, "-", 999),
    ],
)
async def test_int_scalar_operators(data_a, data_b, operator, expected_result):
    """Test binary operators on integer scalars with various inputs."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert result_data == expected_result

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Integer Array Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Addition tests
        ([1, 2, 3], [10, 20, 30], "+", [11, 22, 33]),
        ([0, 0, 0], [1, 2, 3], "+", [1, 2, 3]),
        ([-5, -10, -15], [5, 10, 15], "+", [0, 0, 0]),
        ([100, 200], [50, 75], "+", [150, 275]),
        # Subtraction tests
        ([100, 200, 300], [10, 20, 30], "-", [90, 180, 270]),
        ([10, 20, 30], [10, 20, 30], "-", [0, 0, 0]),
        ([5, 10, 15], [10, 20, 30], "-", [-5, -10, -15]),
        ([1000, 2000], [1, 2], "-", [999, 1998]),
    ],
)
async def test_int_array_operators(data_a, data_b, operator, expected_result):
    """Test binary operators on integer arrays with various inputs."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert result_data == expected_result

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Float Scalar Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Addition tests
        (100.5, 50.25, "+", 150.75),
        (0.0, 0.0, "+", 0.0),
        (-10.5, 5.25, "+", -5.25),
        (3.14159, 2.71828, "+", 5.85987),
        # Subtraction tests
        (100.5, 30.25, "-", 70.25),
        (0.0, 0.0, "-", 0.0),
        (50.5, 100.5, "-", -50.0),
        (10.0, 0.1, "-", 9.9),
    ],
)
async def test_float_scalar_operators(data_a, data_b, operator, expected_result):
    """Test binary operators on float scalars with various inputs."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert abs(result_data - expected_result) < THRESHOLD

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Float Array Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Addition tests
        ([10.0, 20.0, 30.0], [5.0, 10.0, 15.0], "+", [15.0, 30.0, 45.0]),
        ([0.0, 0.0], [1.5, 2.5], "+", [1.5, 2.5]),
        ([-5.5, -10.5], [5.5, 10.5], "+", [0.0, 0.0]),
        ([1.1, 2.2, 3.3], [0.1, 0.2, 0.3], "+", [1.2, 2.4, 3.6]),
        # Subtraction tests
        ([100.5, 200.5, 300.5], [10.5, 20.5, 30.5], "-", [90.0, 180.0, 270.0]),
        ([10.0, 20.0], [10.0, 20.0], "-", [0.0, 0.0]),
        ([5.5, 10.5], [10.5, 20.5], "-", [-5.0, -10.0]),
        ([100.0, 200.0], [0.1, 0.2], "-", [99.9, 199.8]),
    ],
)
async def test_float_array_operators(data_a, data_b, operator, expected_result):
    """Test binary operators on float arrays with various inputs."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    for i, val in enumerate(result_data):
        assert abs(val - expected_result[i]) < THRESHOLD

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Mixed Type Tests (int + float)
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Scalar mixed type tests
        (100, 50.5, "+", 150.5),
        (10.5, 5, "+", 15.5),
        (100, 30.5, "-", 69.5),
        (50.5, 10, "-", 40.5),
        # Array mixed type tests
        ([1, 2, 3], [0.5, 1.5, 2.5], "+", [1.5, 3.5, 5.5]),
        ([10.0, 20.0, 30.0], [1, 2, 3], "+", [11.0, 22.0, 33.0]),
        ([100, 200], [10.5, 20.5], "-", [89.5, 179.5]),
        ([100.5, 200.5], [10, 20], "-", [90.5, 180.5]),
    ],
)
async def test_mixed_type_operators(data_a, data_b, operator, expected_result):
    """Test binary operators on mixed int/float types."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    # Handle both scalar and array results
    if isinstance(expected_result, list):
        for i, val in enumerate(result_data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert abs(result_data - expected_result) < THRESHOLD

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,operator,expected_result",
    [
        # Single element arrays
        ([42], [8], "+", [50]),
        ([100], [30], "-", [70]),
        # Large numbers
        ([1000000], [2000000], "+", [3000000]),
        ([999999999], [1], "+", [1000000000]),
        # Very small floats
        ([0.001, 0.002], [0.003, 0.004], "+", [0.004, 0.006]),
        # Negative numbers
        ([-100, -200], [-50, -75], "+", [-150, -275]),
        ([-10, -20], [-5, -10], "-", [-5, -10]),
    ],
)
async def test_edge_case_operators(data_a, data_b, operator, expected_result):
    """Test binary operators with edge cases."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    # Handle both scalar and array results
    if isinstance(expected_result, list):
        for i, val in enumerate(result_data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert abs(result_data - expected_result) < THRESHOLD

    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


# =============================================================================
# Chained Operations
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,data_c,op1,op2,expected_result",
    [
        # (a + b) - c
        ([10, 20, 30], [5, 10, 15], [3, 6, 9], "+", "-", [12, 24, 36]),
        # (a - b) + c
        ([100, 200], [30, 60], [5, 10], "-", "+", [75, 150]),
        # (a + b) + c
        ([1, 2], [3, 4], [5, 6], "+", "+", [9, 12]),
        # (a - b) - c
        ([100, 200], [10, 20], [5, 10], "-", "-", [85, 170]),
    ],
)
async def test_chained_operators(data_a, data_b, data_c, op1, op2, expected_result):
    """Test chained binary operations."""
    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)
    obj_c = await create_object_from_value(data_c)

    # Apply first operation
    temp = await apply_operator(obj_a, obj_b, op1)

    # Apply second operation
    result = await apply_operator(temp, obj_c, op2)
    result_data = await result.data()

    for i, val in enumerate(result_data):
        assert abs(val - expected_result[i]) < THRESHOLD

    await obj_a.delete_table()
    await obj_b.delete_table()
    await obj_c.delete_table()
    await temp.delete_table()
    await result.delete_table()
