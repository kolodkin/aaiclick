"""
Parametrized tests for binary operators across different data types.

Tests scalar and array operations with various operator combinations,
using pytest parametrization for comprehensive coverage.
"""

import pytest

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
    "data_type,data_a,data_b,operator,expected_result",
    [
        # Addition tests
        pytest.param("int", 100, 50, "+", 150, id="int-add-basic"),
        pytest.param("int", 0, 0, "+", 0, id="int-add-zeros"),
        pytest.param("int", -10, 5, "+", -5, id="int-add-negative"),
        pytest.param("int", 1000, 2000, "+", 3000, id="int-add-large"),
        # Subtraction tests
        pytest.param("int", 100, 30, "-", 70, id="int-sub-basic"),
        pytest.param("int", 0, 0, "-", 0, id="int-sub-zeros"),
        pytest.param("int", 50, 100, "-", -50, id="int-sub-negative-result"),
        pytest.param("int", 1000, 1, "-", 999, id="int-sub-large"),
    ],
)
async def test_int_scalar_operators(ctx, data_type, data_a, data_b, operator, expected_result):
    """Test binary operators on integer scalars with various inputs."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert result_data == expected_result



# =============================================================================
# Integer Array Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,data_a,data_b,operator,expected_result",
    [
        # Addition tests
        pytest.param("int", [1, 2, 3], [10, 20, 30], "+", [11, 22, 33], id="int-add-basic"),
        pytest.param("int", [0, 0, 0], [1, 2, 3], "+", [1, 2, 3], id="int-add-zeros"),
        pytest.param("int", [-5, -10, -15], [5, 10, 15], "+", [0, 0, 0], id="int-add-canceling"),
        pytest.param("int", [100, 200], [50, 75], "+", [150, 275], id="int-add-large"),
        # Subtraction tests
        pytest.param("int", [100, 200, 300], [10, 20, 30], "-", [90, 180, 270], id="int-sub-basic"),
        pytest.param("int", [10, 20, 30], [10, 20, 30], "-", [0, 0, 0], id="int-sub-zeros"),
        pytest.param("int", [5, 10, 15], [10, 20, 30], "-", [-5, -10, -15], id="int-sub-negative"),
        pytest.param("int", [1000, 2000], [1, 2], "-", [999, 1998], id="int-sub-large"),
    ],
)
async def test_int_array_operators(ctx, data_type, data_a, data_b, operator, expected_result):
    """Test binary operators on integer arrays with various inputs."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert result_data == expected_result



# =============================================================================
# Float Scalar Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,data_a,data_b,operator,expected_result",
    [
        # Addition tests
        pytest.param("float", 100.5, 50.25, "+", 150.75, id="float-add-basic"),
        pytest.param("float", 0.0, 0.0, "+", 0.0, id="float-add-zeros"),
        pytest.param("float", -10.5, 5.25, "+", -5.25, id="float-add-negative"),
        pytest.param("float", 3.14159, 2.71828, "+", 5.85987, id="float-add-pi-e"),
        # Subtraction tests
        pytest.param("float", 100.5, 30.25, "-", 70.25, id="float-sub-basic"),
        pytest.param("float", 0.0, 0.0, "-", 0.0, id="float-sub-zeros"),
        pytest.param("float", 50.5, 100.5, "-", -50.0, id="float-sub-negative-result"),
        pytest.param("float", 10.0, 0.1, "-", 9.9, id="float-sub-small"),
    ],
)
async def test_float_scalar_operators(ctx, data_type, data_a, data_b, operator, expected_result):
    """Test binary operators on float scalars with various inputs."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    assert abs(result_data - expected_result) < THRESHOLD



# =============================================================================
# Float Array Tests
# =============================================================================


@pytest.mark.parametrize(
    "data_type,data_a,data_b,operator,expected_result",
    [
        # Addition tests
        pytest.param("float", [10.0, 20.0, 30.0], [5.0, 10.0, 15.0], "+", [15.0, 30.0, 45.0], id="float-add-basic"),
        pytest.param("float", [0.0, 0.0], [1.5, 2.5], "+", [1.5, 2.5], id="float-add-zeros"),
        pytest.param("float", [-5.5, -10.5], [5.5, 10.5], "+", [0.0, 0.0], id="float-add-canceling"),
        pytest.param("float", [1.1, 2.2, 3.3], [0.1, 0.2, 0.3], "+", [1.2, 2.4, 3.6], id="float-add-decimals"),
        # Subtraction tests
        pytest.param("float", [100.5, 200.5, 300.5], [10.5, 20.5, 30.5], "-", [90.0, 180.0, 270.0], id="float-sub-basic"),
        pytest.param("float", [10.0, 20.0], [10.0, 20.0], "-", [0.0, 0.0], id="float-sub-zeros"),
        pytest.param("float", [5.5, 10.5], [10.5, 20.5], "-", [-5.0, -10.0], id="float-sub-negative"),
        pytest.param("float", [100.0, 200.0], [0.1, 0.2], "-", [99.9, 199.8], id="float-sub-small"),
    ],
)
async def test_float_array_operators(ctx, data_type, data_a, data_b, operator, expected_result):
    """Test binary operators on float arrays with various inputs."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    for i, val in enumerate(result_data):
        assert abs(val - expected_result[i]) < THRESHOLD



# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.parametrize(
    "data_type,data_a,data_b,operator,expected_result",
    [
        # Single element arrays
        pytest.param("int", [42], [8], "+", [50], id="int-add-single"),
        pytest.param("int", [100], [30], "-", [70], id="int-sub-single"),
        # Large numbers
        pytest.param("int", [1000000], [2000000], "+", [3000000], id="int-add-large"),
        pytest.param("int", [999999999], [1], "+", [1000000000], id="int-add-very-large"),
        # Very small floats
        pytest.param("float", [0.001, 0.002], [0.003, 0.004], "+", [0.004, 0.006], id="float-add-small"),
        # Negative numbers
        pytest.param("int", [-100, -200], [-50, -75], "+", [-150, -275], id="int-add-negative"),
        pytest.param("int", [-10, -20], [-5, -10], "-", [-5, -10], id="int-sub-negative"),
    ],
)
async def test_edge_case_operators(ctx, data_type, data_a, data_b, operator, expected_result):
    """Test binary operators with edge cases."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)

    result = await apply_operator(obj_a, obj_b, operator)
    result_data = await result.data()

    # Handle both scalar and array results
    if isinstance(expected_result, list):
        for i, val in enumerate(result_data):
            assert abs(val - expected_result[i]) < THRESHOLD
    else:
        assert abs(result_data - expected_result) < THRESHOLD



# =============================================================================
# Chained Operations
# =============================================================================


@pytest.mark.parametrize(
    "data_type,data_a,data_b,data_c,op1,op2,expected_result",
    [
        # (a + b) - c
        pytest.param("int", [10, 20, 30], [5, 10, 15], [3, 6, 9], "+", "-", [12, 24, 36], id="int-add-sub"),
        # (a - b) + c
        pytest.param("int", [100, 200], [30, 60], [5, 10], "-", "+", [75, 150], id="int-sub-add"),
        # (a + b) + c
        pytest.param("int", [1, 2], [3, 4], [5, 6], "+", "+", [9, 12], id="int-add-add"),
        # (a - b) - c
        pytest.param("int", [100, 200], [10, 20], [5, 10], "-", "-", [85, 170], id="int-sub-sub"),
    ],
)
async def test_chained_operators(ctx, data_type, data_a, data_b, data_c, op1, op2, expected_result):
    """Test chained binary operations."""
    obj_a = await ctx.create_object_from_value(data_a)
    obj_b = await ctx.create_object_from_value(data_b)
    obj_c = await ctx.create_object_from_value(data_c)

    # Apply first operation
    temp = await apply_operator(obj_a, obj_b, op1)

    # Apply second operation
    result = await apply_operator(temp, obj_c, op2)
    result_data = await result.data()

    for i, val in enumerate(result_data):
        assert abs(val - expected_result[i]) < THRESHOLD

