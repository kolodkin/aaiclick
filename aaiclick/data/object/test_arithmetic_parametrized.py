"""
Parametrized tests for binary operators across different data types.

Tests scalar and array operations with various operator combinations,
using pytest parametrization for comprehensive coverage.
"""

import operator

import pytest

from aaiclick import create_object_from_value

THRESHOLD = 1e-5


# =============================================================================
# Object <op> Object — scalar and array operands
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,op,expected_result",
    [
        # Integer scalars
        pytest.param(100, 50, operator.add, 150, id="int-scalar-add-basic"),
        pytest.param(0, 0, operator.add, 0, id="int-scalar-add-zeros"),
        pytest.param(-10, 5, operator.add, -5, id="int-scalar-add-negative"),
        pytest.param(1000, 2000, operator.add, 3000, id="int-scalar-add-large"),
        pytest.param(100, 30, operator.sub, 70, id="int-scalar-sub-basic"),
        pytest.param(0, 0, operator.sub, 0, id="int-scalar-sub-zeros"),
        pytest.param(50, 100, operator.sub, -50, id="int-scalar-sub-negative-result"),
        pytest.param(1000, 1, operator.sub, 999, id="int-scalar-sub-large"),
        pytest.param(6, 7, operator.mul, 42, id="int-scalar-mul"),
        pytest.param(10, 3, operator.floordiv, 3, id="int-scalar-floordiv"),
        pytest.param(10, 3, operator.mod, 1, id="int-scalar-mod"),
        # Integer arrays
        pytest.param([1, 2, 3], [10, 20, 30], operator.add, [11, 22, 33], id="int-array-add-basic"),
        pytest.param([0, 0, 0], [1, 2, 3], operator.add, [1, 2, 3], id="int-array-add-zeros"),
        pytest.param([-5, -10, -15], [5, 10, 15], operator.add, [0, 0, 0], id="int-array-add-canceling"),
        pytest.param([100, 200], [50, 75], operator.add, [150, 275], id="int-array-add-large"),
        pytest.param([100, 200, 300], [10, 20, 30], operator.sub, [90, 180, 270], id="int-array-sub-basic"),
        pytest.param([10, 20, 30], [10, 20, 30], operator.sub, [0, 0, 0], id="int-array-sub-zeros"),
        pytest.param([5, 10, 15], [10, 20, 30], operator.sub, [-5, -10, -15], id="int-array-sub-negative"),
        pytest.param([1000, 2000], [1, 2], operator.sub, [999, 1998], id="int-array-sub-large"),
        pytest.param([1, 2, 3], [4, 5, 6], operator.mul, [4, 10, 18], id="int-array-mul"),
        pytest.param([10, 23, 37], [3, 5, 7], operator.floordiv, [3, 4, 5], id="int-array-floordiv"),
        pytest.param([10, 23, 37], [3, 5, 7], operator.mod, [1, 3, 2], id="int-array-mod"),
        # Integer array edge cases
        pytest.param([42], [8], operator.add, [50], id="int-array-add-single"),
        pytest.param([100], [30], operator.sub, [70], id="int-array-sub-single"),
        pytest.param([1000000], [2000000], operator.add, [3000000], id="int-array-add-million"),
        pytest.param([999999999], [1], operator.add, [1000000000], id="int-array-add-very-large"),
        pytest.param([-100, -200], [-50, -75], operator.add, [-150, -275], id="int-array-add-both-negative"),
        pytest.param([-10, -20], [-5, -10], operator.sub, [-5, -10], id="int-array-sub-both-negative"),
        # Float scalars
        pytest.param(100.5, 50.25, operator.add, 150.75, id="float-scalar-add-basic"),
        pytest.param(0.0, 0.0, operator.add, 0.0, id="float-scalar-add-zeros"),
        pytest.param(-10.5, 5.25, operator.add, -5.25, id="float-scalar-add-negative"),
        pytest.param(3.14159, 2.71828, operator.add, 5.85987, id="float-scalar-add-pi-e"),
        pytest.param(100.5, 30.25, operator.sub, 70.25, id="float-scalar-sub-basic"),
        pytest.param(0.0, 0.0, operator.sub, 0.0, id="float-scalar-sub-zeros"),
        pytest.param(50.5, 100.5, operator.sub, -50.0, id="float-scalar-sub-negative-result"),
        pytest.param(10.0, 0.1, operator.sub, 9.9, id="float-scalar-sub-small"),
        pytest.param(10.0, 2.5, operator.mul, 25.0, id="float-scalar-mul"),
        pytest.param(10.0, 4.0, operator.truediv, 2.5, id="float-scalar-div"),
        pytest.param(2.0, 10.0, operator.pow, 1024.0, id="float-scalar-pow"),
        # Float arrays
        pytest.param(
            [10.0, 20.0, 30.0], [5.0, 10.0, 15.0], operator.add, [15.0, 30.0, 45.0], id="float-array-add-basic"
        ),
        pytest.param([0.0, 0.0], [1.5, 2.5], operator.add, [1.5, 2.5], id="float-array-add-zeros"),
        pytest.param([-5.5, -10.5], [5.5, 10.5], operator.add, [0.0, 0.0], id="float-array-add-canceling"),
        pytest.param([1.1, 2.2, 3.3], [0.1, 0.2, 0.3], operator.add, [1.2, 2.4, 3.6], id="float-array-add-decimals"),
        pytest.param([0.001, 0.002], [0.003, 0.004], operator.add, [0.004, 0.006], id="float-array-add-small"),
        pytest.param(
            [100.5, 200.5, 300.5], [10.5, 20.5, 30.5], operator.sub, [90.0, 180.0, 270.0], id="float-array-sub-basic"
        ),
        pytest.param([10.0, 20.0], [10.0, 20.0], operator.sub, [0.0, 0.0], id="float-array-sub-zeros"),
        pytest.param([5.5, 10.5], [10.5, 20.5], operator.sub, [-5.0, -10.0], id="float-array-sub-negative"),
        pytest.param([100.0, 200.0], [0.1, 0.2], operator.sub, [99.9, 199.8], id="float-array-sub-small"),
        pytest.param([10.0, 20.0], [2.0, 4.0], operator.truediv, [5.0, 5.0], id="float-array-div"),
        pytest.param([2.0, 3.0], [3.0, 2.0], operator.pow, [8.0, 9.0], id="float-array-pow"),
    ],
)
async def test_binary_operators(ctx, data_a, data_b, op, expected_result):
    """Binary operators on scalar and array Objects of int and float values."""
    obj_a = await create_object_from_value(data_a, aai_id=True)
    obj_b = await create_object_from_value(data_b, aai_id=True)

    result = op(obj_a, obj_b)
    result_data = await result.data()

    assert result_data == pytest.approx(expected_result, abs=THRESHOLD)


# =============================================================================
# Chained Operations
# =============================================================================


@pytest.mark.parametrize(
    "data_a,data_b,data_c,op1,op2,expected_result",
    [
        # (a + b) - c
        pytest.param([10, 20, 30], [5, 10, 15], [3, 6, 9], operator.add, operator.sub, [12, 24, 36], id="int-add-sub"),
        # (a - b) + c
        pytest.param([100, 200], [30, 60], [5, 10], operator.sub, operator.add, [75, 150], id="int-sub-add"),
        # (a + b) + c
        pytest.param([1, 2], [3, 4], [5, 6], operator.add, operator.add, [9, 12], id="int-add-add"),
        # (a - b) - c
        pytest.param([100, 200], [10, 20], [5, 10], operator.sub, operator.sub, [85, 170], id="int-sub-sub"),
    ],
)
async def test_chained_operators(ctx, data_a, data_b, data_c, op1, op2, expected_result):
    """Test chained binary operations."""
    obj_a = await create_object_from_value(data_a, aai_id=True)
    obj_b = await create_object_from_value(data_b, aai_id=True)
    obj_c = await create_object_from_value(data_c, aai_id=True)

    temp = op1(obj_a, obj_b)
    result = op2(temp, obj_c)
    result_data = await result.data()

    assert result_data == pytest.approx(expected_result, abs=THRESHOLD)
