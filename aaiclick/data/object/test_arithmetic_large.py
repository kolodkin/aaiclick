"""
Tests for arithmetic operations on large arrays.

Each test uses NUM_ITEMS=10000 to validate performance and correctness at scale.
Each operator is tested with different type combinations.
"""

from aaiclick import create_object_from_value

# Number of items for large array tests
NUM_ITEMS = 10000


async def test_add_int_float(ctx):
    """Test addition with int array + float array (10k items)."""
    # Create large arrays
    int_array = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]
    float_array = [float(i) * 0.5 for i in range(NUM_ITEMS)]  # [0.0, 0.5, 1.0, ..., 4999.5]

    # Create objects
    obj_int = await create_object_from_value(int_array, aai_id=True)
    obj_float = await create_object_from_value(float_array, aai_id=True)

    # Perform addition
    result = obj_int.view(order_by="value") + obj_float.view(order_by="value")
    result_data = await result.data(limit=None)

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check first, middle, and last elements
    assert result_data[0] == 0.0  # 0 + 0.0
    assert result_data[NUM_ITEMS // 2] == (NUM_ITEMS // 2) * 1.5  # 5000 + 2500.0
    assert result_data[-1] == 9999 + 4999.5  # 14998.5


async def test_sub_float_float(ctx):
    """Test subtraction with float array - float array (10k items)."""
    # Create large float arrays
    float_array1 = [float(i) * 2.0 for i in range(NUM_ITEMS)]  # [0.0, 2.0, 4.0, ..., 19998.0]
    float_array2 = [float(i) * 0.5 for i in range(NUM_ITEMS)]  # [0.0, 0.5, 1.0, ..., 4999.5]

    # Create objects
    obj1 = await create_object_from_value(float_array1, aai_id=True)
    obj2 = await create_object_from_value(float_array2, aai_id=True)

    # Perform subtraction
    result = obj1.view(order_by="value") - obj2.view(order_by="value")
    result_data = await result.data(limit=None)

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check first, middle, and last elements
    assert result_data[0] == 0.0  # 0.0 - 0.0
    assert result_data[NUM_ITEMS // 2] == (NUM_ITEMS // 2) * 1.5  # 10000.0 - 2500.0 = 7500.0
    assert result_data[-1] == 19998.0 - 4999.5  # 14998.5


async def test_add_int_int(ctx):
    """Test addition with int array + int array (10k items)."""
    # Create large int arrays
    int_array1 = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]
    int_array2 = list(range(NUM_ITEMS, NUM_ITEMS * 2))  # [10000, 10001, ..., 19999]

    # Create objects
    obj1 = await create_object_from_value(int_array1, aai_id=True)
    obj2 = await create_object_from_value(int_array2, aai_id=True)

    # Perform addition
    result = obj1.view(order_by="value") + obj2.view(order_by="value")
    result_data = await result.data(limit=None)

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check sampling of results
    assert result_data[0] == 10000  # 0 + 10000
    assert result_data[100] == 10200  # 100 + 10100
    assert result_data[-1] == 29998  # 9999 + 19999


async def test_sub_int_int(ctx):
    """Test subtraction with int array - int array (10k items)."""
    # Create large int arrays
    int_array1 = list(range(NUM_ITEMS * 2, NUM_ITEMS * 3))  # [20000, 20001, ..., 29999]
    int_array2 = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]

    # Create objects
    obj1 = await create_object_from_value(int_array1, aai_id=True)
    obj2 = await create_object_from_value(int_array2, aai_id=True)

    # Perform subtraction
    result = obj1.view(order_by="value") - obj2.view(order_by="value")
    result_data = await result.data(limit=None)

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # All results should be 20000 (constant difference)
    assert result_data[0] == 20000  # 20000 - 0
    assert result_data[NUM_ITEMS // 2] == 20000  # 25000 - 5000
    assert result_data[-1] == 20000  # 29999 - 9999
