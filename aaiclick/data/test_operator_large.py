"""
Tests for operations on large arrays.

Each test uses NUM_ITEMS=10000 to validate performance and correctness at scale.
Each operator is tested with different type combinations.
"""

from aaiclick import create_object_from_value, create_object

# Number of items for large array tests
NUM_ITEMS = 10000


async def test_add_int_float(ctx):
    """Test addition with int array + float array (10k items)."""
    # Create large arrays
    int_array = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]
    float_array = [float(i) * 0.5 for i in range(NUM_ITEMS)]  # [0.0, 0.5, 1.0, ..., 4999.5]

    # Create objects
    obj_int = await create_object_from_value(int_array)
    obj_float = await create_object_from_value(float_array)

    # Perform addition
    result = await (obj_int + obj_float)
    result_data = await result.data()

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check first, middle, and last elements
    assert result_data[0] == 0.0  # 0 + 0.0
    assert result_data[NUM_ITEMS // 2] == (NUM_ITEMS // 2) * 1.5  # 5000 + 2500.0
    assert result_data[-1] == 9999 + 4999.5  # 14998.5

    # Cleanup


async def test_sub_float_float(ctx):
    """Test subtraction with float array - float array (10k items)."""
    # Create large float arrays
    float_array1 = [float(i) * 2.0 for i in range(NUM_ITEMS)]  # [0.0, 2.0, 4.0, ..., 19998.0]
    float_array2 = [float(i) * 0.5 for i in range(NUM_ITEMS)]  # [0.0, 0.5, 1.0, ..., 4999.5]

    # Create objects
    obj1 = await create_object_from_value(float_array1)
    obj2 = await create_object_from_value(float_array2)

    # Perform subtraction
    result = await (obj1 - obj2)
    result_data = await result.data()

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check first, middle, and last elements
    assert result_data[0] == 0.0  # 0.0 - 0.0
    assert result_data[NUM_ITEMS // 2] == (NUM_ITEMS // 2) * 1.5  # 10000.0 - 2500.0 = 7500.0
    assert result_data[-1] == 19998.0 - 4999.5  # 14998.5

    # Cleanup


async def test_min_int(ctx):
    """Test min() on large int array (10k items)."""
    # Create array with known min
    int_array = list(range(100, NUM_ITEMS + 100))  # [100, 101, ..., 10099]

    # Create object
    obj = await create_object_from_value(int_array)

    # Get minimum
    min_val = await obj.min()

    # Verify
    assert min_val == 100

    # Cleanup


async def test_max_float(ctx):
    """Test max() on large float array (10k items)."""
    # Create array with known max
    float_array = [float(i) * 0.1 for i in range(NUM_ITEMS)]  # [0.0, 0.1, ..., 999.9]

    # Create object
    obj = await create_object_from_value(float_array)

    # Get maximum
    max_val = await obj.max()

    # Verify (allowing for floating point precision)
    assert abs(max_val - 999.9) < 0.001

    # Cleanup


async def test_sum_float(ctx):
    """Test sum() on large float array (10k items)."""
    # Create simple array for easy sum calculation
    float_array = [1.5] * NUM_ITEMS  # All elements are 1.5

    # Create object
    obj = await create_object_from_value(float_array)

    # Get sum
    sum_val = await obj.sum()

    # Verify
    expected_sum = 1.5 * NUM_ITEMS  # 15000.0
    assert abs(sum_val - expected_sum) < 0.001

    # Cleanup


async def test_mean_int(ctx):
    """Test mean() on large int array (10k items)."""
    # Create array with known mean
    int_array = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]

    # Create object
    obj = await create_object_from_value(int_array)

    # Get mean
    mean_val = await obj.mean()

    # Verify: mean of 0..9999 is 4999.5
    expected_mean = (NUM_ITEMS - 1) / 2.0
    assert abs(mean_val - expected_mean) < 0.001

    # Cleanup


async def test_std_float(ctx):
    """Test std() (standard deviation) on large float array (10k items)."""
    # Create array with known values
    float_array = [float(i) for i in range(NUM_ITEMS)]  # [0.0, 1.0, 2.0, ..., 9999.0]

    # Create object
    obj = await create_object_from_value(float_array)

    # Get standard deviation
    std_val = await obj.std()

    # Verify: std of 0..9999 should be approximately 2886.75
    # For a uniform distribution from 0 to N-1, std = sqrt((N^2 - 1) / 12)
    import math
    expected_std = math.sqrt((NUM_ITEMS**2 - 1) / 12.0)
    assert abs(std_val - expected_std) < 1.0  # Allow small variance

    # Cleanup


async def test_add_int_int(ctx):
    """Test addition with int array + int array (10k items)."""
    # Create large int arrays
    int_array1 = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]
    int_array2 = list(range(NUM_ITEMS, NUM_ITEMS * 2))  # [10000, 10001, ..., 19999]

    # Create objects
    obj1 = await create_object_from_value(int_array1)
    obj2 = await create_object_from_value(int_array2)

    # Perform addition
    result = await (obj1 + obj2)
    result_data = await result.data()

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # Check sampling of results
    assert result_data[0] == 10000  # 0 + 10000
    assert result_data[100] == 10200  # 100 + 10100
    assert result_data[-1] == 29998  # 9999 + 19999

    # Cleanup


async def test_sub_int_int(ctx):
    """Test subtraction with int array - int array (10k items)."""
    # Create large int arrays
    int_array1 = list(range(NUM_ITEMS * 2, NUM_ITEMS * 3))  # [20000, 20001, ..., 29999]
    int_array2 = list(range(NUM_ITEMS))  # [0, 1, 2, ..., 9999]

    # Create objects
    obj1 = await create_object_from_value(int_array1)
    obj2 = await create_object_from_value(int_array2)

    # Perform subtraction
    result = await (obj1 - obj2)
    result_data = await result.data()

    # Verify results
    assert len(result_data) == NUM_ITEMS
    # All results should be 20000 (constant difference)
    assert result_data[0] == 20000  # 20000 - 0
    assert result_data[NUM_ITEMS // 2] == 20000  # 25000 - 5000
    assert result_data[-1] == 20000  # 29999 - 9999

    # Cleanup
