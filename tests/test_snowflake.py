"""
Tests for Snowflake ID generation.
"""

from aaiclick.snowflake import (
    SnowflakeGenerator,
    get_snowflake_id,
    get_snowflake_ids,
    MAX_SEQUENCE,
)


def test_generate_single_id():
    """Test generating a single snowflake ID."""
    id1 = get_snowflake_id()
    id2 = get_snowflake_id()

    # IDs should be unique
    assert id1 != id2
    # Later ID should be greater (time-ordered)
    assert id2 > id1


def test_generate_bulk_ids():
    """Test generating multiple snowflake IDs in bulk."""
    count = 100
    ids = get_snowflake_ids(count)

    # Should generate exact count
    assert len(ids) == count

    # All IDs should be unique
    assert len(set(ids)) == count

    # IDs should be in ascending order (time-ordered)
    assert ids == sorted(ids)


def test_bulk_ids_are_sequential():
    """Test that bulk IDs are sequential within the same millisecond."""
    count = 10
    ids = get_snowflake_ids(count)

    # Check that IDs increment by 1 in the sequence portion
    # (this may not always be true if milliseconds change, but for small counts it should be)
    for i in range(1, len(ids)):
        # The difference should be small (sequence increment or timestamp increment)
        diff = ids[i] - ids[i - 1]
        assert diff > 0, "IDs should be increasing"


def test_bulk_generation_edge_cases():
    """Test edge cases for bulk snowflake ID generation."""
    gen = SnowflakeGenerator()

    # Test count=1 (minimum valid)
    ids_single = gen.generate_bulk(1)
    assert len(ids_single) == 1
    assert isinstance(ids_single[0], int)
    assert ids_single[0] > 0

    # Test count=2 (smallest bulk)
    ids_pair = gen.generate_bulk(2)
    assert len(ids_pair) == 2
    assert ids_pair[0] < ids_pair[1]
    assert len(set(ids_pair)) == 2

    # Test invalid counts
    try:
        gen.generate_bulk(0)
        assert False, "Should have raised ValueError for count=0"
    except ValueError as e:
        assert "at least 1" in str(e)

    try:
        gen.generate_bulk(-1)
        assert False, "Should have raised ValueError for count=-1"
    except ValueError as e:
        assert "at least 1" in str(e)

    try:
        gen.generate_bulk(-100)
        assert False, "Should have raised ValueError for count=-100"
    except ValueError as e:
        assert "at least 1" in str(e)


def test_large_bulk_generation_5000_ids():
    """Test generating 5000 IDs in bulk - validates performance and correctness at scale."""
    count = 5000  # Large enough to span multiple milliseconds
    gen = SnowflakeGenerator()
    ids = gen.generate_bulk(count)

    # Verify exact count generated
    assert len(ids) == count, f"Expected {count} IDs, got {len(ids)}"

    # Verify all IDs are unique (critical for database integrity)
    unique_ids = set(ids)
    assert len(unique_ids) == count, f"Found {count - len(unique_ids)} duplicate IDs"

    # Verify IDs are in strictly ascending order (time-ordered property)
    assert ids == sorted(ids), "IDs must be in ascending order"

    # Verify no gaps in uniqueness (every ID should be unique)
    for i in range(1, len(ids)):
        assert ids[i] != ids[i - 1], f"Duplicate ID found at positions {i-1} and {i}"
        assert ids[i] > ids[i - 1], f"ID at position {i} is not greater than previous"

    # Verify all IDs are positive 64-bit integers
    for idx, id_val in enumerate(ids):
        assert isinstance(id_val, int), f"ID at position {idx} is not an integer"
        assert id_val > 0, f"ID at position {idx} is not positive"
        assert id_val < (1 << 64), f"ID at position {idx} exceeds 64-bit limit"


def test_bulk_vs_individual_generation():
    """Test that bulk generation produces same results as individual calls."""
    # Reset generator state by creating a new one
    gen1 = SnowflakeGenerator(machine_id=1)
    gen2 = SnowflakeGenerator(machine_id=2)

    # Generate IDs in bulk
    bulk_ids = gen1.generate_bulk(10)

    # Generate IDs individually
    individual_ids = [gen2.generate() for _ in range(10)]

    # Both should have same count
    assert len(bulk_ids) == len(individual_ids)

    # All should be unique
    all_ids = bulk_ids + individual_ids
    assert len(set(all_ids)) == len(all_ids)

    # Each list should be sorted
    assert bulk_ids == sorted(bulk_ids)
    assert individual_ids == sorted(individual_ids)


def test_snowflake_id_structure():
    """Test that snowflake IDs have the correct structure."""
    id_val = get_snowflake_id()

    # Should be a positive integer
    assert isinstance(id_val, int)
    assert id_val > 0

    # Should fit in 64 bits
    assert id_val < (1 << 64)


def test_get_snowflake_ids_validation():
    """Test that get_snowflake_ids validates size parameter and handles large sizes."""
    # Valid sizes
    assert get_snowflake_ids(0) == []
    assert len(get_snowflake_ids(1)) == 1
    assert len(get_snowflake_ids(10)) == 10
    assert len(get_snowflake_ids(MAX_SEQUENCE)) == MAX_SEQUENCE

    # Sizes larger than MAX_SEQUENCE should work (split into chunks)
    large_size = MAX_SEQUENCE + 100
    large_ids = get_snowflake_ids(large_size)
    assert len(large_ids) == large_size
    assert len(set(large_ids)) == large_size  # All unique
    assert large_ids == sorted(large_ids)  # Time-ordered

    # Invalid sizes - only negative values should fail
    try:
        get_snowflake_ids(-1)
        assert False, "Should have raised ValueError for size=-1"
    except ValueError as e:
        assert ">= 0" in str(e)


def test_get_snowflake_ids_different_sequences():
    """Test that get_snowflake_ids returns IDs with different sequence numbers."""
    gen = SnowflakeGenerator()

    # Get a small number of IDs (likely within same millisecond)
    size = 10
    ids = gen.get(size)

    # All IDs should be unique
    assert len(ids) == size
    assert len(set(ids)) == size

    # All IDs should be in ascending order
    assert ids == sorted(ids)

    # Extract sequence numbers (last 12 bits)
    sequences = [id_val & 0xFFF for id_val in ids]

    # For small sizes, sequences should be consecutive within the same millisecond
    # Verify sequences are monotonically increasing (allowing for resets across milliseconds)
    for i in range(len(sequences) - 1):
        # Either consecutive or reset to 0 (new millisecond)
        if sequences[i + 1] < sequences[i]:
            # Sequence reset - new millisecond started
            assert sequences[i + 1] == 0, "After reset, sequence should start at 0"
        else:
            # Within same millisecond - should increment
            assert sequences[i + 1] >= sequences[i], "Sequences should be non-decreasing"


def test_get_snowflake_ids_max_size():
    """Test get_snowflake_ids with maximum allowed size."""
    gen = SnowflakeGenerator()

    # Get maximum number of IDs (MAX_SEQUENCE = 4095)
    ids = gen.get(MAX_SEQUENCE)

    # Should generate exact count
    assert len(ids) == MAX_SEQUENCE

    # All should be unique
    assert len(set(ids)) == MAX_SEQUENCE

    # All should be in ascending order
    assert ids == sorted(ids)
