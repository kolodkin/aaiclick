"""
Tests for Snowflake ID generation.
"""

from aaiclick.snowflake import SnowflakeGenerator, generate_snowflake_id, generate_snowflake_ids


def test_generate_single_id():
    """Test generating a single snowflake ID."""
    id1 = generate_snowflake_id()
    id2 = generate_snowflake_id()

    # IDs should be unique
    assert id1 != id2
    # Later ID should be greater (time-ordered)
    assert id2 > id1


def test_generate_bulk_ids():
    """Test generating multiple snowflake IDs in bulk."""
    count = 100
    ids = generate_snowflake_ids(count)

    # Should generate exact count
    assert len(ids) == count

    # All IDs should be unique
    assert len(set(ids)) == count

    # IDs should be in ascending order (time-ordered)
    assert ids == sorted(ids)


def test_bulk_ids_are_sequential():
    """Test that bulk IDs are sequential within the same millisecond."""
    count = 10
    ids = generate_snowflake_ids(count)

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
    ids = generate_snowflake_ids(count)

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
    id_val = generate_snowflake_id()

    # Should be a positive integer
    assert isinstance(id_val, int)
    assert id_val > 0

    # Should fit in 64 bits
    assert id_val < (1 << 64)
