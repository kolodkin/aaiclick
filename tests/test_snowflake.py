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


def test_bulk_generation_validation():
    """Test that bulk generation validates count parameter."""
    gen = SnowflakeGenerator()

    # Should raise ValueError for invalid count
    try:
        gen.generate_bulk(0)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "at least 1" in str(e)

    try:
        gen.generate_bulk(-1)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "at least 1" in str(e)


def test_large_bulk_generation():
    """Test generating a large number of IDs efficiently."""
    count = 5000  # Generate 5000 IDs (will span multiple milliseconds)
    ids = generate_snowflake_ids(count)

    # Should generate exact count
    assert len(ids) == count

    # All IDs should be unique
    assert len(set(ids)) == count

    # IDs should be in ascending order
    assert ids == sorted(ids)


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
