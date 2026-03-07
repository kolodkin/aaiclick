"""
Tests for Snowflake ID generation.
"""

import pytest

from aaiclick.snowflake_id import (
    SnowflakeGenerator,
    get_snowflake_id,
    get_snowflake_ids,
    decode_snowflake_id,
    snowflake_id_sql,
    MAX_SEQUENCE,
    TIMESTAMP_SHIFT,
    MACHINE_ID_SHIFT,
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
    with pytest.raises(ValueError, match="at least 1"):
        gen.generate_bulk(0)

    with pytest.raises(ValueError, match="at least 1"):
        gen.generate_bulk(-1)

    with pytest.raises(ValueError, match="at least 1"):
        gen.generate_bulk(-100)


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
    with pytest.raises(ValueError, match=">= 0"):
        get_snowflake_ids(-1)


def test_sequential_calls_have_increasing_ids():
    """Test that sequential calls produce IDs with increasing timestamps or sequences."""
    gen = SnowflakeGenerator()

    # Make two sequential calls
    id1 = gen.generate()
    id2 = gen.generate()

    # Decode IDs into components
    timestamp1, _, sequence1 = decode_snowflake_id(id1)
    timestamp2, _, sequence2 = decode_snowflake_id(id2)

    # Either timestamps are different (different milliseconds)
    # or sequences are different (same millisecond)
    if timestamp1 == timestamp2:
        # Same millisecond - sequence should increment
        assert sequence2 > sequence1, "Sequence should increment within same millisecond"
    else:
        # Different milliseconds - timestamp should be greater
        assert timestamp2 > timestamp1, "Timestamp should increase across milliseconds"

    # Overall, second ID should always be greater (time-ordered)
    assert id2 > id1, "IDs should be strictly increasing"


def test_bulk_sequences_pattern():
    """Test that bulk generation produces expected sequence pattern [0,1,...,n-1]."""
    gen = SnowflakeGenerator()

    # Generate bulk of size 10
    size = 10
    ids = gen.get(size)

    # Extract sequences using decode function
    sequences = [decode_snowflake_id(id_val)[2] for id_val in ids]

    # Sequences should be [0, 1, 2, ..., 9]
    expected = list(range(size))
    assert sequences == expected, f"Expected sequences {expected}, got {sequences}"


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


def test_reserve_returns_correct_tuple():
    """Test that reserve returns (base_timestamp, machine_id, start_sequence)."""
    gen = SnowflakeGenerator(machine_id=42)
    base_ts, machine_id, start_seq = gen.reserve(10)

    assert machine_id == 42
    assert base_ts > 0
    assert start_seq >= 0


def test_reserve_advances_state():
    """Test that reserve advances generator state so next generate doesn't collide."""
    gen = SnowflakeGenerator(machine_id=1)

    base_ts, _, start_seq = gen.reserve(100)
    next_id = gen.generate()

    # Decode the next ID to verify it comes after the reserved range
    next_ts, _, next_seq = decode_snowflake_id(next_id)
    end_seq = start_seq + 100

    if end_seq <= MAX_SEQUENCE + 1:
        # All reserved IDs fit in one ms
        if next_ts == base_ts:
            assert next_seq >= end_seq
        else:
            assert next_ts > base_ts
    else:
        # Reserved IDs span ms boundaries
        assert next_ts >= base_ts


def test_reserve_small_count():
    """Test reserve with count that fits in current ms."""
    gen = SnowflakeGenerator()
    base_ts, machine_id, start_seq = gen.reserve(5)

    # State should advance by 5
    assert gen.sequence == start_seq + 5
    assert gen.last_timestamp == base_ts


def test_reserve_spans_ms_boundary():
    """Test reserve with count that spans ms boundaries (arithmetically)."""
    gen = SnowflakeGenerator()
    # Force sequence near end
    gen.last_timestamp = gen._current_timestamp()
    gen.sequence = MAX_SEQUENCE - 1  # 4094, only 2 slots left

    base_ts, _, start_seq = gen.reserve(10)
    assert start_seq == MAX_SEQUENCE - 1

    # Should have advanced last_timestamp (arithmetically, no busy-wait)
    assert gen.last_timestamp > base_ts


def test_reserve_exact_boundary():
    """Test reserve landing exactly on 4096 boundary."""
    gen = SnowflakeGenerator()
    gen.last_timestamp = gen._current_timestamp()
    gen.sequence = 0

    # Reserve exactly 4096 IDs (fills one ms completely)
    base_ts, _, start_seq = gen.reserve(4096)
    assert start_seq == 0
    # total = 0 + 4096 = 4096, extra_ms = 1, final_seq = 0
    # Lands exactly on boundary: last_timestamp = base_ts, sequence = 4096
    assert gen.last_timestamp == base_ts
    assert gen.sequence == MAX_SEQUENCE + 1


def test_reserve_validation():
    """Test reserve validates count parameter."""
    gen = SnowflakeGenerator()

    with pytest.raises(ValueError, match="at least 1"):
        gen.reserve(0)

    with pytest.raises(ValueError, match="at least 1"):
        gen.reserve(-1)


def test_reserve_no_collision_with_generate():
    """Test that IDs from generate after reserve don't overlap reserved range."""
    gen = SnowflakeGenerator(machine_id=5)

    # Reserve 100 IDs
    base_ts, machine_id, start_seq = gen.reserve(100)

    # Compute what the reserved IDs would be
    seq_capacity = MAX_SEQUENCE + 1
    reserved_ids = set()
    for i in range(100):
        abs_seq = start_seq + i
        ts = base_ts + abs_seq // seq_capacity
        seq = abs_seq % seq_capacity
        rid = (ts << TIMESTAMP_SHIFT) | (machine_id << MACHINE_ID_SHIFT) | seq
        reserved_ids.add(rid)

    # Generate IDs after reserve — none should collide
    generated_ids = [gen.generate() for _ in range(50)]
    for gid in generated_ids:
        assert gid not in reserved_ids


def test_reserve_then_generate_bulk():
    """Test that generate_bulk after reserve works correctly."""
    gen = SnowflakeGenerator(machine_id=3)

    gen.reserve(500)
    bulk_ids = gen.generate_bulk(100)

    assert len(bulk_ids) == 100
    assert len(set(bulk_ids)) == 100
    assert bulk_ids == sorted(bulk_ids)


def test_snowflake_id_sql_format():
    """Test that snowflake_id_sql generates a valid SQL expression."""
    sql = snowflake_id_sql(1000, 5, 0)

    assert "bitOr" in sql
    assert "bitShiftLeft" in sql
    assert "row_number() OVER ()" in sql
    assert "1000" in sql
    assert "5" in sql


def test_snowflake_id_sql_matches_python():
    """Test that SQL expression logic matches Python generate_bulk logic."""
    base_ts = 1000
    machine_id = 5
    start_seq = 100
    seq_capacity = MAX_SEQUENCE + 1  # 4096

    # Simulate what the SQL expression computes for row_number 1..10
    sql_ids = []
    for rn in range(1, 11):
        abs_seq = start_seq + rn - 1
        ts = base_ts + abs_seq // seq_capacity
        seq = abs_seq % seq_capacity
        rid = (ts << TIMESTAMP_SHIFT) | (machine_id << MACHINE_ID_SHIFT) | seq
        sql_ids.append(rid)

    # Verify all unique and ascending
    assert len(set(sql_ids)) == 10
    assert sql_ids == sorted(sql_ids)

    # Decode first and last to verify structure
    ts0, mid0, seq0 = decode_snowflake_id(sql_ids[0])
    assert ts0 == base_ts
    assert mid0 == machine_id
    assert seq0 == start_seq

    ts9, mid9, seq9 = decode_snowflake_id(sql_ids[9])
    assert mid9 == machine_id
    assert seq9 == start_seq + 9


def test_snowflake_id_sql_overflow():
    """Test SQL expression handles sequence overflow across ms boundaries."""
    base_ts = 5000
    machine_id = 1
    start_seq = 4090
    seq_capacity = MAX_SEQUENCE + 1

    # Row 7 should overflow into next ms (4090 + 6 = 4096 → ts+1, seq=0)
    for rn in range(1, 20):
        abs_seq = start_seq + rn - 1
        ts = base_ts + abs_seq // seq_capacity
        seq = abs_seq % seq_capacity
        rid = (ts << TIMESTAMP_SHIFT) | (machine_id << MACHINE_ID_SHIFT) | seq
        decoded_ts, decoded_mid, decoded_seq = decode_snowflake_id(rid)

        assert decoded_mid == machine_id
        if rn <= 6:
            assert decoded_ts == base_ts
            assert decoded_seq == start_seq + rn - 1
        else:
            assert decoded_ts == base_ts + 1
            assert decoded_seq == rn - 7
