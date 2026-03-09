"""
Tests for type compatibility logic - validates _are_types_compatible (strict)
and _are_types_castable (permissive) against actual ClickHouse behavior.

These are simple flat DB-level query tests that verify ClickHouse's UNION ALL
and CAST behavior matches our compatibility functions.
"""

import pytest

from aaiclick import create_object_from_value

from aaiclick.data.ingest import _are_types_compatible, _are_types_castable


# =============================================================================
# _are_types_compatible (strict - for UNION ALL without CAST)
# =============================================================================


def test_compatible_same_type():
    """Same types are always compatible."""
    assert _are_types_compatible("Int64", "Int64")
    assert _are_types_compatible("Float64", "Float64")
    assert _are_types_compatible("String", "String")


def test_compatible_int_to_int():
    """Different integer types are compatible (ClickHouse UNION ALL allows this)."""
    assert _are_types_compatible("Int64", "Int32")
    assert _are_types_compatible("Int32", "Int64")
    assert _are_types_compatible("UInt8", "Int64")


def test_compatible_float_to_float():
    """Different float types are compatible."""
    assert _are_types_compatible("Float64", "Float32")
    assert _are_types_compatible("Float32", "Float64")


def test_incompatible_int_to_float():
    """Int and Float are NOT compatible for UNION ALL without CAST."""
    assert not _are_types_compatible("Int64", "Float64")
    assert not _are_types_compatible("Float64", "Int64")
    assert not _are_types_compatible("Int32", "Float32")
    assert not _are_types_compatible("Float32", "UInt64")


def test_incompatible_numeric_to_string():
    """Numeric and String types are never compatible."""
    assert not _are_types_compatible("Int64", "String")
    assert not _are_types_compatible("String", "Int64")
    assert not _are_types_compatible("Float64", "String")
    assert not _are_types_compatible("String", "Float64")


def test_incompatible_string_types():
    """String and FixedString are not compatible (different types)."""
    assert not _are_types_compatible("String", "FixedString")
    assert not _are_types_compatible("FixedString", "String")


# =============================================================================
# _are_types_castable (permissive - for INSERT with explicit CAST)
# =============================================================================


def test_castable_same_type():
    """Same types are always castable."""
    assert _are_types_castable("Int64", "Int64")
    assert _are_types_castable("Float64", "Float64")
    assert _are_types_castable("String", "String")


def test_castable_int_to_float():
    """Int to Float is castable (explicit CAST works)."""
    assert _are_types_castable("Float64", "Int64")
    assert _are_types_castable("Float32", "Int32")


def test_castable_float_to_int():
    """Float to Int is castable (explicit CAST truncates)."""
    assert _are_types_castable("Int64", "Float64")
    assert _are_types_castable("Int32", "Float32")


def test_castable_int_to_int():
    """Different integer types are castable."""
    assert _are_types_castable("Int64", "Int32")
    assert _are_types_castable("UInt8", "Int64")


def test_not_castable_numeric_to_string():
    """Numeric to String is not castable."""
    assert not _are_types_castable("Int64", "String")
    assert not _are_types_castable("String", "Int64")
    assert not _are_types_castable("Float64", "String")


# =============================================================================
# DB-level tests: verify ClickHouse behavior matches our functions
# =============================================================================


async def test_db_concat_same_int_type(ctx):
    """Concat of same int types succeeds (UNION ALL, no CAST)."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])
    result = await a.concat(b)
    data = await result.data()
    assert sorted(data) == [1, 2, 3, 4, 5, 6]


async def test_db_concat_same_float_type(ctx):
    """Concat of same float types succeeds."""
    a = await create_object_from_value([1.0, 2.0])
    b = await create_object_from_value([3.0, 4.0])
    result = await a.concat(b)
    data = await result.data()
    assert sorted(data) == pytest.approx([1.0, 2.0, 3.0, 4.0])


async def test_db_concat_int_and_float_fails(ctx):
    """Concat of Int64 and Float64 fails - types are incompatible for UNION ALL."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([1.5, 2.5, 3.5])
    with pytest.raises(ValueError, match="incompatible type"):
        await a.concat(b)


async def test_db_concat_int_and_string_fails(ctx):
    """Concat of Int64 and String fails."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value(["x", "y", "z"])
    with pytest.raises(ValueError, match="incompatible type"):
        await a.concat(b)


async def test_db_concat_float_and_string_fails(ctx):
    """Concat of Float64 and String fails."""
    a = await create_object_from_value([1.0, 2.0])
    b = await create_object_from_value(["x", "y"])
    with pytest.raises(ValueError, match="incompatible type"):
        await a.concat(b)


async def test_db_insert_int_into_float(ctx):
    """Insert Int64 into Float64 target succeeds (explicit CAST)."""
    target = await create_object_from_value([1.0, 2.0, 3.0])
    source = await create_object_from_value([4, 5, 6])
    await target.insert(source)
    data = await target.data()
    assert len(data) == 6
    assert sorted(data) == pytest.approx([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])


async def test_db_insert_float_into_int(ctx):
    """Insert Float64 into Int64 target succeeds (explicit CAST truncates)."""
    target = await create_object_from_value([1, 2, 3])
    source = await create_object_from_value([4.7, 5.2, 6.9])
    await target.insert(source)
    data = await target.data()
    assert len(data) == 6


async def test_db_insert_string_into_int_fails(ctx):
    """Insert String into Int64 target fails."""
    target = await create_object_from_value([1, 2, 3])
    source = await create_object_from_value(["x", "y"])
    with pytest.raises(ValueError, match="incompatible"):
        await target.insert(source)
