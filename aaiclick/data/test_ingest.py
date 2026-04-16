"""
Tests for ingest module internals - type compatibility checking functions.

Tests _are_types_compatible (strict, for UNION ALL) and _are_types_castable
(permissive, for INSERT with CAST).
"""

from aaiclick.data.ingest import _are_types_castable, _are_types_compatible

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
