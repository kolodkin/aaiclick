"""Tests for resolve_preserve() — precedence: explicit > registered > None."""

import pytest

from aaiclick.orchestration.factories import resolve_preserve


def test_explicit_none_falls_through_to_registered():
    assert resolve_preserve(explicit=None, registered=["a"]) == ["a"]


def test_explicit_list_overrides_registered():
    assert resolve_preserve(explicit=["a"], registered=["b"]) == ["a"]


def test_explicit_empty_list_is_explicit_no_preserve():
    assert resolve_preserve(explicit=[], registered=["b"]) == []


def test_explicit_star_overrides_registered():
    assert resolve_preserve(explicit="*", registered=["b"]) == "*"


def test_registered_none_returns_none():
    assert resolve_preserve(explicit=None, registered=None) is None


def test_registered_star_passes_through():
    assert resolve_preserve(explicit=None, registered="*") == "*"


def test_explicit_invalid_type_raises():
    with pytest.raises(TypeError, match="preserve"):
        resolve_preserve(explicit=42, registered=None)  # type: ignore[arg-type]


def test_explicit_list_with_non_str_raises():
    with pytest.raises(TypeError, match="preserve list"):
        resolve_preserve(explicit=["a", 1], registered=None)  # type: ignore[list-item]


def test_returns_defensive_copy_of_explicit_list():
    src = ["a", "b"]
    result = resolve_preserve(explicit=src, registered=None)
    assert result == src
    assert result is not src


def test_returns_defensive_copy_of_registered_list():
    src = ["a", "b"]
    result = resolve_preserve(explicit=None, registered=src)
    assert result == src
    assert result is not src
