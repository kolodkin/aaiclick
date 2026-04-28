"""Tests for resolve_preserve_all() — precedence: explicit > registered > False."""

from aaiclick.orchestration.factories import resolve_preserve_all


def test_omitted_falls_through_to_registered_true():
    assert resolve_preserve_all(registered=True) is True


def test_omitted_falls_through_to_registered_false():
    assert resolve_preserve_all(registered=False) is False


def test_explicit_true_overrides_registered_false():
    assert resolve_preserve_all(explicit=True, registered=False) is True


def test_explicit_false_overrides_registered_true():
    assert resolve_preserve_all(explicit=False, registered=True) is False


def test_default_returns_false():
    assert resolve_preserve_all() is False
