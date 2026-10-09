"""Tests for GHCR image tag formatting (pure function: the tag string is the contract)."""

from __future__ import annotations

import pytest

from .ghcr import image_tag


@pytest.mark.parametrize(
    "version, expected",
    [
        pytest.param("1.2.3", "v1.2.3", id="release"),
        pytest.param("0.0.1.dev50+gfc8c68213.d20261008", "v0.0.1.dev50", id="dev-local-segment-dropped"),
    ],
)
def test_image_tag(version, expected):
    assert image_tag(version) == expected
