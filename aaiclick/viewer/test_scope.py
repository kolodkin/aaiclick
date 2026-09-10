from __future__ import annotations

import pytest

from aaiclick.viewer.scope import ScopeRef, parse_scope, scope_key


@pytest.mark.parametrize(
    "key, expected",
    [
        pytest.param("persistent", ScopeRef("persistent", None), id="persistent"),
        pytest.param("job:123", ScopeRef("job", 123), id="job-id"),
        pytest.param("job:nightly_etl", ScopeRef("job", "nightly_etl"), id="job-name"),
    ],
)
def test_parse_scope(key, expected):
    assert parse_scope(key) == expected
    assert scope_key(expected) == key


@pytest.mark.parametrize("key", ["", "global", "job:", "job", "task:1"])
def test_parse_scope_rejects(key):
    with pytest.raises(ValueError):
        parse_scope(key)
