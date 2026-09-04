"""Tests for aaiclick.data.scope helpers."""

from __future__ import annotations

import pytest

from aaiclick.data.scope import (
    JOB_SCOPED_RE,
    TEMP_NAMED_RE,
    is_persistent_table,
    legacy_global_name,
    make_scoped_table_name,
    scope_of,
)
from aaiclick.tenancy import DEFAULT_TENANT_ID


@pytest.mark.parametrize(
    "table, expected",
    [
        pytest.param("t_123", "temp", id="temp"),
        pytest.param("t_999999999999", "temp", id="temp-long-snowid"),
        pytest.param("t_orders_123", "temp_named", id="temp-named"),
        pytest.param("t_my_table_999999999999", "temp_named", id="temp-named-multi-part"),
        pytest.param("t_CamelCase_42", "temp_named", id="temp-named-camel-case"),
        pytest.param("p_7358932045123", "global", id="global"),
        # Legacy tables created before names moved into the registry.
        pytest.param("p_user_catalog", "global", id="global-legacy-name"),
        pytest.param("j_42_bar", "job", id="job"),
        pytest.param("j_1234567890_my_table", "job", id="job-multi-part"),
        # The regex requires digits + underscore after 'j_' to qualify as job-scoped.
        pytest.param("j_nodigit_foo", "temp", id="job-prefix-without-digits"),
        pytest.param("j_42nopost", "temp", id="job-prefix-without-suffix"),
        pytest.param("other_table", "temp", id="unrecognised-prefix"),
    ],
)
def test_scope_of(table, expected):
    assert scope_of(table) == expected


def test_is_persistent_table():
    assert is_persistent_table("p_100") is True
    assert is_persistent_table("j_1_bar") is True
    assert is_persistent_table("t_100") is False
    assert is_persistent_table("t_named_100") is False
    assert is_persistent_table("anything_else") is False


def test_job_scoped_regex():
    assert JOB_SCOPED_RE.match("j_1_a")
    assert JOB_SCOPED_RE.match("j_987654321_table_name")
    assert not JOB_SCOPED_RE.match("j_bar")
    assert not JOB_SCOPED_RE.match("p_1_x")


def test_temp_named_regex():
    assert TEMP_NAMED_RE.match("t_foo_42")
    assert TEMP_NAMED_RE.match("t_my_table_987654321")
    # Trailing must be digits
    assert not TEMP_NAMED_RE.match("t_foo_bar")
    # Pure digits is plain temp, not temp_named
    assert not TEMP_NAMED_RE.match("t_42")
    # Name must start with letter/underscore
    assert not TEMP_NAMED_RE.match("t_1foo_42")


def test_make_scoped_table_name_global_is_opaque():
    """The name is not encoded — the registry maps it to the table."""
    assert make_scoped_table_name("global", "foo", snowid=42) == "p_42"


def test_make_scoped_table_name_job():
    assert make_scoped_table_name("job", "foo", job_id=42) == "j_42_foo"


def test_make_scoped_table_name_temp_named():
    assert make_scoped_table_name("temp_named", "foo", snowid=42) == "t_foo_42"


@pytest.mark.parametrize(
    "scope, match",
    [
        pytest.param("job", "scope='job' requires a job_id", id="job-without-job-id"),
        pytest.param("temp_named", "scope='temp_named' requires a snowid", id="temp-named-without-snowid"),
        pytest.param("global", "scope='global' requires a snowid", id="global-without-snowid"),
    ],
)
def test_make_scoped_table_name_missing_required_id_raises(scope, match):
    with pytest.raises(ValueError, match=match):
        make_scoped_table_name(scope, "foo")


@pytest.mark.parametrize(
    "table, tenant_id, expected",
    [
        pytest.param("p_orders", DEFAULT_TENANT_ID, "orders", id="default-tenant"),
        pytest.param("p_7_sales", 7, "sales", id="tenant-prefixed"),
        # A default-tenant name could legally start with an underscore-digit run.
        pytest.param("p__5_x", DEFAULT_TENANT_ID, "_5_x", id="leading-underscore-not-a-tenant"),
        # Another tenant's prefix is not stripped for this tenant.
        pytest.param("p_7_sales", 8, None, id="foreign-tenant-prefix-fails-identifier"),
        # Today's opaque names carry no name to recover.
        pytest.param("p_7501679039461998598", DEFAULT_TENANT_ID, None, id="opaque"),
        pytest.param("j_42_x", DEFAULT_TENANT_ID, None, id="not-global"),
    ],
)
def test_legacy_global_name(table, tenant_id, expected):
    assert legacy_global_name(table, tenant_id, DEFAULT_TENANT_ID) == expected
