"""Tests for aaiclick.data.scope helpers."""

from __future__ import annotations

import pytest

from aaiclick.data.scope import (
    JOB_SCOPED_RE,
    TEMP_NAMED_RE,
    is_persistent_table,
    make_scoped_table_name,
    name_from_table,
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
        pytest.param("p_foo", "global", id="global"),
        pytest.param("p_user_catalog", "global", id="global-multi-part"),
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
    assert is_persistent_table("p_foo") is True
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


def test_make_scoped_table_name_global():
    assert make_scoped_table_name("global", "foo") == "p_foo"
    # Backward compatibility: the default tenant keeps the bare form, so
    # existing deployments never need a table rename.
    assert make_scoped_table_name("global", "foo", tenant_id=DEFAULT_TENANT_ID) == "p_foo"


def test_make_scoped_table_name_job():
    assert make_scoped_table_name("job", "foo", job_id=42) == "j_42_foo"


def test_make_scoped_table_name_temp_named():
    assert make_scoped_table_name("temp_named", "foo", snowid=42) == "t_foo_42"


@pytest.mark.parametrize(
    "scope, match",
    [
        pytest.param("job", "scope='job' requires a job_id", id="job-without-job-id"),
        pytest.param("temp_named", "scope='temp_named' requires a snowid", id="temp-named-without-snowid"),
    ],
)
def test_make_scoped_table_name_missing_required_id_raises(scope, match):
    with pytest.raises(ValueError, match=match):
        make_scoped_table_name(scope, "foo")


@pytest.mark.parametrize(
    "table, expected",
    [
        pytest.param("p_orders", "orders", id="global"),
        pytest.param("p_user_catalog", "user_catalog", id="global-multi-part"),
        pytest.param("j_12345_staging", "staging", id="job"),
        pytest.param("j_12345_multi_part_name", "multi_part_name", id="job-multi-part"),
        pytest.param("t_orders_42", "orders", id="temp-named"),
        pytest.param("t_my_table_999999999999", "my_table", id="temp-named-multi-part"),
        # A plain temp table carries no name, so the table itself is returned.
        pytest.param("t_9999999999", "t_9999999999", id="temp-falls-back-to-table"),
    ],
)
def test_name_from_table(table, expected):
    assert name_from_table(table) == expected


def test_other_tenants_get_a_prefixed_global_name():
    """The tenant-unique physical name is what stops a cross-tenant overwrite."""
    table = make_scoped_table_name("global", "sales", tenant_id=7)
    assert table == "p_7_sales"
    assert scope_of(table) == "global"
    assert name_from_table(table) == "sales"


def test_leading_underscore_name_does_not_look_tenant_prefixed():
    """``_5_x`` is a legal name; ``p__5_x`` must not strip as a tenant prefix."""
    table = make_scoped_table_name("global", "_5_x", tenant_id=DEFAULT_TENANT_ID)
    assert table == "p__5_x"
    assert name_from_table(table) == "_5_x"


def test_job_and_temp_names_never_take_a_tenant_prefix():
    """They reach their tenant through the owning job — no stacking."""
    assert make_scoped_table_name("job", "x", job_id=42, tenant_id=7) == "j_42_x"
    assert make_scoped_table_name("temp_named", "x", snowid=99, tenant_id=7) == "t_x_99"
