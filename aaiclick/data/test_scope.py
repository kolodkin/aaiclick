"""Tests for aaiclick.data.scope helpers."""

from __future__ import annotations

import pytest

from aaiclick.data.scope import (
    JOB_SCOPED_RE,
    is_persistent_table,
    make_persistent_table_name,
    scope_of,
)


def test_scope_of_temp():
    assert scope_of("t_123") == "temp"
    assert scope_of("t_999999999999") == "temp"


def test_scope_of_global():
    assert scope_of("p_foo") == "global"
    assert scope_of("p_user_catalog") == "global"


def test_scope_of_job():
    assert scope_of("j_42_bar") == "job"
    assert scope_of("j_1234567890_my_table") == "job"


def test_scope_of_miscellaneous_prefixes_are_temp():
    # Regex requires digits + underscore after 'j_' to qualify as job-scoped.
    assert scope_of("j_nodigit_foo") == "temp"
    assert scope_of("j_42nopost") == "temp"
    assert scope_of("other_table") == "temp"


def test_is_persistent_table():
    assert is_persistent_table("p_foo") is True
    assert is_persistent_table("j_1_bar") is True
    assert is_persistent_table("t_100") is False
    assert is_persistent_table("anything_else") is False


def test_job_scoped_regex():
    assert JOB_SCOPED_RE.match("j_1_a")
    assert JOB_SCOPED_RE.match("j_987654321_table_name")
    assert not JOB_SCOPED_RE.match("j_bar")
    assert not JOB_SCOPED_RE.match("p_1_x")


def test_make_persistent_table_name_global():
    assert make_persistent_table_name("global", "foo") == "p_foo"


def test_make_persistent_table_name_job():
    assert make_persistent_table_name("job", "foo", job_id=42) == "j_42_foo"


def test_make_persistent_table_name_job_requires_job_id():
    with pytest.raises(ValueError, match="scope='job' requires a job_id"):
        make_persistent_table_name("job", "foo")
