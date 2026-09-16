"""Tests for the ``AAICLICK_JOB_WAIT_TIMEOUT`` accessor."""

import pytest

from aaiclick.orchestration.env import DEFAULT_JOB_WAIT_TIMEOUT, ENV_JOB_WAIT_TIMEOUT, job_wait_timeout


def test_defaults_to_one_hour_when_env_unset(monkeypatch):
    monkeypatch.delenv(ENV_JOB_WAIT_TIMEOUT, raising=False)
    assert job_wait_timeout() == DEFAULT_JOB_WAIT_TIMEOUT == 3600.0


@pytest.mark.parametrize(
    "env_value, expected",
    [
        pytest.param("600", 600.0, id="integer-seconds"),
        pytest.param("0.5", 0.5, id="fractional-seconds"),
        # 0 means "check once" — a legitimate value, not an unset sentinel.
        pytest.param("0", 0.0, id="zero-checks-once"),
    ],
)
def test_env_var_overrides_default(monkeypatch, env_value, expected):
    monkeypatch.setenv(ENV_JOB_WAIT_TIMEOUT, env_value)
    assert job_wait_timeout() == expected


def test_read_at_call_time_not_import_time(monkeypatch):
    """Freezing it at import would ignore every later change."""
    monkeypatch.setenv(ENV_JOB_WAIT_TIMEOUT, "111")
    assert job_wait_timeout() == 111.0
    monkeypatch.setenv(ENV_JOB_WAIT_TIMEOUT, "222")
    assert job_wait_timeout() == 222.0


def test_invalid_value_raises(monkeypatch):
    monkeypatch.setenv(ENV_JOB_WAIT_TIMEOUT, "banana")
    with pytest.raises(ValueError):
        job_wait_timeout()
