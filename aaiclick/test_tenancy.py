"""Tests for the active-tenant contextvar helpers."""

from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant, get_active_tenant_id


def test_default_tenant_when_unset():
    assert get_active_tenant_id() == DEFAULT_TENANT_ID


def test_active_tenant_sets_and_restores():
    with active_tenant(42):
        assert get_active_tenant_id() == 42
    assert get_active_tenant_id() == DEFAULT_TENANT_ID


def test_active_tenant_nests():
    with active_tenant(2):
        with active_tenant(3):
            assert get_active_tenant_id() == 3
        assert get_active_tenant_id() == 2
