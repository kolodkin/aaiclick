"""Tests for the private parsing helper in ``aaiclick.data.view_models``.

The ``*_to_view`` adapters are exercised end-to-end by
``aaiclick/internal_api/test_objects.py``; ``scope_of`` has its own
tests in ``aaiclick/data/test_scope.py``. Only ``_object_name_from_table``
needs dedicated tests for its parsing branches.
"""

from .view_models import _object_name_from_table


def test_object_name_from_table_global():
    assert _object_name_from_table("p_orders") == "orders"


def test_object_name_from_table_job_scoped():
    assert _object_name_from_table("j_12345_staging") == "staging"
    assert _object_name_from_table("j_12345_multi_part_name") == "multi_part_name"


def test_object_name_from_table_temp_falls_back_to_table():
    assert _object_name_from_table("t_9999999999") == "t_9999999999"
