"""
Tests for ``is_input_task``.
"""

from __future__ import annotations

from aaiclick.orchestration.lineage import is_input_task
from aaiclick.orchestration.models import Task


def _task(result) -> Task:
    return Task(
        id=1,
        job_id=1,
        entrypoint="mymodule.fn",
        name="fn",
        kwargs={},
        result=result,
    )


def test_unrun_task_is_not_input():
    assert is_input_task(_task(None)) is False


def test_persistent_object_result_is_input():
    task = _task(
        {
            "object_type": "object",
            "table": "p_kev_catalog",
            "persistent": True,
        }
    )
    assert is_input_task(task) is True


def test_ephemeral_object_result_is_not_input():
    task = _task(
        {
            "object_type": "object",
            "table": "t_7449116846344634368",
        }
    )
    assert is_input_task(task) is False


def test_persistent_flag_false_is_not_input():
    task = _task(
        {
            "object_type": "object",
            "table": "p_kev_catalog",
            "persistent": False,
        }
    )
    assert is_input_task(task) is False


def test_upstream_ref_is_not_input():
    task = _task({"ref_type": "upstream", "task_id": 42})
    assert is_input_task(task) is False


def test_native_value_is_not_input():
    task = _task({"native_value": 42})
    assert is_input_task(task) is False


def test_pydantic_result_is_not_input():
    task = _task({"pydantic_type": "mymodule.Model", "data": {"x": 1}})
    assert is_input_task(task) is False


def test_non_p_prefix_despite_flag():
    # Defensive: the `p_` prefix is the ground truth; a missing prefix is
    # always wrong even when the flag says persistent.
    task = _task(
        {
            "object_type": "object",
            "table": "t_foo",
            "persistent": True,
        }
    )
    assert is_input_task(task) is False
