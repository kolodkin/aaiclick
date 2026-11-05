"""The capacity gate is a pure function over the Pod-state map."""

from __future__ import annotations

import pytest

from ..kubernetes_config import K8sLimits
from .pod_gate import GateDecision, gate
from .pod_watch import ContainerState, PodState

_WAITING = ContainerState("waiting", -1, None, None)
_RUNNING = ContainerState("running", -1, None, None)
_DONE = ContainerState("terminated", 0, None, None)
_NONE = ContainerState("", -1, None, None)


def _pod(job: int, *, scheduled: bool, task: ContainerState = _RUNNING, n: int = 1) -> list[PodState]:
    return [
        PodState(f"p-{job}-{i}", job, i, 0, "node" if scheduled else None, "", _NONE, task, False, None)
        for i in range(n)
    ]


LIMITS = K8sLimits(max_pending=2, max_pending_total=3, max_pods=None, pending_timeout=900.0)


@pytest.mark.parametrize(
    "states,limits,expected",
    [
        ([], LIMITS, GateDecision(True, frozenset())),
        (_pod(1, scheduled=False, task=_WAITING, n=1), LIMITS, GateDecision(True, frozenset())),
        (_pod(1, scheduled=False, task=_WAITING, n=2), LIMITS, GateDecision(True, frozenset({1}))),
        (
            _pod(1, scheduled=False, task=_WAITING, n=2) + _pod(2, scheduled=False, task=_WAITING, n=1),
            LIMITS,
            GateDecision(False, frozenset({1})),
        ),
        (_pod(1, scheduled=True, n=5), LIMITS, GateDecision(True, frozenset())),
        (_pod(1, scheduled=True, n=2), LIMITS._replace(max_pods=2), GateDecision(False, frozenset())),
        (_pod(1, scheduled=True, task=_DONE, n=2), LIMITS._replace(max_pods=2), GateDecision(True, frozenset())),
        (_pod(1, scheduled=False, task=_DONE, n=2), LIMITS, GateDecision(True, frozenset())),
    ],
    ids=[
        "empty",
        "one-pending",
        "job-saturated",
        "total-closed",
        "running-no-cap",
        "max-pods",
        "done-not-running",
        "done-not-pending",
    ],
)
def test_gate(states, limits, expected):
    assert gate(states, limits) == expected
