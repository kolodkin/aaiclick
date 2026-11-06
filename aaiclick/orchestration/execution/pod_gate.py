"""Capacity gate of the kubernetes worker.

See docs/designs/kubernetes_runner.md.

Unscheduled Pods are the backpressure signal: Kubernetes (and any autoscaler)
does the capacity accounting, the worker only stops claiming while too many of
its Pods wait for a node. The count is per job, so a job stuck on big nodes
does not block jobs that fit now.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import NamedTuple

from ..kubernetes_config import K8sLimits
from .pod_watch import PodState


class GateDecision(NamedTuple):
    open: bool
    """Whether any claim is allowed this tick."""
    blocked_jobs: frozenset[int]
    """Jobs at their unscheduled-Pod cap; the claim query skips their tasks."""


def gate(states: Iterable[PodState], limits: K8sLimits) -> GateDecision:
    """Pure: ``blocked_jobs`` are jobs with ≥ ``max_pending`` unscheduled Pods;
    the gate closes when unscheduled Pods reach ``max_pending_total`` or live
    (not done) Pods reach ``max_pods`` — a just-created Pod counts, so a burst
    cannot overshoot the cap. Done Pods count toward nothing."""
    live = [state for state in states if not state.done]
    pending = Counter(state.job_id for state in live if state.unscheduled)
    blocked = frozenset(job_id for job_id, n in pending.items() if n >= limits.max_pending)
    closed = sum(pending.values()) >= limits.max_pending_total or (
        limits.max_pods is not None and len(live) >= limits.max_pods
    )
    return GateDecision(not closed, blocked)
