"""Tests for the shared Pod watch: Pod JSON → ``PodState``, per-Pod stage
futures, the dispatcher wake event, resync, and the watch-stream decoder.
Everything goes through ``PodWatch.apply`` with literal Pod JSON — no kubectl."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from . import pod_watch
from .pod_watch import POD_NOT_FOUND, PodWatch, decode_stream, parse_pod, pod_labels

NAME = "aaiclick-task-7-2"


def _pod(
    *,
    node: str | None = "node-a",
    phase: str = "Running",
    task: dict | None = None,
    io_in: dict | None = None,
    name: str = NAME,
    conditions: list[dict] | None = None,
) -> dict:
    status: dict = {"phase": phase, "containerStatuses": [{"name": "task", "state": task or {"running": {}}}]}
    if io_in is not None:
        status["initContainerStatuses"] = [{"name": "io-in", "state": io_in}]
    if conditions is not None:
        status["conditions"] = conditions
    spec: dict = {"containers": [{"name": "task"}]}
    if node:
        spec["nodeName"] = node
    return {"metadata": {"name": name, "labels": pod_labels(7, 3, 2)}, "spec": spec, "status": status}


def test_parse_unscheduled_pod():
    unschedulable = [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "0/3 nodes"}]
    state = parse_pod(_pod(node=None, phase="Pending", task={}, conditions=unschedulable))
    assert (state.task_id, state.job_id, state.run_epoch) == (7, 3, 2)
    assert state.unscheduled and not state.running and not state.done
    assert "0/3 nodes" in state.not_started_reason()


def test_parse_shell_pod_container_states():
    state = parse_pod(_pod(phase="Pending", io_in={"running": {}}, task={"waiting": {"reason": "PodInitializing"}}))
    assert state.io_in.state == "running"
    assert state.task.state == "waiting" and state.task.reason == "PodInitializing"
    assert not state.done and state.running


def test_parse_image_pull_backoff_reason():
    waiting = {"waiting": {"reason": "ImagePullBackOff", "message": "Back-off pulling image"}}
    state = parse_pod(_pod(phase="Pending", task=waiting))
    assert state.not_started_reason() == "ImagePullBackOff: Back-off pulling image"


def test_done_when_task_container_terminated_even_if_pod_running():
    state = parse_pod(_pod(phase="Running", task={"terminated": {"exitCode": 3}}))
    assert state.done and state.task.exit_code == 3 and not state.running


async def test_futures_registered_before_apply_see_fast_transitions():
    watch = PodWatch("ns", "kubectl")
    futures = watch.expect(NAME)
    watch.apply(_pod(phase="Pending", io_in={"running": {}}, task={"waiting": {}}), deleted=False)
    watch.apply(_pod(task={"running": {}}), deleted=False)
    watch.apply(_pod(task={"terminated": {"exitCode": 0}}), deleted=False)

    assert futures.io_in_running.done() and futures.task_running.done()
    assert (await futures.done).task.exit_code == 0
    assert watch.states[NAME].done


async def test_deleted_pod_resolves_done_as_gone():
    watch = PodWatch("ns", "kubectl")
    futures = watch.expect(NAME)
    watch.apply(_pod(), deleted=False)
    watch.apply(_pod(), deleted=True)

    state = await futures.done
    assert state.gone and state.phase == POD_NOT_FOUND
    assert NAME not in watch.states


async def test_changed_set_only_on_scheduling_done_or_gone():
    watch = PodWatch("ns", "kubectl")
    watch.apply(_pod(node=None, phase="Pending", task={}), deleted=False)
    assert watch.changed.is_set()
    watch.changed.clear()

    watch.apply(_pod(node=None, phase="Pending", task={"waiting": {"reason": "ContainerCreating"}}), deleted=False)
    assert not watch.changed.is_set()

    watch.apply(_pod(phase="Pending", task={"waiting": {}}), deleted=False)  # scheduled
    assert watch.changed.is_set()
    watch.changed.clear()

    watch.apply(_pod(task={"running": {}}), deleted=False)
    assert not watch.changed.is_set()

    watch.apply(_pod(task={"terminated": {"exitCode": 0}}), deleted=False)
    assert watch.changed.is_set()


async def test_resync_resolves_done_and_vanished(monkeypatch):
    watch = PodWatch("ns", "kubectl")
    finished = watch.expect(NAME)
    vanished = watch.expect("aaiclick-task-8-0")
    watch.apply(_pod(), deleted=False)
    watch.apply(_pod(name="aaiclick-task-8-0"), deleted=False)
    listing = {"items": [_pod(phase="Succeeded", task={"terminated": {"exitCode": 0}})]}
    monkeypatch.setattr(pod_watch.cli, "run", AsyncMock(return_value=(0, json.dumps(listing), "")))

    await watch.resync()

    assert (await finished.done).task.exit_code == 0
    assert (await vanished.done).gone
    assert set(watch.states) == {NAME}


def test_stream_decoder_splits_concatenated_json():
    a = json.dumps({"type": "ADDED", "object": _pod()}, indent=2)
    b = json.dumps({"type": "DELETED", "object": _pod()})
    events, rest = decode_stream(a + "\n" + b + '\n{"type": "MOD')
    assert [e["type"] for e in events] == ["ADDED", "DELETED"]
    assert rest == '{"type": "MOD'


async def test_forget_drops_futures_without_state():
    watch = PodWatch("ns", "kubectl")
    watch.expect(NAME)
    watch.forget(NAME)
    watch.apply(_pod(task={"terminated": {"exitCode": 0}}), deleted=False)
    assert watch.states[NAME].done


@pytest.mark.parametrize("deleted", [False, True])
async def test_apply_ignores_unlabeled_pods(deleted):
    watch = PodWatch("ns", "kubectl")
    pod = _pod()
    pod["metadata"]["labels"] = {}
    watch.apply(pod, deleted=deleted)
    assert watch.states == {}
    await asyncio.sleep(0)
