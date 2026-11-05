"""Shared Pod watch of the kubernetes worker.

See docs/designs/kubernetes_runner.md.

One ``kubectl get pods -w`` stream over every aaiclick-labeled Pod in the
namespace feeds an in-memory ``PodState`` map. Each task coroutine registers
the stage futures of its own Pod *before* creating it (``expect``), so no
transition is missed, and awaits them instead of polling. The map is also what
the capacity gate counts and what restart reclaim starts from.

Holds the Pod labels and ``POD_NOT_FOUND`` so ``kubernetes_worker`` imports
from here, never the reverse.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import NamedTuple

from . import cli

logger = logging.getLogger(__name__)

LABEL_MANAGED = "aaiclick/managed"
LABEL_JOB_ID = "aaiclick/job-id"
LABEL_TASK_ID = "aaiclick/task-id"
LABEL_RUN_EPOCH = "aaiclick/run-epoch"
MANAGED_SELECTOR = f"{LABEL_MANAGED}=true"

# Phase reported for a Pod the API no longer has (deleted on cancellation,
# evicted); never a real Kubernetes phase.
POD_NOT_FOUND = "NotFound"

TASK_CONTAINER = "task"
IO_IN_CONTAINER = "io-in"

# Pause before re-listing after the watch stream ends or fails.
RESYNC_DELAY = 1.0


def pod_labels(task_id: int, job_id: int, run_epoch: int) -> dict[str, str]:
    """Labels every task Pod (and its Secret) carries."""
    return {
        LABEL_MANAGED: "true",
        LABEL_JOB_ID: str(job_id),
        LABEL_TASK_ID: str(task_id),
        LABEL_RUN_EPOCH: str(run_epoch),
    }


class ContainerState(NamedTuple):
    """One container's state from the Pod status."""

    state: str
    """``"waiting"`` / ``"running"`` / ``"terminated"`` / ``""`` (not reported yet)."""
    exit_code: int
    """``-1`` until terminated."""
    reason: str | None
    message: str | None


_NO_CONTAINER = ContainerState("", -1, None, None)


class PodState(NamedTuple):
    """What the watch knows about one task Pod."""

    name: str
    job_id: int
    task_id: int
    run_epoch: int
    node_name: str | None
    phase: str
    io_in: ContainerState
    task: ContainerState
    gone: bool
    scheduling_message: str | None
    """The scheduler's ``PodScheduled=False`` message, while unscheduled."""

    @property
    def unscheduled(self) -> bool:
        return self.node_name is None and not self.gone

    @property
    def done(self) -> bool:
        """The ``task`` container terminated, or the Pod is gone — the same rule
        for module and shell Pods (a shell Pod's ``io`` sidecar never exits)."""
        return self.gone or self.task.state == "terminated"

    @property
    def running(self) -> bool:
        return self.node_name is not None and not self.done

    def not_started_reason(self) -> str:
        """Why the task has not started, for the not-started failure message."""
        if self.unscheduled:
            return self.scheduling_message or "Pod not scheduled"
        if self.task.state == "waiting" and self.task.reason:
            return f"{self.task.reason}: {self.task.message}" if self.task.message else self.task.reason
        if self.task.state == "running":
            return "Pod running but the task never started"
        return self.phase or "Pod not started"


def _container(statuses: list[dict], name: str) -> ContainerState:
    for status in statuses:
        if status.get("name") != name:
            continue
        state = status.get("state", {})
        if "terminated" in state:
            t = state["terminated"]
            return ContainerState("terminated", int(t.get("exitCode", -1)), t.get("reason"), t.get("message"))
        for key in ("running", "waiting"):
            if key in state:
                return ContainerState(key, -1, state[key].get("reason"), state[key].get("message"))
    return _NO_CONTAINER


def parse_pod(obj: dict) -> PodState:
    """``PodState`` of a Pod's JSON. Raises ``KeyError`` for a Pod without the
    aaiclick labels — the caller filters those out."""
    meta = obj.get("metadata", {})
    labels = meta.get("labels", {})
    status = obj.get("status", {})
    scheduling = next(
        (
            c.get("message")
            for c in status.get("conditions", [])
            if c.get("type") == "PodScheduled" and c.get("status") == "False"
        ),
        None,
    )
    return PodState(
        name=meta["name"],
        job_id=int(labels[LABEL_JOB_ID]),
        task_id=int(labels[LABEL_TASK_ID]),
        run_epoch=int(labels[LABEL_RUN_EPOCH]),
        node_name=obj.get("spec", {}).get("nodeName") or None,
        phase=status.get("phase", ""),
        io_in=_container(status.get("initContainerStatuses", []), IO_IN_CONTAINER),
        task=_container(status.get("containerStatuses", []), TASK_CONTAINER),
        gone=False,
        scheduling_message=scheduling,
    )


class PodFutures(NamedTuple):
    """Stage futures of one Pod; each resolves with the ``PodState`` that
    reached the stage. ``done`` also resolves when the Pod is gone."""

    io_in_running: asyncio.Future[PodState]
    task_running: asyncio.Future[PodState]
    done: asyncio.Future[PodState]


def decode_stream(buffer: str) -> tuple[list[dict], str]:
    """Split a buffer of back-to-back JSON objects (``kubectl -w -o json``
    pretty-prints each event) into decoded objects plus the unfinished tail."""
    decoder = json.JSONDecoder()
    events: list[dict] = []
    pos = 0
    while True:
        while pos < len(buffer) and buffer[pos].isspace():
            pos += 1
        if pos >= len(buffer):
            return events, ""
        try:
            obj, end = decoder.raw_decode(buffer, pos)
        except json.JSONDecodeError:
            return events, buffer[pos:]
        events.append(obj)
        pos = end


class PodWatch:
    """The namespace's aaiclick Pods, kept current by one watch stream.

    ``apply`` is the single state-update path (and the unit-test seam):
    ``run`` feeds it watch events, ``resync`` feeds it a fresh list.
    """

    def __init__(self, namespace: str, kubectl: str) -> None:
        self.namespace = namespace
        self.kubectl = kubectl
        self.states: dict[str, PodState] = {}
        self.changed = asyncio.Event()
        self._futures: dict[str, PodFutures] = {}

    def expect(self, name: str) -> PodFutures:
        """Register a Pod's stage futures; call before ``kubectl apply``."""
        loop = asyncio.get_running_loop()
        futures = PodFutures(loop.create_future(), loop.create_future(), loop.create_future())
        self._futures[name] = futures
        if (state := self.states.get(name)) is not None:
            self._resolve(futures, state)
        return futures

    def forget(self, name: str, futures: PodFutures) -> None:
        """Drop ``futures`` — only if they are still the registration for
        ``name``: a detached coroutine finishing late must not drop the futures
        a re-adopting coroutine registered meanwhile."""
        if self._futures.get(name) is futures:
            del self._futures[name]

    def apply(self, obj: dict, deleted: bool) -> None:
        try:
            state = parse_pod(obj)
        except (KeyError, ValueError):
            return  # not an aaiclick task Pod
        previous = self.states.get(state.name)
        if deleted:
            state = state._replace(gone=True, phase=POD_NOT_FOUND)
            self.states.pop(state.name, None)
        else:
            self.states[state.name] = state
        if futures := self._futures.get(state.name):
            self._resolve(futures, state)
        if previous is None or deleted or previous.unscheduled != state.unscheduled or previous.done != state.done:
            self.changed.set()

    @staticmethod
    def _resolve(futures: PodFutures, state: PodState) -> None:
        def settle(future: asyncio.Future[PodState]) -> None:
            if not future.done():
                future.set_result(state)

        if state.io_in.state in ("running", "terminated") or state.task.state in ("running", "terminated"):
            settle(futures.io_in_running)
        if state.task.state in ("running", "terminated"):
            settle(futures.task_running)
        if state.done:
            settle(futures.io_in_running)
            settle(futures.task_running)
            settle(futures.done)

    async def resync(self) -> None:
        """Re-list the namespace: refresh every state, and treat a tracked Pod
        the list no longer has as gone."""
        rc, out, err = await cli.run(
            self.kubectl, "get", "pods", "-n", self.namespace, "-l", MANAGED_SELECTOR, "-o", "json", stream=False
        )
        if rc != 0:
            raise cli.CommandError((self.kubectl, "get", "pods"), rc, err)
        items = json.loads(out).get("items", [])
        listed = {item["metadata"]["name"] for item in items}
        for item in items:
            self.apply(item, deleted=False)
        for name in list(self.states):
            if name not in listed:
                self.apply(_tombstone(self.states[name]), deleted=True)
        # Futures of Pods the watch never saw stay pending: the Pod may have
        # been created after the list started, so "absent" is not "gone".

    async def run(self) -> None:
        """Stream watch events forever; on stream end or error, resync and
        restart. Cancel the task to stop."""
        while True:
            try:
                await self.resync()
                await self._stream()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Pod watch failed; resyncing in %ss", RESYNC_DELAY)
            await asyncio.sleep(RESYNC_DELAY)

    async def _stream(self) -> None:
        proc = await asyncio.create_subprocess_exec(
            self.kubectl,
            "get",
            "pods",
            "-n",
            self.namespace,
            "-l",
            MANAGED_SELECTOR,
            "--watch-only",
            "--output-watch-events",
            "-o",
            "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        assert proc.stdout is not None
        buffer = ""
        try:
            while chunk := await proc.stdout.read(65536):
                events, buffer = decode_stream(buffer + chunk.decode(errors="replace"))
                for event in events:
                    self.apply(event.get("object", {}), deleted=event.get("type") == "DELETED")
        finally:
            if proc.returncode is None:
                proc.kill()
            await proc.wait()


def _gone(name: str) -> PodState:
    return PodState(name, 0, 0, 0, None, POD_NOT_FOUND, _NO_CONTAINER, _NO_CONTAINER, True, None)


def _tombstone(state: PodState) -> dict:
    """Minimal Pod JSON that re-parses to ``state``'s identity, for a delete apply."""
    return {"metadata": {"name": state.name, "labels": pod_labels(state.task_id, state.job_id, state.run_epoch)}}
