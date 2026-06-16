# Kubernetes Runner — Phase 1: Shared CLI Primitive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract one async-subprocess primitive (`execution/cli.py`) that drives external CLIs (`docker`, `kubectl`, `git`), refactor the Docker runner onto it, and rename the now-shared `AAICLICK_DOCKER_REGISTRY` env var to `AAICLICK_REGISTRY`.

**Architecture:** A single module exposes `run(*cmd, check, stream, cwd) -> (rc, stdout, stderr)` over `asyncio.create_subprocess_exec`, with a `stream=True` mode that tees output live to stdout/stderr (for `docker build` progress and, later, `kubectl logs -f`). `docker_worker`, `docker_build`, and `docker_config` drop their three duplicate subprocess helpers and call `cli.run`. This is the Phase 0.5-style prep refactor that the Kubernetes vehicle (Phase 3) will build on.

**Tech Stack:** Python 3.10, asyncio, pytest (asyncio_mode=auto), uv.

**Scope note:** This plan covers Phase 1 only (see `docs/kubernetes_runner_implementation_plan.md`). Phases 2–5 (schema/config, vehicle+logs, CLI, e2e) each get their own writing-plans pass once this lands. Phase 0 (the `TaskVehicle`/`drive_vehicle` extraction) is already complete.

**Behaviour invariant:** This is a pure refactor + env-var rename. The full execution test suite must stay green with no test edits except the registry-var rename in `test_docker_build.py`.

**Run tests with:**
```bash
uv run --no-project python -m pytest <paths> -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto
```

---

## File Structure

- Create: `aaiclick/orchestration/execution/cli.py` — the async CLI primitive (`run`, `CommandError`, `_stream`).
- Create: `aaiclick/orchestration/execution/test_cli.py` — unit tests for `cli.run`.
- Modify: `aaiclick/orchestration/execution/docker_worker.py` — drop `_run_subprocess_capture`, call `cli.run`.
- Modify: `aaiclick/orchestration/execution/docker_build.py` — drop `_run_subprocess` + `_stream_to_stdio`, call `cli.run(stream=True)`.
- Modify: `aaiclick/orchestration/docker_config.py` — `_git` calls `cli.run`; rename registry var.
- Modify: `aaiclick/orchestration/execution/test_docker_build.py` — registry-var rename only.
- Modify: `.github/workflows/_docker-e2e-reusable.yaml` — registry-var rename only.

---

## Task 1: Create the `cli.py` primitive

**Files:**
- Create: `aaiclick/orchestration/execution/cli.py`
- Test: `aaiclick/orchestration/execution/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/orchestration/execution/test_cli.py`:

```python
"""Tests for the async subprocess CLI primitive."""

from __future__ import annotations

import sys

import pytest

from . import cli


async def test_run_captures_stdout():
    rc, out, err = await cli.run(sys.executable, "-c", "print('hello')")
    assert rc == 0
    assert "hello" in out


async def test_run_raises_command_error_on_nonzero():
    with pytest.raises(cli.CommandError) as excinfo:
        await cli.run(sys.executable, "-c", "import sys; sys.stderr.write('boom'); sys.exit(3)")
    assert excinfo.value.returncode == 3
    assert "boom" in excinfo.value.stderr


async def test_run_check_false_returns_returncode():
    rc, out, err = await cli.run(sys.executable, "-c", "import sys; sys.exit(2)", check=False)
    assert rc == 2


async def test_run_stream_tees_to_stdout(capsys):
    rc, out, err = await cli.run(sys.executable, "-c", "print('streamed')", stream=True)
    assert rc == 0
    assert "streamed" in out
    assert "streamed" in capsys.readouterr().out
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_cli.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: collection/import error — `ImportError: cannot import name 'cli'` (module does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `aaiclick/orchestration/execution/cli.py`:

```python
"""Async subprocess primitive for driving external CLIs (docker, kubectl, git).

One ``run`` with two modes:

- default: capture stdout/stderr, raise ``CommandError`` on a non-zero exit
  (unless ``check=False``).
- ``stream=True``: tee each line to the process's own stdout/stderr as it
  arrives (so a long-running ``docker build`` / ``kubectl logs -f`` shows
  progress live and the worker's ``capture_task_output`` picks it up), while
  still capturing the full output to return.
"""

from __future__ import annotations

import asyncio
import sys
from typing import TextIO


class CommandError(RuntimeError):
    """A CLI command exited non-zero while ``check=True``."""

    def __init__(self, cmd: tuple[str, ...], returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"command {' '.join(cmd)!r} failed with exit code {returncode}: {stderr}")


async def _stream(reader: asyncio.StreamReader, sink: TextIO) -> bytes:
    """Forward each line from ``reader`` to ``sink`` as it arrives, while
    accumulating the full contents to return at the end."""
    chunks: list[bytes] = []
    while True:
        line = await reader.readline()
        if not line:
            break
        chunks.append(line)
        sink.write(line.decode(errors="replace"))
        sink.flush()
    return b"".join(chunks)


async def run(
    *cmd: str,
    check: bool = True,
    stream: bool = False,
    cwd: str | None = None,
) -> tuple[int, str, str]:
    """Run ``cmd``; return ``(returncode, stdout, stderr)``.

    Raises :class:`CommandError` on a non-zero exit when ``check`` is True."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    if stream:
        assert proc.stdout is not None and proc.stderr is not None
        stdout_b, stderr_b = await asyncio.gather(
            _stream(proc.stdout, sys.stdout),
            _stream(proc.stderr, sys.stderr),
        )
        await proc.wait()
    else:
        stdout_b, stderr_b = await proc.communicate()

    rc = proc.returncode or 0
    stdout = stdout_b.decode(errors="replace")
    stderr = stderr_b.decode(errors="replace")
    if check and rc != 0:
        raise CommandError(cmd, rc, stderr)
    return rc, stdout, stderr
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_cli.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/cli.py aaiclick/orchestration/execution/test_cli.py
git commit -m "Add async CLI primitive for driving docker/kubectl/git"
```

---

## Task 2: Refactor `docker_worker` onto `cli.run`

`docker_worker._run_subprocess_capture` is a capture-only duplicate of `cli.run`. Replace it. Leave `_wait_for_container` alone — it owns a bespoke `docker wait` proc for timeout/kill and is not a plain capture.

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py`
- Test: `aaiclick/orchestration/execution/test_docker_worker.py` (existing, unchanged)

- [ ] **Step 1: Add the import**

In `docker_worker.py`, in the current-package import group, add:

```python
from . import cli
```

- [ ] **Step 2: Delete `_run_subprocess_capture`**

Remove this entire function from `docker_worker.py`:

```python
async def _run_subprocess_capture(*cmd: str, check: bool = True) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    stdout = stdout_b.decode(errors="replace")
    stderr = stderr_b.decode(errors="replace")
    rc = proc.returncode or 0
    if check and rc != 0:
        raise RuntimeError(f"command {' '.join(cmd)!r} failed with exit code {rc}: {stderr}")
    return rc, stdout, stderr
```

- [ ] **Step 3: Repoint the four call sites to `cli.run`**

Replace each `_run_subprocess_capture(` call with `cli.run(`:

```python
async def _docker_pull_if_registered(image_tag: str) -> None:
    if not os.environ.get("AAICLICK_REGISTRY"):
        return
    await cli.run(_docker_bin(), "pull", image_tag, check=False)


async def _docker_run_detached(cmd: list[str]) -> str:
    """Run ``docker run --detach``; returns the container id."""
    rc, stdout, stderr = await cli.run(*cmd, check=False)
    if rc != 0:
        raise RuntimeError(f"docker run failed (exit {rc}): {stderr.strip() or stdout.strip()}")
    container_id = stdout.strip().splitlines()[-1]
    if not container_id:
        raise RuntimeError("docker run returned no container id")
    return container_id


async def _docker_kill(container_id: str) -> None:
    await cli.run(_docker_bin(), "kill", container_id, check=False)


async def _docker_rm(container_id: str) -> None:
    """Remove the (already-stopped) container. Replaces the ``--rm`` flag
    on ``docker run``; we do it explicitly so the container survives long
    enough for ``docker wait`` to read its exit code without a race."""
    await cli.run(_docker_bin(), "rm", "--force", container_id, check=False)
```

(Note: `_docker_pull_if_registered` also gets the `AAICLICK_REGISTRY` rename here — Task 5 covers the remaining sites.)

- [ ] **Step 4: Run the docker_worker tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_docker_worker.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (same count as before — no test changes).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_worker.py
git commit -m "Refactor docker_worker subprocess calls onto cli.run"
```

---

## Task 3: Refactor `docker_build` onto `cli.run(stream=True)`

`docker_build._run_subprocess` always streams; `_stream_to_stdio` is now in `cli`. Replace both.

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py`
- Test: `aaiclick/orchestration/execution/test_docker_build.py` (existing)

- [ ] **Step 1: Add the import and drop now-unused ones**

In `docker_build.py` add to the current-package group:

```python
from . import cli
```

Remove the now-unused `import sys` and `from typing import TextIO` only if no other code references them (search the file first; `_aaiclick_version` and others do not use `sys`/`TextIO`).

- [ ] **Step 2: Delete `_stream_to_stdio` and `_run_subprocess`**

Remove both functions (`_stream_to_stdio` at the top of the module and `_run_subprocess` below it) entirely — their bodies now live in `cli.py`.

- [ ] **Step 3: Repoint every call site to `cli.run(..., stream=True)`**

Replace each `_run_subprocess(` with `cli.run(` and add `stream=True`. The affected functions become:

```python
async def _docker_image_exists_locally(image_tag: str) -> bool:
    rc, _, _ = await cli.run(_docker_bin(), "image", "inspect", image_tag, check=False, stream=True)
    return rc == 0


async def _docker_pull(image_tag: str) -> bool:
    """Returns True on cache hit, False if the registry doesn't have it."""
    rc, _, _ = await cli.run(_docker_bin(), "pull", image_tag, check=False, stream=True)
    return rc == 0


async def _docker_push(image_tag: str) -> None:
    await cli.run(_docker_bin(), "push", image_tag, stream=True)


async def _git_clone_at_sha(remote: str, sha: str, workdir: str) -> None:
    """Clone the SHA into ``workdir``. Uses ``git init`` + ``fetch`` + ``checkout``
    so we avoid pulling the full default branch when only one commit is needed,
    and so the remote can be a non-default-branch SHA."""
    await cli.run("git", "init", "--quiet", workdir, stream=True)
    await cli.run("git", "-C", workdir, "remote", "add", "origin", remote, stream=True)
    await cli.run("git", "-C", workdir, "fetch", "--depth=1", "--quiet", "origin", sha, stream=True)
    await cli.run("git", "-C", workdir, "checkout", "--quiet", sha, stream=True)
```

And in `_docker_build`, change the final line from `await _run_subprocess(*cmd)` to:

```python
    await cli.run(*cmd, stream=True)
```

- [ ] **Step 4: Run the docker_build tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_docker_build.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (same count as before).

!!! warning "If a test patched the old helper name"
    `test_docker_build.py` patches the higher-level `_docker_*` / `_git_clone_at_sha` functions, not `_run_subprocess`. If any assertion references `_run_subprocess` or a `RuntimeError` message string, update it to the `cli.CommandError` message format (`command '...' failed with exit code N: ...`). Do not weaken assertions.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/execution/docker_build.py
git commit -m "Refactor docker_build streaming subprocess onto cli.run"
```

---

## Task 4: Refactor `docker_config._git` onto `cli.run`

`_git` captures output and raises `GitDetectionError` (a domain error), so it keeps its wrapper but drops the raw `create_subprocess_exec`.

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py`
- Test: `aaiclick/orchestration/execution/` git-detection tests (existing, run full suite in Task 6)

- [ ] **Step 1: Add the import**

In `docker_config.py`, current-package group:

```python
from .execution import cli
```

- [ ] **Step 2: Replace the `_git` body**

Replace:

```python
async def _git(*args: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise GitDetectionError(f"git {' '.join(args)} failed (exit {proc.returncode}): {stderr.decode().strip()}")
    return stdout.decode().strip()
```

with:

```python
async def _git(*args: str) -> str:
    rc, stdout, stderr = await cli.run("git", *args, check=False)
    if rc != 0:
        raise GitDetectionError(f"git {' '.join(args)} failed (exit {rc}): {stderr.strip()}")
    return stdout.strip()
```

Remove the now-unused `import asyncio` from `docker_config.py` only if nothing else in the file uses it (search first).

- [ ] **Step 3: Run the orchestration unit tests touching git detection**

Run: `uv run --no-project python -m pytest aaiclick/orchestration -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto -k "docker_config or git or runner"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/orchestration/docker_config.py
git commit -m "Refactor docker_config git helper onto cli.run"
```

---

## Task 5: Rename `AAICLICK_DOCKER_REGISTRY` → `AAICLICK_REGISTRY`

The registry is shared by both runners (k8s reuses the docker build), so the docker-specific name is wrong. Clean rename, no back-compat shim.

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py` (`compute_image_tag`)
- Modify: `aaiclick/orchestration/execution/docker_build.py` (docstring + `build_image`)
- Modify: `aaiclick/orchestration/execution/test_docker_build.py`
- Modify: `.github/workflows/_docker-e2e-reusable.yaml`
- (`docker_worker._docker_pull_if_registered` was already renamed in Task 2.)

- [ ] **Step 1: Find every remaining occurrence**

Run: `grep -rn "AAICLICK_DOCKER_REGISTRY" aaiclick .github`
Expected matches: `docker_config.py` (1), `docker_build.py` (docstring + 1 code), `test_docker_build.py` (3), `_docker-e2e-reusable.yaml` (1).

- [ ] **Step 2: Replace each occurrence with `AAICLICK_REGISTRY`**

In `docker_config.compute_image_tag`:

```python
    registry = os.environ.get("AAICLICK_REGISTRY")
```

In `docker_build.build_image` and its module docstring, replace `AAICLICK_DOCKER_REGISTRY` with `AAICLICK_REGISTRY` (both the `registry = os.environ.get(...)` line and the docstring mention).

In `test_docker_build.py`, replace all three (`monkeypatch.setenv("AAICLICK_REGISTRY", ...)` and the two `monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)`).

In `.github/workflows/_docker-e2e-reusable.yaml`, rename the env key:

```yaml
          AAICLICK_REGISTRY: "host.docker.internal:5000"
```

- [ ] **Step 3: Verify no occurrences remain**

Run: `grep -rn "AAICLICK_DOCKER_REGISTRY" aaiclick .github`
Expected: no output.

- [ ] **Step 4: Run the docker_build tests**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution/test_docker_build.py -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/docker_config.py aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/execution/test_docker_build.py .github/workflows/_docker-e2e-reusable.yaml
git commit -m "Rename AAICLICK_DOCKER_REGISTRY to AAICLICK_REGISTRY"
```

---

## Task 6: Full-suite verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full execution suite**

Run: `uv run --no-project python -m pytest aaiclick/orchestration/execution -q -p no:cacheprovider -o addopts="" -o asyncio_mode=auto`
Expected: PASS (117 passed, 1 skipped — one more than Phase 0's 116, for the new `test_cli.py` file; the docker-daemon-gated test stays skipped).

- [ ] **Step 2: Lint**

Run: `uv run --no-project ruff check aaiclick/orchestration/execution/cli.py aaiclick/orchestration/execution/docker_worker.py aaiclick/orchestration/execution/docker_build.py aaiclick/orchestration/docker_config.py`
Expected: `All checks passed!` (fixes any unused-import left by Tasks 3–4).

- [ ] **Step 3: Push the branch**

```bash
git push -u origin claude/kubernetes-runner-support-rxv9hm
```

- [ ] **Step 4: Update the phase tracker**

In `docs/kubernetes_runner_implementation_plan.md`, mark Phase 1 ✅ with a reference to `aaiclick/orchestration/execution/cli.py` (`cli.run`). Commit:

```bash
git add docs/kubernetes_runner_implementation_plan.md
git commit -m "Mark Kubernetes runner Phase 1 complete"
```

---

## Self-Review

- **Spec coverage:** Phase 1 of `docs/kubernetes_runner_implementation_plan.md` = `execution/cli.py` (Task 1) + docker_worker/docker_build/docker_config refactor (Tasks 2–4) + registry rename (Task 5). All covered.
- **Type consistency:** `cli.run(*cmd, check, stream, cwd) -> tuple[int, str, str]` and `cli.CommandError(cmd, returncode, stderr)` are used identically across Tasks 2–4. `_stream` is private to `cli`.
- **Placeholder scan:** none — every code step shows full code; every command shows expected output.
- **Behaviour:** docker_build calls use `stream=True` to preserve live-log behaviour; docker_worker capture calls use the default (non-stream) mode, matching the old `_run_subprocess_capture`. `_wait_for_container` deliberately untouched.
