"""The API server's clone of the sandbox repo: commit a submission, push it,
read a committed file back.

Every git call goes through ``execution.cli.run`` like the build task's clone,
so the same credentials (URL-embedded or the host's git config) apply.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import NamedTuple

from ..backend import get_root
from ..orchestration.execution import cli

ENV_SANDBOX = "AAICLICK_SANDBOX"
DEFAULT_BRANCH = "main"
_REJECTED_MARKERS = ("rejected", "non-fast-forward", "fetch first")
# Git must never wait on a terminal for credentials; a prompt would hang the
# request under the lock.
_GIT_ENV = {"GIT_TERMINAL_PROMPT": "0"}


class SandboxGitError(RuntimeError):
    """A git command failed; the message is its stderr."""


class SandboxPushRejected(SandboxGitError):
    """The push lost the race twice in a row."""


class Author(NamedTuple):
    name: str
    email: str


class Committed(NamedTuple):
    path: str
    sha: str


COMMITTER = Author("aaiclick", "sandbox@aaiclick")

# One lock per clone directory, process-wide: every instance on a workdir
# serializes through it, whether or not the instances are the same object.
# Never rebound after import, so a module constant is the right home.
_WORKDIR_LOCKS: dict[Path, asyncio.Lock] = {}


def _workdir_lock(workdir: Path) -> asyncio.Lock:
    return _WORKDIR_LOCKS.setdefault(workdir.resolve(), asyncio.Lock())


class SandboxRepo:
    """A persistent clone of ``remote`` under ``workdir``."""

    def __init__(self, remote: str, workdir: Path | None = None):
        self.remote = remote
        self.workdir = workdir if workdir is not None else get_root() / "sandbox" / "repo"
        self._lock = _workdir_lock(self.workdir)
        self._branch: str | None = None

    async def _run(self, *args: str) -> tuple[int, str, str]:
        return await cli.run("git", *args, check=False, stream=False, env=_GIT_ENV)

    async def _git_raw(self, *args: str) -> str:
        rc, stdout, stderr = await self._run("-C", str(self.workdir), *args)
        if rc != 0:
            raise SandboxGitError(stderr.strip() or f"git {' '.join(args)} failed (exit {rc})")
        return stdout

    async def _git(self, *args: str) -> str:
        return (await self._git_raw(*args)).strip()

    async def _ensure_clone(self) -> str:
        """Clone on first use; return the remote's default branch (``main`` for
        an empty remote, whose first commit this clone will make)."""
        if not (self.workdir / ".git").is_dir():
            self.workdir.parent.mkdir(parents=True, exist_ok=True)
            rc, _, stderr = await self._run("clone", "--quiet", "--depth=1", "--", self.remote, str(self.workdir))
            if rc != 0:
                raise SandboxGitError(stderr.strip())
        if self._branch is None:
            rc, stdout, _ = await self._run(
                "-C", str(self.workdir), "symbolic-ref", "--short", "refs/remotes/origin/HEAD"
            )
            self._branch = stdout.strip().removeprefix("origin/") if rc == 0 and stdout.strip() else DEFAULT_BRANCH
        return self._branch

    async def _remote_has_branch(self, branch: str) -> bool:
        return bool(await self._git("ls-remote", "--heads", "origin", branch))

    async def _sync(self, branch: str) -> None:
        """Make the clone match the remote branch head exactly — or an empty
        tree on an unborn branch when the remote has no such branch yet —
        dropping anything a failed or interrupted submission left behind."""
        if await self._remote_has_branch(branch):
            await self._git("fetch", "--quiet", "--depth=1", "origin", branch)
            await self._git("checkout", "--quiet", "-B", branch, "FETCH_HEAD")
            await self._git("reset", "--quiet", "--hard", "FETCH_HEAD")
        else:
            # ``symbolic-ref`` points HEAD at the branch whether or not a local
            # one exists (``checkout --orphan`` refuses an existing name, which
            # is exactly the state a failed push leaves); the delete then makes
            # it unborn again so the next commit is a root commit.
            await self._git("symbolic-ref", "HEAD", f"refs/heads/{branch}")
            await self._run("-C", str(self.workdir), "update-ref", "-d", f"refs/heads/{branch}")
            await self._git("reset", "--quiet")
        await self._git("clean", "--quiet", "-ffdx")

    def _unique_path(self, path: str) -> str:
        candidate = path
        stem = path.removesuffix(".py")
        n = 1
        while (self.workdir / candidate).exists():
            n += 1
            candidate = f"{stem}_{n}.py"
        return candidate

    async def _commit(self, path: str, content: str, author: Author, message: str) -> str:
        target = self.workdir / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        await self._git("add", "--", path)
        await self._git(
            "-c",
            f"user.name={COMMITTER.name}",
            "-c",
            f"user.email={COMMITTER.email}",
            "commit",
            "--quiet",
            "--author",
            f"{author.name} <{author.email}>",
            "-m",
            message,
        )
        return await self._git("rev-parse", "HEAD")

    async def _push(self, branch: str) -> None:
        await self._git("push", "--quiet", "-u", "origin", f"HEAD:{branch}")

    async def commit_file(self, path: str, content: str, *, author: Author, message: str) -> Committed:
        """Write ``content`` at ``path`` (suffixed ``_2``, ``_3``… when taken),
        commit it as ``author`` and push. A push rejected by a competing
        commit is retried once on a fresh sync; a second rejection raises
        :class:`SandboxPushRejected`."""
        async with self._lock:
            branch = await self._ensure_clone()
            for attempt in (1, 2):
                await self._sync(branch)
                final_path = self._unique_path(path)
                sha = await self._commit(final_path, content, author, message)
                try:
                    await self._push(branch)
                except SandboxGitError as exc:
                    rejected = any(marker in str(exc) for marker in _REJECTED_MARKERS)
                    if not rejected:
                        raise
                    if attempt == 2:
                        await self._sync(branch)
                        raise SandboxPushRejected(str(exc)) from exc
                    continue
                return Committed(path=final_path, sha=sha)
            raise AssertionError("unreachable")

    async def read_file(self, path: str, sha: str) -> str:
        """The committed text of ``path`` at ``sha``.

        The clone is shallow, so a commit another replica pushed, or one
        older than a fresh clone's tip, is fetched on demand by SHA."""
        async with self._lock:
            await self._ensure_clone()
            try:
                return await self._git_raw("show", f"{sha}:{path}")
            except SandboxGitError:
                await self._git("fetch", "--quiet", "--depth=1", "origin", sha)
                return await self._git_raw("show", f"{sha}:{path}")


_repo_override: ContextVar[SandboxRepo | None] = ContextVar("sandbox_repo_override", default=None)
# One instance per remote for the process lifetime, so the branch lookup runs
# once and every request shares the clone. Never rebound: entries are added.
_REPOS: dict[str, SandboxRepo] = {}


@contextmanager
def sandbox_repo_override(repo: SandboxRepo | None) -> Iterator[None]:
    """Make ``get_sandbox_repo`` return ``repo`` inside the block (tests)."""
    token = _repo_override.set(repo)
    try:
        yield
    finally:
        _repo_override.reset(token)


def get_sandbox_repo() -> SandboxRepo | None:
    """The configured sandbox repo, or ``None`` when ``AAICLICK_SANDBOX`` is unset."""
    override = _repo_override.get()
    if override is not None:
        return override
    remote = os.environ.get(ENV_SANDBOX)
    if not remote:
        return None
    repo = _REPOS.get(remote)
    if repo is None:
        repo = _REPOS[remote] = SandboxRepo(remote)
    return repo
