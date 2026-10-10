import asyncio
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from ..testing import init_bare_repo
from .paths import module_parts, submission_path
from .repo import Author, SandboxGitError, SandboxPushRejected, SandboxRepo, get_sandbox_repo

AUTHOR = Author("u", "u@x")


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout.strip()


def _commit_into(remote: Path, worktree: Path, filename: str, branch: str) -> None:
    """Push one commit of ``filename`` into ``remote`` from a throwaway clone."""
    subprocess.run(["git", "clone", "-q", "--", str(remote), str(worktree)], check=True, capture_output=True)
    _git(worktree, "checkout", "-q", "-B", branch)
    (worktree / filename).write_text("seed\n")
    _git(worktree, "add", filename)
    _git(worktree, "-c", "user.name=s", "-c", "user.email=s@x", "commit", "-q", "-m", "seed")
    _git(worktree, "push", "-q", "-u", "origin", f"HEAD:{branch}")


def _bare(tmp_path: Path, *, branch: str = "main", seed: bool = False) -> Path:
    remote = init_bare_repo(tmp_path / "remote.git", branch)
    if seed:
        _commit_into(remote, tmp_path / "seed", "README", branch)
    return remote


async def _rejected(branch: str) -> None:
    raise SandboxGitError("! [rejected] main -> main (non-fast-forward)")


async def test_first_submission_into_empty_remote_creates_main(tmp_path):
    remote = _bare(tmp_path, seed=False)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    committed = await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    assert _git(remote, "rev-parse", "main") == committed.sha
    assert await repo.read_file(committed.path, committed.sha) == "x = 1\n"


async def test_second_submission_stacks_on_master(tmp_path):
    remote = _bare(tmp_path, branch="master", seed=True)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    first = await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    second = await repo.commit_file("20261009/sb_2_b.py", "y = 2\n", author=AUTHOR, message="m")
    assert _git(remote, "rev-parse", "master") == second.sha
    assert _git(remote, "rev-parse", "master^") == first.sha
    assert _git(remote, "log", "-1", "--format=%an <%ae>|%cn", "master") == "u <u@x>|aaiclick"


async def test_duplicate_path_gets_a_suffix(tmp_path):
    remote = _bare(tmp_path, seed=True)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    again = await repo.commit_file("20261009/sb_1_a.py", "x = 2\n", author=AUTHOR, message="m")
    assert again.path == "20261009/sb_1_a_2.py"


async def test_rejected_push_retries_once_then_raises(tmp_path):
    remote = _bare(tmp_path, seed=True)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    _commit_into(remote, tmp_path / "other", "competing.txt", "main")
    ok = await repo.commit_file("20261009/sb_2_b.py", "y\n", author=AUTHOR, message="m")
    assert _git(remote, "rev-parse", "main") == ok.sha
    assert _git(remote, "rev-parse", "main^^") != ok.sha
    with patch.object(repo, "_push", side_effect=_rejected):
        with pytest.raises(SandboxPushRejected):
            await repo.commit_file("20261009/sb_3_c.py", "z\n", author=AUTHOR, message="m")


def test_submission_path_and_module_parts():
    now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    path = submission_path("hello", now)
    assert path == f"20261009/sb_{int(now.timestamp())}_hello.py"
    assert module_parts(path) == ("20261009", f"sb_{int(now.timestamp())}_hello")


def test_get_sandbox_repo_is_one_instance_per_remote(tmp_path, monkeypatch):
    monkeypatch.setenv("AAICLICK_SANDBOX", str(tmp_path / "r.git"))
    assert get_sandbox_repo() is get_sandbox_repo()


async def test_concurrent_submissions_serialize(tmp_path):
    """Concurrent requests share one instance per remote and must not run git
    concurrently in its clone."""
    remote = _bare(tmp_path, seed=True)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    results = await asyncio.gather(
        *(repo.commit_file(f"20261009/sb_{i}_a.py", f"x = {i}\n", author=AUTHOR, message="m") for i in range(4))
    )
    assert len({r.sha for r in results}) == 4
    assert _git(remote, "rev-list", "--count", "main") == "5"


async def test_sync_drops_leftovers_from_a_dirty_clone(tmp_path):
    remote = _bare(tmp_path, seed=True)
    repo = SandboxRepo(str(remote), tmp_path / "clone")
    await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    (tmp_path / "clone" / "20261009" / "leftover.py").write_text("junk\n")
    _git(tmp_path / "clone", "add", "20261009/leftover.py")
    (tmp_path / "clone" / "untracked.txt").write_text("junk\n")
    committed = await repo.commit_file("20261009/sb_2_b.py", "y = 2\n", author=AUTHOR, message="m")
    assert _git(remote, "show", "--name-only", "--format=", committed.sha) == "20261009/sb_2_b.py"


async def test_empty_remote_recovers_after_a_failed_push(tmp_path):
    remote = _bare(tmp_path, seed=False)
    repo = SandboxRepo(str(remote), tmp_path / "clone")

    async def network_down(branch: str) -> None:
        raise SandboxGitError("fatal: unable to access remote")

    with patch.object(repo, "_push", side_effect=network_down):
        with pytest.raises(SandboxGitError):
            await repo.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    committed = await repo.commit_file("20261009/sb_2_b.py", "y = 2\n", author=AUTHOR, message="m")
    assert _git(remote, "rev-parse", "main") == committed.sha
    assert _git(remote, "rev-list", "--count", "main") == "1"


async def test_read_file_from_a_fresh_clone_over_file_url(tmp_path):
    """A shallow clone that never saw a commit still reads it: a restarted
    server, or a second replica, serves older submissions."""
    remote = _bare(tmp_path, seed=True)
    url = f"file://{remote}"
    writer = SandboxRepo(url, tmp_path / "writer")
    first = await writer.commit_file("20261009/sb_1_a.py", "x = 1\n", author=AUTHOR, message="m")
    await writer.commit_file("20261009/sb_2_b.py", "y = 2\n", author=AUTHOR, message="m")
    reader = SandboxRepo(url, tmp_path / "reader")
    assert await reader.read_file(first.path, first.sha) == "x = 1\n"


def test_repo_module_imports_standalone():
    proc = subprocess.run([sys.executable, "-c", "import aaiclick.sandbox.repo"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
