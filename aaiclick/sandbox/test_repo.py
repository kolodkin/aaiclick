import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from .repo import Author, SandboxGitError, SandboxPushRejected, SandboxRepo, module_parts, submission_path

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
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", branch, str(remote)], check=True)
    if seed:
        _commit_into(remote, tmp_path / "seed", "README", branch)
    return remote


def _reject_twice():
    async def fail(branch: str) -> None:
        raise SandboxGitError("! [rejected] main -> main (non-fast-forward)")

    return fail


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
    with patch.object(repo, "_push", side_effect=_reject_twice()):
        with pytest.raises(SandboxPushRejected):
            await repo.commit_file("20261009/sb_3_c.py", "z\n", author=AUTHOR, message="m")


def test_submission_path_and_module_parts():
    now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    path = submission_path("hello", now)
    assert path == f"20261009/sb_{int(now.timestamp())}_hello.py"
    assert module_parts(path) == ("20261009", f"sb_{int(now.timestamp())}_hello")
