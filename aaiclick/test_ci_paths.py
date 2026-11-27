"""Every test file under ``aaiclick/`` sits in some CI ``test-paths`` group.

CI passes explicit path lists to pytest, so a test file outside all of them
silently never runs.
"""

import re
from fnmatch import fnmatch
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "_test-reusable.yaml"
TEST_PATHS_RE = re.compile(r'^\s*test-paths:\s*(?:&\S+\s+)?"([^"]*)"', re.MULTILINE)

pytestmark = pytest.mark.skipif(not WORKFLOW.is_file(), reason="workflows not shipped in the wheel")


def ci_test_paths() -> list[str]:
    return [entry for match in TEST_PATHS_RE.findall(WORKFLOW.read_text()) for entry in match.split()]


def all_test_files() -> list[str]:
    return sorted(p.relative_to(REPO_ROOT).as_posix() for p in (REPO_ROOT / "aaiclick").rglob("test_*.py"))


def covers(entry: str, path: str) -> bool:
    if any(ch in entry for ch in "*?["):
        return fnmatch(path, entry)
    if entry.endswith("/"):
        return path.startswith(entry)
    return path == entry


def test_every_test_file_is_in_a_ci_group():
    entries = ci_test_paths()
    uncovered = [p for p in all_test_files() if not any(covers(entry, p) for entry in entries)]
    assert uncovered == []
