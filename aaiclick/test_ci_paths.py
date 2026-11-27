"""Every test file under ``aaiclick/`` sits in some CI ``test-paths`` group.

CI passes explicit path lists to pytest, so a test file outside all of them
silently never runs.
"""

import re
from pathlib import Path, PurePosixPath

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
        # like the shell glob CI uses: `*` never crosses a `/`
        p, e = PurePosixPath(path), PurePosixPath(entry)
        return len(p.parts) == len(e.parts) and p.match(entry)
    if entry.endswith("/"):
        return path.startswith(entry)
    return path == entry


def test_every_test_file_is_in_a_ci_group():
    entries = ci_test_paths()
    uncovered = [p for p in all_test_files() if not any(covers(entry, p) for entry in entries)]
    assert uncovered == []


@pytest.mark.parametrize(
    ("entry", "path", "expected"),
    [
        pytest.param("aaiclick/test_*.py", "aaiclick/test_cli.py", True, id="glob-root-file"),
        pytest.param("aaiclick/test_*.py", "aaiclick/test_utils/test_x.py", False, id="glob-does-not-cross-dirs"),
        pytest.param("aaiclick/sandbox/", "aaiclick/sandbox/test_parse.py", True, id="directory"),
        pytest.param("aaiclick/test_cli.py", "aaiclick/test_cli.py", True, id="exact-file"),
    ],
)
def test_covers_matches_like_the_ci_shell(entry, path, expected):
    assert covers(entry, path) is expected
