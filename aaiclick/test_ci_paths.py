"""Every test file under ``aaiclick/`` sits in some CI ``test-paths`` group.

CI passes explicit path lists to pytest, so a test file outside all of them
silently never runs.
"""

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "_test-reusable.yaml"

pytestmark = pytest.mark.skipif(not WORKFLOW.is_file(), reason="workflows not shipped in the wheel")


def ci_test_paths() -> list[str]:
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"].values()
    groups = [g for job in jobs for g in job.get("strategy", {}).get("matrix", {}).get("include", [])]
    return [entry for g in groups for entry in g.get("test-paths", "").split()]


def ci_covered_files() -> set[Path]:
    """Expand each entry the way the CI shell glob and pytest do."""
    covered: set[Path] = set()
    for entry in ci_test_paths():
        if entry.endswith("/"):
            covered |= set((REPO_ROOT / entry).rglob("test_*.py"))
        else:
            covered |= set(REPO_ROOT.glob(entry))
    return covered


def test_every_test_file_is_in_a_ci_group():
    missing = set((REPO_ROOT / "aaiclick").rglob("test_*.py")) - ci_covered_files()
    assert sorted(p.relative_to(REPO_ROOT).as_posix() for p in missing) == []
