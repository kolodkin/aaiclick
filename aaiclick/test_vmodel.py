"""V-model check: ``req`` markers on layer pages and ``req`` tags in tests agree.

See .claude/skills/vmodel/SKILL.md — "Tagging".
"""

import re
from collections import Counter
from pathlib import Path
from typing import NamedTuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

LAYER_PAGES = ("user_guide", "architecture", "designs")
LAYER_TEST_ROOTS: dict[str, tuple[str, ...]] = {
    "user_guide": ("aaiclick/", "test_e2e/"),
    "architecture": ("aaiclick/", "test_e2e/"),
    "designs": ("aaiclick/", "src/"),
}
TEST_GLOBS = (
    "aaiclick/**/test_*.py",
    "aaiclick/**/examples/*.py",
    "test_e2e/**/*.py",
    "src/**/*.test.ts",
    "src/**/*.test.tsx",
)
MARKER_RE = re.compile(r"^<!--\s*req:\s*(.+?)\s*-->\s*$")
TAG_RE = re.compile(r"^\s*(?:#|//)\s*req:\s*(.+)$")
SLUG_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


class Marker(NamedTuple):
    id: str
    path: Path
    line: int


class Tag(NamedTuple):
    id: str
    path: Path
    line: int


def find_markers(docs_root: Path) -> tuple[list[Marker], list[str]]:
    markers: list[Marker] = []
    problems: list[str] = []
    for layer in LAYER_PAGES:
        for page in sorted((docs_root / layer).rglob("*.md")):
            prefix = page.relative_to(docs_root).with_suffix("").as_posix()
            for n, text in enumerate(page.read_text().splitlines(), 1):
                match = MARKER_RE.match(text)
                if not match:
                    continue
                slug = match.group(1)
                if SLUG_RE.match(slug):
                    markers.append(Marker(f"{prefix}/{slug}", page, n))
                else:
                    problems.append(f"bad slug {slug!r} at {page}:{n}")
    return markers, problems


def find_tags(repo_root: Path) -> list[Tag]:
    tags: list[Tag] = []
    files = {p for pattern in TEST_GLOBS for p in repo_root.glob(pattern)}
    for path in sorted(files):
        if "node_modules" in path.parts:
            continue
        for n, text in enumerate(path.read_text().splitlines(), 1):
            match = TAG_RE.match(text)
            if match:
                tags.extend(Tag(i.strip(), path, n) for i in match.group(1).split(",") if i.strip())
    return tags


def check(repo_root: Path) -> list[str]:
    markers, problems = find_markers(repo_root / "docs")
    tags = find_tags(repo_root)
    defined = Counter(m.id for m in markers)
    tagged = {t.id for t in tags}
    problems += [f"duplicate {i}" for i, count in defined.items() if count > 1]
    problems += [f"untagged {m.id} at {m.path}:{m.line}" for m in markers if m.id not in tagged]
    for tag in tags:
        where = f"{tag.path}:{tag.line}"
        if tag.id not in defined:
            problems.append(f"unknown id {tag.id} at {where}")
        elif not tag.path.relative_to(repo_root).as_posix().startswith(LAYER_TEST_ROOTS[tag.id.split("/")[0]]):
            problems.append(f"wrong layer {tag.id} at {where}")
    return problems


CASES = {
    "clean": (
        {"docs/user_guide/p.md": "<!-- req: a -->\n", "aaiclick/test_x.py": "# req: user_guide/p/a\n"},
        [],
    ),
    "unknown-id": ({"aaiclick/test_x.py": "# req: user_guide/p/a\n"}, ["unknown id user_guide/p/a"]),
    "untagged": ({"docs/user_guide/p.md": "<!-- req: a -->\n"}, ["untagged user_guide/p/a"]),
    "duplicate": (
        {
            "docs/user_guide/p.md": "<!-- req: a -->\n<!-- req: a -->\n",
            "aaiclick/test_x.py": "# req: user_guide/p/a\n",
        },
        ["duplicate user_guide/p/a"],
    ),
    "wrong-layer": (
        {"docs/user_guide/p.md": "<!-- req: a -->\n", "src/x.test.ts": "// req: user_guide/p/a\n"},
        ["wrong layer user_guide/p/a"],
    ),
    "bad-slug": ({"docs/user_guide/p.md": "<!-- req: Bad_Slug -->\n"}, ["bad slug"]),
    "messy-tag-list": (
        {
            "docs/designs/d.md": "<!-- req: a -->\n<!-- req: b -->\n",
            "aaiclick/test_x.py": "# req: designs/d/a ,  designs/d/b,\n",
        },
        [],
    ),
    "non-layer-page": ({"docs/superpowers/plans/x.md": "<!-- req: a -->\n"}, []),
    "node-modules": ({"src/node_modules/m/x.test.ts": "// req: user_guide/p/a\n"}, []),
    "same-slug-two-pages": (
        {
            "docs/user_guide/p.md": "<!-- req: a -->\n",
            "docs/user_guide/q.md": "<!-- req: a -->\n",
            "aaiclick/test_x.py": "# req: user_guide/p/a, user_guide/q/a\n",
        },
        [],
    ),
}


@pytest.mark.parametrize(("files", "expected"), CASES.values(), ids=CASES.keys())
def test_check(tmp_path, files, expected):
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    problems = check(tmp_path)
    assert len(problems) == len(expected), problems
    for want in expected:
        assert any(want in p for p in problems), problems


@pytest.mark.skipif(not (REPO_ROOT / "docs").is_dir(), reason="docs/ not shipped in the wheel")
def test_repo_markers_and_tags_agree():
    assert check(REPO_ROOT) == []
