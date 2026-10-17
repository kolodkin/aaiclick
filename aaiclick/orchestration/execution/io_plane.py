"""The file data plane shell and jvm tasks speak.

See docs/designs/task_interface.md — "File data plane", "Where it runs".

``AAICLICK_IO_DIR`` names a directory the process reads ``in.json`` from and
may write ``out.json`` to. No library, no DB: a shim in any language is a
JSON parser and two file calls. The host resolves kwargs into ``in.json``
before launch and interprets ``out.json`` after exit.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

IO_DIR_ENV = "AAICLICK_IO_DIR"
IN_JSON = "in.json"
OUT_JSON = "out.json"
# Inside a container the directory is fixed; the subprocess runner uses a
# per-attempt temp dir, since several attempts share one machine.
CONTAINER_IO_DIR = "/aaiclick"


def write_in_json(io_dir: Path, kwargs: dict) -> Path:
    """Write the resolved kwargs the task reads."""
    path = io_dir / IN_JSON
    path.write_text(json.dumps(kwargs))
    return path


def parse_out_json(text: str | None) -> Any:
    """The result a task wrote: ``None`` when it wrote nothing.

    Raises:
        ValueError: when the content is not JSON, naming the file.
    """
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"{OUT_JSON} is not valid JSON: {e}") from e


def read_out_json(io_dir: Path) -> Any:
    """``parse_out_json`` over the file in ``io_dir``; a missing file is no result."""
    path = io_dir / OUT_JSON
    return parse_out_json(path.read_text() if path.exists() else None)
