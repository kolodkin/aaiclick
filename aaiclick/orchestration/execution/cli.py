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
