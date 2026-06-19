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
