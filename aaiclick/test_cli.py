"""Tests for the argparse CLI: new shell/image flags and their forwarding."""

from unittest.mock import MagicMock, patch

import pytest

from aaiclick.__main__ import _parse_command_env, _run_register_job, _run_run_job, build_parser


async def _noop_run_internal_api(awaitable):
    """Stand-in for ``_run_internal_api`` that consumes the coroutine it is given."""
    if hasattr(awaitable, "__await__"):
        awaitable.close()
    return MagicMock()


def test_run_job_parser_shell_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run-job",
            "j",
            "--entry-type",
            "shell",
            "--command",
            "python main.py",
            "--command-env",
            "K=v",
            "--command-env",
            "OTHER=2",
            "--image",
            "python:3.12",
        ]
    )
    assert args.entry_type == "shell"
    assert args.command == "python main.py"
    assert args.command_env == ["K=v", "OTHER=2"]
    assert args.image == "python:3.12"


def test_run_job_parser_entry_type_default_module():
    parser = build_parser()
    args = parser.parse_args(["run-job", "j"])
    assert args.entry_type == "module"
    assert args.command is None
    assert args.command_env is None
    assert args.image is None


def test_register_job_parser_image_flag():
    parser = build_parser()
    args = parser.parse_args(["register-job", "myapp.jobs.etl", "--image", "myrepo/img:1"])
    assert args.image == "myrepo/img:1"


def test_parse_command_env_pairs():
    assert _parse_command_env(["K=v", "A=b=c"]) == {"K": "v", "A": "b=c"}
    assert _parse_command_env(None) is None
    assert _parse_command_env([]) is None


def test_parse_command_env_rejects_missing_equals():
    with pytest.raises(ValueError, match="KEY=VALUE"):
        _parse_command_env(["bad"])


async def test_run_run_job_forwards_shell_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run-job",
            "j",
            "--entry-type",
            "shell",
            "--command",
            "python main.py --flag",
            "--command-env",
            "K=v",
            "--image",
            "python:3.12",
        ]
    )
    with (
        patch("aaiclick.__main__.internal_api.run_job") as run_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_run_job(args)

    request = run_job.call_args.args[0]
    assert request.entry_type == "shell"
    assert request.command == ["python", "main.py", "--flag"]
    assert request.command_env == {"K": "v"}
    assert request.image == "python:3.12"


async def test_run_register_job_forwards_image():
    parser = build_parser()
    args = parser.parse_args(["register-job", "myapp.jobs.etl", "--image", "myrepo/img:1"])
    with (
        patch("aaiclick.__main__.internal_api.register_job") as register_job,
        patch("aaiclick.__main__._run_internal_api", new=_noop_run_internal_api),
        patch("aaiclick.__main__._render"),
    ):
        await _run_register_job(args)

    request = register_job.call_args.args[0]
    assert request.image == "myrepo/img:1"
