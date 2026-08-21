"""Tests for the argparse CLI: new shell/image flags and their forwarding."""

from unittest.mock import MagicMock, patch

import pytest

from aaiclick.__main__ import _parse_command_env, _run_register_job, _run_run_job, build_parser, main


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
    assert args.command == "run-job"
    assert args.entry_type == "shell"
    assert args.command_str == "python main.py"
    assert args.command_env == ["K=v", "OTHER=2"]
    assert args.image == "python:3.12"


def test_run_job_parser_entry_type_default_module():
    parser = build_parser()
    args = parser.parse_args(["run-job", "j"])
    assert args.command == "run-job"
    assert args.entry_type == "module"
    assert args.command_str is None
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


def test_run_job_command_dest_not_shadowed_by_command_flag():
    """Regression: ``run-job --command`` must not overwrite the top-level
    subparser ``dest="command"``.

    The subparsers action and the ``--command`` flag both defaulted to
    ``dest="command"``, so parsing any ``run-job`` invocation left
    ``args.command`` as ``None`` (or the ``--command`` value) instead of
    ``"run-job"``. ``main()`` then matched no dispatch branch and silently
    fell through to ``print_help()`` — exit 0, nothing persisted. Only the
    distributed e2e (which calls ``run-job`` via a subprocess) caught it."""
    parser = build_parser()
    for argv in (
        ["run-job", "j"],
        ["run-job", "j", "--git-sha", "a" * 40],
        ["run-job", "j", "--command", "python main.py"],
    ):
        args = parser.parse_args(argv)
        assert args.command == "run-job", argv


def test_main_dispatches_run_job_to_handler():
    """Full ``main()`` dispatch path: ``run-job`` must route to its handler,
    not the top-level ``print_help()`` no-op fallback.

    Guards the silent-no-op regression end-to-end (argv -> dispatch ->
    handler), which the parse-only tests above would miss if ``main()``'s
    branch comparison ever drifted from the subparser name."""
    with (
        patch("sys.argv", ["aaiclick", "run-job", "myjob", "--git-sha", "b" * 40]),
        patch("aaiclick.__main__.asyncio.run"),
        # Patching an async def defaults to AsyncMock, whose call creates a
        # coroutine the mocked asyncio.run never awaits — an unraisable
        # RuntimeWarning that filterwarnings=["error"] fails the run.
        patch("aaiclick.__main__._run_run_job", new_callable=MagicMock) as handler,
    ):
        main()

    handler.assert_called_once()
    dispatched_args = handler.call_args.args[0]
    assert dispatched_args.command == "run-job"
    assert dispatched_args.name == "myjob"
    assert dispatched_args.git_sha == "b" * 40


def test_tenant_parser():
    parser = build_parser()
    args = parser.parse_args(["tenant", "create", "acme", "--name", "Acme Corp"])
    assert args.command == "tenant" and args.tenant_command == "create"
    assert args.slug == "acme" and args.name == "Acme Corp"
    args = parser.parse_args(["tenant", "list"])
    assert args.tenant_command == "list"


def test_member_parser():
    parser = build_parser()
    args = parser.parse_args(["member", "add", "--tenant", "acme", "--username", "u", "--role", "admin"])
    assert args.command == "member" and args.member_command == "add"
    assert args.tenant == "acme" and args.username == "u" and args.role == "admin"
    args = parser.parse_args(["member", "remove", "--tenant", "acme", "--username", "u"])
    assert args.member_command == "remove"


def test_global_tenant_flag():
    parser = build_parser()
    args = parser.parse_args(["--tenant", "acme", "registered-job", "list"])
    assert args.tenant == "acme" and args.command == "registered-job"
    args = parser.parse_args(["registered-job", "list"])
    assert args.tenant is None
