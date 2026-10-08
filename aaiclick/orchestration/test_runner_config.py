import pytest
from pydantic import ValidationError

from aaiclick.orchestration.runner_config import (
    ImageBuild,
    ImagePrebuilt,
    dump_image_source,
    parse_image_source,
    validate_image_exclusivity,
    validate_task_entry,
)


@pytest.mark.parametrize(
    "sha",
    [
        pytest.param("--upload-pack=touch /tmp/pwned", id="git-option"),
        pytest.param("A" * 40, id="uppercase"),
        pytest.param("a" * 39, id="short"),
        pytest.param("main", id="branch-name"),
    ],
)
def test_image_build_rejects_non_sha_git_sha(sha):
    """``git_sha`` reaches ``git fetch`` argv, so only a full lowercase hex SHA is accepted."""
    with pytest.raises(ValidationError, match="40-char lowercase hex"):
        ImageBuild(git_remote="https://example.com/r.git", git_sha=sha)


def test_prebuilt_requires_nonempty_image_tag():
    with pytest.raises(ValidationError, match="image_tag"):
        ImagePrebuilt(image_tag="")


@pytest.mark.parametrize(
    "build, fields",
    [
        pytest.param(True, (), id="build-flag"),
        pytest.param(False, ("a" * 40,), id="git-field"),
    ],
)
def test_validate_image_exclusivity_rejects_image_with_build(build, fields):
    with pytest.raises(ValueError, match="mutually exclusive"):
        validate_image_exclusivity("python:3.12", *fields, build=build)


def test_validate_image_exclusivity_accepts_one_side():
    validate_image_exclusivity("python:3.12", None)
    validate_image_exclusivity(None, "a" * 40, build=True)


@pytest.mark.parametrize(
    "entry_type, command, match",
    [
        pytest.param("shell", None, "shell.*requires.*command", id="shell-without-command"),
        pytest.param("module", ["echo", "hi"], "module.*command", id="module-with-command"),
        pytest.param("jvm", ["java", "-jar", "app.jar"], "jvm.*command", id="jvm-with-command"),
    ],
)
def test_validate_task_entry_rejects(entry_type, command, match):
    with pytest.raises(ValueError, match=match):
        validate_task_entry(entry_type=entry_type, command=command)


@pytest.mark.parametrize(
    "entry_type, command",
    [
        pytest.param("jvm", None, id="jvm-without-command"),
        # shell is runner-agnostic — valid on subprocess, docker, kubernetes alike
        pytest.param("shell", ["python", "main.py"], id="shell-with-command"),
    ],
)
def test_validate_task_entry_accepts(entry_type, command):
    validate_task_entry(entry_type=entry_type, command=command)


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(
            ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu"),
            id="build",
        ),
        pytest.param(ImagePrebuilt(image_tag="ghcr.io/x/y:1"), id="prebuilt"),
    ],
)
def test_image_source_round_trip(source):
    assert parse_image_source(dump_image_source(source)) == source


def test_parse_image_source_rejects_unknown_type():
    with pytest.raises(ValidationError):
        parse_image_source({"type": "carrier-pigeon"})
