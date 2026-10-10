from decimal import Decimal

import pytest
from pydantic import ValidationError

from aaiclick.orchestration.runner_config import (
    ImageBuild,
    ImagePrebuilt,
    dump_image_source,
    parse_image_source,
    parse_quantity,
    validate_image_exclusivity,
    validate_resources,
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


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("500m", Decimal("0.5"), id="milli"),
        pytest.param(".5", Decimal("0.5"), id="leading-dot"),
        pytest.param(2, Decimal(2), id="json-number"),
        pytest.param("1.5Gi", Decimal(3 * 512 * 1024**2), id="binary-fraction"),
        pytest.param("1k", Decimal(1000), id="decimal-kilo"),
        pytest.param("2E", Decimal(2 * 1000**6), id="exa-not-exponent"),
        pytest.param("1e3", Decimal(1000), id="exponent"),
    ],
)
def test_parse_quantity(value, expected):
    assert parse_quantity("limits.cpu", value) == expected


@pytest.mark.parametrize(
    "resources, match",
    [
        pytest.param({"limits": {"cpu": "fast"}}, "limits.cpu", id="garbage"),
        pytest.param({"requests": {"memory": "1Xi"}}, "requests.memory", id="bad-suffix"),
        pytest.param({"limits": {"memory": "-1Gi"}}, "limits.memory", id="negative"),
        pytest.param({"limits": "2"}, "resources.limits must be a mapping", id="section-not-a-mapping"),
        pytest.param([{"limits": {}}], "resources must be a mapping", id="not-a-mapping"),
        pytest.param({"limit": {"cpu": "1"}}, "unknown resources section", id="misspelled-section"),
        pytest.param({"limits": {"memory": "1e3Ki"}}, "not a valid", id="exponent-and-suffix"),
        pytest.param({"limits": {"cpu": " 2 "}}, "not a valid", id="whitespace"),
        pytest.param({"limits": {"memory": "1e1000000"}}, "out of range", id="overflow"),
        pytest.param({"limits": {"memory": "1e999999999999999999999"}}, "out of range", id="exponent-too-large"),
    ],
)
def test_validate_resources_rejects(resources, match):
    with pytest.raises(ValueError, match=match):
        validate_resources(resources)


def test_validate_resources_passes_other_keys_through():
    """GPUs and storage are Kubernetes-only; they are not parsed here."""
    validate_resources({"limits": {"cpu": "1", "nvidia.com/gpu": 1, "ephemeral-storage": "anything"}})
