import pytest
from pydantic import ValidationError

from aaiclick.orchestration.runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    KubernetesRunner,
    SubprocessRunner,
    dump_image_source,
    dump_runner_config,
    parse_image_source,
    parse_runner_config,
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


def test_parse_docker_runner_is_bare_marker():
    # Pre-migration job rows carry an "image" key in the runner JSON; the
    # parser must ignore it — the image is a task property now.
    cfg = parse_runner_config({"type": "docker", "image": {"type": "prebuilt", "image_tag": "python:3.12"}})
    assert isinstance(cfg, DockerRunner)
    assert not hasattr(cfg, "image")
    assert dump_runner_config(cfg) == {"type": "docker"}


def test_subprocess_runner_has_no_image():
    cfg = parse_runner_config({"type": "subprocess"})
    assert isinstance(cfg, SubprocessRunner)
    assert not hasattr(cfg, "image")


def test_unknown_runner_type_rejected():
    with pytest.raises(ValidationError):
        parse_runner_config({"type": "nope"})


def test_prebuilt_requires_nonempty_image_tag():
    with pytest.raises(ValidationError, match="image_tag"):
        ImagePrebuilt(image_tag="")


def test_kubernetes_runner_optional_cluster_fields():
    cfg = parse_runner_config({"type": "kubernetes", "namespace": "ml"})
    assert isinstance(cfg, KubernetesRunner)
    assert cfg.namespace == "ml"


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
