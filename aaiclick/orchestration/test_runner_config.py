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
    assert cfg.service_account is None


def test_shell_entry_requires_command():
    with pytest.raises(ValueError, match="shell.*requires.*command"):
        validate_task_entry(entry_type="shell", command=None)


def test_module_entry_rejects_command():
    with pytest.raises(ValueError, match="module.*command"):
        validate_task_entry(entry_type="module", command=["echo", "hi"])


def test_jvm_entry_rejects_command():
    with pytest.raises(ValueError, match="jvm.*command"):
        validate_task_entry(entry_type="jvm", command=["java", "-jar", "app.jar"])


def test_jvm_entry_valid_without_command():
    validate_task_entry(entry_type="jvm", command=None)


def test_shell_entry_valid_on_any_runner():
    # shell is runner-agnostic — valid on subprocess, docker, kubernetes alike
    validate_task_entry(entry_type="shell", command=["python", "main.py"])


def test_image_source_round_trip_build():
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")
    parsed = parse_image_source(dump_image_source(source))
    assert isinstance(parsed, ImageBuild)
    assert parsed.git_sha == "a" * 40


def test_image_source_round_trip_prebuilt():
    parsed = parse_image_source(dump_image_source(ImagePrebuilt(image_tag="ghcr.io/x/y:1")))
    assert isinstance(parsed, ImagePrebuilt)
    assert parsed.image_tag == "ghcr.io/x/y:1"


def test_parse_image_source_rejects_unknown_type():
    with pytest.raises(ValidationError):
        parse_image_source({"type": "carrier-pigeon"})
