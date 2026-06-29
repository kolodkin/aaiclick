import pytest
from pydantic import ValidationError

from aaiclick.orchestration.runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    KubernetesRunner,
    SubprocessRunner,
    dump_runner_config,
    parse_runner_config,
    validate_task_entry,
)


def test_parse_docker_build_runner_roundtrips():
    cfg = parse_runner_config(
        {"type": "docker", "image": {"type": "build", "git_remote": "git@x:r.git", "git_sha": "a" * 40}}
    )
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImageBuild)
    assert dump_runner_config(cfg)["image"]["git_sha"] == "a" * 40


def test_parse_docker_prebuilt_runner():
    cfg = parse_runner_config({"type": "docker", "image": {"type": "prebuilt", "image_tag": "python:3.12"}})
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImagePrebuilt)
    assert cfg.image.image_tag == "python:3.12"


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
    cfg = parse_runner_config(
        {"type": "kubernetes", "image": {"type": "prebuilt", "image_tag": "python:3.12"}, "namespace": "ml"}
    )
    assert isinstance(cfg, KubernetesRunner)
    assert cfg.namespace == "ml"
    assert cfg.service_account is None


def test_shell_entry_requires_command():
    with pytest.raises(ValueError, match="shell.*requires.*command"):
        validate_task_entry(entry_type="shell", command=None)


def test_module_entry_rejects_command():
    with pytest.raises(ValueError, match="module.*command"):
        validate_task_entry(entry_type="module", command=["echo", "hi"])


def test_shell_entry_valid_on_any_runner():
    # shell is runner-agnostic — valid on subprocess, docker, kubernetes alike
    validate_task_entry(entry_type="shell", command=["python", "main.py"])
