"""The reference compose stack's ``env.init.sh``, run as a user would."""

import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

COMPOSE_DIR = Path(__file__).resolve().parent.parent / "deploy" / "compose"
CREDENTIALS = ("AAICLICK_PG_PASSWORD", "AAICLICK_CH_PASSWORD", "AAICLICK_JWT_SECRET", "AAICLICK_ADMIN_PASSWORD")


@pytest.fixture
def stack(tmp_path):
    stack = tmp_path / "stack"
    shutil.copytree(COMPOSE_DIR, stack)
    return stack


def run_init(stack: Path, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["sh", str(stack / "env.init.sh")], cwd=cwd, capture_output=True, text=True)


def test_env_init_fills_every_token(stack, tmp_path):
    result = run_init(stack, cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    env_file = stack / ".env"
    text = env_file.read_text()
    values = dict(line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#"))
    assert not [v for v in values.values() if "change-me" in v]
    for name in CREDENTIALS:
        assert re.fullmatch(r"[0-9a-f]{48}", values[name]), name
    assert values["AAICLICK_IMAGE_TAG"] == "latest"
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert f"Admin login: admin / {values['AAICLICK_ADMIN_PASSWORD']}" in result.stdout


def test_env_init_refuses_existing_env(stack, tmp_path):
    (stack / ".env").write_text("keep\n")

    result = run_init(stack, cwd=tmp_path)

    assert result.returncode != 0
    assert (stack / ".env").read_text() == "keep\n"
