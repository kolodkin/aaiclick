import pytest

from .parse import SandboxSourceError, find_job_functions


@pytest.mark.parametrize(
    "source, expected",
    [
        ("from aaiclick.orchestration import job\n@job\ndef a(): ...", ["a"]),
        ("from aaiclick.orchestration import job\n@job('x')\nasync def a(): ...", ["a"]),
        ("from aaiclick.orchestration import job as j\n@j(name='x')\ndef a(): ...", ["a"]),
        ("from aaiclick import orchestration as o\n@o.job\ndef a(): ...\n@o.job\ndef b(): ...", ["a", "b"]),
        (
            "from aaiclick.orchestration import job, task\n@task\ndef t(): ...\n"
            "def outer():\n    @job\n    def inner(): ...\n@job\ndef a(): ...",
            ["a"],
        ),
    ],
)
def test_find_job_functions(source, expected):
    assert find_job_functions(source) == expected


@pytest.mark.parametrize(
    "source, message",
    [
        ("def a(:", "line 1"),
        ("from aaiclick.orchestration import task\n@task\ndef a(): ...", "no @job function found"),
    ],
)
def test_find_job_functions_rejects(source, message):
    with pytest.raises(SandboxSourceError, match=message):
        find_job_functions(source)


@pytest.mark.parametrize(
    "source, expected",
    [
        ("from aaiclick.orchestration.decorators import job\n@job\ndef a(): ...", ["a"]),
        ("import aaiclick.orchestration\n@aaiclick.orchestration.job\ndef a(): ...", ["a"]),
        ("from aaiclick.orchestration import job\n@job\ndef a(): ...\n@job\ndef a(): ...", ["a"]),
    ],
)
def test_find_job_functions_more_forms(source, expected):
    assert find_job_functions(source) == expected


@pytest.mark.parametrize(
    "source",
    [
        "from aaiclick import job\n@job\ndef a(): ...",
        "import aaiclick\n@aaiclick.job\ndef a(): ...",
    ],
)
def test_find_job_functions_rejects_names_aaiclick_does_not_export(source):
    with pytest.raises(SandboxSourceError, match="no @job function found"):
        find_job_functions(source)
