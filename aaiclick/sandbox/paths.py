"""Where a submission lives in the sandbox repo, and how that maps to a
dotted entrypoint. Imports nothing from orchestration so the background
worker can use it without a cycle through ``sandbox.repo``."""

from __future__ import annotations

import calendar
from datetime import datetime
from typing import NamedTuple


class ModuleParts(NamedTuple):
    date_dir: str
    module_name: str


def submission_path(name: str, now: datetime) -> str:
    """``YYYYMMDD/sb_<unix ts>_<name>.py`` for a submission made at ``now``.
    A naive ``now`` is UTC (``utc_now()``), whatever the host's zone."""
    return f"{now:%Y%m%d}/sb_{calendar.timegm(now.utctimetuple())}_{name}.py"


def module_parts(path: str) -> ModuleParts:
    """``("YYYYMMDD", "sb_<ts>_<name>")`` — the dotted-path pieces of a submission."""
    date_dir, _, filename = path.partition("/")
    return ModuleParts(date_dir, filename.removesuffix(".py"))
