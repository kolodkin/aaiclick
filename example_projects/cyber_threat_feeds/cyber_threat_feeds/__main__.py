"""Entry point for ``python -m cyber_threat_feeds`` — run the job inline (local mode).

Accepts ``--params '<JSON object>'`` to override pipeline kwargs, e.g.
``--params '{"shodan_limit": 1000}'``.
"""

import argparse
import asyncio
import json
import sys

from aaiclick.orchestration import ajob_test

from . import cyber_threat_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="python -m cyber_threat_feeds", description="Run the cyber threat feeds job inline."
    )
    parser.add_argument(
        "--params", default="{}", help="JSON object of pipeline kwargs, e.g. '{\"shodan_limit\": 1000}'"
    )
    job = asyncio.run(ajob_test(cyber_threat_pipeline, **json.loads(parser.parse_args().params)))
    sys.exit(0 if job.status == "COMPLETED" else 1)
