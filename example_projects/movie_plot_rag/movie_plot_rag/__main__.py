"""Entry point for ``python -m movie_plot_rag`` — run the job inline (local mode).

Accepts ``--params '<JSON object>'`` to override pipeline kwargs, e.g.
``--params '{"corpus_size": 200, "top_k": 5}'``.
"""

import argparse
import asyncio
import json
import sys

from aaiclick.orchestration import ajob_test

from . import movie_plot_rag_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m movie_plot_rag", description="Run the movie plot RAG job inline.")
    parser.add_argument("--params", default="{}", help="JSON object of pipeline kwargs, e.g. '{\"corpus_size\": 200}'")
    job = asyncio.run(ajob_test(movie_plot_rag_pipeline, **json.loads(parser.parse_args().params)))
    sys.exit(0 if job.status == "COMPLETED" else 1)
