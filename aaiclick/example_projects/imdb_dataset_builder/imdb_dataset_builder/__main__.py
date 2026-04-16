"""Entry point for `python -m imdb_dataset_builder`."""

import asyncio
import sys

from aaiclick.orchestration import ajob_test

from . import imdb_dataset_pipeline, main


async def _run():
    created_job = await imdb_dataset_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")
    await ajob_test(created_job)


if __name__ == "__main__":
    if "--run" in sys.argv:
        asyncio.run(_run())
    else:
        asyncio.run(main())
