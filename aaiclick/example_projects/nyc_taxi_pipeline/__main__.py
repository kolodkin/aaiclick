"""Entry point for `python -m aaiclick.example_projects.nyc_taxi_pipeline`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
