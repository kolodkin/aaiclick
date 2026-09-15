"""Entry point for `python -m nyc_taxi_pipeline`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
