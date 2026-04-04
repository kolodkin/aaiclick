"""Entry point for `python -m imdb_dataset_builder`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
