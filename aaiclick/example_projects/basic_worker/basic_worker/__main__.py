"""Entry point for `python -m basic_worker`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
