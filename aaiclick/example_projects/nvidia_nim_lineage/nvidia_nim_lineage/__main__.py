"""Entry point for `python -m nvidia_nim_lineage`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
