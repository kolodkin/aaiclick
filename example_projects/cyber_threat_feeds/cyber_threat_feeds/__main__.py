"""Entry point for `python -m cyber_threat_feeds`."""

import asyncio

from . import main

if __name__ == "__main__":
    asyncio.run(main())
