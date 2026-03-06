"""
Register a basic job that prints every 0.5 seconds for 3 seconds.

This script creates and registers a job in PostgreSQL. The job is then
executed by a worker started separately (see basic_worker.sh).
"""

import asyncio
import time

from aaiclick.orchestration import OrchContext, create_job


async def periodic_print():
    """Task that prints every 0.5 seconds for 3 seconds."""
    for i in range(6):
        print(f"[{time.strftime('%H:%M:%S')}] Tick {i + 1}/6")
        await asyncio.sleep(0.5)
    print("Done!")


async def main():
    """Register the job."""
    async with OrchContext():
        job = await create_job("periodic_print_job", periodic_print)
        print(f"Registered job: {job.name} (ID: {job.id})")


if __name__ == "__main__":
    asyncio.run(main())
