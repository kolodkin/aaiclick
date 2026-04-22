"""Entry point: ``python -m aaiclick.server`` launches uvicorn on the app.

Host and port come from ``AAICLICK_SERVER_HOST`` (default ``127.0.0.1``) and
``AAICLICK_SERVER_PORT`` (default ``8000``). The app is passed as an
import-string + ``factory=True`` so ``uvicorn --reload`` would work out of
the box; we do not enable reload by default.
"""

from __future__ import annotations

import os
import sys


def main() -> None:
    try:
        import uvicorn
    except ImportError:
        sys.stderr.write(
            "aaiclick.server requires the [server] extra: "
            "pip install 'aaiclick[server]'\n"
        )
        sys.exit(1)

    host = os.environ.get("AAICLICK_SERVER_HOST", "127.0.0.1")
    port = int(os.environ.get("AAICLICK_SERVER_PORT", "8000"))

    uvicorn.run(
        "aaiclick.server.app:create_app",
        factory=True,
        host=host,
        port=port,
    )


if __name__ == "__main__":
    main()
