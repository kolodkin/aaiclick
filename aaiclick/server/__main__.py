from __future__ import annotations

import os
import sys


def main() -> None:
    try:
        import uvicorn
    except ImportError:
        sys.stderr.write(
            "aaiclick.server requires the [server] extra: pip install 'aaiclick[server]'\n"
        )
        sys.exit(1)

    uvicorn.run(
        "aaiclick.server.app:create_app",
        factory=True,
        host=os.environ.get("AAICLICK_SERVER_HOST", "127.0.0.1"),
        port=int(os.environ.get("AAICLICK_SERVER_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
