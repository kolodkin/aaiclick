"""Print the FastAPI app's OpenAPI schema as JSON to stdout.

The SPA's TypeScript types are generated from this schema (``npm run
gen-types`` pipes this module's output through ``openapi-typescript``).
FastAPI builds the schema in-process via ``app.openapi()``, so no running
server is needed.

Run with the server extra::

    uv run --extra server python -m aaiclick.server.dump_openapi
"""

import json
import sys

from .app import app


def main() -> None:
    # ``sort_keys`` keeps the generated TS stable across runs so the CI
    # freshness check only fails on real schema changes.
    json.dump(app.openapi(), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
