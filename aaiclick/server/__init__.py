"""HTTP surface over ``aaiclick/internal_api/``.

Requires the ``aaiclick[server]`` optional extra. See
``docs/api_server.md`` for the contract and
``docs/api_server_implementation_plan.md`` for the rollout plan.
"""

from .app import API_PREFIX, create_app
