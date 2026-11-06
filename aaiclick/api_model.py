"""Base for models that appear in the OpenAPI schema.

See docs/designs/api_server.md — "Required fields on the wire".
"""

from pydantic import BaseModel, ConfigDict


class ApiModel(BaseModel):
    """Pydantic base for request, response and query models of the HTTP API.

    ``json_schema_serialization_defaults_required`` marks defaulted fields
    required in the response (serialization) schema, since the server always
    sends them; the request (validation) schema keeps them optional. Schema
    output only — validation and ``model_dump()`` are unchanged. Config is
    per class, so every API model, nested ones included, must extend this.
    """

    model_config = ConfigDict(json_schema_serialization_defaults_required=True)
