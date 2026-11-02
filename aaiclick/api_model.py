"""Base for models that appear in the OpenAPI schema."""

from pydantic import BaseModel, ConfigDict


class ApiModel(BaseModel):
    """Marks defaulted fields required in response schemas.

    The server always serializes them, so the generated TypeScript types keep
    them required. Request schemas still list them as optional.
    """

    model_config = ConfigDict(json_schema_serialization_defaults_required=True)
