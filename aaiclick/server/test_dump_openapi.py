"""Contract test for the OpenAPI schema the SPA types are generated from."""

from .app import app

REF_PREFIX = "#/components/schemas/"


def _refs(node: object) -> set[str]:
    if isinstance(node, dict):
        ref = node.get("$ref")
        found = {ref.removeprefix(REF_PREFIX)} if isinstance(ref, str) else set()
        return found.union(*(_refs(v) for v in node.values()))
    if isinstance(node, list):
        return set().union(*(_refs(v) for v in node))
    return set()


def test_response_schemas_require_defaulted_fields():
    """Every response model must extend ApiModel: defaulted fields are always sent."""
    spec = app.openapi()
    schemas = spec["components"]["schemas"]
    pending = _refs([op.get("responses", {}) for ops in spec["paths"].values() for op in ops.values()])
    seen: set[str] = set()
    while pending:
        name = pending.pop()
        seen.add(name)
        pending |= _refs(schemas[name]) - seen
    optional = {
        f"{name}.{field}"
        for name in seen
        for field, prop in schemas[name].get("properties", {}).items()
        if "default" in prop and field not in schemas[name].get("required", [])
    }
    assert not optional, f"defaulted response fields not required (extend ApiModel): {sorted(optional)}"
