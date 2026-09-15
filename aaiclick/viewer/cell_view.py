"""Shape validation for ``cell_view`` YAML — interpreted by the SPA, validated here."""

from __future__ import annotations

import yaml

PARAMS_KEY = "params"


def cell_view_error(text: object) -> str | None:
    """``None`` when ``text`` is empty or a mapping of ``col: {type: str, value: str}``
    (plus an optional ``params`` list); otherwise the reason."""
    if text is None or text == "":
        return None
    if not isinstance(text, str):
        return "cell_view must be a YAML string"
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return f"invalid cell_view YAML: {exc}"
    if doc is None:
        return None
    if not isinstance(doc, dict):
        return "cell_view must be a mapping of column name to {type, value}"
    for col, view in doc.items():
        if col == PARAMS_KEY:
            if not isinstance(view, list):
                return "cell_view params must be a list"
            continue
        if not isinstance(view, dict):
            return f"cell_view[{col!r}] must be a mapping with type and value"
        if not isinstance(view.get("type"), str):
            return f"cell_view[{col!r}].type must be a string"
        if not isinstance(view.get("value"), str):
            return f"cell_view[{col!r}].value must be a string"
    return None
