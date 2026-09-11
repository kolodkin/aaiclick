from __future__ import annotations

import pytest

from aaiclick.viewer.cell_view import cell_view_error


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(None, id="none"),
        pytest.param("", id="empty"),
        pytest.param("id:\n  type: link\n  value: https://x/{cell}\n", id="link"),
        pytest.param("params:\n  - name: p\n    options: [a, b]\n", id="params-only"),
    ],
)
def test_cell_view_error_accepts(text):
    assert cell_view_error(text) is None


@pytest.mark.parametrize(
    "text, fragment",
    [
        pytest.param("col: [unclosed", "invalid cell_view", id="bad-yaml"),
        pytest.param("- a\n- b\n", "mapping", id="not-a-mapping"),
        pytest.param("id:\n  type: link\n", "value", id="missing-value"),
        pytest.param("id:\n  type: 7\n  value: x\n", "type", id="type-not-string"),
        pytest.param({"id": {"type": "link"}}, "string", id="not-text"),
    ],
)
def test_cell_view_error_rejects(text, fragment):
    err = cell_view_error(text)
    assert err is not None and fragment in err
