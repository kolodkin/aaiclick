"""Markdown report for the basic_lineage example project."""

from __future__ import annotations

from aaiclick.oplog.lineage import OplogGraph
from aaiclick.orchestration.models import Task


def print_report(
    *,
    tasks: list[Task],
    target_table: str,
    graph: OplogGraph,
    explanation: str,
) -> None:
    """Print the full example report as markdown."""
    labels = graph.build_labels()

    print("## Pipeline Tasks\n")
    for t in tasks:
        if t.result:
            table = t.result.get("table", "")
            label = labels.get(table, "")
            suffix = f" ({label})" if label else ""
            print(f"- **{t.name}**: {t.status.value}{suffix}")
        else:
            print(f"- **{t.name}**: {t.status.value}")

    print("\n## Lineage Graph\n")
    print(f"- Target: `{target_table}`")
    print(f"- Operations: {len(graph.nodes)}")
    print(f"- Edges: {len(graph.edges)}\n")

    for edge in graph.edges:
        src = labels.get(edge.source, edge.source)
        tgt = labels.get(edge.target, edge.target)
        print(f"- `{src}` -> `{tgt}` (via `{edge.operation}`)")

    print("\n## AI Explanation\n")
    print("**Question**: How was this table produced? What arithmetic was applied?\n")
    print(explanation)
