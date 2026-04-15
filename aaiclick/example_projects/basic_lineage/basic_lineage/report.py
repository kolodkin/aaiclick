"""Markdown report for the basic_lineage example project."""

from __future__ import annotations

from aaiclick.oplog.lineage import OplogGraph
from aaiclick.orchestration.models import Task


def _print_graph(
    graph: OplogGraph,
    title: str,
    root: str,
    labels: dict[str, str],
) -> None:
    print(f"\n## {title}\n")
    print(f"- Root: `{labels.get(root, root)}`")
    print(f"- Operations: {len(graph.nodes)}")
    print(f"- Edges: {len(graph.edges)}\n")
    for edge in graph.edges:
        src = labels.get(edge.source, edge.source)
        tgt = labels.get(edge.target, edge.target)
        print(f"- `{src}` -> `{tgt}` (via `{edge.operation}`)")


def print_report(
    *,
    tasks: list[Task],
    target_table: str,
    backward_graph: OplogGraph,
    forward_graph: OplogGraph,
    source_table: str,
    explanation: str,
    debug_answer: str,
    sampling_strategy: dict[str, str],
) -> None:
    """Print the full example report as markdown."""
    backward_labels = backward_graph.build_labels()
    forward_labels = forward_graph.build_labels()

    print("## Pipeline Tasks\n")
    for t in tasks:
        if t.result:
            table = t.result.get("table", "")
            label = backward_labels.get(table, "")
            suffix = f" ({label})" if label else ""
            print(f"- **{t.name}**: {t.status.value}{suffix}")
        else:
            print(f"- **{t.name}**: {t.status.value}")

    print("\n## Sampling Strategy\n")
    for table, clause in sampling_strategy.items():
        print(f"- `{table}`: `{clause}`")

    _print_graph(backward_graph, "Backward Lineage Graph", target_table, backward_labels)
    _print_graph(forward_graph, "Forward Lineage Graph", source_table, forward_labels)

    print("\n## AI Explanation (backward lineage)\n")
    print("**Question**: How was this table produced? What arithmetic was applied?\n")
    print(explanation)

    print("\n## AI Debug (agentic tool-calling)\n")
    print("**Question**: Which row has the highest value and which inputs drove it?\n")
    print(debug_answer)
