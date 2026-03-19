"""
aaiclick.ai.oplog - Format oplog data structures for LLM consumption.
"""

from aaiclick.oplog.graph import OplogGraph


def oplog_graph_to_prompt_context(graph: OplogGraph) -> str:
    """Format an OplogGraph as plain text for LLM consumption."""
    if not graph.nodes:
        return "No operation log information found."

    lines: list[str] = ["Data operation graph:", ""]
    for node in graph.nodes:
        sources = list(node.args) + list(node.kwargs.values())
        src_str = ", ".join(sources) if sources else "(none)"
        lines.append(f"  {node.table}")
        lines.append(f"    operation : {node.operation}")
        lines.append(f"    inputs    : {src_str}")
        if node.sql_template:
            lines.append(f"    sql       : {node.sql_template}")
        lines.append("")

    lines.append("Edges:")
    for edge in graph.edges:
        lines.append(f"  {edge.source} -> {edge.target} [{edge.operation}]")

    return "\n".join(lines)
