"""
aaiclick.flow - Flow visualization for ClickHouse operations using Rich.

This module provides terminal-based visualization of the translation flow
from Python code to ClickHouse operations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from rich.console import Console
from rich.tree import Tree
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.syntax import Syntax
from rich.live import Live
from rich.layout import Layout
from rich import box


class FlowNode:
    """
    Represents a single node in the execution flow.
    """

    def __init__(
        self,
        node_type: str,
        name: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.node_type = node_type  # 'function', 'query', 'aggregation', etc.
        self.name = name
        self.description = description or ""
        self.metadata = metadata or {}
        self.children: List['FlowNode'] = []
        self.timestamp = datetime.now()
        self.status = "pending"  # pending, running, completed, failed

    def add_child(self, child: 'FlowNode') -> None:
        """Add a child node to this node."""
        self.children.append(child)


class FlowVisualizer:
    """
    Visualizes the execution flow of aaiclick operations in the terminal.
    """

    def __init__(self, title: str = "aaiclick Execution Flow"):
        self.console = Console()
        self.title = title
        self.root_nodes: List[FlowNode] = []
        self.current_node: Optional[FlowNode] = None

    def add_node(
        self,
        node_type: str,
        name: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent: Optional[FlowNode] = None,
    ) -> FlowNode:
        """
        Add a new node to the flow.

        Args:
            node_type: Type of operation (function, query, aggregation, etc.)
            name: Name of the operation
            description: Optional description
            metadata: Optional metadata dictionary
            parent: Parent node, if this is a child operation

        Returns:
            FlowNode: The created node
        """
        node = FlowNode(node_type, name, description, metadata)

        if parent:
            parent.add_child(node)
        else:
            self.root_nodes.append(node)

        return node

    def _build_tree(self, node: FlowNode, tree: Optional[Tree] = None) -> Tree:
        """
        Recursively build a Rich Tree from a FlowNode.
        """
        # Status icon
        status_icons = {
            "pending": "⏳",
            "running": "▶️ ",
            "completed": "✅",
            "failed": "❌",
        }
        icon = status_icons.get(node.status, "❓")

        # Node label with styling
        label = f"{icon} [{self._get_color(node.node_type)}]{node.node_type.upper()}[/] {node.name}"
        if node.description:
            label += f" - [dim]{node.description}[/dim]"

        if tree is None:
            tree = Tree(label)
            current_tree = tree
        else:
            current_tree = tree.add(label)

        # Add metadata as sub-items
        if node.metadata:
            for key, value in node.metadata.items():
                current_tree.add(f"[dim]{key}:[/dim] {value}")

        # Recursively add children
        for child in node.children:
            self._build_tree(child, current_tree)

        return tree

    def _get_color(self, node_type: str) -> str:
        """Get color for node type."""
        colors = {
            "function": "cyan",
            "query": "green",
            "aggregation": "yellow",
            "filter": "magenta",
            "transform": "blue",
            "table": "red",
            "column": "white",
        }
        return colors.get(node_type, "white")

    def display(self) -> None:
        """Display the complete flow tree."""
        self.console.print(Panel(f"[bold]{self.title}[/bold]", border_style="blue"))

        if not self.root_nodes:
            self.console.print("[yellow]No operations to display[/yellow]")
            return

        for root in self.root_nodes:
            tree = self._build_tree(root)
            self.console.print(tree)
            self.console.print()

    def display_summary(self) -> None:
        """Display a summary table of all operations."""
        table = Table(title=self.title, box=box.ROUNDED)
        table.add_column("Type", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Timestamp", style="dim")

        def add_node_to_table(node: FlowNode, indent: int = 0):
            status_icons = {
                "pending": "⏳ Pending",
                "running": "▶️  Running",
                "completed": "✅ Completed",
                "failed": "❌ Failed",
            }
            table.add_row(
                "  " * indent + node.node_type,
                node.name,
                status_icons.get(node.status, "❓ Unknown"),
                node.timestamp.strftime("%H:%M:%S"),
            )
            for child in node.children:
                add_node_to_table(child, indent + 1)

        for root in self.root_nodes:
            add_node_to_table(root)

        self.console.print(table)

    def show_query(self, query: str, title: str = "Generated ClickHouse Query") -> None:
        """
        Display a formatted SQL query.

        Args:
            query: The SQL query to display
            title: Title for the panel
        """
        syntax = Syntax(query, "sql", theme="monokai", line_numbers=True)
        panel = Panel(syntax, title=title, border_style="green")
        self.console.print(panel)

    def show_progress(self, tasks: List[str]) -> Progress:
        """
        Create and return a progress bar for tracking operations.

        Args:
            tasks: List of task names to track

        Returns:
            Progress: Rich Progress object
        """
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console,
        )

        return progress

    def live_display(self) -> Live:
        """
        Create a live-updating display for real-time flow visualization.

        Returns:
            Live: Rich Live object for updating display
        """
        layout = Layout()
        return Live(layout, console=self.console, refresh_per_second=4)


class FlowTracker:
    """
    Tracks and visualizes the execution flow of aaiclick operations.
    """

    def __init__(self):
        self.visualizer = FlowVisualizer()
        self.node_stack: List[FlowNode] = []

    def start_operation(
        self,
        node_type: str,
        name: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FlowNode:
        """Start tracking a new operation."""
        parent = self.node_stack[-1] if self.node_stack else None
        node = self.visualizer.add_node(node_type, name, description, metadata, parent)
        node.status = "running"
        self.node_stack.append(node)
        return node

    def complete_operation(self, success: bool = True) -> None:
        """Mark the current operation as complete."""
        if self.node_stack:
            node = self.node_stack.pop()
            node.status = "completed" if success else "failed"

    def show(self) -> None:
        """Display the current flow."""
        self.visualizer.display()

    def show_summary(self) -> None:
        """Display a summary of all operations."""
        self.visualizer.display_summary()

    def show_query(self, query: str, title: str = "Generated ClickHouse Query") -> None:
        """Display a formatted query."""
        self.visualizer.show_query(query, title)


# Global flow tracker instance
_flow_tracker = FlowTracker()


def get_flow_tracker() -> FlowTracker:
    """Get the global flow tracker instance."""
    return _flow_tracker


def reset_flow_tracker() -> None:
    """Reset the global flow tracker."""
    global _flow_tracker
    _flow_tracker = FlowTracker()
