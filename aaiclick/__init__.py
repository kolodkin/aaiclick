"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import client management
from .client import get_client, is_connected

# Import core objects
from .object import Object

# Import factory functions
from .factories import create_object, create_object_from_value

# Import flow visualization
from .flow import (
    FlowNode,
    FlowVisualizer,
    FlowTracker,
    get_flow_tracker,
    reset_flow_tracker,
)
