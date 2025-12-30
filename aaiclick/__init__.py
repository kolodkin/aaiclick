"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import core objects
from .object import (
    ClickHouseObject,
    DataFrameObject,
    ColumnObject,
    ExpressionObject,
    AggregationObject,
)

# Import flow visualization
from .flow import (
    FlowNode,
    FlowVisualizer,
    FlowTracker,
    get_flow_tracker,
    reset_flow_tracker,
)
