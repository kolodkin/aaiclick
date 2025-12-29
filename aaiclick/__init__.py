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

# Import decorators
from .decorator import (
    clickhouse_function,
    clickhouse_class,
    clickhouse_property,
    aggregate,
    lazy_evaluation,
    batch_operation,
    get_context,
    reset_context,
    TranslationContext,
)

# Import flow visualization
from .flow import (
    FlowNode,
    FlowVisualizer,
    FlowTracker,
    get_flow_tracker,
    reset_flow_tracker,
)

__all__ = [
    # Version
    "__version__",
    # Objects
    "ClickHouseObject",
    "DataFrameObject",
    "ColumnObject",
    "ExpressionObject",
    "AggregationObject",
    # Decorators
    "clickhouse_function",
    "clickhouse_class",
    "clickhouse_property",
    "aggregate",
    "lazy_evaluation",
    "batch_operation",
    "get_context",
    "reset_context",
    "TranslationContext",
    # Flow
    "FlowNode",
    "FlowVisualizer",
    "FlowTracker",
    "get_flow_tracker",
    "reset_flow_tracker",
]
