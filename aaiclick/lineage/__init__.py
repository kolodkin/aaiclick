"""
aaiclick.lineage - Data operation provenance tracking.

This module captures lineage metadata for Object operations, enabling
backward explanation ("how was this produced?") and forward impact
analysis ("what did this data affect?").
"""

from .collector import LineageCollector, get_lineage_collector
from .graph import LineageContext, LineageGraph, LineageNode, backward_explain, forward_impact
from .models import OperationLog
