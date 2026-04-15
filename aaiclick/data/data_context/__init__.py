"""
aaiclick.data.data_context - Context management, client, and lifecycle subpackage.
"""

from .ch_client import ChClient, get_ch_client
from .data_context import (
    _engine_var,
    _objects_var,
    create_object,
    create_object_from_value,
    data_context,
    decref,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    get_engine,
    incref,
    list_persistent_objects,
    open_object,
    register_object,
)
from .lifecycle import LifecycleHandler, LocalLifecycleHandler, get_data_lifecycle
