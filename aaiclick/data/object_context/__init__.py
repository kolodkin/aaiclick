"""
aaiclick.data.object_context - Context management, client, and lifecycle subpackage.
"""

from .ch_client import ChClient, get_ch_client
from .lifecycle import LifecycleHandler, LocalLifecycleHandler, get_data_lifecycle
from .object_context import (
    InvalidObjectNameError,
    ObjectNotFoundError,
    _engine_var,
    _objects_var,
    create_object,
    create_object_from_value,
    decref,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    get_engine,
    incref,
    list_job_tables,
    list_persistent_objects,
    list_persistent_tables,
    object_context,
    open_object,
    register_object,
)
