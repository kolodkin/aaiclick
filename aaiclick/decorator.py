"""
aaiclick.decorator - Decorators for translating Python code to ClickHouse operations.

This module provides decorators that mark Python functions and classes
for translation to ClickHouse operations.
"""

import functools
import inspect
from typing import Callable, Optional, Any, Dict, List
from .object import ClickHouseObject, ExpressionObject


class TranslationContext:
    """
    Context manager for tracking translation state during function execution.
    """

    def __init__(self):
        self.operations: List[str] = []
        self.variables: Dict[str, Any] = {}
        self.current_function: Optional[str] = None

    def add_operation(self, operation: str) -> None:
        """Record an operation for translation."""
        self.operations.append(operation)

    def set_variable(self, name: str, value: Any) -> None:
        """Track a variable assignment."""
        self.variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Retrieve a tracked variable."""
        return self.variables.get(name)


# Global translation context
_context = TranslationContext()


def clickhouse_function(name: Optional[str] = None, description: Optional[str] = None):
    """
    Decorator that marks a Python function for translation to ClickHouse operations.

    Args:
        name: Optional custom name for the ClickHouse function
        description: Optional description of what this function does

    Usage:
        @clickhouse_function()
        def my_computation(data):
            return data.filter(lambda x: x > 0).sum()
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Store original function info
            func._is_clickhouse_function = True
            func._clickhouse_name = name or func.__name__
            func._description = description

            # Track this function call in context
            _context.current_function = func._clickhouse_name
            _context.add_operation(f"CALL {func._clickhouse_name}")

            # Execute the original function
            result = func(*args, **kwargs)

            # If result is a ClickHouseObject, track it
            if isinstance(result, ClickHouseObject):
                _context.set_variable(f"result_{func._clickhouse_name}", result)

            return result

        # Mark wrapper with metadata
        wrapper._is_clickhouse_function = True
        wrapper._clickhouse_name = name or func.__name__
        wrapper._description = description
        wrapper._original_func = func

        return wrapper

    return decorator


def clickhouse_class(cls):
    """
    Class decorator that marks a Python class for translation to ClickHouse operations.

    Usage:
        @clickhouse_class
        class DataProcessor:
            def process(self, data):
                return data.filter(...).aggregate(...)
    """
    cls._is_clickhouse_class = True
    cls._clickhouse_methods = []

    # Find all methods that should be translated
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        if not name.startswith('_'):
            cls._clickhouse_methods.append(name)

    return cls


def clickhouse_property(func: Callable) -> property:
    """
    Decorator for properties that should be translated to ClickHouse column references.

    Usage:
        @clickhouse_property
        def user_id(self):
            return self._user_id
    """

    @functools.wraps(func)
    def wrapper(self):
        result = func(self)
        if isinstance(result, str):
            # Convert string property to ClickHouse column reference
            return ExpressionObject(result, name=func.__name__)
        return result

    prop = property(wrapper)
    prop._is_clickhouse_property = True
    return prop


def aggregate(function_name: str):
    """
    Decorator that marks a function as an aggregation operation.

    Args:
        function_name: Name of the ClickHouse aggregation function (e.g., 'sum', 'avg', 'count')

    Usage:
        @aggregate('sum')
        def total_sales(column):
            return column
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func._is_aggregate = True
            func._aggregate_function = function_name

            result = func(*args, **kwargs)
            _context.add_operation(f"AGGREGATE {function_name}")

            return result

        wrapper._is_aggregate = True
        wrapper._aggregate_function = function_name

        return wrapper

    return decorator


def lazy_evaluation(func: Callable) -> Callable:
    """
    Decorator that marks a function for lazy evaluation.
    The function won't execute immediately but will build up a query plan.

    Usage:
        @lazy_evaluation
        def process_data(data):
            return data.filter(...).transform(...)
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func._is_lazy = True

        # Don't execute immediately, return a placeholder
        result = func(*args, **kwargs)

        # If result is ClickHouseObject, it already supports lazy evaluation
        if isinstance(result, ClickHouseObject):
            result.set_metadata('lazy', True)

        return result

    wrapper._is_lazy = True
    return wrapper


def batch_operation(batch_size: int = 1000):
    """
    Decorator that marks a function as a batch operation.

    Args:
        batch_size: Number of records to process in each batch

    Usage:
        @batch_operation(batch_size=5000)
        def process_large_dataset(data):
            return data.transform(...)
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func._is_batch_operation = True
            func._batch_size = batch_size

            _context.add_operation(f"BATCH {batch_size}")
            return func(*args, **kwargs)

        wrapper._is_batch_operation = True
        wrapper._batch_size = batch_size

        return wrapper

    return decorator


def get_context() -> TranslationContext:
    """Get the current translation context."""
    return _context


def reset_context() -> None:
    """Reset the translation context."""
    global _context
    _context = TranslationContext()
