"""Exceptions raised by the data layer.

Lives in its own module so both ``data_context`` (which raises) and callers
that map the failure onto their own error shape (``internal_api``, the CLI)
can import it without pulling either side into a cycle.
"""

from __future__ import annotations


class ObjectNotFoundError(RuntimeError):
    """No persistent object exists under the requested name and scope.

    Subclasses ``RuntimeError`` so code written against the previous
    contract of :func:`open_object` keeps working, while callers that need
    to tell "the object is missing" apart from "the context is wrong" can
    catch this instead.
    """
