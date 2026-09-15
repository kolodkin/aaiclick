"""
aaiclick.ai.importing - On-demand import of modules that need the ``ai`` extra.

Stdlib-only, so it is safe to import from the core package and the CLI.
"""

import importlib
from types import ModuleType

AI_EXTRA_HINT = "AI features require the aaiclick[ai] extra. Install with: pip install aaiclick[ai]"


def import_ai_module(name: str) -> ModuleType:
    """Import ``name`` (a module that pulls in litellm) when first needed.

    Raises ``ImportError`` carrying the install hint when the ``ai`` extra is
    missing, so callers surface one consistent message.
    """
    try:
        return importlib.import_module(name)
    except ImportError as err:
        raise ImportError(AI_EXTRA_HINT) from err
