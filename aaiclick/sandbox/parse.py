"""Find the ``@job`` functions a sandbox file declares without importing it."""

from __future__ import annotations

import ast

# Modules that export ``job``; ``from aaiclick import orchestration`` binds a
# module alias, but ``aaiclick`` itself exports no ``job``.
_JOB_MODULES = {"aaiclick.orchestration", "aaiclick.orchestration.decorators"}


class SandboxSourceError(ValueError):
    """The submitted source cannot be run: a syntax error or no ``@job``."""


def _job_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Local names bound to ``job`` and local aliases of the orchestration
    modules, from the file's top-level imports."""
    job_names: set[str] = set()
    module_aliases: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in _JOB_MODULES:
            job_names.update(alias.asname or alias.name for alias in node.names if alias.name == "job")
        elif isinstance(node, ast.ImportFrom) and node.module == "aaiclick":
            module_aliases.update(alias.asname or alias.name for alias in node.names if alias.name == "orchestration")
        elif isinstance(node, ast.Import):
            module_aliases.update(alias.asname or alias.name for alias in node.names if alias.name in _JOB_MODULES)
    return job_names, module_aliases


def _is_job_decorator(dec: ast.expr, job_names: set[str], module_aliases: set[str]) -> bool:
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Name):
        return target.id in job_names
    if isinstance(target, ast.Attribute) and target.attr == "job":
        value = target.value
        if isinstance(value, ast.Name):
            return value.id in module_aliases
        # ``aaiclick.orchestration.job`` after ``import aaiclick.orchestration``
        if isinstance(value, ast.Attribute) and isinstance(value.value, ast.Name):
            return f"{value.value.id}.{value.attr}" in module_aliases
    return False


def find_job_functions(source: str) -> list[str]:
    """Names of the top-level functions decorated with ``@job`` / ``@job(...)``,
    in source order. Nested functions are ignored.

    Raises:
        SandboxSourceError: on a syntax error (``line N: message``) or when
            no job function is found.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise SandboxSourceError(f"line {exc.lineno}: {exc.msg}") from exc
    job_names, module_aliases = _job_bindings(tree)
    # dict.fromkeys dedupes a redefined job while keeping source order.
    found = list(
        dict.fromkeys(
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and any(_is_job_decorator(dec, job_names, module_aliases) for dec in node.decorator_list)
        )
    )
    if not found:
        raise SandboxSourceError("no @job function found")
    return found
