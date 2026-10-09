"""Find the ``@job`` functions a sandbox file declares without importing it."""

from __future__ import annotations

import ast

_ORCHESTRATION_MODULES = {"aaiclick", "aaiclick.orchestration"}


class SandboxSourceError(ValueError):
    """The submitted source cannot be run: a syntax error or no ``@job``."""


def _job_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Local names bound to ``job`` and local aliases of the orchestration
    modules, from the file's top-level imports."""
    job_names: set[str] = set()
    module_aliases: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in _ORCHESTRATION_MODULES:
            for alias in node.names:
                local = alias.asname or alias.name
                if alias.name == "job":
                    job_names.add(local)
                elif alias.name == "orchestration":
                    module_aliases.add(local)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _ORCHESTRATION_MODULES:
                    module_aliases.add(alias.asname or alias.name)
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
    found = [
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(_is_job_decorator(dec, job_names, module_aliases) for dec in node.decorator_list)
    ]
    if not found:
        raise SandboxSourceError("no @job function found")
    return found
