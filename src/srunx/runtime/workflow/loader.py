"""YAML workflow loader helpers: Jinja var discovery + dependency resolution.

Pulled unchanged out of :mod:`srunx.runtime.workflow.runner` as part of Phase 7 (#163). All
functions here are pure — they do not touch SLURM, the DB, or the filesystem
beyond YAML parsing at the call site.
"""

import re
from textwrap import dedent
from typing import Any

import jinja2

from srunx.common.logging import get_logger
from srunx.runtime.workflow.safe_eval import _safe_eval, _safe_exec

logger = get_logger(__name__)


def _has_python_prefix(value: str) -> bool:
    """Check if a string value has a 'python:' prefix (case-insensitive)."""
    return value.lstrip().lower().startswith("python:")


def _strip_python_prefix(value: str) -> str:
    """Strip the 'python:' prefix (case-insensitive) and leading whitespace."""
    stripped = value.lstrip()
    # Remove the prefix preserving the original case length
    return stripped[len("python:") :].lstrip()


_JINJA_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def _find_jinja_refs(text: str) -> set[str]:
    """Find all Jinja2 variable names referenced in *text*."""
    return set(_JINJA_VAR_RE.findall(text))


def _dependency_closure(jobs_data: list[dict[str, Any]], target: str) -> set[str]:
    """Return *target* plus every job it transitively depends on."""
    name_to_deps: dict[str, list[str]] = {
        jd["name"]: list(jd.get("depends_on") or []) for jd in jobs_data if "name" in jd
    }
    closure: set[str] = set()
    stack = [target]
    while stack:
        node = stack.pop()
        if node in closure:
            continue
        if node not in name_to_deps:
            continue
        closure.add(node)
        stack.extend(name_to_deps[node])
    return closure


class _DepsNamespace:
    """Jinja-friendly wrapper for ``{{ deps.<job>.<key> }}`` access.

    Plain dicts shadow user-declared keys with built-in methods — e.g.
    ``{{ deps.a.items }}`` resolves to ``dict.items`` rather than the
    user's export named ``items``. This wrapper forces attribute access
    to go through dict lookup, wraps nested dict values recursively, and
    raises ``AttributeError`` for missing keys so ``StrictUndefined``
    can surface a clear error.
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getattr__(self, name: str) -> Any:
        try:
            value = self._data[name]
        except KeyError as exc:
            raise AttributeError(
                f"'deps' has no entry '{name}'. "
                "Check that it is listed in this job's 'depends_on' "
                "and that the referenced key is declared in the parent's 'exports'."
            ) from exc
        if isinstance(value, dict):
            return _DepsNamespace(value)
        return value

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def __contains__(self, name: str) -> bool:
        return name in self._data


def _find_required_variables(jobs_yaml: str, args: dict[str, Any]) -> set[str]:
    """Return the set of arg keys that the jobs YAML transitively depends on.

    Starts from variables directly referenced in the YAML, then walks
    through each arg value to discover transitive dependencies.
    """

    # Build a dependency graph: key -> set of keys it depends on
    graph: dict[str, set[str]] = {}
    for key, value in args.items():
        deps: set[str] = set()
        if isinstance(value, str):
            deps = _find_jinja_refs(value) & args.keys()
        graph[key] = deps

    # Seed: variables referenced directly in the jobs section
    seeds = _find_jinja_refs(jobs_yaml) & args.keys()

    # Expand seeds transitively
    required: set[str] = set()
    queue = list(seeds)
    while queue:
        var = queue.pop()
        if var in required:
            continue
        required.add(var)
        queue.extend(graph.get(var, set()) - required)

    return required


def _eval_python_var(code_raw: str, evaluated: dict[str, Any]) -> Any:
    """Evaluate a single python: variable value."""
    code = jinja2.Template(code_raw, undefined=jinja2.DebugUndefined).render(
        **evaluated
    )
    code = dedent(code).lstrip()
    try:
        return _safe_eval(code, {"args": evaluated})
    except SyntaxError:
        ns = _safe_exec(code, {"args": evaluated})
        return ns.get("result")


def _evaluate_variables(args: dict[str, Any], required: set[str]) -> dict[str, Any]:
    """Evaluate all *required* args in topological order.

    Uses ``graphlib.TopologicalSorter`` instead of ad-hoc while loops.
    Python-prefixed values are evaluated via ``_safe_eval``/``_safe_exec``;
    Jinja-only values are rendered via ``jinja2.Template``.
    """
    from graphlib import CycleError, TopologicalSorter

    # Partition into python vs plain, only keeping required keys
    plain: dict[str, Any] = {}
    python: dict[str, str] = {}
    for key in required:
        if key not in args:
            continue
        value = args[key]
        if isinstance(value, str) and _has_python_prefix(value):
            python[key] = _strip_python_prefix(value)
        else:
            plain[key] = value

    # Build dependency graph for ALL required variables
    all_vars = {**plain, **{k: args[k] for k in python}}
    graph: dict[str, set[str]] = {}
    for key, value in all_vars.items():
        if isinstance(value, str):
            graph[key] = _find_jinja_refs(value) & required
        elif key in python:
            # For python vars, scan the code for variable name references
            code = python[key]
            deps = set()
            for other in required:
                if other != key and other in code:
                    deps.add(other)
            graph[key] = deps
        else:
            graph[key] = set()

    # Topological sort
    try:
        sorter = TopologicalSorter(graph)
        order = list(sorter.static_order())
    except CycleError:
        logger.warning("Circular variable dependencies detected, using fallback order")
        order = list(required)

    # Evaluate in order
    evaluated: dict[str, Any] = {}
    for key in order:
        if key not in required or key not in args:
            continue

        if key in python:
            try:
                evaluated[key] = _eval_python_var(python[key], evaluated)
            except Exception as e:
                logger.warning(f"Failed to evaluate python variable '{key}': {e}")
        elif isinstance(plain.get(key), str):
            value = plain[key]
            refs = _find_jinja_refs(value)
            if refs:
                tmpl = jinja2.Template(value, undefined=jinja2.DebugUndefined)
                evaluated[key] = tmpl.render(**evaluated)
            else:
                evaluated[key] = value
        else:
            evaluated[key] = plain.get(key)

    return evaluated
