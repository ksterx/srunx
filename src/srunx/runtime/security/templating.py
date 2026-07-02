"""Sandboxed Jinja2 rendering for workflow args / job fields / scripts.

Workflow YAML (args, exports, job fields, ShellJob scripts) is rendered with
Jinja2. A plain :class:`jinja2.Environment` permits dunder traversal such as
``{{ cycler.__init__.__globals__['os'].system(...) }}`` — a textbook SSTI that
yields arbitrary code execution and bypasses the ``python:``-prefix guard
entirely (a ``{{ }}`` payload has no ``python:`` prefix). Every workflow-path
template must therefore be built from a :class:`SandboxedEnvironment`, which
blocks unsafe attribute access.
"""

import jinja2
from jinja2.sandbox import SandboxedEnvironment


def sandboxed_template(
    source: str,
    *,
    undefined: type[jinja2.Undefined] = jinja2.Undefined,
    keep_trailing_newline: bool = False,
) -> jinja2.Template:
    """Compile ``source`` into a template bound to a sandboxed environment.

    Drop-in replacement for ``jinja2.Template(source, undefined=..., ...)`` in
    the workflow rendering path. ``autoescape`` is intentionally ``False``:
    the output is shell/YAML text, not HTML, and the sandbox — not escaping —
    is what prevents code execution.
    """
    env = SandboxedEnvironment(
        undefined=undefined,
        keep_trailing_newline=keep_trailing_newline,
        autoescape=False,
    )
    return env.from_string(source)
