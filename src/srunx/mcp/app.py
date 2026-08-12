"""The MCP server singleton for srunx tools.

The server instance lives here (not in ``server.py``) so that tool modules
under :mod:`srunx.mcp.tools` can register themselves with ``@mcp.tool()``
decorators without creating a circular import against the entry-point
module. ``server.py`` imports this module + every tool module to trigger
decorator side-effects, then calls :meth:`mcp.run`.

Both ``mcp`` majors are supported from this one place. 2.0 renamed the class
and the module holding it — ``mcp.server.fastmcp.FastMCP`` became
``mcp.server.mcpserver.MCPServer`` — with no compatibility alias, so an
unbounded requirement resolved 2.x on a fresh install and the server died at
import while reporting that ``mcp`` was not installed at all.

Everything srunx uses is identical across both: the constructor's ``name`` and
``instructions``, the ``tool()`` decorator, and ``run()`` defaulting to stdio.
This module holds the only import of ``mcp`` in the codebase, so the entire
difference is which name gets bound here. Verified against 2.0.0 — all fifteen
tools register and dispatch unchanged.
"""

from __future__ import annotations

import sys


def _installed_mcp_version() -> str | None:
    """The installed ``mcp`` distribution's version, or None if there is none.

    Tells "no mcp at all" apart from "an mcp whose API moved". Reporting the
    second as the first sends a user to reinstall a package they already have,
    which is what happened when 2.0 shipped: every call failed while the error
    said the package was missing.
    """
    import importlib.metadata

    try:
        return importlib.metadata.version("mcp")
    except importlib.metadata.PackageNotFoundError:
        return None


def _unsupported_version_message(version: str) -> str:
    return (
        f"srunx-mcp: found 'mcp' {version}, but it provides neither\n"
        "'mcp.server.mcpserver' (2.x) nor 'mcp.server.fastmcp' (1.x).\n"
        "srunx supports mcp 1.x and 2.x.\n"
        "\n"
        "Fix — reinstall within the supported range:\n"
        "  uv tool install --force --with 'mcp[cli]>=1.27.0,<3' srunx\n"
        "\n"
        "or, for a zero-install run (the range comes from srunx's own\n"
        "metadata, so no --with is needed):\n"
        "  uvx --from 'srunx[mcp]' srunx-mcp\n"
    )


_NOT_INSTALLED_MESSAGE = (
    "srunx-mcp: the 'mcp' package is not installed in this Python environment.\n"
    "\n"
    "Fix:\n"
    "  1. Preferred (zero-install):\n"
    "       uvx --from 'srunx[mcp]' srunx-mcp\n"
    "     Register it with Claude Code as:\n"
    "       claude mcp add --scope user srunx -- "
    "uvx --from 'srunx[mcp]' srunx-mcp\n"
    "\n"
    "  2. Globally installed binary:\n"
    "       uv tool install --force --with 'mcp[cli]' srunx\n"
    "     then register:\n"
    "       claude mcp add --scope user srunx -- srunx-mcp\n"
    "\n"
    "Note: 'uv run --extra mcp srunx-mcp' resolves extras against the\n"
    "current working directory's pyproject.toml, so it only works when\n"
    "launched from inside the srunx source tree.\n"
)


try:  # mcp >= 2
    from mcp.server.mcpserver import MCPServer as _Server
except ModuleNotFoundError:
    try:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server
    except ModuleNotFoundError:
        _version = _installed_mcp_version()
        # Installed but offering neither entry point means a major past what
        # this knows about, or a broken install. Saying so beats the
        # "not installed" advice, which would send the user in circles.
        sys.stderr.write(
            _NOT_INSTALLED_MESSAGE
            if _version is None
            else _unsupported_version_message(_version)
        )
        sys.exit(1)


mcp = _Server(
    "srunx",
    instructions=(
        "SLURM job management tools. Use these to submit jobs, monitor status, "
        "manage workflows, check GPU resources, and sync files to remote clusters. "
        "Most operations require either local SLURM access or a configured SSH profile."
    ),
)
