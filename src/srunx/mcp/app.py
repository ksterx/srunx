"""FastMCP singleton for srunx tools.

The ``mcp`` instance lives here (not in ``server.py``) so that tool modules
under :mod:`srunx.mcp.tools` can register themselves with ``@mcp.tool()``
decorators without creating a circular import against the entry-point
module. ``server.py`` imports this module + every tool module to trigger
decorator side-effects, then calls :meth:`mcp.run`.
"""

from __future__ import annotations

import sys

try:
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:
    sys.stderr.write(
        "srunx-mcp: the 'mcp' package is not installed in this Python "
        "environment.\n"
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
    sys.exit(1)


mcp = FastMCP(
    "srunx",
    instructions=(
        "SLURM job management tools. Use these to submit jobs, monitor status, "
        "manage workflows, check GPU resources, and sync files to remote clusters. "
        "Most operations require either local SLURM access or a configured SSH profile."
    ),
)
