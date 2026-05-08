"""MCP server entry point.

Tool registration happens at import time of the modules under
:mod:`srunx.mcp.tools` (each ``@mcp.tool()`` decorator registers against
the FastMCP singleton in :mod:`srunx.mcp.app`). This module imports them
purely for side-effect, then exposes :func:`main` for the
``srunx-mcp`` console script (``[project.scripts]`` in ``pyproject.toml``).
"""

from __future__ import annotations

from srunx.mcp.app import mcp
from srunx.mcp.tools import config, jobs, resources, sync, workflows

# Tool registration runs at import time via @mcp.tool() decorators in each
# module; binding the modules to a tuple keeps the imports "used" without
# needing an unused-import suppression on the import line.
_REGISTERED_TOOL_MODULES = (config, jobs, resources, sync, workflows)


def main() -> None:
    """Run the srunx MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
