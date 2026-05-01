"""MCP tool implementations grouped by domain.

Modules under this package register their tools with the FastMCP singleton
in :mod:`srunx.mcp.app` via ``@mcp.tool()`` decorators. The package
``__init__`` deliberately does NOT re-export tool functions — see the
parallel decision in ``cli/commands/jobs/__init__.py``: re-exports here
shadow the submodule attributes and make ``srunx.mcp.tools.jobs`` resolve
to the function instead of the module, which breaks ``importlib`` and
test ``@patch`` paths.
"""
