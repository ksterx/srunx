"""The published metadata must not let an unsupported ``mcp`` major resolve.

``srunx.mcp.app`` binds whichever server class the installed mcp provides, so
1.x and 2.x both work. What must not happen again is an *unbounded* range: 2.0
renamed ``mcp.server.fastmcp.FastMCP`` to ``mcp.server.mcpserver.MCPServer``
with no alias, and ``>=1.27.0`` resolved it happily — a fresh install got an
MCP server that died at import, reporting that mcp was not installed at all.
The next major can rename things just as freely.

Read from the installed distribution's metadata rather than by locating and
parsing ``pyproject.toml``: that is what a user's resolver actually sees, and
it keeps the test free of any assumption about where the file sits.
"""

from __future__ import annotations

import importlib.metadata

import pytest
from packaging.requirements import Requirement


def _mcp_requirement() -> Requirement:
    for raw in importlib.metadata.requires("srunx") or []:
        req = Requirement(raw)
        if req.name == "mcp":
            return req
    raise AssertionError("srunx no longer declares an 'mcp' dependency")


@pytest.mark.parametrize("version", ["1.27.0", "1.29.0", "2.0.0"])
def test_supported_majors_resolve(version: str):
    """Both are verified to work; excluding either would be its own bug."""
    assert _mcp_requirement().specifier.contains(version)


def test_an_untested_major_cannot_resolve():
    """Not a guess about 3.0 — the point is that the range stays bounded, so a
    rename lands as a resolution result rather than a broken server."""
    assert not _mcp_requirement().specifier.contains("3.0.0")
