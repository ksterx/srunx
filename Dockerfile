# syntax=docker/dockerfile:1

# Container image that runs the srunx MCP server over stdio. MCP clients
# (Claude Desktop, Claude Code, etc.) and MCP registries such as Glama
# connect by spawning the container and exchanging JSON-RPC frames on
# stdin/stdout.
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir 'srunx[mcp]>=2.2.1'

ENTRYPOINT ["srunx-mcp"]
