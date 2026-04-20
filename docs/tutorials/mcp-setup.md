# Getting Started with MCP

MCP (Model Context Protocol) lets Claude Code call srunx tools directly,
so you can manage SLURM jobs through natural language instead of memorizing
CLI flags.

This tutorial walks through installation, configuration, and your first
interaction.

## Prerequisites

- srunx installed (see `installation`)
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed
- A working SLURM cluster (local or via SSH profile)

## Install the MCP Extra

The MCP server is an optional dependency. Install it with:

``` bash
uv sync --extra mcp
```

Or if you are adding srunx to another project:

``` bash
uv add "srunx[mcp]"
```

This pulls in the `mcp[cli]` package required to run the server.

## Configure the MCP Server

There are two ways to register the server with Claude Code: a project config
file or the `claude mcp add` CLI command.

### Option A: Project config file

Create a `.mcp.json` file in your project root:

``` json
{
  "mcpServers": {
    "srunx": {
      "command": "uv",
      "args": ["run", "--extra", "mcp", "srunx-mcp"]
    }
  }
}
```

Claude Code reads this file automatically when you open the project.

### Option B: `claude mcp add` command

The `claude mcp add` CLI supports three scopes:

**Local** (current project only, written to `.mcp.json`):

``` bash
claude mcp add srunx -- uv run --extra mcp srunx-mcp
```

**Project** (shared with collaborators via `.mcp.json` in version control):

``` bash
claude mcp add --scope project srunx -- uv run --extra mcp srunx-mcp
```

**User** (available in all your projects):

``` bash
claude mcp add --scope user srunx -- uv run --extra mcp srunx-mcp
```

!!! tip
    Use **project** scope if your team shares the same SLURM cluster setup.
    Use **user** scope if you want srunx available everywhere without
    per-project config.

## Verify the Connection

Check that Claude Code sees the srunx server:

``` bash
claude mcp list
```

You should see `srunx` listed with its tools. If the server does not
appear, ensure the `uv` command is on your `PATH` and that
`uv sync --extra mcp` completed without errors.

## First Interaction

Open Claude Code in your project and try these prompts:

**List your SLURM jobs:**

``` text
> List my current SLURM jobs
```

Claude Code calls the `list_jobs` tool and returns a formatted table
of your queued and running jobs.

**Check GPU resources:**

``` text
> How many GPUs are available on the gpu partition?
```

Claude Code calls `get_resources` with `partition="gpu"` and reports
total, in-use, and available GPU counts.

**Submit a simple job:**

``` text
> Submit a job to run "python train.py" with 2 GPUs, using the ml_env
> conda environment
```

Claude Code calls `submit_job` with the appropriate parameters and
returns the SLURM job ID.

### Using SSH Mode

If your SLURM cluster is remote, ensure you have an SSH profile configured
(see [Sync](../how-to/sync.md) for mount setup). Then include "via SSH" in your
prompt:

``` text
> List my jobs on the remote cluster
```

Claude Code detects the SSH context and passes `use_ssh=True` to the
underlying tools, routing commands through your active SSH profile.

## Next Steps

- [MCP Usage](../how-to/mcp-usage.md) -- task-oriented recipes for common operations
- [MCP Tools](../reference/mcp-tools.md) -- complete reference for all 14 MCP tools
- [MCP Architecture](../explanation/mcp-architecture.md) -- how the MCP integration works under the hood
