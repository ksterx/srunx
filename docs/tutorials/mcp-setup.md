# Getting Started with MCP

MCP (Model Context Protocol) lets Claude Code call srunx tools directly,
so you can manage SLURM jobs through natural language instead of memorizing
CLI flags.

This tutorial walks through installation, configuration, and your first
interaction.

## Prerequisites

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed
- [uv](https://docs.astral.sh/uv/) installed
- A working SLURM cluster (local or via SSH profile)

## Install srunx with the MCP extra

Pick whichever install style matches how you plan to call `srunx-mcp`.

### Option 1: `uvx` (recommended, zero-install)

No install step at all — `uvx` resolves and runs `srunx-mcp` with its
`mcp` extra on demand, regardless of the current working directory:

```bash
uvx --from 'srunx[mcp]' srunx-mcp --help
```

This is the pattern used in the registration command below.

### Option 2: `uv tool install` (globally installed binary)

If you prefer a `~/.local/bin/srunx-mcp` binary on `PATH`, you **must**
include the `mcp` extra at install time — `uv tool install srunx` alone
does **not** pull it in and will fail with `ModuleNotFoundError: No module named 'mcp'`:

```bash
uv tool install --with 'mcp[cli]' srunx
```

Then the binary is callable directly:

```bash
srunx-mcp --help
```

### Option 3: inside a uv project

When srunx is a dependency of the current uv project, add the extra:

```bash
uv add "srunx[mcp]"
```

Then `uv run srunx-mcp` works from that project's directory.

!!! warning "Don't use `uv run --extra mcp srunx-mcp` globally"
    `uv run --extra mcp` resolves extras against the **current working
    directory's** `pyproject.toml`, not srunx's. Registering
    `uv run --extra mcp srunx-mcp` as an MCP command only works when
    Claude Code is launched from inside the srunx source tree — from any
    other project it fails with
    `error: Extra 'mcp' is not defined in the project's optional-dependencies`.

## Register with Claude Code

There are two equally valid registration styles — pick whichever matches
the install option you used above. Both are CWD-independent.

### Trade-off at a glance

| Style | Startup | Updates | Installed binary |
|---|---|---|---|
| `uvx --from 'srunx[mcp]' srunx-mcp` | ~50–200 ms (after first run) | Automatic — every launch resolves the latest version | None |
| `uv tool install` + `srunx-mcp` | ~50 ms | Manual (`uv tool upgrade srunx`) | `~/.local/bin/srunx-mcp` |

For long-lived MCP sessions the difference is negligible — go with
whichever fits your update preference.

### Style A: `uvx` (zero-install, auto-updates)

**User scope** (available in every project):

```bash
claude mcp add --scope user srunx -- uvx --from 'srunx[mcp]' srunx-mcp
```

**Project scope** (shared via a checked-in `.mcp.json`):

```bash
claude mcp add --scope project srunx -- uvx --from 'srunx[mcp]' srunx-mcp
```

**Local scope** (current project only):

```bash
claude mcp add srunx -- uvx --from 'srunx[mcp]' srunx-mcp
```

### Style B: installed binary (fastest startup, pinned version)

Install once with the `mcp` extra, then register the bare binary:

```bash
uv tool install --with 'mcp[cli]' srunx

# User scope
claude mcp add --scope user srunx -- srunx-mcp

# Project scope
claude mcp add --scope project srunx -- srunx-mcp

# Local scope
claude mcp add srunx -- srunx-mcp
```

### `.mcp.json` hand-written

For project scope you can commit a `.mcp.json` instead of running
`claude mcp add`. Pick the block matching the style above:

```json title=".mcp.json (Style A — uvx)"
{
  "mcpServers": {
    "srunx": {
      "command": "uvx",
      "args": ["--from", "srunx[mcp]", "srunx-mcp"]
    }
  }
}
```

```json title=".mcp.json (Style B — installed binary)"
{
  "mcpServers": {
    "srunx": {
      "command": "srunx-mcp"
    }
  }
}
```

!!! tip
    Use **project** scope if your team shares the same SLURM cluster setup.
    Use **user** scope if you want srunx available everywhere without
    per-project config.

## Verify the Connection

Check that Claude Code sees the srunx server:

```bash
claude mcp list
```

You should see `srunx` listed with its tools. If the server shows
`Failed to connect`, inspect the MCP log Claude Code prints at startup —
`srunx-mcp` now emits a clear error (with fix instructions) when the
`mcp` package is missing from its runtime.

## First Interaction

Open Claude Code in your project and try these prompts:

**List your SLURM jobs:**

```text
> List my current SLURM jobs
```

Claude Code calls the `list_jobs` tool and returns a formatted table
of your queued and running jobs.

**Check GPU resources:**

```text
> How many GPUs are available on the gpu partition?
```

Claude Code calls `get_resources` with `partition="gpu"` and reports
total, in-use, and available GPU counts.

**Submit a simple job:**

```text
> Submit a job to run "python train.py" with 2 GPUs, using the ml_env
> conda environment
```

Claude Code calls `submit_job` with the appropriate parameters and
returns the SLURM job ID.

### Using SSH Mode

If your SLURM cluster is remote, ensure you have an SSH profile configured
(see [Sync](../how-to/sync.md) for mount setup). Then include "via SSH" in your
prompt:

```text
> List my jobs on the remote cluster
```

Claude Code detects the SSH context and passes `use_ssh=True` to the
underlying tools, routing commands through your active SSH profile.

## Next Steps

- [MCP Usage](../how-to/mcp-usage.md) -- task-oriented recipes for common operations
- [MCP Tools](../reference/mcp-tools.md) -- complete reference for all 14 MCP tools
- [MCP Architecture](../explanation/mcp-architecture.md) -- how the MCP integration works under the hood
