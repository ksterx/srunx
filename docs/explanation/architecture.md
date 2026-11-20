---
description: How srunx is organized internally — a 6-layer modular architecture spanning interfaces, runtime, SLURM wrappers, observability, integrations, and common code.
---

# Architecture

This document explains the internal architecture and design decisions of srunx.

## Overview

srunx is organized as a modular Python library with clear separation of concerns:

``` text
src/srunx/
├── callbacks.py       # Callback base class (job state notifications)
├── utils.py           # Job status helpers
├── common/            # Layered TOML config, XDG paths, exceptions, logging
├── domain/            # Pydantic models: Job / ShellJob / Workflow / JobStatus
├── runtime/           # Script rendering, submission planning, workflow runner, sweeps
├── slurm/             # SLURM wrappers: local client, SSH client, protocols, state parsing
├── transport/         # Resolves a cluster name to its transport (local / ssh:<cluster>)
├── observability/     # Monitoring, notifications, SQLite state, background pollers
├── containers/        # Container runtime adapters (Pyxis, Apptainer)
├── ssh/               # SSH connection, SSH SLURM client, ProxyJump, remote secrets
├── sync/              # rsync client, per-mount lock, owner marker, manifest, first-sync approval
├── snapshot/          # Per-commit read-only code snapshots on a cluster
├── cli/               # Command-line interface (Typer)
├── mcp/               # MCP server for AI agent integration
└── web/               # Web UI (FastAPI + React)
```

Imports flow one way: `interfaces (cli, mcp, web) → runtime / slurm /
observability / integrations (ssh, sync, snapshot, containers) → domain →
common`. A test in `tests/architecture/` enforces this layering.

## Two Execution Paths

Every command targets a **cluster** — a `[clusters.<name>]` entry in the user
(or system) config with `transport = "local"` or `"ssh"`. The CLI picks it from
`--cluster`, `$SRUNX_CLUSTER`, the project's `cluster`, or `default_cluster`
(and errors if none applies); the Web UI and MCP always name it explicitly.
srunx has two independent paths for interacting with SLURM:

**Local execution** (`srunx.slurm.local.LocalClient`)  
Calls `sbatch` directly via `subprocess`. Used when the CLI runs on the
same machine (or a login node) where SLURM is available.

**Remote/SSH execution** (`srunx.ssh.core.client.SSHSlurmClient`)  
Connects to a remote SLURM server via Paramiko SSH and executes `sbatch`
remotely. Supports ProxyJump for multi-hop connections.

## File Transfer Strategy

The SSH path moves code and scripts three ways:

**SFTP (Paramiko)** — for generated scripts  
Scripts that have no file in a mount (`--wrap`, `--template`, Jinja-rendered
ShellJobs) are uploaded to `$SRUNX_TEMP_DIR` and submitted from there.

**rsync (subprocess)** — for mounted project directories  
A project's mounts on a cluster (from `.srunx/config.toml`) map local
directories to remote ones. `sbatch` and
`flow run` rsync the touched mount (additive by default; `--delete` mirrors)
under a per-mount lock, then run the script in place at its remote path. rsync
provides delta transfers and exclude patterns, and works through ProxyJump via
the `-e` flag. `SSHSlurmClient.sync_project()` is the Python entry point for a
single directory.

**Git snapshots** — for reproducible jobs  
`sbatch --snapshot` pushes a commit to a bare repo on the cluster and checks it
out as a read-only tree, so later syncs cannot change the code under pending or
requeued jobs. Each submission writes a lease file; `snapshot prune` deletes
only trees that no job can still use. The logic lives in `srunx.snapshot`.

## Web UI Architecture

The Web UI adds a third execution path: a browser-based interface that runs
locally and connects to SLURM clusters via SSH.

```mermaid
flowchart LR
  subgraph local["Local Machine"]
    Browser["Browser :8000"]
    FastAPI["FastAPI\n(srunx ui)"]
    Client["SlurmSSHClient"]
    Browser --> FastAPI --> Client
  end
  subgraph remote["DGX / SLURM Server"]
    squeue["squeue / sinfo"]
    sbatch["sbatch / scancel"]
    sacct["sacct"]
  end
  Client -- SSH --> squeue
  Client -- SSH --> sbatch
  Client -- SSH --> sacct
```

**Key design decisions:**

REST polling over WebSocket  
SSH adds latency that makes real-time WebSocket updates impractical.
The frontend polls REST endpoints at configurable intervals (3–15 seconds)
depending on the page and job state.

SlurmSSHClient  
The SSH-transport sibling of `LocalClient` (`src/srunx/slurm/clients/ssh.py`),
wrapping the lower-level `srunx.ssh.core.client.SSHSlurmClient` and adding
the operations the Web UI needs (`list_jobs`, `cancel_job`, `get_resources`).
SLURM CLI invocations route through `client.slurm.execute_slurm_command`
on the inner ssh-core client, which handles SLURM path resolution and
login-shell environment setup. The client manages SSH reconnection and
keep-alive.

No core modifications  
The Web UI is implemented entirely in `src/srunx/web/` and the SLURM
transport in `src/srunx/slurm/clients/ssh.py`, both built on the
`srunx.ssh.core` primitives without modifying them.

Input validation  
All user-supplied identifiers (user names, partition names, workflow
filenames) are validated against a strict regex pattern before being
interpolated into SSH commands to prevent command injection.

Frontend architecture  
React + TypeScript with Vite. Pages use a custom `useApi` hook for
data fetching with automatic polling. React Flow provides DAG
visualization for workflow dependencies.

## Sweep Architecture

Parameter sweeps extend the single-workflow execution model into a bounded
fan-out of "cells" -- each cell is an ordinary workflow run, but the
whole set is tracked and orchestrated as one unit. The design deliberately
reuses the workflow runner rather than inventing a parallel execution
mechanism: a sweep is "one sweep_run, N workflow_runs", not "one
workflow_run with N internal branches".

### Load-time matrix materialization

A sweep is expanded at **load time**, before any job is submitted to
SLURM. `expand_matrix` takes the `sweep.matrix` block and produces the
scalar cross-product, rejecting nested structures and capping the total
cell count at 1000 (SLURM's `MaxSubmitJobs` is typically ~4096, so this
is a safety valve and not a hard ceiling).

Each resulting cell becomes its own `workflow_runs` row that is persisted
up front, with the child's `sweep_run_id` pointing back to the parent
`sweep_runs` row. Rendering substitutes the cell's args into every job
field using Jinja2 with `StrictUndefined`, so a missing key fails loudly
at expansion time instead of producing an empty arg inside `sbatch`.

This is the same design judgement that underlies `exports` / `deps`:
inter-job values are resolved at workflow load time rather than through
a runtime env-file mechanism. Load-time resolution gives sweeps three
properties that runtime expansion could not: the full plan is inspectable
before submission (enabling `--dry-run`), every cell's args are stable
and auditable in the DB, and reconciliation after a crash is purely a
function of the persisted rows.

### Orchestration and concurrency control

The `SweepOrchestrator` runs cells under an `anyio.Semaphore` whose
capacity is `min(max_parallel, cell_count)`. When `max_parallel` is
larger than the actual cell count the semaphore is silently clamped
(with a warning log) instead of erroring, keeping the CLI / Web /
MCP surfaces tolerant of "over-provisioned" sweeps.

Cell failures interact with `fail_fast` in one direction only: when
`fail_fast=true` and a cell reaches a terminal failure state, the
orchestrator transitions the sweep into `draining` and cancels not-yet-
running cells. Cells already running are allowed to finish -- draining
is cooperative, not pre-emptive. With the default `fail_fast=false` a
single failure has no effect on peers, which matches how hyperparameter
sweeps are typically run in practice.

### Per-cell execution paths

The orchestrator constructs each cell's `WorkflowRunner` with two
injected dependencies:

- **`executor_factory`** -- `None` for local execution (`Slurm`
  singleton, `subprocess`-based), or a callable that leases a
  `SlurmSSHExecutor` from a pool for remote execution.
- **`submission_context`** -- a `SubmissionRenderContext` that
  encapsulates the per-cell render concerns: mount-aware translation of
  `work_dir` and `log_dir` from local paths to their remote equivalents,
  and the canonical rendering step for `ShellJob` scripts that are
  staged via SFTP.

This keeps the local path and the SSH path on the **same code path**:
the runner does not branch on execution mode. Sweeps on a local
cluster pass `executor_factory=None`, which preserves the pre-sweep
behaviour bit-for-bit -- cells go through the `Slurm` singleton exactly as
a non-swept workflow would. CLI sweeps use the resolved cluster's executor
factory; Web UI sweeps, and MCP sweeps on an SSH cluster, pass an
`executor_factory` backed by a per-sweep `SlurmSSHExecutorPool`.

### SSH pool and mount-aware rendering

`SlurmSSHExecutorPool` is a bounded pool of pre-built SSH executors,
sized at `max(1, min(max_parallel, 8))`. The floor at 1 guards against
a zero-sized pool when `max_parallel=0` would otherwise be pathological;
the ceiling at 8 avoids drowning the remote SSH server in long-lived
control sessions even when the sweep's nominal parallelism is higher.
The pool's lifecycle is strictly **per sweep**: the Web UI's
`_run_sweep_background` closes it in a `finally` block, and the MCP
`run_workflow` tool closes it in its own `finally` -- there is no
process-wide pool.

`SubmissionRenderContext` is the responsibility boundary for
path translation. Where the local `Slurm` client can use absolute local
paths verbatim, the SSH path must convert a job's `work_dir` /
`log_dir` from the developer's machine into the matching remote path
under the SSH mount. The render context owns that conversion (plus
`ShellJob` script rendering), so the runner itself stays agnostic
about whether it is submitting locally or through SSH.

A related SSH-path subtlety is the **scontrol fallback** in the status
query: the lookup chain is `sacct` → `squeue` → `scontrol`. The third
stage exists specifically for Pyxis-style environments where
`slurmdbd` is unreachable and `sacct` returns empty, which previously
surfaced as silent `FAILED` statuses. If even `scontrol` cannot find
the job we return `JobStatus.UNKNOWN` rather than coercing it to
`FAILED` -- the ambiguity is preserved for the caller.

### Aggregation via the sweep_runs counters

Every cell transition goes through `WorkflowRunStateService`, which
performs an optimistic-lock `UPDATE` on the cell's `workflow_runs` row
and, in the same transaction, increments or decrements the parent
sweep's `cells_*` counters. The counters on `sweep_runs`
(`cells_pending` / `cells_running` / `cells_completed` /
`cells_failed` / `cells_cancelled`) converge atomically with each cell
transition rather than being re-derived from a scan, so the parent
status is always a consistent snapshot of its children.

Notifications follow a parent-only rule: only the `sweep_run` gets a
`watch` + `subscription`, and only the parent fires a
`sweep_run.status_changed` event (at first-cell-start and at the final
terminal transition). Individual cells do not produce Slack messages.
This is the opposite of the workflow-run default, and is a deliberate
choice -- a 9-cell sweep would otherwise produce 18+ Slack messages
for a single intent.

### Crash recovery

The `SweepReconciler` runs during Web lifespan startup and scans for
sweeps in non-terminal states whose pending cells have no active
orchestrator. For each such sweep it resumes execution from the DB
snapshot -- the cells' args are already persisted, so reconciliation
is a matter of rebuilding the orchestrator with the right
`executor_factory` and letting it pick up the pending cells.

The executor factory is reconstructed via an
`executor_factory_provider` callback that the Web lifespan injects.
The provider inspects the persisted sweep (submission source, cluster)
and returns either `None` (local path) or a fresh SSH pool
lease factory. This indirection exists because the reconciler itself
must stay pure with respect to transport choice: it does not know about
clusters, and it should not have to.

## DB schema migrations

The SQLite schema at `<state_dir>/srunx.db` (default
`$XDG_STATE_HOME/srunx/srunx.db`) evolves through
numbered migrations. Each migration is applied idempotently and leaves a
row in a `schema_migrations` table; the runtime refuses to start against
a DB whose latest migration is unknown to the current binary.

**V1 -- initial persistence layer.** Introduces the five core concepts
for notification and state (`jobs`, `workflow_runs`, `workflow_run_jobs`,
`job_state_transitions`, `resource_snapshots`, plus
`endpoints` / `watches` / `subscriptions` / `events` / `deliveries`).
The original allowlists are conservative: `workflow_runs.triggered_by`
is `('cli', 'web', 'schedule')` and `events.kind` / `watches.kind`
do not yet know about sweeps.

**V3 -- sweep tables and widened allowlists.** Adds the `sweep_runs`
table with the per-cell counters described above, and adds
`workflow_runs.sweep_run_id` as a nullable FK that links each cell
back to its parent. The `events.kind` CHECK is widened via a table
rebuild to admit `sweep_run.status_changed`, and `watches.kind` is
widened to admit `sweep_run`. The rebuild is required because SQLite
cannot `ALTER CHECK` in place. (V2 is reserved for dashboard indexes
and does not change the schema shape.)

**V4 -- widen `workflow_runs.triggered_by` to include `'mcp'`.** V3
shipped with MCP-originated cells marked `triggered_by='web'` as a
workaround, because the V1 CHECK allowlist did not admit `'mcp'`
and the CHECK can only be widened by rebuilding the table. V4 does
exactly that: the migration is flagged `requires_fk_off=True` so
SQLite can temporarily disable foreign-key enforcement while the old
table is renamed, the new table is created, rows are copied with
their preserved `sweep_run_id`, and the old table is dropped.
`'schedule'` is kept in the widened allowlist as a reserved value
for planned scheduled-workflow triggers even though no writer emits
it yet.

After V4, MCP-originated sweeps record their true provenance end-to-end:
the parent `sweep_runs.submission_source` is `'mcp'` and each cell's
`workflow_runs.triggered_by` is also `'mcp'`, which the notification
fan-out and audit queries consume verbatim.

## DAG Builder Architecture

The DAG builder is an interactive workflow editor that lets users construct
SLURM pipelines visually instead of writing YAML by hand.

ReactFlow canvas and state management  
The builder page renders a ReactFlow canvas where each job is a custom
node (`BuilderJobNode`) with source and target handles for edge
connections. The `useWorkflowBuilder` hook encapsulates all builder
state: a ReactFlow node array, an edge array, and a `Map<string, BuilderJob>`
ref for O(1) job lookups. Node positions, edge connections, and job
property edits all flow through this single hook, keeping the page
component thin.

Job property panel  
Clicking a node opens the `JobPropertyPanel` sidebar, which exposes
every field from the `BuilderJob` type: name, command, resources
(nodes, GPUs, memory, time limit, partition), environment (conda, venv,
container, env vars), working directory, log directory, and retry
settings. Changes propagate immediately to the ReactFlow node data,
updating the canvas in real time.

Client-side validation  
Before saving, `useWorkflowBuilder.validate()` checks four rules:
every job must have a non-empty name, every job must have a non-empty
command, job names must be unique, and the graph must be acyclic
(detected via DFS with an explicit recursion-stack set). Validation
errors are displayed in a banner above the canvas.

Serialization and persistence  
`useWorkflowBuilder.serialize()` converts the ReactFlow graph into a
`WorkflowCreateRequest` payload: job names, commands (split on
whitespace), `depends_on` lists (derived from incoming edges, with
optional dependency type prefixes like `afternotok:preprocess`),
resources, and environment settings. The payload is POSTed to
`/api/workflows/create`, which validates it again server-side using
Pydantic models and `Workflow.validate()`, serializes it to YAML, and
writes it to the workflow directory on disk.

```mermaid
flowchart TD
  subgraph frontend["Frontend (React)"]
    Canvas["ReactFlow Canvas"]
    Hook["useWorkflowBuilder\n(nodes, edges, jobMap)"]
    Panel["JobPropertyPanel"]
    Canvas -- node click --> Panel
    Panel -- updateJob --> Hook
    Canvas -- onConnect --> Hook
    Hook -- nodes/edges --> Canvas
  end
  subgraph save["Save Flow"]
    Validate["Client-side\nvalidation"]
    Serialize["serialize()\n→ WorkflowCreateRequest"]
    API["POST /api/workflows/create"]
    ServerValidate["Server-side validation\n(Pydantic + cycle detection)"]
    YAML["Serialize to YAML"]
    Disk["Write to disk"]
    Validate --> Serialize --> API --> ServerValidate --> YAML --> Disk
  end
  Hook -- Save Workflow --> Validate
```

Mount-based path resolution  
The file browser bridges local development directories and remote SLURM
paths. Mount points come from the selected project's config on the selected
cluster and define a `local` path (on the developer's machine) and a `remote` path (on the
SLURM cluster). When the user browses files, the backend reads the local
filesystem under the mount root and returns entries with their computed
remote paths. The frontend never sees local filesystem paths.

The translation is straightforward: for a mount with
`local=/home/user/project` and `remote=/scratch/user/project`, a file
at `local + /src/train.py` maps to `remote + /src/train.py`. The
`rsync` sync operation pushes local contents to the remote side so that
selected paths are valid when the workflow executes.

Security model  
The file browser enforces strict containment:

- **Path traversal prevention** — The resolved path must be relative to
  the mount root. Attempts to escape via `../` return `403 Forbidden`.
- **Symlink containment** — Symlinks are resolved and checked against
  the mount boundary. Links pointing outside are marked
  `accessible: false` and cannot be followed in the browser.
- **Local path isolation** — The API response includes only the remote
  prefix, mount name, and entry metadata. Local filesystem paths are
  never sent to the frontend.

### Workflow Execution Pipeline

When the user clicks **Run Workflow**, the backend orchestrates a multi-phase
pipeline that bridges the local development environment and the remote SLURM
cluster.

**Phase 1: Mount resolution and sync**

The backend inspects each job's `work_dir` and matches it against the mount
remote paths using longest-prefix matching. For example, if a job has
`work_dir=/scratch/user/ml-project/experiments` and a mount maps
`/scratch/user/ml-project` to `~/projects/ml-project`, that mount is
selected. If the workflow has a `default_project` field, that mount is
included as well. Each matched mount is synced via rsync before any jobs are
submitted, ensuring the remote cluster has the latest source files.

**Phase 2: Script rendering**

Each `Job` in the workflow is rendered through the `base.slurm.jinja`
template to produce a complete `sbatch` script. `ShellJob` instances use
their script content directly. Rendering happens in a temporary directory and
is purely CPU-bound (no network I/O).

**Phase 3: Topological submission**

Jobs are submitted in BFS topological order. For each job, the backend
constructs a `--dependency` flag from the SLURM job IDs of its parent jobs,
using the dependency type specified on each edge (`afterok`, `after`,
`afterany`, or `afternotok`). This delegates scheduling entirely to SLURM:
the backend does not wait between submissions.

**Phase 4: Background monitoring**

After all jobs are submitted, a background `anyio` task polls each job's
status via `sacct` every 10 seconds. When all jobs reach a terminal state
(`COMPLETED`, `FAILED`, `CANCELLED`, or `TIMEOUT`), the run is marked
accordingly. If the backend loses contact with SLURM for 30 consecutive
polling failures (~5 minutes), the run is marked as failed.

```mermaid
flowchart LR
  subgraph phase1["1. Sync"]
    Resolve["Resolve mounts\n(longest prefix)"]
    Rsync["rsync to remote"]
    Resolve --> Rsync
  end
  subgraph phase2["2. Render"]
    Template["Jinja2 template\n→ sbatch script"]
  end
  subgraph phase3["3. Submit"]
    Topo["BFS topological\nordering"]
    Sbatch["sbatch\n--dependency=afterok:ID"]
    Topo --> Sbatch
  end
  subgraph phase4["4. Monitor"]
    Poll["Poll sacct\nevery 10s"]
    Terminal["All terminal?\n→ mark run"]
    Poll --> Terminal
  end
  phase1 --> phase2 --> phase3 --> phase4
```

The entire pipeline is tracked by an in-memory `RunRegistry` that stores the
run ID, status, per-job SLURM IDs, and per-job statuses. The frontend polls
`GET /api/workflows/runs/{run_id}` to reflect live progress in the DAG view.

## Settings UI Architecture

The Settings page is a tab-based configuration interface that exposes the full
srunx config surface through the Web UI.

Stateless per-request reads  
Settings are loaded from disk on every API request, so edits made outside the
Web UI (e.g. via the CLI) are picked up immediately. Every request carries the
cluster and project it is about (`?cluster=` / `?project=`); a middleware binds
them to the request context, so there is no process-global "current" cluster
or project and two browser tabs never affect each other.

File-scoped edits  
The UI never writes the merged, effective config back. `PATCH
/api/config/file` edits individual keys of one file (the user config or the
selected project's `.srunx/config.toml`) through the same TOML writer the CLI
uses. Each read returns a version token; a write with a stale token gets
`409 Conflict` instead of silently overwriting a concurrent change. `GET
/api/config` reports the effective values together with the file each one came
from.

Explicit project model  
Projects are not guessed per request from the server's working directory. The
server serves a fixed set of project roots — the project containing the
directory `srunx ui` was started from, plus every `--project PATH` /
`SRUNX_WEB_PROJECTS` entry — and a request names one of them by project name.

Environment variable read-only design  
The `/api/config/env` endpoint exposes srunx-related environment variables for
inspection but does not allow modification. Environment variables are set
at server startup and cannot be safely mutated at runtime. The UI makes
this explicit by rendering the tab as read-only.

## File Explorer Architecture

The file explorer is a VS Code-style tree panel integrated into the Web UI
sidebar for browsing project files and submitting scripts to SLURM.

Lazy directory loading  
The explorer does not fetch the entire file tree upfront. Each directory
is loaded on demand when the user clicks to expand it, calling
`GET /api/files/browse?mount={name}&path={relative}` (plus the request's
`cluster` / `project`) per expansion.
This keeps initial load fast and avoids transferring large directory trees.

Mount-scoped state isolation  
Each mount maintains its own expanded-directory state in the frontend.
Internally, paths are tracked as `{mountName}:{fullPath}` to prevent
collisions between mounts that may have similarly-named subdirectories.
Syncing, refreshing, and expanding are all per-mount operations.

Right-click submission flow  
The context menu identifies submittable files by extension (`.sh`,
`.slurm`, `.sbatch`, `.bash`). On submit, the frontend reads the
script content via `GET /api/files/read` and posts it to
`POST /api/jobs` with the script body and a user-editable job name.
This two-step flow lets the user preview the script before submission.

Security model  
The file explorer enforces the same containment rules as the DAG builder
file browser:

- **Path traversal prevention** — Resolved paths must stay within the
  mount root. Attempts to escape via `../` return `403 Forbidden`.
- **Symlink containment** — Symlinks are resolved and checked against
  the mount boundary. Links outside are marked `accessible: false`.
- **Local path isolation** — The browse and read APIs include only remote
  prefixes and entry metadata. Local filesystem paths are not sent to the
  frontend. Note that the config/project management APIs intentionally
  expose local paths for administration purposes.
- **File size limit** — `GET /api/files/read` rejects files over 1 MB.

## Configuration Hierarchy

Configuration is TOML. Files (see `srunx config paths`):

1.  System: `/etc/srunx/config.toml`
2.  User: `$XDG_CONFIG_HOME/srunx/config.toml` — clusters (connection
    settings), `default_cluster`, defaults, notifications, sync, optional
    `state_dir`, and personal per-project overrides
3.  Project: `<root>/.srunx/config.toml` — `name`, `cluster`, defaults, and
    per-cluster `remote_dir` / `sync` / `exclude` / `mounts` / `defaults`. It
    cannot contain connection settings, so cloning a repository never makes
    srunx connect to a host you have not configured. It is found by walking up
    from the current directory, stopping at a git root or `$HOME`
    (`--project PATH` overrides)

Job defaults are merged from lowest to highest priority: built-in, system
`defaults`, user `defaults`, user `clusters.<c>.defaults`, project `defaults`,
project `clusters.<c>.defaults`, script `#SBATCH` / workflow job fields, and
CLI flags. Any table can list `unset = [...]` keys to clear inherited values.
Every effective value records the file it came from (`srunx config show`).

State — the SQLite DB, sync locks, the remote-env cache, and approved sync
targets — lives under `$XDG_STATE_HOME/srunx` or the user config's `state_dir`.

The full design (in Japanese) is in [Clusters & Config Design](../design/clusters.md).
