Architecture
============

This document explains the internal architecture and design decisions of srunx.

Overview
--------

srunx is organized as a modular Python library with clear separation of concerns:

.. code-block:: text

   src/srunx/
   ├── models.py          # Data models and validation (Pydantic)
   ├── client.py          # Local SLURM client (subprocess-based)
   ├── runner.py          # Workflow execution engine
   ├── callbacks.py       # Notification system (Slack, etc.)
   ├── config.py          # Configuration management
   ├── exceptions.py      # Custom exceptions
   ├── formatters.py      # Output formatting utilities
   ├── history.py         # Job execution history tracking
   ├── logging.py         # Centralized logging (Loguru)
   ├── template.py        # SLURM script template rendering
   ├── utils.py           # Utility functions
   ├── cli/               # Command-line interface (Typer)
   ├── monitor/           # Job and resource monitoring
   ├── ssh/               # SSH integration for remote SLURM
   ├── sync/              # rsync-based file synchronization
   ├── templates/         # SLURM script templates (Jinja2)
   └── web/               # Web UI (FastAPI + React)

Two Execution Paths
-------------------

srunx has two independent paths for interacting with SLURM:

**Local execution** (``srunx.client.Slurm``)
   Calls ``sbatch`` directly via ``subprocess``. Used when the CLI runs on the
   same machine (or a login node) where SLURM is available.

**Remote/SSH execution** (``srunx.ssh.core.client.SSHSlurmClient``)
   Connects to a remote SLURM server via Paramiko SSH and executes ``sbatch``
   remotely. Supports ProxyJump for multi-hop connections.

File Transfer Strategy
----------------------

The SSH path uses two complementary file transfer mechanisms:

**SFTP (Paramiko)** — for ephemeral single-file staging
   ``upload_file()`` transfers a single script to ``/tmp/srunx/`` on the
   remote server. ``_write_remote_file()`` writes in-memory script content.
   Both use Paramiko's SFTP subsystem.

**rsync (subprocess)** — for project directory synchronization
   ``sync_project()`` uses the ``RsyncClient`` to sync an entire project
   directory to ``~/.config/srunx/workspace/{repo_name}/`` on the remote
   server. This enables scripts that import local modules to work correctly
   on the remote side. rsync provides delta transfers, exclude patterns, and
   works through ProxyJump via the ``-e`` flag.

Web UI Architecture
-------------------

The Web UI adds a third execution path: a browser-based interface that runs
locally and connects to SLURM clusters via SSH.

.. mermaid::

   flowchart LR
     subgraph local["Local Machine"]
       Browser["Browser :8000"]
       FastAPI["FastAPI\n(srunx ui)"]
       Adapter["SlurmSSHAdapter"]
       Browser --> FastAPI --> Adapter
     end
     subgraph remote["DGX / SLURM Server"]
       squeue["squeue / sinfo"]
       sbatch["sbatch / scancel"]
       sacct["sacct"]
     end
     Adapter -- SSH --> squeue
     Adapter -- SSH --> sbatch
     Adapter -- SSH --> sacct

**Key design decisions:**

REST polling over WebSocket
   SSH adds latency that makes real-time WebSocket updates impractical.
   The frontend polls REST endpoints at configurable intervals (3–15 seconds)
   depending on the page and job state.

SlurmSSHAdapter
   A thin adapter wrapping ``SSHSlurmClient`` that adds missing operations
   (``list_jobs``, ``cancel_job``, ``get_resources``) using
   ``_execute_slurm_command()`` for SLURM path resolution and environment
   setup. The adapter manages SSH reconnection and keep-alive.

No core modifications
   The Web UI is implemented entirely in ``src/srunx/web/`` without modifying
   existing core modules. It accesses ``SSHSlurmClient``'s private
   ``_execute_slurm_command()`` method for proper SLURM path handling.

Input validation
   All user-supplied identifiers (user names, partition names, workflow
   filenames) are validated against a strict regex pattern before being
   interpolated into SSH commands to prevent command injection.

Frontend architecture
   React + TypeScript with Vite. Pages use a custom ``useApi`` hook for
   data fetching with automatic polling. React Flow provides DAG
   visualization for workflow dependencies.

DAG Builder Architecture
------------------------

The DAG builder is an interactive workflow editor that lets users construct
SLURM pipelines visually instead of writing YAML by hand.

ReactFlow canvas and state management
   The builder page renders a ReactFlow canvas where each job is a custom
   node (``BuilderJobNode``) with source and target handles for edge
   connections. The ``useWorkflowBuilder`` hook encapsulates all builder
   state: a ReactFlow node array, an edge array, and a ``Map<string, BuilderJob>``
   ref for O(1) job lookups. Node positions, edge connections, and job
   property edits all flow through this single hook, keeping the page
   component thin.

Job property panel
   Clicking a node opens the ``JobPropertyPanel`` sidebar, which exposes
   every field from the ``BuilderJob`` type: name, command, resources
   (nodes, GPUs, memory, time limit, partition), environment (conda, venv,
   container, env vars), working directory, log directory, and retry
   settings. Changes propagate immediately to the ReactFlow node data,
   updating the canvas in real time.

Client-side validation
   Before saving, ``useWorkflowBuilder.validate()`` checks four rules:
   every job must have a non-empty name, every job must have a non-empty
   command, job names must be unique, and the graph must be acyclic
   (detected via DFS with an explicit recursion-stack set). Validation
   errors are displayed in a banner above the canvas.

Serialization and persistence
   ``useWorkflowBuilder.serialize()`` converts the ReactFlow graph into a
   ``WorkflowCreateRequest`` payload: job names, commands (split on
   whitespace), ``depends_on`` lists (derived from incoming edges, with
   optional dependency type prefixes like ``afternotok:preprocess``),
   resources, and environment settings. The payload is POSTed to
   ``/api/workflows/create``, which validates it again server-side using
   Pydantic models and ``Workflow.validate()``, serializes it to YAML, and
   writes it to the workflow directory on disk.

.. mermaid::

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

Mount-based path resolution
   The file browser bridges local development directories and remote SLURM
   paths. Mount points are stored in the SSH profile configuration and define
   a ``local`` path (on the developer's machine) and a ``remote`` path (on the
   SLURM cluster). When the user browses files, the backend reads the local
   filesystem under the mount root and returns entries with their computed
   remote paths. The frontend never sees local filesystem paths.

   The translation is straightforward: for a mount with
   ``local=/home/user/project`` and ``remote=/scratch/user/project``, a file
   at ``local + /src/train.py`` maps to ``remote + /src/train.py``. The
   ``rsync`` sync operation pushes local contents to the remote side so that
   selected paths are valid when the workflow executes.

Security model
   The file browser enforces strict containment:

   * **Path traversal prevention** — The resolved path must be relative to
     the mount root. Attempts to escape via ``../`` return ``403 Forbidden``.
   * **Symlink containment** — Symlinks are resolved and checked against
     the mount boundary. Links pointing outside are marked
     ``accessible: false`` and cannot be followed in the browser.
   * **Local path isolation** — The API response includes only the remote
     prefix, mount name, and entry metadata. Local filesystem paths are
     never sent to the frontend.

Workflow Execution Pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the user clicks **Run Workflow**, the backend orchestrates a multi-phase
pipeline that bridges the local development environment and the remote SLURM
cluster.

**Phase 1: Mount resolution and sync**

The backend inspects each job's ``work_dir`` and matches it against the mount
remote paths using longest-prefix matching. For example, if a job has
``work_dir=/scratch/user/ml-project/experiments`` and a mount maps
``/scratch/user/ml-project`` to ``~/projects/ml-project``, that mount is
selected. If the workflow has a ``default_project`` field, that mount is
included as well. Each matched mount is synced via rsync before any jobs are
submitted, ensuring the remote cluster has the latest source files.

**Phase 2: Script rendering**

Each ``Job`` in the workflow is rendered through the ``base.slurm.jinja``
template to produce a complete ``sbatch`` script. ``ShellJob`` instances use
their script content directly. Rendering happens in a temporary directory and
is purely CPU-bound (no network I/O).

**Phase 3: Topological submission**

Jobs are submitted in BFS topological order. For each job, the backend
constructs a ``--dependency`` flag from the SLURM job IDs of its parent jobs,
using the dependency type specified on each edge (``afterok``, ``after``,
``afterany``, or ``afternotok``). This delegates scheduling entirely to SLURM:
the backend does not wait between submissions.

**Phase 4: Background monitoring**

After all jobs are submitted, a background ``anyio`` task polls each job's
status via ``sacct`` every 10 seconds. When all jobs reach a terminal state
(``COMPLETED``, ``FAILED``, ``CANCELLED``, or ``TIMEOUT``), the run is marked
accordingly. If the backend loses contact with SLURM for 30 consecutive
polling failures (~5 minutes), the run is marked as failed.

.. mermaid::

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

The entire pipeline is tracked by an in-memory ``RunRegistry`` that stores the
run ID, status, per-job SLURM IDs, and per-job statuses. The frontend polls
``GET /api/workflows/runs/{run_id}`` to reflect live progress in the DAG view.

Settings UI Architecture
-------------------------

The Settings page is a tab-based configuration interface that exposes the full
srunx config surface through the Web UI.

Stateless per-request reads
   The ``ConfigManager`` is instantiated fresh on every API request, reading
   config from disk each time. This avoids stale in-memory state when the user
   modifies config files outside the Web UI (e.g. via CLI). The tradeoff is
   slightly higher I/O per request, but SSH profile configs are small JSON
   files and the overhead is negligible.

Mount-based project model
   Projects are not derived from the current working directory (which is
   meaningless for a web server). Instead, each SSH profile mount defines a
   project: the mount's ``local`` directory is scanned for ``srunx.json``.
   This makes project configuration remote-friendly — the same mount mapping
   used for file browsing and rsync sync also drives per-project settings.

Environment variable read-only design
   The ``/api/config/env`` endpoint exposes active ``SRUNX_*`` variables for
   inspection but does not allow modification. Environment variables are set
   at server startup and cannot be safely mutated at runtime. The UI makes
   this explicit by rendering the tab as read-only.

File Explorer Architecture
---------------------------

The file explorer is a VS Code-style tree panel integrated into the Web UI
sidebar for browsing project files and submitting scripts to SLURM.

Lazy directory loading
   The explorer does not fetch the entire file tree upfront. Each directory
   is loaded on demand when the user clicks to expand it, calling
   ``GET /api/files/browse?mount={name}&path={relative}`` per expansion.
   This keeps initial load fast and avoids transferring large directory trees.

Mount-scoped state isolation
   Each mount maintains its own expanded-directory state in the frontend.
   Internally, paths are tracked as ``{mountName}:{fullPath}`` to prevent
   collisions between mounts that may have similarly-named subdirectories.
   Syncing, refreshing, and expanding are all per-mount operations.

Right-click submission flow
   The context menu identifies submittable files by extension (``.sh``,
   ``.slurm``, ``.sbatch``, ``.bash``). On submit, the frontend reads the
   script content via ``GET /api/files/read`` and posts it to
   ``POST /api/jobs`` with the script body and a user-editable job name.
   This two-step flow lets the user preview the script before submission.

Security model
   The file explorer enforces the same containment rules as the DAG builder
   file browser:

   * **Path traversal prevention** — Resolved paths must stay within the
     mount root. Attempts to escape via ``../`` return ``403 Forbidden``.
   * **Symlink containment** — Symlinks are resolved and checked against
     the mount boundary. Links outside are marked ``accessible: false``.
   * **Local path isolation** — The browse and read APIs include only remote
     prefixes and entry metadata. Local filesystem paths are not sent to the
     frontend. Note that the config/project management APIs intentionally
     expose local paths for administration purposes.
   * **File size limit** — ``GET /api/files/read`` rejects files over 1 MB.

Configuration Hierarchy
-----------------------

Configuration is loaded in order of precedence (lowest to highest):

1. System-wide: ``/etc/srunx/config.json``
2. User-wide: ``~/.config/srunx/config.json``
3. Project-wide: ``srunx.json`` in the working directory
4. Environment variables: ``SRUNX_DEFAULT_*``
