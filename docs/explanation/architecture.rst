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
   ├── logging.py         # Centralized logging (Loguru)
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

.. code-block:: text

   Local Machine                      DGX / SLURM Server
   ┌────────────────────┐   SSH     ┌──────────────────┐
   │ Browser :8000      │           │                  │
   │   ↓                │           │ squeue/sinfo     │
   │ FastAPI (srunx-web)│───SSH───→ │ sbatch/scancel   │
   │ SlurmSSHAdapter    │           │ sacct            │
   └────────────────────┘           └──────────────────┘

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

Configuration Hierarchy
-----------------------

Configuration is loaded in order of precedence (lowest to highest):

1. System-wide: ``/etc/srunx/config.json``
2. User-wide: ``~/.config/srunx/config.json``
3. Project-wide: ``.srunx.json`` or ``srunx.json`` in the working directory
4. Environment variables: ``SRUNX_DEFAULT_*``
