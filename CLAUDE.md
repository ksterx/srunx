# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Package Management
- `uv sync` - Install dependencies
- `uv add <package>` - Add new dependency
- `uv run <command>` - Run commands in virtual environment

### CLI Usage

#### Job Management (SLURM-aligned commands)
Command names mirror SLURM's CLI (`sbatch` / `squeue` / `scancel` / `sinfo` /
`sacct` / `sreport`) so a SLURM user can map their muscle memory directly.
`tail` and `watch` are srunx-specific wrappers that have no direct SLURM
counterpart.

- `uv run srunx sbatch <script>` - Submit a sbatch script (positional, like real sbatch)
- `uv run srunx sbatch --wrap "cmd ..."` - Wrap a command into a SLURM job (mutually exclusive with the positional script)
- `uv run srunx sbatch --profile <name> ...` - Submit over SSH to a configured profile
- `uv run srunx squeue` - List user's jobs in the queue
- `uv run srunx squeue --show-gpus` - Include GPU allocation column
- `uv run srunx squeue --format json` - Emit JSON instead of a table
- `uv run srunx scancel <job_id>` - Cancel a job
- `uv run srunx sinfo` - Display current GPU/node resource availability
- `uv run srunx sinfo --partition gpu --format json` - Partition resources as JSON
- `uv run srunx sacct` - DB-backed job execution history (uses srunx.db, not real sacct)
- `uv run srunx sreport` - Aggregated execution report from srunx.db
- `uv run srunx tail <job_id> --follow` - Stream job logs (use `--profile` for SSH)

`sbatch` accepts the standard SLURM short flags (`-J` / `-N` / `-n` / `-c` /
`-t` / `-p` / `-w` / `-D`) and `--gres=gpu:N`. srunx-specific extensions
(`--profile` / `--conda` / `--venv` / `--container` / `--template` / etc.)
layer on top. `status` was intentionally dropped — use `squeue -j <id>` or
`sacct -j <id>` depending on whether the job is active or historical.

##### Auto-sync + in-place execution

When the positional script lives under one of the SSH profile's mounts
(`mount.local`), srunx:

1. **Auto-rsyncs** that mount to the remote (`mount.remote`) under a
   per-mount file lock (default ON; opt out with `--no-sync` or
   `[sync] auto = false`).
2. Translates the script path to its remote equivalent and invokes
   `sbatch` **directly on the remote file** — no tmp copy, no
   ``-o $SLURM_LOG_DIR/%x_%j.log`` auto-injection, your script's own
   `#SBATCH` directives win.
3. ``cd``s into the script's mount-translated parent directory before
   sbatch, so relative paths inside the script (e.g.
   ``#SBATCH --output=./logs/%j.out``) resolve where you'd expect.

Generated artifacts (``--wrap``, ``--template``, workflow ShellJobs
with Jinja substitution) always go through the historical
``$SRUNX_TEMP_DIR`` upload path because the rendered bytes have no
canonical home in the mount.

Sync defaults are configured under `[sync]` in
`~/.config/srunx/config.json`:

```json
{
  "sync": {
    "auto": true,
    "lock_timeout_seconds": 120,
    "warn_dirty": true,
    "require_clean": false
  }
}
```

Per-invocation overrides:

- `srunx sbatch --sync` / `--no-sync`
- `SRUNX_SYNC_AUTO=0` / `SRUNX_SYNC_REQUIRE_CLEAN=1` etc.

##### Transport Selection (unified CLI)
All job-management commands above accept `--profile <name>` / `--local` /
`--quiet`. Resolution order:
1. `--profile <name>` (explicit)
2. `--local` (mutually exclusive with `--profile`)
3. `$SRUNX_SSH_PROFILE` environment variable
4. local SLURM fallback (no banner, preserves legacy CLI behaviour)

When a non-default transport is selected, a 1-line banner is emitted on stderr
(`→ transport: ssh:<profile> (from --profile)`). `--quiet` suppresses it.

`srunx ssh submit` / `srunx ssh logs` have been removed — use `srunx sbatch
--profile <name> <script>` / `srunx tail --profile <name> <job_id>` instead.

#### Watching / monitoring
- `uv run srunx watch jobs <job_id>` - Block until a job reaches a terminal state
- `uv run srunx watch jobs <job_id> --continuous` - Stream state transitions (no timeout)
- `uv run srunx watch jobs --all` - Watch every user job
- `uv run srunx watch jobs <job_id> --interval 30` - Custom polling interval
- `uv run srunx watch resources --min-gpus 4` - Block until N GPUs are free
- `uv run srunx watch resources --min-gpus 2 --continuous` - Stream GPU availability changes
- `uv run srunx watch resources --min-gpus 4 --partition gpu` - Scoped to one partition
- `uv run srunx watch cluster --schedule 1h --notify $WEBHOOK` - Periodic cluster reports

#### SSH Integration
- `uv run srunx ssh profile list` - List SSH connection profiles
- `uv run srunx ssh profile add <name>` - Add SSH connection profile
- `uv run srunx ssh profile mount add <profile> <name> --local <path> --remote <path>` - Add mount point
- `uv run srunx ssh profile mount list <profile>` - List mount points
- `uv run srunx ssh profile mount remove <profile> <name>` - Remove mount point
- `uv run srunx ssh test` - Test connectivity for a profile
- `uv run srunx ssh sync` - Sync current directory's mount (auto-detect profile and mount from cwd)
- `uv run srunx ssh sync <profile> <name>` - Sync a specific mount
- `uv run srunx ssh sync --dry-run` - Preview sync without transferring

#### Workflows
- `uv run srunx flow run <yaml_file>` - Execute workflow from YAML
- `uv run srunx flow run <yaml_file> --validate` - Validate without executing (replaces the old `flow validate` subcommand)

#### Configuration
- `uv run srunx config show` - Show current configuration
- `uv run srunx config paths` - Show configuration file paths

#### Templates
- `uv run srunx template list` - List available SLURM script templates
- `uv run srunx template show <name>` - Show template contents
- Submit with a specific template: `srunx sbatch --template <name> --wrap "<cmd>"`

### Testing
- `uv run pytest` - Run all tests
- `uv run pytest --cov=srunx` - Run tests with coverage
- `uv run pytest tests/test_models.py` - Run specific test file
- `uv run pytest -v` - Run tests with verbose output

### Direct Usage Examples

#### Job Submission
- `uv run srunx sbatch --wrap "python train.py" --name ml_job --gpus-per-node 1`
- `uv run srunx sbatch train.sh --conda ml_env --nodes 2`
- `uv run srunx sbatch --wrap "python eval.py" --gres=gpu:4`  # SLURM-native --gres form

#### Monitoring Workflows
```bash
# Submit a job and watch until completion
job_id=$(uv run srunx sbatch --wrap "python train.py" --gpus-per-node 2 | grep "Job ID" | awk '{print $3}')
uv run srunx watch jobs $job_id

# Wait for GPUs to become available, then submit
uv run srunx watch resources --min-gpus 4
uv run srunx sbatch --wrap "python train.py" --gpus-per-node 4

# Continuously watch all user jobs with notifications
uv run srunx watch jobs --all --continuous --interval 30

# Send periodic cluster reports
uv run srunx watch cluster --schedule 1h --notify $SLACK_WEBHOOK

# Check current resource availability
uv run srunx sinfo --partition gpu
```

#### SSH Integration
- `uv run srunx sbatch train.sh --profile dgx-server --name remote_training`
- `uv run srunx ssh profile add myserver --hostname dgx.example.com --username researcher`

#### Workflows
- `uv run srunx flow run workflow.yaml`

## Architecture Overview

### Current Modular Structure
```
src/srunx/
├── models.py          # Data models and validation
├── client.py          # SLURM client for job operations
├── client_protocol.py # SlurmClientProtocol (unified queue_by_ids) + JobStatusInfo
├── runner.py          # Workflow execution engine
├── callbacks.py       # Callback system for job notifications
├── config.py          # Configuration management and defaults
├── exceptions.py      # Custom exceptions
├── formatters.py      # Output formatting helpers (tables, JSON, status icons)
├── logging.py         # Centralized logging configuration
├── template.py        # SLURM script template rendering (Jinja2)
├── utils.py           # Utility functions
├── containers/        # Container runtime adapters
│   ├── base.py        # ContainerRuntime abstract base
│   ├── pyxis.py       # Pyxis/SLURM srun --container-* integration
│   └── apptainer.py   # Apptainer / Singularity integration
├── db/                # DB-backed state persistence (SQLite, ~/.config/srunx/srunx.db)
│   ├── connection.py  # XDG path resolution, open_connection, init_db, transaction
│   ├── migrations.py  # SCHEMA_V1 DDL + apply_migrations + bootstrap_from_config
│   ├── models.py      # Pydantic row models (Endpoint/Watch/.../Delivery, WorkflowRun, Job...)
│   ├── cli_helpers.py # DB helpers used by CLI commands
│   └── repositories/  # Thin CRUD per table (JobRepository, DeliveryRepository, ...)
├── notifications/     # Notification domain
│   ├── sanitize.py    # sanitize_slack_text (shared with callbacks.SlackCallback)
│   ├── presets.py     # should_deliver(preset, event_kind, to_status) filter
│   ├── service.py     # NotificationService.fan_out (events → deliveries)
│   └── adapters/      # DeliveryAdapter + SlackWebhookDeliveryAdapter + registry
├── pollers/           # Long-running lifespan tasks
│   ├── reload_guard.py      # is_reload_mode, should_start_pollers (pure functions)
│   ├── supervisor.py        # PollerSupervisor (anyio task group + crash/grace)
│   ├── active_watch_poller.py  # producer: SLURM → events → deliveries
│   ├── delivery_poller.py   # consumer: claim → send → mark_delivered/retry
│   └── resource_snapshotter.py # periodic ResourceSnapshot writes
├── cli/               # Command-line interfaces
│   ├── main.py        # Main CLI commands (submit, status, list, cancel, resources)
│   ├── monitor.py     # Monitor subcommands (jobs, resources, cluster)
│   ├── workflow.py    # Workflow CLI
│   └── notification_setup.py  # Interactive endpoint setup helpers
├── mcp/               # MCP server for AI agent integration
│   └── server.py      # FastMCP tool surface (submit_job, list_jobs, run_workflow, ...)
├── monitor/           # Job and resource monitoring
│   ├── base.py        # BaseMonitor abstract class
│   ├── job_monitor.py # JobMonitor for job state tracking (also writes to job_state_transitions for SSOT)
│   ├── resource_monitor.py  # ResourceMonitor for GPU availability
│   ├── resource_source.py   # Adapter-backed resource query abstraction
│   ├── scheduler.py   # Periodic report scheduler (APScheduler)
│   ├── report_types.py # Report payload dataclasses
│   └── types.py       # MonitorConfig, ResourceSnapshot, WatchMode
├── ssh/               # SSH integration for remote SLURM
│   ├── core/          # Core SSH + SLURM-over-SSH building blocks
│   │   ├── client.py        # SSHSlurmClient (high-level facade)
│   │   ├── slurm.py         # SLURM command wrappers
│   │   ├── connection.py    # Paramiko connection management
│   │   ├── proxy_client.py  # ProxyJump / multi-hop SSH
│   │   ├── file_manager.py  # SFTP upload / workspace sync
│   │   ├── log_reader.py    # Remote log streaming
│   │   ├── config.py        # SSH profile configuration
│   │   ├── ssh_config.py    # ~/.ssh/config parser
│   │   ├── client_types.py  # Shared dataclasses
│   │   └── utils.py         # Misc SSH helpers
│   ├── cli/           # SSH CLI interfaces
│   │   ├── commands.py      # Typer command wiring (submit/logs/test/sync + profile)
│   │   └── profile_impl.py  # Profile management implementations
│   └── helpers/       # SSH utility tools
│       └── proxy_helper.py  # Proxy connection analysis
├── sync/              # rsync-based project directory synchronization
│   └── rsync.py       # RsyncClient (delta transfers, ProxyJump via -e)
├── templates/         # SLURM script templates
│   └── base.slurm.jinja
└── web/               # Web UI (FastAPI + React)
    ├── routers/       # API endpoints (jobs, workflows, resources, endpoints, deliveries, ...)
    └── frontend/      # React SPA
        └── src/
            ├── components/  # Reusable components (KeyValueEditor, JobPropertyPanel, etc.)
            ├── hooks/       # Custom hooks (use-workflow-builder, etc.)
            ├── pages/       # Page components (WorkflowBuilder, etc.)
            └── lib/         # Types, API client
```

### Core Components

#### Models (`models.py`)
- **BaseJob**: Base class for all job types with common fields (name, job_id, depends_on, exports, status)
- **Job**: Complete SLURM job configuration with command, resources, and environment
- **ShellJob**: Job that executes a shell script with variables (script_path, script_vars)
- **JobResource**: Resource allocation (nodes, GPUs, memory, time, partition, nodelist)
- **JobEnvironment**: Environment setup (conda, venv, container, env_vars)
- **ContainerResource**: Container configuration (image, mounts, workdir)
- **JobStatus**: Job status enumeration (PENDING, RUNNING, COMPLETED, FAILED, etc.)
- **Workflow**: Workflow definitions with job dependencies and validation
- **render_job_script()**: Template rendering function for Job instances
- **render_shell_job_script()**: Template rendering function for ShellJob instances

#### Client (`client.py`)
- **Slurm**: Main interface for SLURM operations
  - `submit()`: Submit jobs with full configuration
  - `retrieve()`: Query job status
  - `cancel()`: Cancel running jobs
  - `queue()`: List user jobs
  - `monitor()`: Wait for job completion
  - `run()`: Submit and monitor job

#### SSH Integration (`ssh/`)
- **SSHSlurmClient**: Main SSH client for remote SLURM operations
  - `connect()`: Establish SSH connection
  - `submit_sbatch_job()`: Submit script content to remote SLURM
  - `submit_sbatch_file()`: Submit script file to remote SLURM
  - `monitor_job()`: Monitor remote job until completion
  - `get_job_status()`: Query remote job status
  - `upload_file()`: Upload local files to remote server
  - Context manager support for automatic connection handling
- **ConfigManager**: SSH profile management
  - `add_profile()`: Add new SSH connection profile
  - `get_profile()`: Retrieve profile by name
  - `list_profiles()`: List all profiles
  - `set_current_profile()`: Set default profile
- **SSHConfigParser**: SSH config file parsing
  - `get_host()`: Get SSH host configuration
  - `list_hosts()`: List available hosts
- **ProxySSHClient**: SSH ProxyJump connection handling

#### Monitoring System (`monitor/`)
- **BaseMonitor**: Abstract base class for monitoring operations
  - `watch_until()`: Monitor until condition met (blocking)
  - `watch_continuous()`: Monitor continuously with state change notifications
  - `check_condition()`: Abstract method for condition checking
  - `get_current_state()`: Abstract method for state retrieval
  - Signal handling (SIGTERM, SIGINT) for graceful shutdown
  - Configurable polling intervals with aggressive polling warnings
- **JobMonitor**: SLURM job monitoring until terminal states
  - Monitor single or multiple jobs simultaneously
  - Track state transitions (PENDING → RUNNING → COMPLETED/FAILED)
  - Configurable target statuses (default: COMPLETED, FAILED, CANCELLED, TIMEOUT)
  - Duplicate notification prevention
  - Integration with Slurm client for job status queries
- **ResourceMonitor**: GPU resource availability monitoring
  - Query partition resources using `sinfo` and `squeue`
  - Track available/in-use/total GPUs
  - Threshold-based availability detection
  - Node statistics (total, idle, down nodes)
  - DOWN/DRAIN node filtering for accurate availability
  - Partition-specific or cluster-wide monitoring
- **MonitorConfig**: Configuration dataclass
  - `poll_interval`: Polling frequency in seconds (default: 60)
  - `timeout`: Maximum monitoring duration (None = no timeout)
  - `mode`: WatchMode.UNTIL_CONDITION or WatchMode.CONTINUOUS
  - `notify_on_change`: Enable state change notifications
- **ResourceSnapshot**: Immutable resource state snapshot
  - Timestamp, partition, GPU metrics, node statistics
  - Computed fields: `gpu_utilization`, `has_available_gpus`
  - `meets_threshold()`: Check minimum GPU availability

#### Workflow Runner (`runner.py`)
- **WorkflowRunner**: YAML workflow execution engine
  - `from_yaml()`: Load workflow from YAML file
  - `run()`: Execute workflow with dynamic job scheduling
  - `get_independent_jobs()`: Find jobs without dependencies
  - `parse_job()`: Parse job configuration from YAML

#### Callbacks (`callbacks.py`)
- **Callback**: Base class for job state notifications
- **SlackCallback**: Send notifications to Slack via webhook

#### Configuration (`config.py`)
- **SrunxConfig**: Main configuration class with resource and environment defaults
- **ResourceDefaults**: Default resource allocation settings
- **EnvironmentDefaults**: Default environment setup
- **get_config()**: Get global configuration instance
- **load_config()**: Load configuration from files and environment variables
- **save_user_config()**: Save configuration to user config file

#### Logging (`logging.py`)
- **configure_logging()**: General logging configuration
- **configure_cli_logging()**: CLI-specific logging
- **configure_workflow_logging()**: Workflow-specific logging
- **get_logger()**: Get logger instance for module

#### Utilities (`utils.py`)
- **get_job_status()**: Query job status from SLURM
- **job_status_msg()**: Format status messages with icons

#### Exceptions (`exceptions.py`)
- **WorkflowError**: Base workflow exception
- **WorkflowValidationError**: Workflow validation errors
- **WorkflowExecutionError**: Workflow execution errors

#### CLI (`cli/`)
- **Main CLI**: Job management commands (submit, status, list, cancel, resources)
- **Monitor CLI**: Monitor subcommands (jobs, resources, cluster)
- **Workflow CLI**: YAML workflow execution with validation

### Template System
- Enhanced Jinja2 templates with conditional resource allocation
- `base.slurm.jinja`: Full-featured template with all options; inter-job values are resolved at workflow load time via `exports` / `deps` (no runtime env-file wiring in the template)
- Automatic environment setup integration

### Workflow Definition
Enhanced YAML workflow format with variables and exports:
```yaml
name: ml_pipeline
args:
  base_dir: /data/experiments
  model_name: resnet50

jobs:
  - name: preprocess
    command: ["python", "preprocess.py", "--output", "{{ base_dir }}/data"]
    exports:
      data_path: "{{ base_dir }}/data/processed"
    nodes: 1

  - name: train
    command: ["python", "train.py", "--data", "{{ deps.preprocess.data_path }}"]
    depends_on: [preprocess]
    exports:
      model_path: "{{ base_dir }}/models/best.pt"
    gpus_per_node: 1
    conda: ml_env
    memory_per_node: "32GB"
    time_limit: "4:00:00"

  - name: evaluate
    command: ["python", "evaluate.py", "--model", "{{ deps.train.model_path }}"]
    depends_on: [train]
```

#### Workflow Variables (`args`)
- Defined at workflow level, expanded into job fields via Jinja2 (`{{ var_name }}`) at workflow load time
- Supports `python:` prefix for dynamic evaluation (CLI only, rejected from web API for security)
- Variables can reference each other with automatic dependency resolution

#### Inter-Job Exports (Load-Time)
- Jobs declare static `exports` (dict of KEY=value) — string literals expanded with workflow `args` at load time
- Downstream jobs reference parent values as `{{ deps.<parent_name>.<key> }}`; Jinja (with `StrictUndefined`) substitutes literals into the child's fields at workflow load time, before any job is submitted
- `deps.X` is only populated for names listed in the child's `depends_on`; referencing a non-dep or a missing key fails the workflow at load time
- No runtime env-file mechanism: `$SRUNX_OUTPUTS`, `$SRUNX_OUTPUTS_DIR`, and dynamic `echo "key=value" >> $SRUNX_OUTPUTS` writes are gone
- Values that can only be computed at runtime are out of scope — pass them explicitly (e.g. `--out-file /shared/path/result.json`) or make the path deterministic from `args`

### Parameter Sweeps

srunx supports matrix parameter sweeps for running the same workflow with
different parameter combinations without copying YAML. A sweep expands the
matrix into N workflow_runs (cells) that execute independently, each with
its own materialized sbatch. Sweeps are usable from CLI, Web API, and MCP.

Minimal sweep YAML:

```yaml
name: train
args:
  lr: 0.01
  seed: 1
  dataset: cifar10

sweep:
  matrix:
    lr: [0.001, 0.01, 0.1]
    seed: [1, 2, 3]
  fail_fast: false       # default
  max_parallel: 4        # required

jobs:
  - name: train
    command: ["python", "train.py", "--lr", "{{ lr }}", "--seed", "{{ seed }}"]
    gpus_per_node: 1
```

CLI invocations:

```bash
# YAML-declared sweep
srunx flow run train.yaml

# Ad-hoc sweep (CLI overrides/augments YAML)
srunx flow run --sweep lr=0.001,0.01,0.1 --max-parallel 2 train.yaml

# Single-arg override (no sweep)
srunx flow run --arg dataset=imagenet train.yaml

# Dry-run preview (prints cell args without submitting)
srunx flow run --sweep lr=0.001,0.01 --max-parallel 2 --dry-run train.yaml
```

Constraints:

- `max_parallel` is required (YAML or `--max-parallel`; Web API defaults to 4).
- Matrix values must be scalar (str/int/float/bool); nested structures rejected.
- Cell count capped at 1000 (safety valve; SLURM MaxSubmitJobs is typically ~4096).
- `fail_fast` defaults to false; one cell failing does not abort peers.
- Web UI + MCP sweep submissions can route through the configured SSH adapter
  via a per-sweep `SlurmSSHExecutorPool` (capped at `min(max_parallel, 8)`
  pooled connections); the pool is closed when the orchestrator returns.
  MCP opts in explicitly with the `mount=` tool arg; when omitted MCP stays
  on the local `Slurm` singleton (same as CLI). The orchestrator's default
  `executor_factory=None` preserves local-SLURM behaviour bit-for-bit.
- MCP `run_workflow(mount=...)` applies the same ShellJob script-root guard
  as the Web sweep path (paths outside every profile mount's `local` root
  are rejected before render), so the MCP and Web security surfaces match.
- MCP-originated sweep cells record `workflow_runs.triggered_by='mcp'`
  (V4 migration widened the CHECK allowlist); parent
  `sweep_runs.submission_source` is `'mcp'` for the same sweep so cell
  and parent origins always agree.

DB schema (V3 migration): `sweep_runs` table + `workflow_runs.sweep_run_id`
FK + widened `events.kind` / `watches.kind` CHECK allowlist. V4 migration
widens `workflow_runs.triggered_by` CHECK to admit `'mcp'` alongside
`'cli'`/`'web'`/`'schedule'`. Each cell has a
per-cell `workflow_runs` row and inherits the parent's `sweep_run_id`. The
parent `sweep_runs` row tracks aggregate counters (cells_pending /
cells_running / cells_completed / cells_failed / cells_cancelled) that the
`WorkflowRunStateService` updates atomically on every cell transition.

Notifications for sweeps: only the parent `sweep_run` gets a
watch+subscription (if `--endpoint` is provided); cells do not produce
individual Slack messages — a single `sweep_run.status_changed` event fires
at first-cell-start and at final terminal.

### Key Improvements
- **Unified Job Model**: Single `Job` class with comprehensive configuration
- **Modular Architecture**: Clear separation of concerns
- **Enhanced CLI**: Subcommands with rich options
- **Better Error Handling**: Comprehensive validation and error messages
- **Resource Management**: Full SLURM resource specification
- **Workflow Validation**: Dependency checking and cycle detection
- **Load-Time Value Propagation**: Parent-job `exports` are substituted into child jobs via `{{ deps.<parent>.<key> }}` at workflow load time, with StrictUndefined validation

### Notification + State Persistence (new in 2026-Q2)

srunx stores durable state in a SQLite DB at **`$XDG_CONFIG_HOME/srunx/srunx.db`** (or `~/.config/srunx/srunx.db` when the env var is unset). Schema lives in `src/srunx/db/migrations.py` (`SCHEMA_V1`).

Tables (abbreviated):
- `jobs` — every SLURM submission, annotated with `submission_source` (`cli` / `web` / `workflow`) and `workflow_run_id`.
- `workflow_runs` + `workflow_run_jobs` — Web UI workflow runs, replacing the former in-memory `RunRegistry`.
- `job_state_transitions` — single source of truth for observed state changes, fed by both `ActiveWatchPoller` (`source='poller'`) and `JobMonitor` (`source='cli_monitor'`).
- `resource_snapshots` — periodic GPU/node stats; `gpu_utilization` is a STORED generated column (NULL when `gpus_total=0`).
- `endpoints` + `watches` + `subscriptions` + `events` + `deliveries` — the notification 5-concept outbox. `events` has a UNIQUE `(kind, source_ref, payload_hash)` dedup index; `deliveries` has UNIQUE `(endpoint_id, idempotency_key)`. `deliveries` uses a SELECT-then-UPDATE claim pattern inside `BEGIN IMMEDIATE` (stock Python `sqlite3` lacks `UPDATE ... LIMIT RETURNING`).

Background pollers (lifespan tasks managed by `PollerSupervisor`):
- `ActiveWatchPoller` (producer) — polls SLURM every 15 s, writes `job_state_transitions`, `jobs` status, `events`, and fan-outs into `deliveries`.
- `DeliveryPoller` (consumer) — claims `pending` deliveries every 10 s, sends via `SlackWebhookDeliveryAdapter` (or future channels), handles retry/abandon with exponential backoff (base 10 s, cap 1 h, max 5 attempts).
- `ResourceSnapshotter` — every 5 min, writes one `resource_snapshots` row.

All pollers are crash-resilient via a lease mechanism (`leased_until`, `worker_id`) and a `reclaim_expired_leases()` sweep at the start of every `DeliveryPoller` cycle.

**Environment variables** that affect poller startup:
- `SRUNX_DISABLE_POLLER=1` — disable ALL pollers (also applied automatically in `uvicorn --reload` dev mode).
- `SRUNX_DISABLE_ACTIVE_WATCH_POLLER=1` — skip the SLURM → events producer.
- `SRUNX_DISABLE_DELIVERY_POLLER=1` — skip the outbox consumer.
- `SRUNX_DISABLE_RESOURCE_SNAPSHOTTER=1` — skip resource time-series capture.
- `UVICORN_RELOAD` — anything truthy enables dev-mode reload detection in `pollers.reload_guard`.

Notification settings UI lives in `Settings → Notifications`; Phase 1 supports endpoint CRUD for `slack_webhook` only. Webhook URL validation (both UI and backend): `^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$`.

See `.claude/specs/notification-and-state-persistence/` for full requirements, design, and task list.

## Dependencies
- **Jinja2**: Template rendering
- **Pydantic**: Data validation and serialization
- **Loguru**: Structured logging
- **PyYAML**: YAML parsing
- **Rich**: Terminal UI and tables
- **slack-sdk**: Slack notifications

## Code Quality and Linting

### Quality Checks
- `uv run mypy .` - Type checking with mypy
- `uv run ruff check .` - Code linting
- `uv run ruff format .` - Code formatting

### Pre-commit Quality Checks
Always run these before committing:
```bash
uv run pytest && uv run mypy . && uv run ruff check .
```

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.

## Active Technologies
- Python 3.11+ (project already uses Python 3.12) (001-slurm-job-resource-monitor)
- N/A (stateless monitoring, no persistence in v1) (001-slurm-job-resource-monitor)

## Recent Changes
- 001-slurm-job-resource-monitor: Added Python 3.11+ (project already uses Python 3.12)
