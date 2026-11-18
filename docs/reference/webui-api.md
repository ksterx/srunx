---
description: REST API reference for the srunx Web UI server — jobs, resources, workflows and runs, sweep runs, files and mounts, config and SSH profiles, history, notification endpoints, subscriptions, watches, deliveries, and templates.
---

# Web UI REST API Reference

The srunx Web UI exposes a REST API at `http://127.0.0.1:8000/api/`. All responses are JSON.

## Jobs

| Method | Endpoint                  | Description                            |
|--------|---------------------------|----------------------------------------|
| GET    | `/api/jobs`               | List all SLURM jobs in the queue       |
| GET    | `/api/jobs/{job_id}`      | Get detailed status for a specific job |
| POST   | `/api/jobs`               | Submit a new job                       |
| POST   | `/api/jobs/preview`       | Render a SLURM script without submitting |
| DELETE | `/api/jobs/{job_id}`      | Cancel a running or pending job        |
| GET    | `/api/jobs/{job_id}/logs` | Get stdout/stderr log contents         |

### GET /api/jobs

Returns a list of all jobs from `squeue`.

**Response:**

``` json
[
  {
    "name": "train-resnet",
    "job_id": 18431,
    "status": "RUNNING",
    "depends_on": [],
    "command": [],
    "resources": {
      "nodes": 1,
      "gpus_per_node": 8,
      "partition": "gpu",
      "time_limit": "8:00:00"
    },
    "partition": "gpu",
    "nodes": 1,
    "gpus": 8,
    "elapsed_time": "1:30:00"
  }
]
```

### POST /api/jobs

Submit a new job with a SLURM script.

**Request body:**

``` json
{
  "name": "my-job",
  "script_content": "#!/bin/bash\n#SBATCH --gpus=1\npython train.py",
  "job_name": "training-run"
}
```

**Response (201):**

``` json
{
  "name": "my-job",
  "job_id": 18500,
  "status": "PENDING",
  "depends_on": [],
  "command": [],
  "resources": {}
}
```

### GET /api/jobs/{job_id}

Returns one job (looked up with `sacct`, so finished jobs are included). Same shape as a `GET /api/jobs` item.

**Error responses:**

- `400` — `job_id` is not positive
- `404` — Job not found
- `502` — SLURM command failed

### POST /api/jobs/preview

Render the SLURM script for a job configuration without submitting it.

**Request body:**

``` json
{
  "name": "preview",
  "command": ["python", "train.py"],
  "resources": {"gpus_per_node": 1, "partition": "gpu"},
  "environment": {"conda": "ml_env"},
  "work_dir": "/home/user/proj",
  "log_dir": "/home/user/logs",
  "template_name": "base"
}
```

Only `command` is required. `name` defaults to `preview`, `template_name` to `base`, and `work_dir` / `log_dir` to `.`.

**Response:**

``` json
{
  "script": "#!/bin/bash\n#SBATCH --job-name=preview\n...",
  "template_used": "base"
}
```

**Error responses:**

- `404` — Template not found
- `422` — Invalid `resources` / `environment` (e.g. bad env-var key)

### DELETE /api/jobs/{job_id}

Cancel a job. Returns `204 No Content` on success.

### GET /api/jobs/{job_id}/logs

**Response:**

**Query parameters:**

- `stdout_offset` (optional, default `0`) — Byte offset to read stdout from
- `stderr_offset` (optional, default `0`) — Byte offset to read stderr from

Pass the offsets returned by the previous call to receive only new content.

``` json
{
  "stdout": "Epoch 1/10: loss=0.85...",
  "stderr": "WARNING: GPU memory high",
  "stdout_offset": 24,
  "stderr_offset": 24
}
```

## Resources

| Method | Endpoint         | Description                                 |
|--------|------------------|---------------------------------------------|
| GET    | `/api/resources` | Get GPU and node availability per partition |

### GET /api/resources

**Query parameters:**

- `partition` (optional) — Filter to a specific partition

**Response:**

``` json
[
  {
    "timestamp": "2026-03-30T09:00:00+00:00",
    "partition": "gpu",
    "total_gpus": 32,
    "gpus_in_use": 24,
    "gpus_available": 8,
    "jobs_running": 3,
    "nodes_total": 4,
    "nodes_idle": 1,
    "nodes_down": 0,
    "gpu_utilization": 0.75,
    "has_available_gpus": true
  }
]
```

!!! note
    Multi-node jobs are correctly accounted for: `gpus_in_use = gpus_per_node * num_nodes`.

## Workflows

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/workflows` | List all workflow definitions |
| GET | `/api/workflows/{name}` | Get a specific workflow with its jobs |
| POST | `/api/workflows/validate` | Validate YAML content |
| POST | `/api/workflows/upload` | Upload and save a workflow YAML |
| POST | `/api/workflows/create` | Create a workflow from structured JSON (DAG builder) |
| DELETE | `/api/workflows/{name}` | Delete a workflow YAML file |
| POST | `/api/workflows/{name}/run` | Run a workflow (sync mounts, submit jobs, monitor) |
| GET | `/api/workflows/runs` | List workflow run records |
| GET | `/api/workflows/runs/{run_id}` | Get run status with live job statuses |
| POST | `/api/workflows/runs/{run_id}/cancel` | Cancel all jobs in a run |

### GET /api/workflows

List the workflow YAML files of a mount. YAML files that fail to load are skipped.

**Query parameters:**

- `mount` (required) — Mount name

**Response:**

``` json
[
  {
    "name": "ml-pipeline",
    "jobs": [
      {
        "name": "train",
        "job_id": null,
        "status": "UNKNOWN",
        "depends_on": ["preprocess"],
        "exports": {},
        "command": ["python", "train.py"],
        "resources": {"nodes": 1, "gpus_per_node": 1, "partition": "gpu", "time_limit": "4:00:00"}
      }
    ],
    "args": {"base_dir": "/data"}
  }
]
```

`args`, `default_project`, and a per-job `template` are present only when the YAML defines them. A `ShellJob` carries `script_path` instead of populated `command` / `resources`.

### GET /api/workflows/{name}

Get one workflow in the shape of a `GET /api/workflows` item.

**Query parameters:**

- `mount` (required) — Mount name

**Error responses:**

- `404` — Workflow not found
- `422` — Invalid workflow name
- `502` — Workflow failed to load

### POST /api/workflows/upload

**Request body:**

``` json
{
  "yaml": "name: my-pipeline\njobs:\n  - name: step1\n    command: ['echo', 'hello']",
  "filename": "my-pipeline.yaml"
}
```

**Validation rules:**

- Filename must be alphanumeric with hyphens/underscores only
- File extension must be `.yaml` or `.yml`
- Content size limit: 1MB
- `python:` args are rejected (security)

### POST /api/workflows/validate

**Request body:**

``` json
{"yaml": "name: test\njobs: []"}
```

**Response:**

``` json
{"valid": true}

// or
{"valid": false, "errors": ["Duplicate job name: step1"]}
```

### POST /api/workflows/create

Create a workflow from a structured JSON payload. Used by the DAG builder.

**Request body:**

``` json
{
  "name": "ml-pipeline",
  "jobs": [
    {
      "name": "preprocess",
      "command": ["python", "preprocess.py"],
      "depends_on": [],
      "resources": {"nodes": 1},
      "work_dir": "/home/researcher/ml-project"
    },
    {
      "name": "train",
      "command": ["python", "train.py", "--epochs", "100"],
      "depends_on": ["preprocess"],
      "resources": {"nodes": 1, "gpus_per_node": 4, "time_limit": "4:00:00"},
      "environment": {"conda": "ml_env"}
    }
  ]
}
```

**Job fields:**

| Field | Required | Description |
|----|----|----|
| `name` | Yes | Job name (alphanumeric, hyphens, underscores) |
| `command` | Yes | Command as a list of strings |
| `depends_on` | No | List of upstream job names, optionally prefixed with dependency type (e.g., `afternotok:preprocess`) |
| `resources` | No | Object with `nodes`, `gpus_per_node`, `ntasks_per_node`, `cpus_per_task`, `memory_per_node`, `time_limit`, `partition`, `nodelist` |
| `environment` | No | Object with `conda`, `venv`, `container`, `env_vars` |
| `work_dir` | No | Working directory on the remote cluster |
| `log_dir` | No | Log output directory |
| `retry` | No | Number of retry attempts |
| `retry_delay` | No | Delay between retries in seconds |

**Response (200):**

``` json
{
  "name": "ml-pipeline",
  "jobs": [
    {
      "name": "preprocess",
      "job_id": null,
      "status": "UNKNOWN",
      "depends_on": [],
      "command": ["python", "preprocess.py"],
      "resources": {"nodes": 1, "gpus_per_node": null, "partition": null, "time_limit": null}
    }
  ]
}
```

**Error responses:**

- `409` — Workflow with the same name already exists
- `422` — Validation error (invalid name, duplicate job names, dependency cycle, Pydantic validation failure)

### DELETE /api/workflows/{name}

Delete a workflow YAML file from the workflow directory.

**Path parameters:**

- `name` — Workflow name (alphanumeric, hyphens, underscores)

**Response (200):**

``` json
{"status": "deleted", "name": "ml-pipeline"}
```

**Error responses:**

- `404` — Workflow not found
- `422` — Invalid workflow name

### POST /api/workflows/{name}/run

Run a workflow end-to-end: identify and sync referenced mounts, render SLURM scripts, submit jobs in topological order with `--dependency` flags, and start a background monitor that polls `sacct` every 10 seconds.

**Path parameters:**

- `name` — Workflow name

**Response (202):**

``` json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "workflow_name": "ml-pipeline",
  "started_at": "2026-03-30T12:00:00+00:00",
  "completed_at": null,
  "status": "running",
  "job_ids": {
    "preprocess": "18500",
    "train": "18501",
    "evaluate": "18502"
  },
  "job_statuses": {
    "preprocess": "PENDING",
    "train": "PENDING",
    "evaluate": "PENDING"
  },
  "error": null
}
```

The `status` field transitions through: `syncing`, `submitting`, `running`, then a terminal state (`completed`, `failed`, or `cancelled`).

**Error responses:**

- `404` — Workflow not found
- `422` — Invalid workflow name
- `500` — Script rendering failed
- `502` — Mount sync failed or sbatch submission failed

### GET /api/workflows/runs

List workflow runs, newest data from the state database.

**Query parameters:**

- `name` (optional) — Only runs of this workflow

**Response:** an array of run objects, each with the shape shown under `GET /api/workflows/runs/{run_id}`.

### GET /api/workflows/runs/{run_id}

Get the current status and job-level details for a single workflow run. Job statuses are updated by the background monitor every 10 seconds.

**Path parameters:**

- `run_id` — UUID of the run (returned by the `POST /{name}/run` endpoint)

**Response (200):**

``` json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "workflow_name": "ml-pipeline",
  "started_at": "2026-03-30T12:00:00+00:00",
  "completed_at": null,
  "status": "running",
  "job_ids": {"preprocess": "18500", "train": "18501"},
  "job_statuses": {"preprocess": "COMPLETED", "train": "RUNNING"},
  "error": null
}
```

**Error responses:**

- `404` — Run not found

### POST /api/workflows/runs/{run_id}/cancel

Cancel all SLURM jobs associated with a workflow run. Each submitted job is cancelled via `scancel`. The run status is set to `cancelled`.

**Path parameters:**

- `run_id` — UUID of the run

**Response (200):**

``` json
{"status": "cancelled", "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
```

If some jobs fail to cancel (e.g., already completed), the response includes a `warnings` array:

``` json
{
  "status": "cancelled",
  "run_id": "a1b2c3d4-...",
  "warnings": ["evaluate: Job 18502 not found"]
}
```

**Error responses:**

- `404` — Run not found
- `422` — Run is already in a terminal state (completed, failed, or cancelled)

## Sweep Runs

Parameter sweeps expand a workflow into N independent cells across a
matrix of axes. Each cell is a regular `workflow_runs` row that inherits
the parent sweep's `sweep_run_id`; the parent `sweep_runs` row tracks
aggregate counters (`cells_pending`, `cells_running`, `cells_completed`,
`cells_failed`, `cells_cancelled`) that are updated atomically on every
cell state transition.

| Method | Endpoint                          | Description                                                     |
|--------|-----------------------------------|-----------------------------------------------------------------|
| GET    | `/api/sweep_runs`                 | List all sweep runs (most recent first, capped at 200)          |
| GET    | `/api/sweep_runs/{id}`            | Get one sweep run with counters and matrix spec                 |
| GET    | `/api/sweep_runs/{id}/cells`      | List every cell (workflow_run) linked to the sweep              |
| POST   | `/api/sweep_runs/{id}/cancel`     | Request sweep cancellation (drain pending + scancel running)    |

**Sweep status enum:** `pending | running | draining | completed | failed | cancelled`.
`draining` is entered after a cancel request while running cells are
still being stopped; the sweep advances to `cancelled` once every cell
reaches a terminal state.

Per-cell cancellation reuses the existing workflow-run endpoint —
`POST /api/workflows/runs/{cell_id}/cancel`, where `cell_id` is the
`workflow_runs.id` returned by `GET /api/sweep_runs/{id}/cells`.

### GET /api/sweep_runs

List every sweep run. Returns an array ordered by `started_at`
descending, bounded to 200 rows.

**Response (200):**

``` json
[
  {
    "id": 42,
    "name": "train-lr-seed-sweep",
    "workflow_yaml_path": "/workflows/train.yaml",
    "status": "running",
    "matrix": {"lr": [0.001, 0.01, 0.1], "seed": [1, 2, 3]},
    "args": {"dataset": "cifar10"},
    "fail_fast": false,
    "max_parallel": 4,
    "cell_count": 9,
    "cells_pending": 3,
    "cells_running": 2,
    "cells_completed": 4,
    "cells_failed": 0,
    "cells_cancelled": 0,
    "submission_source": "web",
    "started_at": "2026-04-22T09:00:00+00:00",
    "completed_at": null,
    "cancel_requested_at": null,
    "error": null
  }
]
```

### GET /api/sweep_runs/{id}

Get the full row for a single sweep run. Fields mirror the list
response.

**Path parameters:**

- `id` — Sweep run ID (integer)

**Example:**

``` http
GET /api/sweep_runs/42
```

**Response (200):**

``` json
{
  "id": 42,
  "name": "train-lr-seed-sweep",
  "workflow_yaml_path": "/workflows/train.yaml",
  "status": "completed",
  "matrix": {"lr": [0.001, 0.01, 0.1], "seed": [1, 2, 3]},
  "args": {"dataset": "cifar10"},
  "fail_fast": false,
  "max_parallel": 4,
  "cell_count": 9,
  "cells_pending": 0,
  "cells_running": 0,
  "cells_completed": 9,
  "cells_failed": 0,
  "cells_cancelled": 0,
  "submission_source": "web",
  "started_at": "2026-04-22T09:00:00+00:00",
  "completed_at": "2026-04-22T10:15:00+00:00",
  "cancel_requested_at": null,
  "error": null
}
```

**Error responses:**

- `404` — Sweep run not found

### GET /api/sweep_runs/{id}/cells

List every cell belonging to the sweep. Each cell is a `workflow_runs`
row and carries the resolved `args` used for its Jinja render. Ordering
is `(status, started_at ASC, id ASC)` — grouped by status so callers see
running/pending cells together.

**Path parameters:**

- `id` — Sweep run ID

**Response (200):**

``` json
[
  {
    "id": "c1f2e3d4-0001-4e00-a000-000000000001",
    "workflow_name": "train",
    "status": "completed",
    "started_at": "2026-04-22T09:00:00+00:00",
    "completed_at": "2026-04-22T09:20:00+00:00",
    "args": {"lr": 0.001, "seed": 1, "dataset": "cifar10"},
    "error": null,
    "triggered_by": "web"
  },
  {
    "id": "c1f2e3d4-0002-4e00-a000-000000000002",
    "workflow_name": "train",
    "status": "running",
    "started_at": "2026-04-22T09:05:00+00:00",
    "completed_at": null,
    "args": {"lr": 0.01, "seed": 1, "dataset": "cifar10"},
    "error": null,
    "triggered_by": "web"
  }
]
```

`triggered_by` is `web`, `cli`, `mcp`, or `schedule` (post-V4 migration).
To cancel a single cell, use `POST /api/workflows/runs/{cell_id}/cancel`
with the cell's `id` as `cell_id`.

**Error responses:**

- `404` — Sweep run not found

### POST /api/sweep_runs/{id}/cancel

Request sweep cancellation. Stamps `cancel_requested_at` on the sweep
row, drains all `pending` cells, and fires `scancel` for any `running`
cells. The sweep transitions to `draining` until every cell reaches a
terminal state, then to `cancelled` via the aggregator. A
`sweep_run.status_changed` event is emitted for any subscriber watches.

Calling this endpoint a second time on the same sweep is a no-op: the
existing `cancel_requested_at` timestamp is preserved and the current
row is returned.

**Path parameters:**

- `id` — Sweep run ID

**Example:**

``` http
POST /api/sweep_runs/42/cancel
```

**Response (202):**

``` json
{
  "id": 42,
  "name": "train-lr-seed-sweep",
  "workflow_yaml_path": "/workflows/train.yaml",
  "status": "draining",
  "matrix": {"lr": [0.001, 0.01, 0.1], "seed": [1, 2, 3]},
  "args": {"dataset": "cifar10"},
  "fail_fast": false,
  "max_parallel": 4,
  "cell_count": 9,
  "cells_pending": 0,
  "cells_running": 2,
  "cells_completed": 4,
  "cells_failed": 0,
  "cells_cancelled": 3,
  "submission_source": "web",
  "started_at": "2026-04-22T09:00:00+00:00",
  "completed_at": null,
  "cancel_requested_at": "2026-04-22T09:30:00+00:00",
  "error": null
}
```

**Error responses:**

- `404` — Sweep run not found

## Files

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/files/mounts` | List configured mount points (name and remote only) |
| GET | `/api/files/mounts/config` | List mounts with full details (name, local, remote) |
| POST | `/api/files/mounts` | Add a mount to the current SSH profile |
| DELETE | `/api/files/mounts/{mount_name}` | Remove a mount from the current SSH profile |
| GET | `/api/files/browse` | Browse local filesystem under a mount's local root |
| GET | `/api/files/read` | Read text file contents from a mount |
| POST | `/api/files/sync` | Sync a mount's local directory to the remote via rsync |

### GET /api/files/mounts

Returns the list of mount points from the current SSH profile. Only mount names and remote prefixes are returned; local paths are never exposed.

**Response:**

``` json
[
  {
    "name": "ml-project",
    "remote": "/home/researcher/ml-project"
  }
]
```

Returns an empty list if no SSH profile is configured or the profile has no mounts.

### GET /api/files/mounts/config

Returns all mounts with full details including local paths. Used by the mount management UI.

**Response:**

``` json
[
  {
    "name": "ml-project",
    "local": "/home/user/projects/ml-project",
    "remote": "/home/researcher/ml-project"
  }
]
```

Returns an empty list if no SSH profile is configured or the profile has no mounts.

### POST /api/files/mounts

Add a new mount to the current SSH profile. The mount is persisted to the profile configuration file.

**Request body:**

``` json
{
  "name": "ml-project",
  "local": "/home/user/projects/ml-project",
  "remote": "/home/researcher/ml-project"
}
```

**Response (200):**

``` json
{
  "name": "ml-project",
  "local": "/home/user/projects/ml-project",
  "remote": "/home/researcher/ml-project"
}
```

**Error responses:**

- `409` — A mount with the same name already exists
- `422` — Validation error (missing required fields or invalid values)
- `503` — No SSH profile configured

### DELETE /api/files/mounts/{mount_name}

Remove a mount from the current SSH profile.

**Path parameters:**

- `mount_name` — Mount name to remove

**Response (200):**

``` json
{"status": "deleted", "name": "ml-project"}
```

**Error responses:**

- `404` — Mount not found
- `503` — No SSH profile configured

### GET /api/files/browse

Browse directory contents under a mount's local root, returning entries with their corresponding remote paths.

**Query parameters:**

- `mount` (required) — Mount name (must match a configured mount)
- `path` (optional) — Relative path within the mount root (default: root directory)

**Response:**

``` json
{
  "entries": [
    {"name": "train.py", "type": "file", "size": 2048},
    {"name": "data", "type": "directory", "size": null},
    {"name": "latest", "type": "symlink", "size": null, "accessible": true, "target_kind": "directory"}
  ],
  "remote_prefix": "/home/researcher/ml-project/src",
  "mount_name": "ml-project"
}
```

**Entry types:** `file`, `directory`, `symlink`. Symlinks include an `accessible` field indicating whether the link target is within the mount boundary, and a `target_kind` field (`"file"` or `"directory"`) indicating the type of the link target. Accessible symlinks to directories can be expanded in the file explorer.

!!! warning
    **Security:** The resolved path must stay within the mount's local root. Path traversal attempts (e.g., `../../etc/passwd`) return `403 Forbidden`. Symlinks pointing outside the mount boundary are marked `accessible: false` and cannot be followed. Local filesystem paths are never included in the response.

**Error responses:**

- `400` — Path is not a directory
- `403` — Path outside mount boundary or permission denied
- `404` — Mount not found, directory not found, or no SSH profile configured

### GET /api/files/read

Read text file contents from a mount's local root. Used by the file explorer to preview scripts before submission.

**Query parameters:**

- `mount` (required) — Mount name
- `path` (required) — Relative path within the mount root

**Response:**

``` json
{
  "content": "#!/bin/bash\n#SBATCH --gpus=1\npython train.py",
  "path": "scripts/train.sh",
  "mount": "ml-project"
}
```

**Error responses:**

- `400` — `path` parameter missing or path is not a file
- `403` — Path outside mount boundary
- `404` — File not found or no SSH profile configured
- `413` — File exceeds 1 MB size limit

### POST /api/files/sync

Sync a mount's local directory to the remote server via rsync.

**Request body:**

``` json
{"mount": "ml-project"}
```

**Response (200):**

``` json
{"status": "synced", "mount": "ml-project"}
```

**Error responses:**

- `404` — Mount not found
- `502` — rsync command failed (includes missing rsync binary)
- `503` — No SSH profile configured

## Config

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/config` | Get current merged configuration |
| PUT | `/api/config` | Update user configuration |
| GET | `/api/config/paths` | List config file paths with existence status |
| POST | `/api/config/reset` | Reset user config to defaults |
| GET | `/api/config/ssh/profiles` | List SSH profiles and current active profile |
| POST | `/api/config/ssh/profiles` | Add a new SSH profile |
| PUT | `/api/config/ssh/profiles/{name}` | Update an existing SSH profile |
| DELETE | `/api/config/ssh/profiles/{name}` | Delete an SSH profile |
| POST | `/api/config/ssh/profiles/{name}/activate` | Set profile as current active |
| POST | `/api/config/ssh/profiles/{name}/connect` | Switch the live SSH connection to a profile |
| POST | `/api/config/ssh/profiles/{name}/test` | Test SSH and SLURM availability without switching |
| GET | `/api/config/ssh/status` | Current SSH connection status |
| POST | `/api/config/ssh/profiles/{name}/mounts` | Add a mount to a profile |
| DELETE | `/api/config/ssh/profiles/{name}/mounts/{mount_name}` | Remove a mount from a profile |
| GET | `/api/config/env` | List active [SRUNX]()\* environment variables |
| GET | `/api/config/projects` | List projects from current profile's mounts |
| GET | `/api/config/projects/{mount_name}` | Read project config (srunx.json) |
| PUT | `/api/config/projects/{mount_name}` | Update project config |
| POST | `/api/config/projects/{mount_name}/init` | Initialize project config with example values |

### GET /api/config

Returns the current merged configuration (system + user + project).

**Response:**

``` json
{
  "resources": {
    "nodes": 1,
    "gpus_per_node": 0,
    "ntasks_per_node": 1,
    "cpus_per_task": 1,
    "memory_per_node": null,
    "time_limit": null,
    "partition": null,
    "nodelist": null
  },
  "environment": {
    "conda": null,
    "venv": null,
    "container": null,
    "env_vars": {}
  },
  "notifications": {
    "slack_webhook_url": null
  },
  "log_dir": "logs",
  "work_dir": null
}
```

### PUT /api/config

Validate and save configuration to the user config file (`~/.config/srunx/config.json`).

**Request body:** A full or partial `SrunxConfig` object (same shape as GET response).

**Response:** The updated merged configuration.

**Error responses:**

- `422` — Validation error
- `500` — Failed to write config file

### GET /api/config/paths

Returns all config file paths with their existence status and source label.

**Response:**

``` json
[
  {"path": "/etc/srunx/config.json", "exists": false, "source": "system"},
  {"path": "/home/user/.config/srunx/config.json", "exists": true, "source": "user"},
  {"path": "srunx.json", "exists": false, "source": "project"}
]
```

### POST /api/config/reset

Reset user config to defaults. Overwrites `~/.config/srunx/config.json` with a fresh `SrunxConfig`.

**Response:** The default configuration.

### GET /api/config/ssh/profiles

Returns all SSH profiles and identifies the current active profile.

**Response:**

``` json
{
  "current": "dgx-server",
  "profiles": {
    "dgx-server": {
      "hostname": "dgx.example.com",
      "username": "researcher",
      "key_filename": "~/.ssh/id_ed25519",
      "port": 22,
      "description": "Main DGX cluster",
      "ssh_host": null,
      "proxy_jump": null,
      "mounts": [],
      "env_vars": {}
    }
  }
}
```

### POST /api/config/ssh/profiles

Add a new SSH profile.

**Request body:**

``` json
{
  "name": "dgx-server",
  "hostname": "dgx.example.com",
  "username": "researcher",
  "key_filename": "~/.ssh/id_ed25519",
  "port": 22,
  "description": "Main DGX cluster",
  "ssh_host": null,
  "proxy_jump": null
}
```

**Response:** The created profile object.

**Error responses:**

- `409` — Profile with the same name already exists

### PUT /api/config/ssh/profiles/{name}

Update an existing SSH profile. Only valid `ServerProfile` fields are applied: `hostname`, `username`, `key_filename`, `port`, `description`, `ssh_host`, `proxy_jump`, `env_vars`.

**Request body:** Object with fields to update (partial update).

**Response:** The updated profile object.

**Error responses:**

- `404` — Profile not found

### DELETE /api/config/ssh/profiles/{name}

Delete an SSH profile and all its mounts.

**Response:**

``` json
{"ok": true}
```

**Error responses:**

- `404` — Profile not found

### POST /api/config/ssh/profiles/{name}/activate

Set a profile as the current active profile.

**Response:**

``` json
{"ok": true}
```

**Error responses:**

- `404` — Profile not found

### POST /api/config/ssh/profiles/{name}/connect

Open an SSH connection with the profile and make it the server's live connection and current profile. Connection failures are reported in the body with status `200`.

**Response:**

``` json
{"connected": true, "profile_name": "dgx", "hostname": "dgx.example.com"}
```

On failure: `{"connected": false, "profile_name": "dgx", "hostname": "dgx.example.com", "error": "SSH connection failed"}`.

**Error responses:**

- `404` — Profile not found

### POST /api/config/ssh/profiles/{name}/test

Connect with a temporary connection, check SLURM, and disconnect. The live connection is not changed.

**Response:**

``` json
{
  "ssh_connected": true,
  "slurm_available": true,
  "hostname": "dgx.example.com",
  "user": "researcher",
  "slurm_version": "slurm 24.05.1",
  "error": null
}
```

**Error responses:**

- `404` — Profile not found

### GET /api/config/ssh/status

**Response:**

``` json
{"connected": true, "profile_name": "dgx"}
```

`profile_name` is `null` when no profile is active.

### POST /api/config/ssh/profiles/{name}/mounts

Add a mount point to a profile.

**Request body:**

``` json
{
  "name": "ml-project",
  "local": "/home/user/projects/ml-project",
  "remote": "/home/researcher/ml-project"
}
```

**Response:** The created mount object.

**Error responses:**

- `404` — Profile not found
- `422` — Validation error

### DELETE /api/config/ssh/profiles/{name}/mounts/{mount_name}

Remove a mount from a profile.

**Response:**

``` json
{"ok": true}
```

**Error responses:**

- `404` — Profile or mount not found

### GET /api/config/env

Returns all `SRUNX_*` and `SLACK_WEBHOOK_URL` environment variables currently set in the server process.

**Response:**

``` json
[
  {
    "name": "SRUNX_DEFAULT_PARTITION",
    "value": "gpu",
    "description": "Default SLURM partition"
  },
  {
    "name": "SRUNX_SSH_PROFILE",
    "value": "dgx-server",
    "description": "SSH profile for web server"
  }
]
```

Returns an empty list if no `SRUNX_*` variables are set.

### GET /api/config/projects

List projects derived from the current SSH profile's mounts.

**Response:**

``` json
[
  {
    "mount_name": "ml-project",
    "local_path": "/home/user/projects/ml-project",
    "remote_path": "/home/researcher/ml-project",
    "config_exists": true,
    "config_path": "/home/user/projects/ml-project/srunx.json"
  }
]
```

Returns an empty list if no SSH profile is active.

### GET /api/config/projects/{mount_name}

Read `srunx.json` from a mount's local directory.

**Response:**

``` json
{
  "mount_name": "ml-project",
  "local_path": "/home/user/projects/ml-project",
  "config_path": "/home/user/projects/ml-project/srunx.json",
  "exists": true,
  "config": {
    "resources": {"gpus_per_node": 4, "time_limit": "8:00:00"},
    "environment": {"conda": "ml_env"}
  }
}
```

**Error responses:**

- `400` — No active SSH profile
- `404` — Mount not found in active profile

### PUT /api/config/projects/{mount_name}

Save `srunx.json` to a mount's local directory.

**Request body:** A `SrunxConfig` object.

**Response:** The saved project config response.

**Error responses:**

- `400` — No active SSH profile
- `404` — Mount not found
- `422` — Validation error
- `500` — Failed to write file

### POST /api/config/projects/{mount_name}/init

Initialize `srunx.json` with example values in a mount's local directory.

**Response:** The created project config response with example values.

**Error responses:**

- `400` — No active SSH profile
- `404` — Mount not found
- `409` — `srunx.json` already exists

## History

| Method | Endpoint             | Description                      |
|--------|----------------------|----------------------------------|
| GET    | `/api/history`       | Get recent job execution history |
| GET    | `/api/history/stats` | Get aggregate job statistics     |

### GET /api/history

Recent job submissions recorded by srunx.

**Query parameters:**

- `limit` (optional, default `50`) — Maximum rows

**Response:**

``` json
[
  {
    "job_id": 18431,
    "job_name": "train-resnet",
    "command": "python train.py",
    "status": "COMPLETED",
    "submitted_at": "2026-03-30 09:00:00",
    "completed_at": "2026-03-30 10:30:00",
    "workflow_name": null,
    "partition": "gpu",
    "nodes": 1,
    "gpus": 8
  }
]
```

### GET /api/history/stats

**Query parameters:**

- `from` (optional) — Start date (ISO format)
- `to` (optional) — End date (ISO format)

**Response:**

``` json
{
  "total": 42,
  "completed": 35,
  "failed": 4,
  "cancelled": 3,
  "avg_runtime_seconds": 3600.0
}
```

## Notification Endpoints

Destinations that receive notifications. Phase 1 supports `slack_webhook` only.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/endpoints` | List endpoints |
| POST | `/api/endpoints` | Create an endpoint |
| PATCH | `/api/endpoints/{id}` | Rename or change the config |
| POST | `/api/endpoints/{id}/disable` | Disable an endpoint |
| POST | `/api/endpoints/{id}/enable` | Re-enable an endpoint |
| DELETE | `/api/endpoints/{id}` | Delete an endpoint |

### GET /api/endpoints

**Query parameters:**

- `include_disabled` (optional, default `true`)

**Response:**

``` json
[
  {
    "id": 1,
    "kind": "slack_webhook",
    "name": "team-alerts",
    "config": {"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"},
    "created_at": "2026-03-30T09:00:00Z",
    "disabled_at": null
  }
]
```

### POST /api/endpoints

**Request body:**

``` json
{
  "kind": "slack_webhook",
  "name": "team-alerts",
  "config": {"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"}
}
```

`name` is 1–80 characters. `webhook_url` must match `https://hooks.slack.com/services/<A>/<B>/<C>`.

**Response (201):** the created endpoint.

**Error responses:**

- `409` — An endpoint with the same `kind` and `name` exists
- `422` — Unsupported `kind` or invalid `webhook_url`

### PATCH /api/endpoints/{id}

Body fields `name` and `config` are both optional; `config` is re-validated for the endpoint's kind. Returns the updated endpoint. `404` if not found, `422` if the config is invalid.

### POST /api/endpoints/{id}/disable

Sets `disabled_at` and returns the endpoint. `404` if not found.

### POST /api/endpoints/{id}/enable

Clears `disabled_at` and returns the endpoint. `404` if not found.

### DELETE /api/endpoints/{id}

Returns `204 No Content`; `404` if not found.

## Subscriptions

A subscription connects a watch to an endpoint with a delivery preset.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/subscriptions` | List subscriptions |
| POST | `/api/subscriptions` | Create a subscription |
| DELETE | `/api/subscriptions/{id}` | Delete a subscription |

### GET /api/subscriptions

**Query parameters:**

- `watch_id` (optional) — Subscriptions of one watch
- `endpoint_id` (optional) — Subscriptions of one endpoint
- `limit` (optional, default `200`, max `500`) — Used only when neither filter is given

**Response:**

``` json
[
  {"id": 3, "watch_id": 7, "endpoint_id": 1, "preset": "terminal", "created_at": "2026-03-30T09:00:00Z"}
]
```

### POST /api/subscriptions

**Request body:**

``` json
{"watch_id": 7, "endpoint_id": 1, "preset": "terminal"}
```

`preset` is `terminal` (default), `running_and_terminal`, or `all`. `digest` is not implemented and is rejected.

**Response (201):** the created subscription.

**Error responses:**

- `409` — The (watch, endpoint) pair already has a subscription
- `422` — Invalid or unimplemented preset

### DELETE /api/subscriptions/{id}

Returns `204 No Content`; `404` if not found.

## Watches

A watch is a monitored target (job, workflow run, sweep run) whose state changes produce events.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/watches` | List watches |
| GET | `/api/watches/{id}` | Get one watch |
| POST | `/api/watches` | Attach a notification to an existing job |
| POST | `/api/watches/{id}/close` | Close a watch |

### GET /api/watches

**Query parameters:**

- `open` (optional, default `true`) — Only open watches
- `kind`, `target_ref` (optional) — Filter by target; both must be given together

Unfiltered listing (`open=false` without `kind` and `target_ref`) returns `400`.

**Response:**

``` json
[
  {
    "id": 7,
    "kind": "job",
    "target_ref": "job:local:18431",
    "filter": null,
    "created_at": "2026-03-30T09:00:00Z",
    "closed_at": null
  }
]
```

### GET /api/watches/{id}

Returns one watch; `404` if not found.

### POST /api/watches

Create a watch and a subscription for an already-submitted job. Idempotent: repeating the same (job, endpoint, preset) returns the same ids with `created: false`. The scheduler is taken from the active SSH connection.

**Request body:**

``` json
{"kind": "job", "job_id": 18431, "endpoint_id": 1, "preset": "terminal"}
```

**Response (201):**

``` json
{"watch_id": 7, "subscription_id": 3, "created": true}
```

**Error responses:**

- `404` — Endpoint not found
- `422` — Endpoint disabled, or invalid or unsupported preset

### POST /api/watches/{id}/close

Closes the watch (sets `closed_at`) and returns it. Closing an already-closed watch succeeds. `404` if the id is unknown.

## Deliveries

Read-only view of the notification outbox. Sending is done by the background delivery poller.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/deliveries` | List deliveries |
| GET | `/api/deliveries/stuck` | Count deliveries stuck in `pending` |
| GET | `/api/deliveries/{id}` | Get one delivery |

### GET /api/deliveries

**Query parameters:**

- `subscription_id` (optional) — Deliveries of one subscription
- `status` (optional) — Filter by delivery status
- `limit` (optional, default `100`, max `500`) — Applies when `subscription_id` is omitted

**Response:**

``` json
[
  {
    "id": 12,
    "event_id": 40,
    "subscription_id": 3,
    "endpoint_id": 1,
    "idempotency_key": "...",
    "status": "delivered",
    "attempt_count": 1,
    "next_attempt_at": "2026-03-30T10:30:00Z",
    "leased_until": null,
    "worker_id": null,
    "last_error": null,
    "delivered_at": "2026-03-30T10:30:02Z",
    "created_at": "2026-03-30T10:30:00Z"
  }
]
```

### GET /api/deliveries/stuck

**Query parameters:**

- `older_than_sec` (optional, default `300`)

**Response:**

``` json
{"count": 0, "older_than_sec": 300}
```

### GET /api/deliveries/{id}

Returns one delivery; `404` if not found.

## Templates

Job script templates: built-in ones plus user-defined ones.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/templates` | List templates |
| GET | `/api/templates/{name}` | Get a template with its Jinja source |
| POST | `/api/templates` | Create a user-defined template |
| PUT | `/api/templates/{name}` | Update a user-defined template |
| DELETE | `/api/templates/{name}` | Delete a user-defined template |
| POST | `/api/templates/{name}/apply` | Submit a job with a template, or preview it |

### GET /api/templates

**Response:**

``` json
[
  {"name": "base", "description": "Full-featured template", "use_case": "General jobs", "user_defined": false}
]
```

### GET /api/templates/{name}

**Response:**

``` json
{"name": "base", "description": "...", "use_case": "...", "content": "#!/bin/bash\n..."}
```

`404` if the template does not exist.

### POST /api/templates

**Request body:** `{"name": "...", "description": "...", "use_case": "...", "content": "<jinja>"}`

**Response (201):** the template list item. `409` if the name is taken or invalid.

### PUT /api/templates/{name}

Body fields `description`, `use_case`, `content` are optional. Returns the template as in `GET /api/templates/{name}`. `404` if the user-defined template does not exist.

### DELETE /api/templates/{name}

Returns `204 No Content`; `400` if the template is built-in or missing.

### POST /api/templates/{name}/apply

Render the template for a job and submit it, or only return the rendered script.

**Request body:**

``` json
{
  "command": ["python", "train.py"],
  "job_name": "train",
  "resources": {"gpus_per_node": 1},
  "environment": {"conda": "ml_env"},
  "work_dir": "/home/user/proj",
  "log_dir": "/home/user/logs",
  "mount_name": "ml-project",
  "preview_only": false
}
```

Only `command` is required; `job_name` defaults to `job`. With `mount_name`, that mount is synced before submission.

**Response:** with `preview_only: true`, `{"script": "...", "template_used": "base"}`; otherwise the submission result (same shape as `POST /api/jobs`).

**Error responses:**

- `404` — Template or mount not found
- `422` — Invalid `resources` / `environment`
- `502` — Mount sync or sbatch failed

## Error Responses

All errors follow this format:

``` json
{"detail": "Error message description"}
```

**Status codes:**

- `400` — Invalid input (e.g., negative job ID, path is not a directory)
- `403` — Path outside mount boundary or permission denied
- `404` — Resource not found (job, workflow, mount, directory, run)
- `409` — Resource already exists (e.g., workflow or mount with duplicate name)
- `413` — YAML content too large
- `422` — Validation error or invalid state transition (e.g., cancelling an already-terminal run)
- `500` — Internal error (e.g., script rendering failure)
- `502` — SLURM command, rsync command, or sbatch submission failed
- `503` — SSH connection not configured or rsync not installed

## Configuration

The Web UI is configured via environment variables:

| Variable             | Default           | Description             |
|----------------------|-------------------|-------------------------|
| `SRUNX_SSH_PROFILE`  | (current profile) | srunx SSH profile name  |
| `SRUNX_SSH_HOSTNAME` | —                 | Direct SSH hostname     |
| `SRUNX_SSH_USERNAME` | —                 | Direct SSH username     |
| `SRUNX_SSH_KEY`      | —                 | Path to SSH private key |
| `SRUNX_SSH_PORT`     | 22                | SSH port                |
|                      |                   |                         |
