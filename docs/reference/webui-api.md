---
description: REST API reference for the srunx Web UI server — jobs, resources, workflows and runs, sweep runs, files and mounts, clusters, projects and config, history, notification endpoints, subscriptions, watches, deliveries, and templates.
---

# Web UI REST API Reference

The srunx Web UI exposes a REST API at `http://127.0.0.1:8000/api/`. All responses are JSON.

## Cluster and Project Selection

Every `/api/*` request may carry two query parameters:

- `?cluster=<name>` — the cluster (from `GET /api/clusters`) the request targets. Without it, the selected project's `cluster`, then `default_cluster`, is used. SLURM endpoints return `503` when no cluster can be determined; they need an SSH cluster (a local cluster is rejected with `400`).
- `?project=<name>` — one of the projects the server serves (`GET /api/projects`). It decides mounts, project defaults, and project-scoped config edits. An unknown name returns `400`. Without it, no project applies (the server never falls back to its own working directory).

The selection is per request; there is no server-side "current" cluster or project.

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
| GET | `/api/files/mounts` | List the project's mounts on the cluster (name and remote only) |
| GET | `/api/files/mounts/config` | List mounts with full details (name, local, remote, excludes, sync) |
| GET | `/api/files/browse` | Browse local filesystem under a mount's local root |
| GET | `/api/files/read` | Read text file contents from a mount |
| POST | `/api/files/sync` | Sync a mount's local directory to the remote via rsync |

### GET /api/files/mounts

Returns the selected project's mounts on the selected cluster (`?project=` / `?cluster=`). Only mount names and remote prefixes are returned; local paths are never exposed.

**Response:**

``` json
[
  {
    "name": "ml-project",
    "remote": "/home/researcher/ml-project"
  }
]
```

Returns an empty list if no project or SSH cluster is selected, or the project has no mounts there.

### GET /api/files/mounts/config

Returns the same mounts with full details including local paths. Mounts are read-only here; they are declared in the project's `.srunx/config.toml` (edit `remote_dir` etc. via `PATCH /api/config/file`).

**Response:**

``` json
[
  {
    "name": "ml-project",
    "local": "/home/user/projects/ml-project",
    "remote": "/home/researcher/ml-project",
    "exclude_patterns": ["data/"],
    "sync": true
  }
]
```

Returns an empty list if no project or SSH cluster is selected, or the project has no mounts there.

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
- `404` — Mount not found, directory not found, or no SSH cluster selected

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
- `404` — File not found or no SSH cluster selected
- `413` — File exceeds 1 MB size limit

### POST /api/files/sync

Sync a mount's local directory to the remote server via rsync. Triggering the sync from the UI counts as approval of a new remote target (the CLI asks for `--yes` instead).

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
- `503` — No SSH cluster selected

## Clusters

Clusters are `[clusters.<name>]` tables in the user config. There is no server-side "current cluster": the UI keeps its selection client-side and sends it as `?cluster=<name>`.

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/clusters` | List clusters and the default |
| POST | `/api/clusters` | Add a cluster |
| PUT | `/api/clusters/{name}` | Update connection fields |
| DELETE | `/api/clusters/{name}` | Remove a cluster |
| POST | `/api/clusters/{name}/test` | Check reachability |

### GET /api/clusters

**Response:**

``` json
{
  "default": "dgx",
  "clusters": [
    {
      "name": "dgx",
      "transport": "ssh",
      "host": "dgx-login",
      "hostname": null,
      "username": null,
      "key_filename": null,
      "port": 22,
      "proxy_jump": null,
      "description": "Main DGX cluster",
      "snapshot_root": null
    }
  ]
}
```

`default` is the request project's `cluster`, else `default_cluster` (`null` if neither names a configured cluster).

### POST /api/clusters

**Request body:** the fields of a cluster entry (`name`, `transport` = `"local"` or `"ssh"`, and for SSH `host` or `hostname`, plus optional `username`, `key_filename`, `port`, `proxy_jump`, `description`, `snapshot_root`), and optionally `"make_default": true` to also set `default_cluster`.

**Response:** The created cluster object.

**Error responses:**

- `409` — A cluster with that name exists
- `422` — Invalid name or invalid cluster (e.g. SSH without `host`/`hostname`, local with connection fields)

### PUT /api/clusters/{name}

Update connection fields (`host`, `hostname`, `username`, `key_filename`, `port`, `proxy_jump`, `description`, `snapshot_root`). A value of `null` or `""` removes the field.

**Response:** The updated cluster object.

**Error responses:** `404` — cluster not found; `422` — the result is invalid.

### DELETE /api/clusters/{name}

Remove the cluster from the user config (and clear `default_cluster` if it pointed at it). Closes the server's cached SSH connection to it.

**Response:** `{"ok": true}`

### POST /api/clusters/{name}/test

Check reachability. For a local cluster, checks that `sbatch` is on `PATH`; for an SSH cluster, connects and caches the remote `$HOME` / XDG directories (used for default workspaces).

**Response:**

``` json
{"ok": true, "transport": "ssh", "detail": "connected", "home": "/home/researcher"}
```

## Projects

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/projects` | Projects this server serves |
| GET | `/api/projects/{name}` | A project with its resolved mounts on every SSH cluster |

The server serves the project containing the directory `srunx ui` was started from, plus every `srunx ui --project PATH` / `SRUNX_WEB_PROJECTS` root.

### GET /api/projects

**Response:**

``` json
[
  {
    "name": "ml-project",
    "root": "/home/user/projects/ml-project",
    "config_path": "/home/user/projects/ml-project/.srunx/config.toml",
    "cluster": "dgx"
  }
]
```

### GET /api/projects/{name}

Same fields as above plus `mounts`: `{cluster name: [{name, local, remote, sync, exclude}]}` for every SSH cluster. Mounts whose remote path needs the remote environment (no `remote_dir`, environment not yet cached by a test) are omitted.

**Error responses:** `404` — unknown project; `422` — invalid project config.

## Config

| Method | Endpoint | Description |
|----|----|----|
| GET | `/api/config` | Effective defaults for the request's cluster/project, with sources |
| GET | `/api/config/paths` | Config files in precedence order, plus the state dir |
| GET | `/api/config/file` | Raw contents of one config file plus a version token |
| PATCH | `/api/config/file` | Edit individual keys of one config file |
| GET | `/api/config/env` | srunx-related environment variables that are set |

There is no endpoint that writes the merged configuration back; edits always target one file.

### GET /api/config

Returns the effective configuration for the request's `?cluster=` (or the default cluster) and `?project=`, and the file each value came from.

**Response:**

``` json
{
  "cluster": "dgx",
  "project": "ml-project",
  "project_root": "/home/user/projects/ml-project",
  "values": {
    "resources": {"nodes": 1, "gpus_per_node": 0, "partition": "gpu", "...": "..."},
    "environment": {"conda": null, "venv": null, "container": null, "env_vars": {}},
    "notifications": {"default_endpoint_name": null, "default_preset": "terminal"},
    "sync": {"auto": true, "lock_timeout_seconds": 120, "...": "..."},
    "log_dir": "logs",
    "work_dir": null
  },
  "sources": {
    "resources.partition": "/home/user/.config/srunx/config.toml [clusters.dgx]",
    "resources.nodes": "built-in"
  }
}
```

**Error responses:** `404` — unknown cluster; `422` — a config file is invalid.

### GET /api/config/paths

**Response:**

``` json
[
  {"path": "/etc/srunx/config.toml", "exists": false, "source": "system"},
  {"path": "/home/user/.config/srunx/config.toml", "exists": true, "source": "user"},
  {"path": "/home/user/projects/ml-project/.srunx/config.toml", "exists": true, "source": "project"},
  {"path": "/home/user/.local/state/srunx", "exists": true, "source": "state"}
]
```

The `project` entry is present only when `?project=` is given.

### GET /api/config/file

**Query parameters:**

- `scope` — `user` (default) or `project` (requires `?project=`)

**Response:**

``` json
{
  "scope": "user",
  "path": "/home/user/.config/srunx/config.toml",
  "exists": true,
  "version": "<opaque token>",
  "data": {"default_cluster": "dgx", "clusters": {"dgx": {"transport": "ssh", "host": "dgx-login"}}}
}
```

### PATCH /api/config/file

Edit individual keys of one file. Keys are dotted paths from the file root, e.g. `defaults.resources.partition` or `clusters.dgx.defaults.resources.time_limit`.

**Request body:**

``` json
{
  "scope": "project",
  "version": "<token from GET /api/config/file>",
  "set": {"clusters.dgx.remote_dir": "/home/researcher/ml-project"},
  "delete": ["defaults.resources.partition"],
  "unset": ["defaults.resources.time_limit"]
}
```

- `set` — set values
- `delete` — remove keys (the value is inherited again)
- `unset` — add the key to its table's `unset` list (explicitly cleared)

**Response:** The updated file (same shape as `GET /api/config/file`).

**Error responses:**

- `400` — `scope=project` without `?project=`
- `409` — `version` is stale (the file changed since it was read)
- `422` — The edited file would be invalid

### GET /api/config/env

Lists srunx-related environment variables that are currently set (`SRUNX_CLUSTER`, `SRUNX_TEMP_DIR`, `SRUNX_WEB_PROJECTS`, `SRUNX_WEB_TOKEN` (masked), `SRUNX_WEB_ALLOWED_HOSTS`, `SLACK_WEBHOOK_URL`, `XDG_CONFIG_HOME`, `XDG_STATE_HOME`).

**Response:**

``` json
[
  {
    "name": "SRUNX_CLUSTER",
    "value": "dgx",
    "description": "Cluster selected for CLI commands (ignored by the Web UI)"
  }
]
```

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
- `409` — Resource already exists (e.g., workflow or cluster with duplicate name), or a config edit with a stale version token
- `413` — YAML content too large
- `422` — Validation error or invalid state transition (e.g., cancelling an already-terminal run)
- `500` — Internal error (e.g., script rendering failure)
- `502` — SLURM command, rsync command, or sbatch submission failed
- `503` — No cluster selected (no `?cluster=` and no default) or rsync not installed

## Configuration

The Web UI server is configured with `srunx ui` options and environment variables:

| Variable | Description |
|----|----|
| `SRUNX_WEB_PROJECTS` | Extra project roots to serve, separated by `:` (same as repeated `srunx ui --project PATH`) |
| `SRUNX_WEB_TOKEN` | Bearer token required on `/api/*` (unset by default) |
| `SRUNX_WEB_ALLOWED_HOSTS` | Extra `Host` header values accepted (comma-separated) |

Clusters come from the srunx config files (see [Clusters](#clusters)); `SRUNX_CLUSTER` is ignored by the Web UI.
