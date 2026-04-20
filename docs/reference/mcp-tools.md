# MCP Tool Reference

Complete reference for all 14 tools exposed by the srunx MCP server.

The server is started with `uv run --extra mcp srunx-mcp` and communicates
over stdio using the Model Context Protocol.

All tools return a JSON object with a `success` boolean. On success,
additional fields carry the result data. On failure, an `error` string
describes what went wrong.

## Job Management

### submit_job

Submit a SLURM job.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `command` | str | Yes |  | Shell command to execute (e.g. `"python train.py --epochs 100"`) |
| `name` | str | No | `"job"` | Job name for identification in the SLURM queue |
| `nodes` | int | No | `1` | Number of compute nodes to allocate |
| `gpus_per_node` | int | No | `0` | Number of GPUs per node (0 for CPU-only) |
| `ntasks_per_node` | int | No | `1` | Number of tasks per node |
| `cpus_per_task` | int | No | `1` | Number of CPUs per task |
| `memory_per_node` | str \| null | No | `null` | Memory per node (e.g. `"32GB"`, `"64G"`) |
| `time_limit` | str \| null | No | `null` | Wall time limit (e.g. `"4:00:00"`, `"1-00:00:00"`) |
| `partition` | str \| null | No | `null` | SLURM partition name (e.g. `"gpu"`, `"cpu"`) |
| `nodelist` | str \| null | No | `null` | Specific nodes to use (e.g. `"node001,node002"`) |
| `conda` | str \| null | No | `null` | Conda environment name to activate before running |
| `venv` | str \| null | No | `null` | Path to Python virtual environment to activate |
| `env_vars` | dict \| null | No | `null` | Additional environment variables as key-value pairs |
| `log_dir` | str | No | `"logs"` | Directory for stdout/stderr log files |
| `work_dir` | str \| null | No | `null` | Working directory for the job (defaults to cwd; required when `use_ssh=true`) |
| `use_ssh` | bool | No | `false` | Submit via SSH to remote SLURM cluster |

**Return value:**

``` json
{
  "success": true,
  "job_id": "12345",
  "name": "training",
  "status": "PENDING"
}
```

**Example:**

``` text
> Submit "python train.py" with 2 GPUs, conda env ml_env, 8 hour time limit
```

### list_jobs

List current user's SLURM jobs in the queue.

**Parameters:**

| Name      | Type | Required | Default | Description                          |
|-----------|------|----------|---------|--------------------------------------|
| `use_ssh` | bool | No       | `false` | Query jobs via SSH on remote cluster |

**Return value:**

``` json
{
  "success": true,
  "jobs": [
    {
      "name": "training",
      "job_id": "12345",
      "status": "RUNNING",
      "partition": "gpu",
      "nodes": "1"
    }
  ],
  "count": 1
}
```

### get_job_status

Get the status of a specific SLURM job.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `job_id` | str | Yes |  | SLURM job ID to check (numeric, e.g. `"12345"` or `"12345_1"`) |
| `use_ssh` | bool | No | `false` | Query via SSH on remote cluster |

**Return value:**

``` json
{
  "success": true,
  "job_id": "12345",
  "name": "training",
  "status": "RUNNING",
  "partition": "gpu",
  "nodes": "1"
}
```

### cancel_job

Cancel a running or pending SLURM job.

**Parameters:**

| Name      | Type | Required | Default | Description                      |
|-----------|------|----------|---------|----------------------------------|
| `job_id`  | str  | Yes      |         | SLURM job ID to cancel           |
| `use_ssh` | bool | No       | `false` | Cancel via SSH on remote cluster |

**Return value:**

``` json
{
  "success": true,
  "job_id": "12345",
  "message": "Job cancelled"
}
```

### get_job_logs

Get stdout/stderr logs for a SLURM job.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `job_id` | str | Yes |  | SLURM job ID |
| `job_name` | str \| null | No | `null` | Job name to help locate log files |
| `use_ssh` | bool | No | `false` | Fetch logs via SSH from remote cluster |

**Return value:**

``` json
{
  "success": true,
  "job_id": "12345",
  "stdout": "Epoch 1/10: loss=0.45 ...",
  "stderr": "",
  "log_files": ["logs/training-12345.out"]
}
```

!!! note
    The `log_files` field is only present for local (non-SSH) queries.

## Resources

### get_resources

Get current GPU and node resource availability on the SLURM cluster.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `partition` | str \| null | No | `null` | Specific partition to check (null for all partitions) |
| `use_ssh` | bool | No | `false` | Query resources via SSH on remote cluster |

**Return value (local):**

``` json
{
  "success": true,
  "partition": "gpu",
  "total_gpus": 32,
  "gpus_in_use": 24,
  "gpus_available": 8,
  "gpu_utilization": 0.75,
  "jobs_running": 12,
  "nodes_total": 8,
  "nodes_idle": 2,
  "nodes_down": 0
}
```

**Return value (SSH):**

When using SSH mode, the return includes a `raw_output` field with the
`sinfo` output instead of parsed metrics:

``` json
{
  "success": true,
  "partition": "gpu",
  "raw_output": "node001 gpu:4 idle gpu*\nnode002 gpu:4 mixed gpu*"
}
```

## Workflows

### create_workflow

Create a SLURM workflow YAML file. Generates a workflow definition that can
be executed with `run_workflow`.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `name` | str | Yes |  | Workflow name for identification |
| `jobs` | list\[dict\] | Yes |  | List of job definitions (see schema below) |
| `output_path` | str | Yes |  | File path to write the YAML (e.g. `"workflow.yaml"`) |
| `args` | dict \| null | No | `null` | Template variables for Jinja2 templating in job definitions |
| `default_project` | str \| null | No | `null` | Default SSH mount name for file syncing |

**Job definition schema:**

Each entry in the `jobs` list is a dict with these fields:

| Field | Type | Required | Description |
|----|----|----|----|
| `name` | str | Yes | Job identifier (must be unique within the workflow) |
| `command` | str \| list\[str\] | Yes\* | Command to execute (\*required for regular jobs) |
| `script_path` | str | Yes\* | Path to shell script (\*required for shell jobs, mutually exclusive with `command`) |
| `depends_on` | list\[str\] | No | Job names this job depends on. Supports dependency types: `"afterok:preprocess"`, `"after:job_a"`, `"afterany:job_a"`, `"afternotok:job_a"` |
| `retry` | int | No | Number of retry attempts on failure (default 0) |
| `retry_delay` | int | No | Seconds between retries (default 60) |
| `resources` | dict | No | Resource allocation: `nodes`, `gpus_per_node`, `ntasks_per_node`, `cpus_per_task`, `memory_per_node`, `time_limit`, `partition`, `nodelist` |
| `environment` | dict | No | Environment setup: `conda`, `venv`, `env_vars`, `container` |
| `log_dir` | str | No | Log directory path |
| `work_dir` | str | No | Working directory path |

**Return value:**

``` json
{
  "success": true,
  "path": "/absolute/path/to/workflow.yaml",
  "name": "ml_pipeline",
  "job_count": 3,
  "job_names": ["preprocess", "train", "evaluate"],
  "message": "Workflow 'ml_pipeline' created at workflow.yaml"
}
```

### validate_workflow

Validate a workflow YAML file for correctness. Checks YAML syntax, job
structure, dependency resolution, and circular dependency detection.

**Parameters:**

| Name        | Type | Required | Default | Description                                |
|-------------|------|----------|---------|--------------------------------------------|
| `yaml_path` | str  | Yes      |         | Path to the YAML workflow file to validate |

**Return value:**

``` json
{
  "success": true,
  "name": "ml_pipeline",
  "valid": true,
  "job_count": 3,
  "jobs": [
    {"name": "preprocess", "depends_on": [], "command": "python preprocess.py"},
    {"name": "train", "depends_on": ["preprocess"], "command": "python train.py"},
    {"name": "evaluate", "depends_on": ["train"], "command": "python evaluate.py"}
  ]
}
```

### run_workflow

Execute a SLURM workflow from a YAML file. Jobs are executed in dependency
order -- independent jobs run in parallel, dependent jobs wait for their
prerequisites to complete.

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `yaml_path` | str | Yes |  | Path to the YAML workflow file |
| `from_job` | str \| null | No | `null` | Start execution from this job (skip earlier jobs) |
| `to_job` | str \| null | No | `null` | Stop execution at this job (skip later jobs) |
| `single_job` | str \| null | No | `null` | Execute only this specific job, ignoring dependencies |
| `dry_run` | bool | No | `false` | Show what would be executed without running |

**Return value (execution):**

``` json
{
  "success": true,
  "workflow": "ml_pipeline",
  "results": {
    "preprocess": {"job_id": "12345", "status": "COMPLETED"},
    "train": {"job_id": "12346", "status": "COMPLETED"},
    "evaluate": {"job_id": "12347", "status": "COMPLETED"}
  },
  "all_completed": true
}
```

**Return value (dry run):**

``` json
{
  "success": true,
  "dry_run": true,
  "workflow": "ml_pipeline",
  "jobs_to_execute": [
    {"name": "preprocess", "depends_on": [], "command": "python preprocess.py"},
    {"name": "train", "depends_on": ["preprocess"], "command": "python train.py"}
  ],
  "count": 2
}
```

### list_workflows

List workflow YAML files in a directory. Scans for YAML files that contain a
valid srunx workflow structure (must have `name` and `jobs` keys). Skips
hidden directories, `node_modules`, `.venv`, and `__pycache__`.

**Parameters:**

| Name        | Type | Required | Default | Description                            |
|-------------|------|----------|---------|----------------------------------------|
| `directory` | str  | No       | `"."`   | Directory to search for workflow files |

**Return value:**

``` json
{
  "success": true,
  "workflows": [
    {
      "path": "/home/user/project/workflows/ml_pipeline.yaml",
      "name": "ml_pipeline",
      "job_count": 3,
      "job_names": ["preprocess", "train", "evaluate"]
    }
  ],
  "count": 1
}
```

### get_workflow

Read and parse a workflow YAML file, returning its full structure including
resource and environment configuration for each job.

**Parameters:**

| Name        | Type | Required | Default | Description                    |
|-------------|------|----------|---------|--------------------------------|
| `yaml_path` | str  | Yes      |         | Path to the YAML workflow file |

**Return value:**

``` json
{
  "success": true,
  "name": "ml_pipeline",
  "args": null,
  "default_project": null,
  "jobs": [
    {
      "name": "train",
      "depends_on": ["preprocess"],
      "retry": 0,
      "retry_delay": 60,
      "command": "python train.py",
      "resources": {
        "nodes": 1,
        "gpus_per_node": 2,
        "ntasks_per_node": 1,
        "cpus_per_task": 1,
        "memory_per_node": "32GB",
        "time_limit": "8:00:00",
        "partition": null,
        "nodelist": null
      },
      "environment": {
        "conda": "ml_env",
        "venv": null,
        "env_vars": {}
      }
    }
  ],
  "raw_yaml": "name: ml_pipeline\njobs:\n  ..."
}
```

## File Sync

### sync_files

Sync files between local machine and remote SLURM cluster using rsync.
Supports two modes: mount-based (using a named mount from the SSH profile)
or path-based (using explicit local and remote paths).

**Parameters:**

| Name | Type | Required | Default | Description |
|----|----|----|----|----|
| `profile_name` | str \| null | No | `null` | SSH profile name (uses current profile if not specified) |
| `mount_name` | str \| null | No | `null` | Mount point name from the SSH profile to sync |
| `local_path` | str \| null | No | `null` | Local directory path (alternative to `mount_name`) |
| `remote_path` | str \| null | No | `null` | Remote directory path (alternative to `mount_name`) |
| `dry_run` | bool | No | `false` | Show what would be transferred without actually syncing |

!!! note
    You must provide either `mount_name` or `local_path`. When using
    `mount_name`, the local and remote paths are read from the SSH profile
    configuration. When using `local_path` without `remote_path`, a
    default remote path is derived.

**Return value:**

``` json
{
  "success": true,
  "profile": "myserver",
  "mount": "ml-project",
  "local": "/home/user/projects/ml-project",
  "remote": "/home/researcher/projects/ml-project",
  "dry_run": false,
  "output": "sending incremental file list\nsrc/train.py\n..."
}
```

## Configuration

### get_config

Get the current srunx configuration including resource defaults and
environment settings.

**Parameters:** None.

**Return value:**

``` json
{
  "success": true,
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
    "env_vars": {}
  },
  "log_dir": "logs",
  "work_dir": null
}
```

### list_ssh_profiles

List all configured SSH connection profiles for remote SLURM clusters.
Shows profile names, hostnames, and configured mount points.

**Parameters:** None.

**Return value:**

``` json
{
  "success": true,
  "profiles": [
    {
      "name": "dgx",
      "hostname": "dgx.example.com",
      "username": "researcher",
      "port": 22,
      "description": "Main DGX cluster",
      "is_current": true,
      "mounts": [
        {
          "name": "ml-project",
          "local": "/home/user/projects/ml-project",
          "remote": "/home/researcher/projects/ml-project"
        }
      ]
    }
  ],
  "current": "dgx",
  "count": 1
}
```
