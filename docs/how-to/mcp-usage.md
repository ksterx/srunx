# Using srunx MCP Tools

This guide shows how to accomplish specific tasks through Claude Code using the
srunx MCP tools. Each section is a self-contained recipe.

For setup instructions, see [MCP Setup](../tutorials/mcp-setup.md).
For the full tool reference, see [MCP Tools](../reference/mcp-tools.md).

## Submit a Job

Ask Claude Code to submit a job with the resources you need:

``` text
> Submit a job named "training" that runs "python train.py --epochs 50"
> with 4 GPUs, 64GB memory, on the gpu partition, using conda env pytorch
```

Claude Code calls `submit_job` and returns the job ID. You can then
reference that ID in follow-up prompts:

``` text
> What's the status of that job?
```

## Create a Workflow from Natural Language

Describe your pipeline and let Claude Code generate the workflow YAML:

``` text
> Create a workflow called "ml_pipeline" with three jobs:
> 1. "preprocess" runs "python preprocess.py" on 1 node
> 2. "train" runs "python train.py" with 2 GPUs, depends on preprocess,
>    uses conda env ml_env, time limit 8 hours
> 3. "evaluate" runs "python evaluate.py", depends on train
> Save it to workflows/ml_pipeline.yaml
```

Claude Code calls `create_workflow` with the job definitions and writes
the YAML file. You can then validate and run it:

``` text
> Validate the workflow at workflows/ml_pipeline.yaml
> Run it
```

## Monitor Resources and Make Decisions

Use resource checks to decide when and where to submit jobs:

``` text
> Check GPU availability on all partitions. If there are at least 4 GPUs
> free on any partition, submit my training job there.
```

Claude Code calls `get_resources`, inspects the result, and conditionally
calls `submit_job` with the partition that has capacity.

## Check Job Logs

Retrieve stdout and stderr from completed or running jobs:

``` text
> Show me the logs for job 12345
```

Claude Code calls `get_job_logs` and displays the output. For SSH jobs,
it fetches logs from the remote cluster.

## Sync Files Before Job Submission

Ensure your latest code is on the remote cluster before submitting:

``` text
> Sync the ml-project mount and then submit "python train.py" with
> 2 GPUs via SSH
```

Claude Code calls `sync_files` with the mount name, then calls
`submit_job` with `use_ssh=True`. The sync uses your configured mount
points from the SSH profile.

For explicit paths instead of named mounts:

``` text
> Sync ./src to ~/workspace/src on the remote cluster (dry run first)
```

Claude Code calls `sync_files` with `local_path` and `remote_path`,
first with `dry_run=True` to preview, then again to execute.

## Use SSH Mode for Remote Clusters

Most tools accept a `use_ssh` flag. You can tell Claude Code to operate
remotely:

``` text
> List jobs on the remote cluster
```

``` text
> Cancel job 12345 on the remote cluster
```

``` text
> Check resources on the gpu partition via SSH
```

Claude Code routes these through the active SSH profile. To see which
profiles are configured:

``` text
> List my SSH profiles
```

## Run Partial Workflows

Execute specific portions of a workflow:

``` text
> Run only the "train" job from workflows/ml_pipeline.yaml
```

``` text
> Run the workflow from "train" to "evaluate", skipping preprocess
```

``` text
> Do a dry run of workflows/ml_pipeline.yaml to see what would execute
```

Claude Code uses the `single_job`, `from_job`, `to_job`, and
`dry_run` parameters of `run_workflow`.

## Parameter Sweeps

Run the same workflow over a cross-product of hyperparameters by passing
a `sweep=` argument to `run_workflow`. Each cell executes as an
independent `workflow_run` under one parent `sweep_run`.

### Basic sweep (local Slurm)

``` text
> Run the train workflow at /projects/train.yaml with seed 1/2/3 and
> lr 0.001/0.01, max 2 in parallel
```

Claude Code calls:

``` python
run_workflow(
    yaml_path="/projects/train.yaml",
    sweep={
        "matrix": {"seed": [1, 2, 3], "lr": [0.001, 0.01]},
        "max_parallel": 2,
    },
)
```

Without `mount=`, cells run through the local `Slurm` singleton.

### Sweep over SSH (``mount=``)

``` text
> Submit the train workflow on the cookbook2 project with seed 1-3,
> max 2 parallel
```

Claude Code calls:

``` python
run_workflow(
    yaml_path="/projects/cookbook2/train.yaml",
    sweep={
        "matrix": {"seed": [1, 2, 3]},
        "max_parallel": 2,
    },
    mount="cookbook2",
)
```

With `mount=<profile>`, cells route through a per-sweep SSH executor
pool. The named mount must exist on the active SSH profile -- unknown
names return an error envelope.

### Combine ``args`` and ``sweep``

`args` overrides base workflow args for every cell; `sweep` defines
the matrix axes. They can be used together:

``` python
run_workflow(
    yaml_path="/projects/train.yaml",
    args={"dataset": "imagenet"},
    sweep={
        "matrix": {"lr": [0.001, 0.01, 0.1]},
        "max_parallel": 3,
    },
)
```

### Return value

``` json
{
  "success": true,
  "sweep_run_id": "sr_abc123",
  "status": "running",
  "cell_count": 6,
  "cells_completed": 0,
  "cells_failed": 0,
  "cells_cancelled": 0
}
```

!!! warning
    `python:` arg prefixes and `ShellJob.script_path` values outside the
    mount root are rejected for security.

See also: [Parameter Sweeps in workflows how-to](workflows.md#parameter-sweeps).

## Combine Multiple Operations

Claude Code can chain tools in a single conversation turn:

``` text
> Check if there are at least 2 GPUs available. If yes, sync my
> ml-project mount and submit "python train.py --lr 0.001" with
> 2 GPUs via SSH. Show me the job ID when done.
```

This triggers a sequence: `get_resources` -\> `sync_files` -\> `submit_job`.

Another multi-step example:

``` text
> Find all workflows in this project, validate each one, and tell me
> which ones have issues.
```

This calls `list_workflows` then `validate_workflow` for each file found.

## Inspect Configuration

Review your srunx setup:

``` text
> Show my srunx configuration
```

``` text
> What SSH profiles do I have configured? Show their mount points.
```

These call `get_config` and `list_ssh_profiles` respectively.

## Tips

- Claude Code picks the right tool based on your intent. You do not need
  to name tools explicitly.
- Include "via SSH" or "on the remote cluster" to trigger SSH mode.
- Use "dry run" to preview any destructive operation before executing.
- Reference job IDs from earlier in the conversation -- Claude Code tracks
  context across turns.
