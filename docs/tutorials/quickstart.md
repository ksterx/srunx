# Quick Start

This guide will help you get started with srunx quickly.

## Basic Job Submission

Submit a simple Python script:

``` bash
srunx submit python my_script.py
```

Submit with specific resources:

``` bash
srunx submit python train.py --name ml_job --gpus-per-node 1 --nodes 2
```

Submit with conda environment:

``` bash
srunx submit python process.py --conda ml_env --memory 32GB
```

## Job Management

Check job status:

``` bash
srunx status <job_id>
```

List your jobs:

``` bash
srunx list
```

Cancel a job:

``` bash
srunx cancel <job_id>
```

## Workflow Example

Create a workflow YAML file (`workflow.yaml`):

``` yaml
name: ml_pipeline
jobs:
  - name: preprocess
    command: ["python", "preprocess.py"]
    resources:
      nodes: 1

  - name: train
    command: ["python", "train.py"]
    depends_on: [preprocess]
    resources:
      gpus_per_node: 1
      memory_per_node: "32GB"
      time_limit: "4:00:00"
    environment:
      conda: ml_env

  - name: evaluate
    command: ["python", "evaluate.py"]
    depends_on: [train]
```

Run the workflow:

``` bash
srunx flow run workflow.yaml
```

Validate a workflow:

``` bash
srunx flow validate workflow.yaml
```

## Environment Setup

srunx supports multiple environment types:

### Conda Environment

``` bash
srunx submit python script.py --conda my_env
```

### Python Virtual Environment

``` bash
srunx submit python script.py --venv /path/to/venv
```

### Container (Pyxis)

``` bash
srunx submit python script.py --container /path/to/container.sqsh
```

### Apptainer / Singularity Container

``` bash
srunx submit python script.py \
  --container "runtime=apptainer,image=/path/to/image.sif,nv=true"
```

Or specify the runtime separately:

``` bash
srunx submit python script.py \
  --container /path/to/image.sif \
  --container-runtime apptainer
```

### Conda Inside a Container

Containers can be combined with conda or venv:

``` bash
srunx submit python script.py \
  --container "runtime=apptainer,image=pytorch.sif,nv=true,bind=/data:/data" \
  --conda ml_env
```

## Parameter Sweep

So far you have run a single workflow. In this last part of the tutorial you
will run the *same* workflow several times with different parameters -- a
**parameter sweep** -- without copying YAML files.

You will:

1. Write a tiny workflow that just echoes the parameters it received.
2. Launch a sweep over three values from the command line.
3. Watch the three cells run and inspect the aggregated result.

The example uses only `echo`, so no GPU, conda, or cluster-specific setup
is required.

### 1. Write the workflow

Save the following as `sweep_demo.yaml`:

``` yaml
name: sweep_demo
args:
  seed: 1

jobs:
  - name: echo
    command: ["bash", "-lc", "echo 'seed={{ seed }}'"]
```

Notice the `{{ seed }}` placeholder. The workflow already runs on its own
(it will just use `seed=1`, the default), but it is ready to be swept.

### 2. Launch the sweep

Ask srunx to run the workflow three times, once per seed, with at most two
cells running at the same time:

``` bash
srunx flow run --sweep seed=1,2,3 --max-parallel 2 sweep_demo.yaml
```

srunx expands the matrix at load time into **three independent cells**,
each with its own `seed` value. The command prints a sweep ID and the IDs
of the three child workflow runs, then streams their progress.

### 3. Observe the cells

While the sweep is running, list your jobs in another terminal:

``` bash
srunx list
```

You should see up to two `echo` jobs in `RUNNING` state at a time, with
the third one queued until a slot frees up. When everything is done, the
sweep converges to `completed` and each cell reports its own result.

Because `fail_fast` defaults to `false`, one misbehaving cell would **not**
cancel the others -- the sweep would simply end with a mix of
`completed` and `failed` cells.

### 4. You made it work

You just ran the same workflow three times under a single sweep parent,
with automatic concurrency control. From here:

- To re-run only the cells that failed, or to learn the full sweep
  surface (ad-hoc overrides, dry-run previews, Web UI / MCP sweeps),
  read the how-to guide: [Parameter Sweeps](../how-to/workflows.md#parameter-sweeps).
- To drive sweeps from an AI agent via Claude Code, see the
  [MCP tools reference](../reference/mcp-tools.md).

## Next Steps

- Read the [User Guide](../how-to/user_guide.md) for detailed usage instructions
- Check the [API Reference](../reference/api.md) for programmatic usage
- Explore [Workflows](../how-to/workflows.md) for complex job orchestration
