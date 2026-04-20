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

## Next Steps

- Read the [User Guide](../how-to/user_guide.md) for detailed usage instructions
- Check the [API Reference](../reference/api.md) for programmatic usage
- Explore [Workflows](../how-to/workflows.md) for complex job orchestration
