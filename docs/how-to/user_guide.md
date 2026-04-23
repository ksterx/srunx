# User Guide

This comprehensive guide covers all aspects of using srunx for SLURM job management.

## Core Concepts

### Jobs

A job in srunx represents a computational task that will be executed on a SLURM cluster. Jobs are defined with:

- **Command**: The command to execute
- **Resources**: CPU, memory, GPU, and time requirements
- **Environment**: Conda, virtual environment, or container setup
- **Dependencies**: Job dependencies for workflow orchestration

### Resources

srunx provides fine-grained control over resource allocation:

- `--nodes`: Number of compute nodes
- `--tasks-per-node`: Tasks per node
- `--cpus-per-task`: CPUs per task
- `--gpus-per-node`: GPUs per node
- `--memory` / `--mem`: Memory per node
- `--time-limit`: Maximum execution time

### Environment Management

srunx supports conda and virtual environment activation:

- **Conda**: `--conda env_name`
- **Virtual Environment**: `--venv /path/to/venv`

Only one of conda or venv can be specified per job.

### Container Runtimes

srunx supports multiple container runtimes for job execution. Containers are orthogonal to conda/venv — they can be used together.

**Pyxis (NVIDIA Enroot)** — default runtime, uses `srun --container-*` flags:

``` bash
srunx sbatch --wrap "python train.py" --container /path/to/image.sqsh
```

**Apptainer** — wraps the command with `apptainer exec`:

``` bash
srunx sbatch --wrap "python train.py \"
  --container "runtime=apptainer,image=/path/to/image.sif,nv=true"
```

**Singularity** — same as Apptainer with `singularity` binary:

``` bash
srunx sbatch --wrap "python train.py \"
  --container "runtime=singularity,image=/path/to/image.sif,nv=true"
```

The runtime can also be specified with a separate flag:

``` bash
srunx sbatch --wrap "python train.py \"
  --container /path/to/image.sif \
  --container-runtime apptainer
```

#### Container Options

The `--container` flag accepts a key=value format for detailed configuration:

| Key | Runtime | Description |
|----|----|----|
| `image` | All | Container image path (SIF, sqsh, or Docker URI) |
| `runtime` | All | `pyxis` (default), `apptainer`, or `singularity` |
| `mounts` / `bind` | All | Bind mounts (semicolon-separated, e.g. `/data:/data;/scratch:/scratch`) |
| `workdir` | All | Working directory inside container |
| `nv` | Apptainer | NVIDIA GPU passthrough (`true`/`false`) |
| `rocm` | Apptainer | AMD GPU passthrough (`true`/`false`) |
| `cleanenv` | Apptainer | Start with clean environment (`true`/`false`) |
| `fakeroot` | Apptainer | Run as fake root (`true`/`false`) |
| `writable_tmpfs` | Apptainer | Writable tmpfs overlay (`true`/`false`) |
| `overlay` | Apptainer | Overlay image path |
| `env` | Apptainer | Container environment variables (`KEY1=VAL1;KEY2=VAL2`) |

Example with multiple options:

``` bash
srunx sbatch --wrap "python train.py" --container \
  "runtime=apptainer,image=pytorch.sif,nv=true,bind=/data:/data;/models:/models,cleanenv=true"
```

#### Container + Conda/Venv

Containers can be combined with conda or venv. The environment activation runs on the host before the containerized command:

``` bash
srunx sbatch --wrap "python train.py \"
  --container "runtime=apptainer,image=pytorch.sif,nv=true,bind=/opt/conda:/opt/conda" \
  --conda ml_env
```

!!! note
    When using `cleanenv=true` with Apptainer, host environment variables (including those set by conda/venv activation) are stripped. Pass needed variables explicitly via `env=` or ensure the relevant paths are bind-mounted.

#### Suppressing Default Containers

If a default container is configured (via `SRUNX_DEFAULT_CONTAINER` or config file), you can suppress it for individual jobs:

``` bash
srunx sbatch --wrap "python train.py" --no-container
```

## Command Line Interface

### Job Submission

Basic submission:

``` bash
srunx sbatch --wrap "<command>"
```

With resource specification:

``` bash
srunx sbatch --wrap "python train.py \"
  --name "training_job" \
  --nodes 2 \
  --gpus-per-node 2 \
  --memory "64GB" \
  --time-limit "8:00:00" \
  --conda ml_env
```

### Job Monitoring

Check a specific job's state (active queue):

``` bash
srunx squeue -j 12345
```

For finished jobs (srunx state DB):

``` bash
srunx sacct -j 12345
```

List all jobs:

``` bash
srunx squeue
```

List with GPU allocation info:

``` bash
srunx squeue --show-gpus
```

List in JSON format:

``` bash
srunx squeue --format json
```

### Job Control

Cancel a job:

``` bash
srunx scancel 12345
```

Monitor job until completion:

``` bash
srunx sbatch --wrap "python script.py" --wait
```

## Workflows

### Workflow Definition

Workflows are defined in YAML format with jobs and dependencies:

``` yaml
name: data_pipeline

jobs:
  - name: download_data
    command: ["python", "download.py"]
    resources:
      nodes: 1
      memory_per_node: "8GB"

  - name: preprocess
    command: ["python", "preprocess.py", "--input", "data/raw"]
    depends_on: [download_data]
    resources:
      nodes: 1
      cpus_per_task: 4

  - name: train_model
    command: ["python", "train.py"]
    depends_on: [preprocess]
    resources:
      nodes: 2
      gpus_per_node: 1
      time_limit: "12:00:00"
    environment:
      conda: pytorch_env

  - name: evaluate
    command: ["python", "evaluate.py"]
    depends_on: [train_model]
```

### Workflow Execution

Run a workflow:

``` bash
srunx flow run pipeline.yaml
```

Validate workflow syntax:

``` bash
srunx flow validate pipeline.yaml
```

Run with custom parameters:

``` bash
srunx flow run pipeline.yaml --dry-run
```

## Advanced Features

### Callbacks and Notifications

srunx supports job completion callbacks, including Slack notifications:

``` python
from srunx.callbacks import SlackCallback
from srunx.client import Slurm

callback = SlackCallback(webhook_url="https://hooks.slack.com/...")
client = Slurm()

job = Job(
    name="training_job",
    command=["python", "train.py"],
)
result = client.submit(job, callbacks=[callback])
```

### Template Customization

srunx uses Jinja2 templates for SLURM script generation. You can customize templates by:

1.  Copying default templates from `srunx/templates/`
2.  Modifying them for your needs
3.  Specifying custom template path

### Programmatic Usage

Use srunx from Python code:

``` python
from srunx.client import Slurm
from srunx.models import Job, JobResource, JobEnvironment, ContainerResource

# Create client
client = Slurm()

# Define job with conda
job = Job(
    name="my_job",
    command=["python", "script.py"],
    resources=JobResource(
        nodes=2,
        gpus_per_node=1,
        memory_per_node="32GB",
        time_limit="4:00:00"
    ),
    environment=JobEnvironment(conda="ml_env")
)

# Define job with Apptainer container
container_job = Job(
    name="container_job",
    command=["python", "train.py"],
    resources=JobResource(gpus_per_node=2),
    environment=JobEnvironment(
        container=ContainerResource(
            runtime="apptainer",
            image="/path/to/pytorch.sif",
            nv=True,
            mounts=["/data:/data"],
        )
    )
)

# Submit and monitor
result = client.submit(job)
status = client.retrieve(result.job_id)
print(f"Job {result.job_id} status: {status}")
```

## Best Practices

### Resource Planning

1.  **Right-size your jobs**: Don't over-allocate resources
2.  **Use time limits**: Prevent runaway jobs
3.  **Monitor resource usage**: Optimize for future jobs

### Environment Management

1.  **Use environment isolation**: Conda, venv, or containers
2.  **Pin dependencies**: Ensure reproducibility
3.  **Test environments**: Validate before large runs
4.  **Prefer Apptainer for reproducibility**: SIF files are immutable and portable across clusters
5.  **Combine containers with conda**: Use containers for system-level dependencies and conda for Python packages

### Workflow Design

1.  **Break down jobs**: Smaller, focused jobs are easier to debug
2.  **Use dependencies wisely**: Minimize blocking dependencies
3.  **Handle failures**: Design for partial workflow recovery

## Configuration

### Container Defaults

Set default container settings via environment variables:

``` bash
# Default container image
export SRUNX_DEFAULT_CONTAINER=/path/to/default.sif

# Default container runtime (pyxis, apptainer, singularity)
export SRUNX_DEFAULT_CONTAINER_RUNTIME=apptainer
```

Or in a config file (`~/.config/srunx/config.json`):

``` json
{
  "environment": {
    "container": {
      "runtime": "apptainer",
      "image": "/shared/containers/pytorch.sif"
    }
  }
}
```

The runtime resolution order (highest priority first):

1.  Explicit `--container-runtime` CLI flag
2.  `runtime=` key in `--container` value
3.  `SRUNX_DEFAULT_CONTAINER_RUNTIME` environment variable
4.  Config file setting
5.  `pyxis` (default fallback)

## Troubleshooting

### Common Issues

**Job fails to start**  
- Check resource availability
- Verify environment exists
- Review SLURM script syntax

**Workflow hangs**  
- Check for circular dependencies
- Verify all dependencies are satisfiable
- Review job logs

**Environment errors**  
- Ensure conda/venv paths are correct
- Check environment activation
- Verify package availability

**Container errors**  
- Verify the image path exists and is accessible from compute nodes
- For Apptainer, ensure `--nv` is set when using GPUs
- Check bind mount paths exist on both host and container
- If using `--cleanenv`, pass required environment variables via `env=`
- Apptainer-specific flags (`nv`, `rocm`, etc.) raise an error if used with `runtime=pyxis`

### Debug Mode

Enable debug logging:

``` bash
export SRUNX_LOG_LEVEL=DEBUG
srunx sbatch --wrap "python script.py"
```

Preview job submission (show summary without submitting):

``` bash
srunx sbatch --dry-run python script.py
```

View rendered SLURM scripts:

``` bash
srunx flow run pipeline.yaml --debug
```
