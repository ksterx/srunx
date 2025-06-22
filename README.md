# srunx

[![PyPI](https://img.shields.io/pypi/v/srunx)](https://pypi.org/project/srunx/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Actions Status](https://github.com/ksterx/srunx/workflows/CI/badge.svg)](https://github.com/ksterx/srunx/actions)

A modern Python library for SLURM workload manager integration with workflow orchestration capabilities.

## Features

- 🧩 **Workflow Orchestration**: YAML-based workflow definitions with Prefect integration
- ⚡ **Fine-Grained Parallel Execution**: Jobs execute immediately when their specific dependencies complete, not entire workflow phases
- 🔗 **Branched Dependency Control**: Independent branches in dependency graphs run simultaneously without false dependencies
- 📝 **Template System**: Customizable Jinja2 templates for SLURM scripts
- 🛡️ **Type Safe**: Full type hints and mypy compatibility
- 🖥️ **CLI Tools**: Command-line interfaces for both job management and workflows
- 🚀 **Simple Job Submission**: Easy-to-use API for submitting SLURM jobs
- ⚙️ **Flexible Configuration**: Support for various environments (conda, venv, sqsh)
- 📋 **Job Management**: Submit, monitor, cancel, and list jobs

## Installation

### Using uv (Recommended)

```bash
uv add srunx
```

### Using pip

```bash
pip install srunx
```

### Development Installation

```bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync --dev
```

## Quick Start

You can try the workflow example:

```bash
cd examples
srunx flow run sample_workflow.yaml
```

```mermaid
graph TD
    A["Job A"]
    B1["Job B1"]
    B2["Job B2"]
    C["Job C"]
    D["Job D"]

    A --> B1
    A --> C
    B1 --> B2
    B2 --> D
    C --> D
```

The workflow engine provides fine-grained dependency control: when Job A completes, B1 and C start immediately in parallel. As soon as B1 finishes, B2 starts regardless of C's status. Job D waits only for both B2 and C to complete, enabling maximum parallelization.

### Workflow Orchestration

Create a workflow YAML file:

```yaml
# workflow.yaml
name: ml_pipeline
tasks:
  - name: preprocess
    command: ["python", "preprocess.py"]
    nodes: 1
    memory_per_node: "16GB"

  - name: train
    command: ["python", "train.py"]
    depends_on: [preprocess]
    nodes: 1
    gpus_per_node: 2
    memory_per_node: "32GB"
    time_limit: "8:00:00"
    conda: ml_env

  - name: evaluate
    command: ["python", "evaluate.py"]
    depends_on: [train]
    nodes: 1

  - name: notify
    command: ["python", "notify.py"]
    depends_on: [train, evaluate]
```

Execute the workflow:

```bash
# Run workflow
srunx flow run workflow.yaml

# Validate workflow without execution
srunx flow validate workflow.yaml

# Show execution plan
srunx flow run workflow.yaml --dry-run
```

## Advanced Usage

### Custom Templates

Create a custom SLURM template:

```bash
#!/bin/bash
#SBATCH --job-name={{ job_name }}
#SBATCH --nodes={{ nodes }}
{% if gpus_per_node > 0 -%}
#SBATCH --gpus-per-node={{ gpus_per_node }}
{% endif -%}
#SBATCH --time={{ time_limit }}
#SBATCH --output={{ log_dir }}/%x_%j.out

{{ environment_setup }}

srun {{ command }}
```

Use it with your job:

```python
job = client.run(job, template_path="custom_template.slurm.jinja")
```

### Environment Configuration

#### Conda Environment

```python
environment = JobEnvironment(
    conda="my_env",
    env_vars={"CUDA_VISIBLE_DEVICES": "0,1"}
)
```

### Programmatic Workflow Execution

```python
from srunx.workflows import WorkflowRunner

runner = WorkflowRunner.from_yaml("workflow.yaml")
results = runner.run()

print("Job IDs:")
for task_name, job_id in results.items():
    print(f"  {task_name}: {job_id}")
```

### Job Submission

```python
# Submit job without waiting
job = client.submit(job)

# Later, wait for completion
completed_job = client.monitor(job, poll_interval=30)
print(f"Job completed with status: {completed_job.status}")

# Subit and wait for completion
completed_job = client.run(job)
print(f"Job completed with status: {completed_job.status}")
```

## API Reference

### Core Classes

#### `Job`

Main job configuration class with resources and environment settings.

#### `JobResource`

Resource allocation specification (nodes, GPUs, memory, time).

#### `JobEnvironment`

Environment setup (conda, venv, sqsh, environment variables).

#### `Slurm`

Main interface for SLURM operations (submit, status, cancel, list).

#### `WorkflowRunner`

Workflow execution engine with YAML support.

### CLI Commands

#### Main CLI (`srunx`)

- `submit` - Submit SLURM jobs
- `status` - Check job status
- `queue` - List jobs
- `cancel` - Cancel jobs

#### Workflow CLI (`srunx flow`)

- Execute YAML-defined workflows
- Validate workflow files
- Show execution plans

## Configuration

### Environment Variables

- `SLURM_LOG_DIR`: Default directory for SLURM logs (default: `logs`)

### Template Locations

srunx includes built-in templates:

- `base.slurm.jinja`: Basic job template
- `advanced.slurm.jinja`: Full-featured template with all options

## Development

### Setup Development Environment

```bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync --dev
```

### Run Tests

```bash
uv run pytest
```

### Type Checking

```bash
uv run mypy .
```

### Code Formatting

```bash
uv run ruff check .
uv run ruff format .
```

## Examples

### Machine Learning Pipeline

```python
# Complete ML pipeline example
from srunx import Job, JobResource, JobEnvironment, Slurm

def create_ml_job(script: str, **kwargs) -> Job:
    return Job(
        name=f"ml_{script.replace('.py', '')}",
        command=["python", script] + [f"--{k}={v}" for k, v in kwargs.items()],
        resources=JobResource(
            nodes=1,
            gpus_per_node=1,
            memory_per_node="32GB",
            time_limit="4:00:00"
        ),
        environment=JobEnvironment(conda="pytorch")
    )

client = Slurm()

# Submit preprocessing job
prep_job = create_ml_job("preprocess.py", data_path="/data", output_path="/processed")
prep_job = client.run(prep_job)

# Wait for preprocessing to complete
client.monitor(prep_job)

# Submit training job
train_job = create_ml_job("train.py", data_path="/processed", model_path="/models")
train_job = client.run(train_job)

print(f"Training job {train_job.job_id} submitted")
```

### Distributed Computing

```python
# Multi-node distributed job
distributed_job = Job(
    name="distributed_training",
    command=[
        "mpirun", "-np", "16",
        "python", "distributed_train.py"
    ],
    resources=JobResource(
        nodes=4,
        ntasks_per_node=4,
        cpus_per_task=8,
        gpus_per_node=2,
        memory_per_node="128GB",
        time_limit="12:00:00"
    ),
    environment=JobEnvironment(
        conda="distributed_ml"
    )
)

job = client.run(distributed_job)
```

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run type checking and tests
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 🐞 **Issues**: [GitHub Issues](https://github.com/ksterx/srunx/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/ksterx/srunx/discussions)

## Acknowledgments

- Built with [Pydantic](https://pydantic.dev/) for data validation
- Template rendering with [Jinja2](https://jinja.palletsprojects.com/)
- Package management with [uv](https://github.com/astral-sh/uv)
