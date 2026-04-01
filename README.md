<div align="center">

# srunx

**A unified CLI, web dashboard, and Python API for SLURM job management.**

Stop juggling `sbatch` scripts, `squeue` loops, and SSH sessions.

[![PyPI](https://img.shields.io/pypi/v/srunx)](https://pypi.org/project/srunx/)
[![Downloads](https://img.shields.io/pypi/dm/srunx)](https://pypi.org/project/srunx/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![CI](https://github.com/ksterx/srunx/workflows/CI/badge.svg)](https://github.com/ksterx/srunx/actions)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://ksterx.github.io/srunx/)

</div>

<div align="center">
  <img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/screenshots/dashboard.png" width="800" alt="srunx web dashboard">
</div>

- **Submit & manage** SLURM jobs from CLI, browser, or Python
- **Orchestrate** multi-step workflows with YAML and dependency graphs
- **Monitor** GPU availability and job states with Slack notifications
- **SSH remote** — submit jobs, sync files, and browse remote clusters from your laptop
- **Container-native** — Pyxis, Apptainer, and Singularity support built in

## Installation

Requires Python 3.12+ and access to a SLURM cluster (local or via SSH).

```bash
pip install srunx
```

For the web dashboard:

```bash
pip install "srunx[web]"
```

## Quick Start

```bash
# Submit a job
srunx submit python train.py --name training --gpus-per-node 2 --conda ml_env

# Check status and resources
srunx list --show-gpus
srunx resources

# Run a YAML workflow
srunx flow run workflow.yaml
```

## CLI

| Command | Description |
|---------|-------------|
| `srunx submit` | Submit a SLURM job |
| `srunx status` | Check job status |
| `srunx list` | List jobs in queue |
| `srunx cancel` | Cancel a job |
| `srunx logs` | View / stream job logs |
| `srunx resources` | Display GPU availability |
| `srunx monitor` | Monitor jobs, resources, or cluster |
| `srunx flow` | Run / validate YAML workflows |
| `srunx ssh` | Remote SLURM operations over SSH |
| `srunx history` | Show job execution history |
| `srunx report` | Generate job execution report |
| `srunx config` | Manage configuration |
| `srunx template` | Manage job templates |

## Web Dashboard

A dashboard for visual cluster management. Connect to your SLURM cluster over SSH and manage jobs, workflows, and resources from a browser.

```bash
srunx-web  # -> http://127.0.0.1:8000
```

<details>
<summary><b>Screenshots</b></summary>

**Jobs** — Browse, search, filter, and cancel jobs.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/screenshots/jobs.png" width="800" alt="Jobs page">

**Workflow DAG** — Visualize job dependencies. Run workflows directly from the UI.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/screenshots/workflow_dag.png" width="800" alt="Workflow DAG visualization">

**Resources** — GPU and node availability per partition.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/screenshots/resources.png" width="800" alt="Resources page">

**Explorer** — Browse remote files via SSH mounts. Shell scripts can be submitted as sbatch jobs directly from the file tree.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/screenshots/explorer_sbatch.gif" width="800" alt="Explorer sbatch submission">

</details>

## Workflow Orchestration

Define pipelines in YAML with dependency graphs and Jinja2-parameterized variables:

```yaml
name: experiment
args:
  model: "bert-base-uncased"
  output_dir: "/outputs/{{ model }}"

jobs:
  - name: preprocess
    command: ["python", "preprocess.py"]
    nodes: 1

  - name: train
    command: ["python", "train.py", "--model", "{{ model }}"]
    depends_on: [preprocess]
    gpus_per_node: 2
    conda: ml_env

  - name: evaluate
    command: ["python", "eval.py", "--output", "{{ output_dir }}"]
    depends_on: [train]
```

Jobs run as soon as their dependencies complete — independent branches execute in parallel automatically.

- `args` with Jinja2 templates for reusable, parameterized pipelines
- Retry support with configurable delay
- Dry-run mode and partial execution (`--from`, `--to`, `--job`)

## Monitoring

```bash
# Monitor a job until completion
srunx monitor jobs 12345

# Wait for GPUs, then submit
srunx monitor resources --min-gpus 4
srunx submit python train.py --gpus-per-node 4

# Periodic cluster reports to Slack
srunx monitor cluster --schedule 1h --notify $SLACK_WEBHOOK
```

## Remote SSH

Keep your local editor workflow while running on the cluster:

```bash
# Submit to remote cluster
srunx ssh submit train.py --host dgx-server

# Manage connection profiles
srunx ssh profile add myserver --ssh-host dgx1

# Map local directories to remote and sync with rsync
srunx ssh profile mount add myserver workspace \
  --local ~/projects/ml-exp --remote /home/user/ml-exp
srunx ssh sync
```

- SSH config hosts, saved profiles, and proxy jump support
- Environment variable passthrough (`--env KEY=VALUE`, `--env-local WANDB_API_KEY`)
- File sync via rsync — auto-detects profile from current directory

## Slack Notifications

<div align="center">
  <img src="https://raw.githubusercontent.com/ksterx/srunx/main/public/slack_screenshot.png" width="400" alt="Slack notification">
</div>

```bash
srunx flow run workflow.yaml --slack
```

## Python API

```python
from srunx import Job, JobResource, JobEnvironment, Slurm

job = Job(
    name="training",
    command=["python", "train.py"],
    resources=JobResource(nodes=1, gpus_per_node=2, time_limit="4:00:00"),
    environment=JobEnvironment(conda="ml_env"),
)

client = Slurm()
completed = client.run(job)  # submit and wait for completion
```

## Why srunx?

Tools like `submitit` and `simple-slurm` handle job submission, and workflow engines like Snakemake or Nextflow handle pipelines. srunx covers both — plus monitoring, SSH remote access, a web dashboard, and container support — in a single, lightweight package. If you want one tool that covers the full SLURM workflow without heavyweight infrastructure, srunx is a good fit.

## Documentation

Full documentation at **[ksterx.github.io/srunx](https://ksterx.github.io/srunx/)**.

## Development

```bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync --dev
uv run pytest
```

## License

[Apache-2.0](LICENSE)
