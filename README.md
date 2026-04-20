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
  <img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-dashboard.png" width="800" alt="srunx web dashboard">
</div>

- **Submit & manage** SLURM jobs from CLI, browser, or Python
- **Orchestrate** multi-step workflows with YAML and dependency graphs
- **Monitor** GPU availability and job states with Slack notifications
- **SSH remote** — submit jobs, sync files, and browse remote clusters from your laptop
- **Container-native** — Pyxis, Apptainer, and Singularity support built in

## Installation

Requires Python 3.12+ and access to a SLURM cluster (local or via SSH).

```bash
uv add srunx             # with uv (recommended)
pip install srunx        # or with pip
```

The web dashboard and Slack notifications are included in the base install — no extras required.

For AI agent integration (MCP server), add the `mcp` extra:

```bash
uv add "srunx[mcp]"
```

## Quick Start

Submit a job, wait for it, and view the logs — end to end:

```bash
# 1. Submit (use -- to separate srunx flags from the command)
$ srunx submit --name training --gpus-per-node 2 --conda ml_env -- python train.py
✅ Submitted job training (id=847291)

# 2. Follow until completion
$ srunx monitor jobs 847291
⠋ 847291 training  PENDING  →  RUNNING  →  COMPLETED (4m 12s)

# 3. Inspect output
$ srunx logs 847291 -n 20
```

Or describe the whole pipeline once and let srunx drive it:

```bash
srunx flow run workflow.yaml
```

## Why srunx?

Instead of stitching together `sbatch`, `squeue`, SSH, and a pipeline runner, srunx offers one coherent surface that covers the day-to-day SLURM loop.

| Capability | srunx | submitit | simple-slurm | Snakemake |
|---|:---:|:---:|:---:|:---:|
| CLI for submit / status / cancel | ✅ | ❌ | ❌ | ⚠️ partial |
| Python API | ✅ | ✅ | ✅ | ✅ |
| Web dashboard | ✅ | ❌ | ❌ | ❌ |
| Workflow DAG with dependencies | ✅ | ❌ | ❌ | ✅ |
| Inter-job runtime variable passing | ✅ | ❌ | ❌ | ⚠️ via files |
| GPU availability monitoring | ✅ | ❌ | ❌ | ❌ |
| SSH remote submit + file sync | ✅ | ❌ | ❌ | ❌ |
| Container support (Pyxis / Apptainer / Singularity) | ✅ | ⚠️ limited | ❌ | ⚠️ via rules |
| Slack notifications | ✅ | ❌ | ❌ | ⚠️ plugin |

If you need full-featured scientific workflow tooling, Snakemake / Nextflow are still the right call. srunx targets the sweet spot of *"SLURM + a few dependencies + a nice UI"* without Airflow-scale infrastructure.

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
| `srunx ui` | Launch the web dashboard |

## Web Dashboard

A dashboard for visual cluster management. Connect to your SLURM cluster over SSH and manage jobs, workflows, and resources from a browser.

```bash
srunx ui                # -> http://127.0.0.1:8000
srunx ui --port 3000    # custom port
```

### Jobs

Browse, search, filter, and cancel jobs.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-jobs.png" width="800" alt="Jobs page">

### Workflow DAG

Visualize job dependencies. Run workflows directly from the UI.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-workflow-dag.png" width="800" alt="Workflow DAG visualization">

### Resources

GPU and node availability per partition.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-resources.png" width="800" alt="Resources page">

### Explorer

Browse remote files via SSH mounts. Shell scripts can be submitted as sbatch jobs directly from the file tree.

<img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-explorer-sbatch.gif" width="800" alt="Explorer sbatch submission">

## Workflow Orchestration

Define pipelines in YAML. Jobs run as soon as their dependencies complete — independent branches execute in parallel automatically.

```yaml
name: experiment
args:
  model: "bert-base-uncased"
  output_dir: "/outputs/{{ model }}"

jobs:
  - name: preprocess
    command: ["python", "preprocess.py", "--out", "{{ output_dir }}/data"]
    outputs:
      DATA_PATH: "{{ output_dir }}/data/processed.parquet"

  - name: train
    command: ["python", "train.py", "--model", "{{ model }}", "--data", "$DATA_PATH"]
    depends_on: [preprocess]
    gpus_per_node: 2
    environment:
      container:
        image: nvcr.io/nvidia/pytorch:24.01-py3
        mounts:
          - /data:/data
    outputs:
      MODEL_PATH: "{{ output_dir }}/models/best.pt"

  - name: evaluate
    command: ["python", "eval.py", "--model", "$MODEL_PATH"]
    depends_on: [train]
```

**What this shows off:**

- **`args` with Jinja2** — reusable, parameterized pipelines (`{{ model }}`, `{{ output_dir }}`)
- **Inter-job outputs** — downstream jobs read `$DATA_PATH` / `$MODEL_PATH` at runtime; you can also append from the job with `echo "key=value" >> $SRUNX_OUTPUTS`
- **Containers per job** — Pyxis / Apptainer / Singularity are first-class (`environment.container`)
- **Dependency-driven scheduling** — `evaluate` blocks on `train`; parallel branches run automatically

Run it:

```bash
srunx flow run workflow.yaml              # execute
srunx flow run workflow.yaml --dry-run    # show plan only
srunx flow run workflow.yaml --from train # resume / partial execution
```

Retry with `retry: N` and `retry_delay: <seconds>` per job.

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

Get notified when jobs finish — set `SLACK_WEBHOOK_URL` (or configure it in the web dashboard), then append `--slack` to any `srunx flow run` command. In Python, pass `SlackCallback` to the runner (see the Python API section below).

<div align="center">
  <img src="https://raw.githubusercontent.com/ksterx/srunx/main/docs/assets/images/ui-slack-notification.png" width="400" alt="Slack notification">
</div>

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
srunx flow run workflow.yaml --slack
```

## Python API

The full CLI surface is available as a Python library. Use it inside notebooks, existing Python pipelines, or custom tooling.

**Submit and wait:**

```python
from srunx import Job, JobResource, JobEnvironment, Slurm

job = Job(
    name="training",
    command=["python", "train.py"],
    resources=JobResource(nodes=1, gpus_per_node=2, time_limit="4:00:00"),
    environment=JobEnvironment(conda="ml_env"),
)

client = Slurm()
completed = client.run(job)  # submit, poll, and return when terminal
print(completed.status, completed.job_id)
```

**Fire-and-track:**

```python
submitted = client.submit(job)                 # returns Job with job_id populated
info = client.retrieve(submitted.job_id)       # poll status on demand
client.cancel(submitted.job_id)                # if you change your mind
```

**Run a YAML workflow programmatically, with callbacks:**

```python
from srunx.callbacks import SlackCallback
from srunx.runner import WorkflowRunner

runner = WorkflowRunner.from_yaml(
    "workflow.yaml",
    callbacks=[SlackCallback(webhook_url="...")],
)
runner.run()                                    # blocks until the DAG finishes
```

## Documentation

Full documentation at **[ksterx.github.io/srunx](https://ksterx.github.io/srunx/)**.

## Development

```bash
git clone https://github.com/ksterx/srunx.git
cd srunx
uv sync --dev

# Full pre-commit quality gate
uv run pytest && uv run mypy . && uv run ruff check .
```

Contributions welcome — please open an issue or PR on [GitHub](https://github.com/ksterx/srunx).

## License

[Apache-2.0](LICENSE)
