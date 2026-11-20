---
description: Comprehensive guide to srunx CLI commands for SLURM — sbatch / squeue / scancel / sinfo, plus clusters, layered configuration, conda/venv setup, and SSH integration.
---

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
- `--ntasks-per-node`: Tasks per node
- `--cpus-per-task`: CPUs per task
- `--gpus-per-node`: GPUs per node
- `--mem`: Memory per node
- `--time`: Maximum execution time

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
srunx sbatch --wrap "python train.py" \
  --container "runtime=apptainer,image=/path/to/image.sif,nv=true"
```

**Singularity** — same as Apptainer with `singularity` binary:

``` bash
srunx sbatch --wrap "python train.py" \
  --container "runtime=singularity,image=/path/to/image.sif,nv=true"
```

The runtime can also be specified with a separate flag:

``` bash
srunx sbatch --wrap "python train.py" \
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
srunx sbatch --wrap "python train.py" \
  --container "runtime=apptainer,image=pytorch.sif,nv=true,bind=/opt/conda:/opt/conda" \
  --conda ml_env
```

!!! note
    When using `cleanenv=true` with Apptainer, host environment variables (including those set by conda/venv activation) are stripped. Pass needed variables explicitly via `env=` or ensure the relevant paths are bind-mounted.

#### Suppressing Default Containers

If a default container is configured (via `environment.container` in a config file), you can suppress it for individual jobs:

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
srunx sbatch --wrap "python train.py" \
  --job-name "training_job" \
  --nodes 2 \
  --gpus-per-node 2 \
  --mem "64GB" \
  --time "8:00:00" \
  --conda ml_env
```

Flags follow real `sbatch` (`-J`, `-N`, `-c`, `-t`, `-p`, `-w`, `-D`,
`--gres=gpu:N`, `-Q` for `--quiet`, `-q` for `--qos`). Native sbatch options
that srunx does not model itself pass straight through to `sbatch` in all their
real spellings:

``` bash
srunx sbatch --array=1-10 --account=proj --qos=normal train.sh
srunx sbatch --wrap "python train.py" --dependency=afterok:123
```

For an option that is not recognized, pass one raw token with `--sbatch-arg`
(repeatable), for example `--sbatch-arg=--arr=1-10`. Options that would break
srunx's reading of the submission result (`--parsable`, `--test-only`,
`--quiet`, `--wrap`, `--help`, `--version`) are rejected with exit 2. Long-option
abbreviations are not recognized.

Submit to another cluster by adding `--cluster <name>`; see
[Choosing a Cluster](#choosing-a-cluster) and
[Project Synchronization](sync.md) for auto-sync and `--snapshot`.

#### Script directives and config defaults

When you submit a script (`srunx sbatch train.sh`), srunx reads its `#SBATCH`
directives. Configured defaults (for example a partition from your config) are
forwarded to `sbatch` only for options the script does not set and you did not
type; srunx's built-in defaults are never forwarded, because Slurm has its own.

- A warning is printed when the script contradicts a cluster-specific default
  (`[clusters.<name>.defaults]`), since that default usually exists for a reason.
- Long-option abbreviations (`--part=gpu`) and bundled short options
  (`-vp gpu`) in `#SBATCH` lines are recognised like sbatch does.
- Alternative spellings of the same resource count as set: a script's
  `--mem-per-cpu` / `--mem-per-gpu` suppresses an inherited `--mem`,
  `--cpus-per-gpu` an inherited `--cpus-per-task`, and `--gres` / `--gpus` /
  `--gpus-per-task` an inherited `--gpus-per-node`.
- `--gres` is passed through verbatim; with `--wrap` it also drops an
  inherited `gpus_per_node`.
- On an SSH cluster, inherited `work_dir` / `log_dir` values that are absolute
  local paths under a mount are translated to the remote path.
- `--unset KEY` (repeatable) drops an inherited default for this submission,
  e.g. `--unset partition`.
- `--dry-run` shows the selected cluster and where each value came from.
- Slurm's `--clusters` / `-M` is rejected everywhere (flags and `#SBATCH`);
  use srunx's `--cluster` instead.
- `--yes` / `-y` approves the first sync to a new remote path without a prompt.

#### Secrets on a remote cluster

API keys can be stored once per remote account instead of being typed into
scripts. The value is read from a hidden prompt (or `--from-env VAR`) and is
never placed on a command line:

``` bash
srunx cluster secret set myserver HF_TOKEN
srunx cluster secret list myserver              # names only
srunx cluster secret unset myserver HF_TOKEN
```

Secrets live in a `0600` file at `~/.config/srunx/secrets.env` on the cluster.
srunx sources it before each submission, so jobs see the variables; a same-named
`--env KEY=VALUE` takes precedence. When the file exists, srunx adds
`--export=ALL` to `sbatch`, which overrides a script's own `#SBATCH --export=NONE`.

### Job Monitoring

Check a specific job's state (active queue):

``` bash
srunx squeue -j 12345
```

For finished jobs (srunx state DB):

``` bash
srunx history -j 12345
```

List all jobs:

``` bash
srunx squeue
```

Filter to a single user (default is all users):

``` bash
srunx squeue --user alice
```

Live in-place refresh (like native `squeue -i`; Ctrl+C to exit):

``` bash
srunx squeue -i 5
```

Default columns are Job ID, User, Name, Status, CPUs, GPUs, Elapsed and
NodeList. Show additional columns (each `--show-*` flag adds one; `-a`
enables all):

``` bash
srunx squeue --show-partition --show-limit --show-nodes
srunx squeue -a
```

List in JSON format (always includes every field, regardless of `--show-*`):

``` bash
srunx squeue --format json
```

Inspect cluster resources:

``` bash
srunx gpus                 # GPU aggregate summary across partitions
srunx sinfo                # partition / state / nodelist (native-sinfo parity)
```

Cluster-side accounting history (real SLURM `sacct`, requires `slurmdbd`):

``` bash
srunx sacct -a -S now-1day                      # all users, last 24h
srunx sacct -j 12345 --show-steps               # include .batch / .extern sub-steps
srunx sacct -s FAILED,TIMEOUT -p gpu            # filter by state / partition
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
srunx flow run --validate pipeline.yaml
```

Run with custom parameters:

``` bash
srunx flow run pipeline.yaml --dry-run
```

## Advanced Features

### Callbacks and Notifications

srunx supports job completion callbacks, including Slack notifications:

``` python
from srunx.observability.notifications.legacy_slack import SlackCallback
from srunx.slurm.local import Slurm

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
from srunx.slurm.local import Slurm
from srunx.domain import Job, JobResource, JobEnvironment, ContainerResource

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

## Choosing a Cluster

A **cluster** is a job target defined in your user config — either the local
`sbatch` or a host reached over SSH:

``` bash
srunx cluster add local --local                 # submit with the local sbatch
srunx cluster add gpu --host gpu-login --default  # alias from ~/.ssh/config
srunx cluster list
srunx cluster show gpu
srunx cluster test gpu
```

Other subcommands: `srunx cluster update`, `srunx cluster remove`,
`srunx cluster sync`, and `srunx cluster secret set|list|unset`.

Every job command (`sbatch`, `squeue`, `scancel`, `sinfo`, `gpus`, `tail`,
`history`, `sacct`, `watch`, `flow run`, `snapshot`) accepts `--cluster <name>`.
Without it, the cluster is chosen in this order:

1.  `--cluster <name>`
2.  `$SRUNX_CLUSTER`
3.  the project's `cluster` (in `.srunx/config.toml`)
4.  `default_cluster` in the user config
5.  otherwise an error — there is no implicit local fallback

A one-line banner on stderr names the cluster and why it was picked; `-Q` /
`--quiet` hides it. There is no `-C` short form (`-C` is sbatch's
`--constraint`).

`srunx scancel <id>` and `srunx tail <id>` without `--cluster` look the job up
in srunx's history: exactly one match selects its cluster (the banner says
`via job history`); no match, or matches on several clusters, is an error asking
for `--cluster`.

`srunx tail` also finds logs of finished jobs, which Slurm no longer reports
through `scontrol` / `squeue`: it asks accounting (`sacct`) for the job's
`StdOut` / `StdErr` where the Slurm version provides them, and otherwise looks
for a file named after the job ID under the job's `WorkDir` (this covers
`--output=logs/%x_%j.log`-style relative paths).

## Configuration

Configuration is TOML, merged from several files:

| File | Contents |
|----|----|
| `/etc/srunx/config.toml` | System config (same schema as the user config) |
| `$XDG_CONFIG_HOME/srunx/config.toml` | User config: `default_cluster`, `[clusters.<name>]`, `[defaults]`, `[notifications]`, `[sync]`, optional `state_dir`, personal per-project overrides `[projects.<name>.clusters.<cluster>]` |
| `<project>/.srunx/config.toml` | Project config (commit it): `name`, `cluster`, `[defaults]`, `[clusters.<cluster>]` (`remote_dir`, `sync`, `exclude`, `[[mounts]]`, `defaults`). No connection settings |

The project config is found by walking up from the current directory, stopping
at a git repository root or `$HOME`; `--project PATH` overrides it. Project
workflows live in `<project>/.srunx/workflows/`.

State (the history DB `srunx.db`, sync locks, the remote-env cache, approved
sync targets) lives in `$XDG_STATE_HOME/srunx` (default
`~/.local/state/srunx`), or in `state_dir` when set in the user config — for
example to keep the DB off an NFS home.

### Default precedence

Job defaults are merged from lowest to highest priority:

1.  built-in defaults
2.  system `[defaults]`
3.  user `[defaults]`
4.  user `[clusters.<c>.defaults]`
5.  project `[defaults]`
6.  project `[clusters.<c>.defaults]`
7.  script `#SBATCH` directives / workflow job fields
8.  CLI flags

Any table may contain `unset = ["key", ...]` to clear an inherited value.

Example user config:

``` toml
default_cluster = "gpu"

[defaults]
log_dir = "logs"

[defaults.resources]
time_limit = "01:00:00"

[clusters.local]
transport = "local"

[clusters.gpu]
transport = "ssh"
host = "gpu-login"          # alias from ~/.ssh/config

[clusters.gpu.defaults.resources]
partition = "gpu"
```

### Managing config from the CLI

``` bash
srunx config init                    # create the user config with a commented example
srunx config init-project            # create .srunx/config.toml in the current directory
srunx config paths                   # files read, lowest precedence first, plus the state dir
srunx config show [--cluster gpu]    # effective values and the file each came from

srunx config set resources.partition gpu                 # user [defaults]
srunx config set resources.partition gpu --cluster gpu   # user [clusters.gpu.defaults]
srunx config set resources.time_limit 04:00:00 --project # project [defaults]
srunx config delete resources.partition                  # inherit again
srunx config unset resources.partition --project         # explicitly clear (adds to unset)
```

`resources.*`, `environment.*`, `log_dir`, and `work_dir` are stored under
`defaults` (or `clusters.<c>.defaults` with `--cluster`); other keys are
top-level (or `clusters.<c>.<key>` with `--cluster`). Values are parsed as TOML;
bare words are strings.

The old `SRUNX_DEFAULT_*` and `SRUNX_SYNC_*` environment variables no longer
exist; `SRUNX_CLUSTER` is the only environment variable that selects anything.

### Container Defaults

Set a default container in a config file:

``` toml
[defaults.environment.container]
runtime = "apptainer"
image = "/shared/containers/pytorch.sif"
```

The runtime resolution order (highest priority first):

1.  Explicit `--container-runtime` CLI flag
2.  `runtime=` key in `--container` value
3.  Config file setting
4.  `pyxis` (default fallback)

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
srunx sbatch -v --wrap "python script.py"
srunx flow run pipeline.yaml --log-level DEBUG
```

Preview job submission (show summary without submitting):

``` bash
srunx sbatch --dry-run --wrap "python script.py"
```

View rendered SLURM scripts:

``` bash
srunx flow run pipeline.yaml --debug
```
