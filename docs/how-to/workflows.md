---
description: Define and execute multi-step SLURM pipelines with YAML workflows in srunx — dependencies, parameter sweeps, retries, and partial execution.
---

# Workflows

srunx provides a powerful workflow system for orchestrating complex multi-step computational pipelines on SLURM clusters.

## Overview

Workflows in srunx are defined using YAML files that specify:

- **Jobs**: Individual computational steps
- **Dependencies**: Execution order and prerequisites
- **Resources**: Computational requirements for each task
- **Environments**: Software environments for execution

## Workflow Definition

### Basic Structure

``` yaml
name: workflow_name
description: "Optional workflow description"

jobs:
  - name: task1
    command: ["python", "script1.py"]
    # ... task configuration

  - name: task2
    command: ["python", "script2.py"]
    depends_on: [task1]
    # ... task configuration
```

### Task Configuration

Each task supports the following configuration options:

**Command and Environment**

``` yaml
- name: my_task
  command: ["python", "train.py", "--epochs", "100"]
  environment:
    conda: ml_environment
    # OR
    # venv: /path/to/virtualenv

# Container (can be combined with conda/venv)
- name: containerized_task
  command: ["python", "train.py"]
  environment:
    conda: ml_env
    container:
      runtime: apptainer        # or pyxis, singularity
      image: /path/to/image.sif
      nv: true                  # NVIDIA GPU passthrough
      mounts:
        - /data:/data
        - /models:/models

# Simple Pyxis container (default runtime)
- name: pyxis_task
  command: ["python", "inference.py"]
  environment:
    container:
      image: nvcr.io/nvidia/pytorch:24.01-py3
      mounts:
        - /data:/workspace/data
```

**Resource Allocation**

``` yaml
- name: gpu_task
  command: ["python", "gpu_training.py"]
  resources:
    nodes: 2
    ntasks_per_node: 1
    cpus_per_task: 8
    gpus_per_node: 2
    memory_per_node: "64GB"
    time_limit: "12:00:00"
```

**Dependencies**

``` yaml
- name: dependent_task
  command: ["python", "process.py"]
  depends_on: [preprocess, download]
```

## Dependencies

### Linear Dependencies

Simple sequential execution:

``` yaml
name: linear_pipeline
jobs:
  - name: step1
    command: ["python", "step1.py"]

  - name: step2
    command: ["python", "step2.py"]
    depends_on: [step1]

  - name: step3
    command: ["python", "step3.py"]
    depends_on: [step2]
```

### Parallel Dependencies

Multiple jobsdepending on the same prerequisite:

``` yaml
name: parallel_pipeline
jobs:
  - name: preprocess
    command: ["python", "preprocess.py"]

  - name: train_model_a
    command: ["python", "train_a.py"]
    depends_on: [preprocess]

  - name: train_model_b
    command: ["python", "train_b.py"]
    depends_on: [preprocess]

  - name: ensemble
    command: ["python", "ensemble.py"]
    depends_on: [train_model_a, train_model_b]
```

### Complex Dependencies

Advanced dependency patterns:

``` yaml
name: complex_pipeline
jobs:
  - name: data_download
    command: ["python", "download.py"]

  - name: data_validation
    command: ["python", "validate.py"]
    depends_on: [data_download]

  - name: feature_engineering
    command: ["python", "features.py"]
    depends_on: [data_validation]

  - name: model_training
    command: ["python", "train.py"]
    depends_on: [feature_engineering]

  - name: model_evaluation
    command: ["python", "evaluate.py"]
    depends_on: [model_training]

  - name: report_generation
    command: ["python", "report.py"]
    depends_on: [model_evaluation, data_validation]
```

## Workflow Examples

### Machine Learning Pipeline

``` yaml
name: ml_pipeline

jobs:
  - name: data_preprocessing
    command: ["python", "preprocess.py", "--input", "raw_data/"]
    resources:
      nodes: 1
      cpus_per_task: 4
      memory_per_node: "16GB"
      time_limit: "2:00:00"

  - name: feature_selection
    command: ["python", "feature_selection.py"]
    depends_on: [data_preprocessing]
    resources:
      nodes: 1
      cpus_per_task: 8
      memory_per_node: "32GB"

  - name: hyperparameter_tuning
    command: ["python", "hyperopt.py", "--trials", "100"]
    depends_on: [feature_selection]
    resources:
      nodes: 4
      gpus_per_node: 1
      time_limit: "8:00:00"
    environment:
      conda: pytorch_env

  - name: final_training
    command: ["python", "train_final.py"]
    depends_on: [hyperparameter_tuning]
    resources:
      nodes: 2
      gpus_per_node: 2
      time_limit: "12:00:00"
    environment:
      conda: pytorch_env

  - name: model_validation
    command: ["python", "validate.py"]
    depends_on: [final_training]
    resources:
      nodes: 1
      gpus_per_node: 1
    environment:
      conda: pytorch_env

  - name: deployment_prep
    command: ["python", "prepare_deployment.py"]
    depends_on: [model_validation]
    resources:
      nodes: 1
```

### Bioinformatics Pipeline

``` yaml
name: genomics_pipeline

jobs:
  - name: quality_control
    command: ["fastqc", "*.fastq.gz"]
    resources:
      nodes: 1
      cpus_per_task: 16

  - name: trimming
    command: ["trim_galore", "--paired", "sample_R1.fastq.gz", "sample_R2.fastq.gz"]
    depends_on: [quality_control]
    resources:
      nodes: 1
      cpus_per_task: 8

  - name: alignment
    command: ["STAR", "--runThreadN", "32", "--genomeDir", "genome_index"]
    depends_on: [trimming]
    resources:
      nodes: 1
      cpus_per_task: 32
      memory_per_node: "64GB"
      time_limit: "4:00:00"

  - name: quantification
    command: ["featureCounts", "-T", "16", "-a", "annotation.gtf"]
    depends_on: [alignment]
    resources:
      nodes: 1
      cpus_per_task: 16

  - name: differential_expression
    command: ["Rscript", "deseq2_analysis.R"]
    depends_on: [quantification]
    resources:
      nodes: 1
      cpus_per_task: 4
    environment:
      conda: r_env
```

## Workflow Execution

### Running Workflows

Execute a workflow:

``` bash
srunx flow run pipeline.yaml
```

Validate workflow before execution:

``` bash
srunx flow run --validate pipeline.yaml
```

Dry run (show what would be executed):

``` bash
srunx flow run pipeline.yaml --dry-run
```

### Monitoring Workflows

srunx provides built-in workflow monitoring:

- **Progress tracking**: See which jobs are running/completed
- **Dependency resolution**: Automatic job scheduling based on dependencies
- **Error handling**: Failed jobs don't block independent jobs
- **Logging**: Comprehensive logging of workflow execution

## Workflow Management

### Error Handling

When a job fails:

1.  **Dependent jobs are blocked**: Jobs depending on failed job won't run
2.  **Independent jobs continue**: Other jobs in the workflow continue
3.  **Detailed logging**: Error information is captured and logged
4.  **Manual intervention**: You can fix issues and restart failed jobs

### Restart and Recovery

srunx supports partial workflow execution:

``` bash
# Start execution from a specific job (skips dependencies before it)
srunx flow run pipeline.yaml --from job_name

# Stop execution at a specific job (inclusive)
srunx flow run pipeline.yaml --to job_name

# Execute only a single job (ignoring all dependencies)
srunx flow run pipeline.yaml --job job_name
```

## Best Practices

### Workflow Design

1.  **Modular jobs**: Keep jobs focused and independent when possible
2.  **Resource optimization**: Right-size resources for each job
3.  **Checkpointing**: Save intermediate results for recovery
4.  **Testing**: Test individual jobs before full workflow execution

### Dependency Management

1.  **Minimize dependencies**: Reduce blocking relationships
2.  **Parallel execution**: Design for maximum parallelism
3.  **Data dependencies**: Ensure data flow matches job dependencies
4.  **Avoid cycles**: srunx will detect and reject circular dependencies

### Resource Planning

1.  **Job profiling**: Understand resource needs for each job
2.  **Queue management**: Consider cluster queue policies
3.  **Time limits**: Set appropriate time limits for each job
4.  **Resource sharing**: Balance resource allocation across jobs

## Advanced Features

### Template Variables with `args`

Workflows support Jinja2 template variables via the `args` section. Variables defined in `args` are substituted into job fields before parsing:

``` yaml
name: parameterized_workflow
args:
  dataset: "experiment_1"
  epochs: 100
  model_type: resnet50

jobs:
  - name: training
    command: ["python", "train.py", "--dataset", "{{dataset}}", "--epochs", "{{epochs}}", "--model", "{{model_type}}"]

  - name: evaluation
    command: ["python", "evaluate.py", "--dataset", "{{dataset}}"]
    depends_on: [training]
```

### Shell Jobs

Instead of specifying a command, you can point to an existing shell script:

``` yaml
jobs:
  - name: run_script
    script_path: scripts/train.sh
    script_vars:
      EPOCHS: 100
      LR: 0.001
```

## Parameter Sweeps

Run the same workflow over a cross-product of hyperparameters without
copying YAML files. srunx expands the matrix into N independent
``workflow_runs`` (cells), so a cell failure does not abort peers unless
``fail_fast: true``.

### YAML declaration

Declare the matrix in a `sweep` block at the workflow root. Matrix keys
reference values via ordinary `{{ arg }}` substitution in job fields.

``` yaml
name: train
args:
  lr: 0.01
  seed: 1

sweep:
  matrix:
    lr: [0.001, 0.01, 0.1]
    seed: [1, 2, 3]
  fail_fast: false
  max_parallel: 4

jobs:
  - name: train
    command: ["python", "train.py", "--lr", "{{ lr }}", "--seed", "{{ seed }}"]
    gpus_per_node: 1
```

The example above produces a 3 x 3 cross-product (9 cells). Each cell
runs with its own `lr` / `seed` pair and is tracked as a separate
`workflow_run` under one parent `sweep_run`.

### CLI usage

Run a YAML-declared sweep:

``` bash
srunx flow run train.yaml
```

Override a single arg without triggering a sweep:

``` bash
srunx flow run --arg dataset=imagenet train.yaml
```

Launch an ad-hoc sweep from the CLI (takes precedence over, or augments,
any YAML `sweep` block):

``` bash
srunx flow run --sweep lr=0.001,0.01,0.1 --max-parallel 2 train.yaml
```

### Constraints

- ``max_parallel`` is required (YAML or ``--max-parallel``; Web UI defaults to 4).
- Matrix values must be scalar (str/int/float/bool) -- nested structures are rejected at validation.
- Total cell count is capped at 1000 (safety valve; SLURM ``MaxSubmitJobs`` is typically ~4096).
- ``fail_fast`` defaults to ``false`` -- one cell failing does not cancel peers.

### Ad-hoc overrides (``--arg`` / ``--sweep``)

- ``--arg key=value`` overrides a single workflow arg for every cell (no sweep triggered on its own).
- ``--sweep key=v1,v2,v3`` adds or replaces a matrix axis. Repeat the flag to sweep multiple keys.
- CLI sweep axes merge with the YAML `sweep.matrix`: CLI wins on conflicting keys.
- ``--max-parallel N`` is required when a sweep is active via CLI.

### Dry-run preview

Preview the expanded matrix without submitting any jobs:

``` bash
srunx flow run --sweep lr=0.001,0.01 --max-parallel 2 --dry-run train.yaml
```

The dry-run prints the resolved args for each cell so you can sanity-check
the cross-product before launching.

### Execution paths

CLI and MCP sweeps (without a `mount=` argument) run cells through the
local `Slurm` singleton. Web UI sweeps, and MCP sweeps that specify
`mount=<profile>`, route every cell through a per-sweep
`SlurmSSHExecutorPool` (capped at `min(max_parallel, 8)` pooled SSH
connections) which is closed when the background sweep task exits.

See also: [MCP sweep recipes](mcp-usage.md#parameter-sweeps) and
[Web UI sweep recipes](webui.md#run-a-parameter-sweep).
