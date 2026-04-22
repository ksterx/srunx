---
hide:
  - navigation
  - toc
---

<div class="srunx-hero" markdown>

<p class="srunx-status">v1.2 · SLURM · Apache-2.0</p>

# Orchestrate SLURM jobs<br>like <span class="accent">code</span>.

<p class="lede">A Python toolkit for submitting, monitoring, and chaining compute jobs on HPC clusters — with a web UI, an MCP server, and YAML workflows that feel like CI.</p>

[Install :material-arrow-right:](tutorials/installation.md){ .md-button .md-button--primary }
[Quickstart](tutorials/quickstart.md){ .md-button }

</div>

<span class="srunx-kicker">01 / Capabilities</span>

<div class="grid cards" markdown>

-   :material-rocket-launch-outline:{ .lg .middle } __Simple submission__

    ---

    One-line SLURM submits with conda, venv, Apptainer, and Pyxis wiring included.

-   :material-chip:{ .lg .middle } __Resource control__

    ---

    Declare nodes, GPUs, memory, and partitions — or let the defaults ride.

-   :material-sitemap-outline:{ .lg .middle } __Workflows as YAML__

    ---

    Typed jobs with `depends_on`, retry, and Jinja-templated args.

-   :material-tune-variant:{ .lg .middle } __Parameter sweeps__

    ---

    Matrix cross-product over hyperparameters. Per-cell tracking, bounded SSH pool, Web UI progress.

    [:octicons-arrow-right-24: Parameter sweeps](how-to/workflows.md#parameter-sweeps)

-   :material-pulse:{ .lg .middle } __Live monitoring__

    ---

    Poll state, fan out Slack deliveries, and snapshot GPU utilization.

-   :material-sync:{ .lg .middle } __rsync project sync__

    ---

    Delta-transfer your repo to any cluster via ProxyJump-aware SSH.

-   :material-monitor-dashboard:{ .lg .middle } __Web UI__

    ---

    Browser dashboard for queue, DAG visualization, run history, and sweep detail pages.

-   :material-robot-outline:{ .lg .middle } __MCP server__

    ---

    Claude Code and other MCP clients drive srunx over stdio — including `run_workflow(sweep=..., mount=...)`.

-   :material-file-code-outline:{ .lg .middle } __Jinja templates__

    ---

    Every sbatch script is a rendered template you can override.

</div>

<span class="srunx-kicker">02 / In action</span>

=== "Submit"

    ```bash
    srunx submit python train.py --gpus-per-node 2 --conda ml_env
    ```

=== "Container"

    ```bash
    srunx submit python train.py \
      --container "runtime=apptainer,image=pytorch.sif,nv=true"
    ```

=== "Workflow"

    ```yaml
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
          time_limit: "8:00:00"
        environment:
          conda: ml_env
    ```

<span class="srunx-kicker">03 / Documentation</span>

<div class="grid cards" markdown>

-   :material-school-outline:{ .lg .middle } __Tutorials__

    ---

    Start here. Install, submit your first job, tour the Web UI, and set up MCP.

    [:octicons-arrow-right-24: Tutorials](tutorials/installation.md)

-   :material-tools:{ .lg .middle } __How-to guides__

    ---

    Task-oriented recipes for workflows, monitoring, sync, and MCP usage.

    [:octicons-arrow-right-24: How-to](how-to/user_guide.md)

-   :material-api:{ .lg .middle } __Reference__

    ---

    Python API, Web UI endpoints, and the full MCP tool surface.

    [:octicons-arrow-right-24: Reference](reference/api.md)

-   :material-book-open-variant:{ .lg .middle } __Explanation__

    ---

    Architectural decisions, design trade-offs, and how the pieces fit together.

    [:octicons-arrow-right-24: Explanation](explanation/architecture.md)

</div>
