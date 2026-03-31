# Spec: Workflow Execution via Web UI

## Overview
Implement the complete remote workflow execution pipeline: sync → submit → monitor. When a user clicks "Run Workflow", the system syncs referenced mounts, submits all jobs to remote SLURM with native dependency flags, and provides real-time execution monitoring.

## Background
The DAG builder can create and save workflows as YAML, but the `POST /{name}/run` endpoint is a stub. No jobs are actually submitted. The infrastructure exists (SSHSlurmClient, render_job_script, RsyncClient, SlurmSSHAdapter) but is not wired together for the web execution path.

## Requirements

### Must Have
- REQ-1: **Run workflow** — `POST /api/workflows/{name}/run` syncs mounts, renders SLURM scripts, submits all jobs via SSH with SLURM-native `--dependency` flags in topological order, and returns a run record with job IDs
- REQ-2: **SLURM dependency chaining** — Submit jobs with `sbatch --dependency=afterok:JOB_ID` (or after/afterany/afternotok per edge type). Multiple dependencies comma-separated. Jobs with no dependencies submitted immediately. Dependency is passed as a CLI flag to `sbatch`, not as a template directive
- REQ-3: **Pre-execution sync** — Before submitting, identify mounts to sync by matching job `work_dir` values against mount remote paths (longest prefix match) or via `default_project` name. Use a shared sync utility (extracted from existing `/api/files/sync` logic) to avoid duplication
- REQ-4: **Execution monitoring** — `GET /api/workflows/runs/{run_id}` returns current job statuses. Backend polls `sacct` via a background anyio task managed in a lifespan task group. The SSH adapter serializes commands via anyio's thread limiter to prevent concurrent channel access
- REQ-5: **Frontend run trigger** — "Run Workflow" button on WorkflowDetail calls the API, shows sync/submission progress, then polls for status updates
- REQ-6: **Frontend status display** — DAGView and list view show live job statuses (PENDING → RUNNING → COMPLETED/FAILED) with colored nodes/badges, updated via polling
- REQ-7: **Default project binding** — Workflow YAML supports an optional `default_project` field (mount name). When set, the DAG builder pre-fills `work_dir` from this mount for new jobs. The run endpoint syncs this mount automatically

### Nice to Have
- REQ-N1: Cancel a running workflow (cancel all submitted jobs via scancel)
- REQ-N2: Retry failed jobs within a run

## Acceptance Criteria
- AC-1: Given a saved workflow with dependencies, when user clicks "Run Workflow", then mounts are synced, jobs are submitted to SLURM with `--dependency` flags, and job IDs are returned
- AC-2: Given a running workflow, when polling the run status endpoint, then job statuses reflect the actual SLURM states (PENDING/RUNNING/COMPLETED/FAILED)
- AC-3: Given a workflow with `default_project: ml-project`, when running, then the `ml-project` mount is synced before submission regardless of individual job work_dirs
- AC-4: Given a workflow where job A depends on job B (afterok), when submitted, then job A's sbatch includes `--dependency=afterok:{B_JOB_ID}`
- AC-5: Given the WorkflowDetail page during execution, when a job transitions PENDING → RUNNING → COMPLETED, then the DAG node color updates accordingly
- AC-6: Given a workflow run, when a submitted job has a job_id, then the "View Logs" link becomes available
- AC-7: Given a sync failure, when running a workflow, then the error is reported and jobs are not submitted
- AC-8: Given a submission failure mid-way through topological order, then the run is marked `failed` with the list of successfully submitted job IDs, and already-submitted SLURM jobs continue running (dependent jobs that were not submitted simply won't exist)

## Out of Scope
- Application-level dependency orchestration (SLURM handles scheduling)
- WebSocket-based real-time updates (polling is sufficient)
- Partial workflow execution (run from/to specific jobs) — future enhancement
- Multi-cluster workflow execution
- Automatic retry of failed runs (manual re-run is sufficient for v1)

## Constraints
- Must use SLURM-native `--dependency` CLI flags (not template directives, not application-level polling)
- Must use existing `SSHSlurmClient.submit_sbatch_job()` (extended with dependency parameter)
- Must bridge `render_job_script()` (returns file path) with `submit_job()` (accepts content string) — render to temp dir, read content, submit
- Must use shared sync utility extracted from existing `/api/files/sync` rsync logic
- Background job status polling must use an anyio task group managed in the FastAPI lifespan
- SSH command concurrency must be serialized (single adapter, anyio thread limiter)
- Template path resolved via `importlib.resources` or package-relative path
