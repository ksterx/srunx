# Tasks: Workflow Execution via Web UI

## Prerequisites
- [x] Spec approved: `specs/workflow-exec/spec.md`
- [x] Plan approved: `specs/workflow-exec/plan.md`

## Phase 1: Backend Infrastructure
- [ ] T1.1: Extend SSHSlurmClient.submit_sbatch_job with `dependency` param — append `--dependency={dep}` to sbatch command (REQ-2)
      Files: `src/srunx/ssh/core/client.py`
- [ ] T1.2: Extend SlurmSSHAdapter.submit_job to pass through `dependency` param (REQ-2)
      Files: `src/srunx/web/ssh_adapter.py`
- [ ] T1.3: Extract shared sync utility from files.py into `web/sync_utils.py` — `build_rsync_client()`, `sync_mount_by_name()`. Refactor files.py POST /sync to use it (REQ-3)
      Files: `src/srunx/web/sync_utils.py` (NEW), `src/srunx/web/routers/files.py`
- [ ] T1.4: Extend WorkflowRun model — add `job_ids`, `error`, new statuses. Add RunRegistry methods: `update_job_status`, `set_job_ids`, `complete_run`, `fail_run` (REQ-4)
      Files: `src/srunx/web/state.py`

## Phase 2: Workflow Execution Endpoint
- [ ] T2.1: Implement `POST /api/workflows/{name}/run` — load workflow, identify mounts, sync, render scripts (tempdir→read content), topological submit with --dependency flags, create run record, handle partial failures (REQ-1, REQ-3, AC-1, AC-4, AC-7, AC-8)
      Files: `src/srunx/web/routers/workflows.py`
      Depends: T1.1, T1.2, T1.3, T1.4
- [ ] T2.2: Implement background status monitor — anyio task in lifespan task group, polls sacct every 10s, updates RunRegistry, auto-completes when all terminal (REQ-4, AC-2)
      Files: `src/srunx/web/routers/workflows.py`, `src/srunx/web/app.py`
      Depends: T1.4
- [ ] T2.3: Implement `GET /api/workflows/runs/{run_id}` — return WorkflowRun with live statuses (REQ-4)
      Files: `src/srunx/web/routers/workflows.py`
      Depends: T1.4

## Phase 3: Default Project
- [ ] T3.1: Support `default_project` in workflow YAML — parse in from_yaml, include in _serialize_workflow response, auto-sync on run (REQ-7, AC-3)
      Files: `src/srunx/web/routers/workflows.py`, `src/srunx/runner.py` (or YAML parsing)

## Phase 4: Frontend
- [ ] T4.1: Add frontend types and API — WorkflowRun type, `workflows.run(name)`, `workflows.getRun(runId)` (REQ-5)
      Files: `frontend/src/lib/types.ts`, `frontend/src/lib/api.ts`
- [ ] T4.2: Wire Run button on WorkflowDetail — onClick handler, sync/submit progress display, error handling (REQ-5, AC-1)
      Files: `frontend/src/pages/WorkflowDetail.tsx`
      Depends: T4.1
- [ ] T4.3: Add run status polling to WorkflowDetail — poll GET /runs/{runId}, update DAG node colors and list view with live statuses, show View Logs links when job_id available (REQ-6, AC-5, AC-6)
      Files: `frontend/src/pages/WorkflowDetail.tsx`, `frontend/src/components/DAGView.tsx`
      Depends: T4.2
- [ ] T4.4: Add default project dropdown to WorkflowBuilder toolbar — loads mounts, pre-fills work_dir for new jobs (REQ-7)
      Files: `frontend/src/pages/WorkflowBuilder.tsx`, `frontend/src/hooks/use-workflow-builder.ts`

## Phase 5: Tests
- [ ] T5.1: Backend tests — topological sort, dependency flag construction, partial failure, run status updates, sync utility (AC-1 through AC-8)
      Files: `tests/web/test_routers.py`
- [ ] T5.2: Playwright E2E — Run button click, polling, status update (mocked API)
      Files: `frontend/e2e/workflow-run.spec.ts`

## Verification Checklist
### Acceptance Criteria
- [ ] AC-1: Run syncs mounts, submits with --dependency, returns job IDs
- [ ] AC-2: Poll returns live SLURM statuses
- [ ] AC-3: default_project mount synced automatically
- [ ] AC-4: Dependent job's sbatch has --dependency=afterok:{PARENT_ID}
- [ ] AC-5: DAG node colors update during execution
- [ ] AC-6: View Logs link available when job_id set
- [ ] AC-7: Sync failure prevents submission
- [ ] AC-8: Partial submission failure marks run failed with submitted job IDs

### Quality Gates
- [ ] All tests pass
- [ ] Lint/Type-check pass
- [ ] No security vulnerabilities
