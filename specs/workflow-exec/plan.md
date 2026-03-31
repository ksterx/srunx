# Plan: Workflow Execution via Web UI

## Spec Reference
`specs/workflow-exec/spec.md` — Sync → Submit → Monitor pipeline for remote SLURM workflow execution.

## Approach
Submit all jobs upfront using SLURM-native `--dependency` CLI flags in topological order. This delegates scheduling to SLURM. A background anyio task (managed in the FastAPI lifespan task group) polls job statuses and updates the RunRegistry.

### Trade-offs Considered
| Option | Pros | Cons |
|--------|------|------|
| **SLURM-native --dependency** (chosen) | Simple, reliable, SLURM handles scheduling | Must submit in topological order |
| Application-level orchestration | More control, complex retry | Persistent polling loop, complex error handling |

## Architecture

### Components
| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| `run_workflow` endpoint | `web/routers/workflows.py` | Orchestrates sync → submit → start monitor |
| `_submit_workflow_to_slurm` | `web/routers/workflows.py` | Topological submit with --dependency CLI flags |
| `sync_mount_by_name` | `web/sync_utils.py` (NEW) | Shared sync utility (extracted from files.py) |
| `submit_job` (extended) | `web/ssh_adapter.py` | Accepts `dependency` param |
| `submit_sbatch_job` (extended) | `ssh/core/client.py` | Appends `--dependency=...` to sbatch command |
| `WorkflowRun` (extended) | `web/state.py` | Add `job_ids`, update/complete methods |
| `_monitor_run` | `web/routers/workflows.py` | Background anyio task polling sacct |
| `GET /runs/{run_id}` | `web/routers/workflows.py` | Return run with live statuses |
| Frontend run trigger | `pages/WorkflowDetail.tsx` | onClick → API → progress |
| Frontend status polling | `pages/WorkflowDetail.tsx` | Poll run status, update DAG |
| `workflows.run()` | `lib/api.ts` | API client method |
| `WorkflowRun` type | `lib/types.ts` | Frontend type |

### Updated Data Models

**WorkflowRun (state.py):**
```python
class WorkflowRun(BaseModel):
    id: str
    workflow_name: str
    started_at: str
    completed_at: str | None = None
    status: Literal["syncing", "submitting", "running", "completed", "failed", "cancelled"]
    job_ids: dict[str, str] = {}       # job_name -> SLURM job ID
    job_statuses: dict[str, str] = {}  # job_name -> status string
    error: str | None = None           # error message if failed
```

**RunRegistry new methods:**
```python
def update_job_status(self, run_id: str, job_name: str, status: str) -> None
def set_job_ids(self, run_id: str, job_ids: dict[str, str]) -> None
def complete_run(self, run_id: str, status: str = "completed") -> None
def fail_run(self, run_id: str, error: str) -> None
```

### Data Flow
```
User clicks "Run Workflow"
    ↓
POST /api/workflows/{name}/run
    ↓
1. Load workflow from YAML (WorkflowRunner.from_yaml)
    ↓
2. Create WorkflowRun with status="syncing"
    ↓
3. Identify mounts to sync:
   - For each job: match work_dir against mount.remote (longest prefix)
   - If default_project set: include that mount
   - Deduplicate
    ↓
4. Sync each mount via sync_mount_by_name() (shared utility)
   - On failure: fail_run(error), return 502
    ↓
5. Render SLURM scripts:
   - For each job: render_job_script(template_path, job, output_dir=tmpdir)
   - Read rendered file content
   - Template path: resolved via importlib.resources or Path(__file__).parent
    ↓
6. Update run status="submitting"
    ↓
7. Topological submit (BFS from roots):
   For each job in BFS order:
     a. Build dependency flag from submitted parents:
        dep_parts = [f"{dep.dep_type}:{submitted[dep.job_name]}"
                     for dep in job.parsed_dependencies]
        dependency = ",".join(dep_parts) or None
     b. Submit: adapter.submit_job(script_content, job.name, dependency=dependency)
        - adapter wraps SSHSlurmClient.submit_sbatch_job(content, name, dependency)
        - SSHSlurmClient appends --dependency={dep} to sbatch command if set
        - Returns dict with job_id or raises RuntimeError on None result
     c. Record: submitted[job.name] = result["job_id"]
     d. On failure: fail_run with partial job_ids, break
    ↓
8. set_job_ids(run_id, submitted), update status="running"
    ↓
9. Start background monitor task (in lifespan task group)
    ↓
10. Return run record (202 Accepted)

Background monitor (anyio task):
    while True:
        all_terminal = True
        for job_name, job_id in job_ids.items():
            status = await anyio.to_thread.run_sync(
                lambda: adapter.get_job_status(job_id)
            )
            registry.update_job_status(run_id, job_name, status)
            if status not in TERMINAL_STATUSES:
                all_terminal = False
        if all_terminal:
            registry.complete_run(run_id)
            break
        await anyio.sleep(10)
```

### Shared Sync Utility (extracted from files.py)
```python
# web/sync_utils.py
def build_rsync_client(profile: ServerProfile) -> RsyncClient:
    """Create RsyncClient from profile, handling ssh_host vs hostname."""
    if profile.ssh_host:
        return RsyncClient(hostname=profile.ssh_host, username="",
                           ssh_config_path=str(Path.home() / ".ssh" / "config"))
    return RsyncClient(hostname=profile.hostname, username=profile.username,
                       key_filename=profile.key_filename, port=profile.port,
                       proxy_jump=profile.proxy_jump)

def sync_mount_by_name(profile: ServerProfile, mount_name: str) -> None:
    """Sync a named mount. Raises on failure."""
    mount = next((m for m in profile.mounts if m.name == mount_name), None)
    if not mount: raise ValueError(f"Mount '{mount_name}' not found")
    rsync = build_rsync_client(profile)
    result = rsync.push(mount.local, mount.remote)
    if result.returncode != 0: raise RuntimeError(f"rsync failed: {result.stderr}")
```

### SSH Concurrency
The SlurmSSHAdapter is a singleton. All SSH commands go through `anyio.to_thread.run_sync()` which by default uses a shared thread limiter (40 threads). Since Paramiko's transport is multiplexed, concurrent commands are safe at the transport level. However, to be explicit: the background monitor and interactive API requests can run concurrently without issues because each `exec_command` creates its own channel.

### Monitor Task Lifecycle
```python
# In app.py lifespan:
async with anyio.create_task_group() as tg:
    app.state.task_group = tg
    yield
    tg.cancel_scope.cancel()  # Cancel all monitors on shutdown

# When starting a monitor:
app.state.task_group.start_soon(_monitor_run, run_id, job_ids, adapter)
```

## Integration Points
- **SSHSlurmClient.submit_sbatch_job**: Add `dependency: str | None = None` → append `--dependency={dep}` to sbatch command
- **SlurmSSHAdapter.submit_job**: Pass through `dependency` param
- **files.py POST /sync**: Refactor to use `sync_utils.build_rsync_client` + `sync_mount_by_name`
- **_serialize_workflow**: Include `default_project` in response
- **WorkflowBuilder toolbar**: Add default project dropdown
- **WorkflowDetail.tsx**: Wire Run button, add run polling

## Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| SSH drop during submission | High | Mark run as failed with partial job_ids; submitted jobs continue in SLURM |
| rsync sync slow for large projects | Med | Status "syncing" shown in UI; sync per mount, not per job |
| Paramiko concurrent access | Low | Each SSH command creates own channel; transport is multiplexed |
| Monitor task leak | Med | Task group in lifespan; cancel on shutdown |
| render_job_script returns path not content | Med | Render to tempdir, read content, submit |

## Testing Strategy
- Backend: Test topological sort, dependency flag construction, partial failure handling
- Integration: Mock SSHSlurmClient to test full submit flow
- E2E: Playwright test for Run button + status polling (mocked API)
