# srunx Web UI — Server Specification

> FastAPI backend bridging the React frontend to the existing srunx Python core layer.

## 1. REST API Endpoints

All endpoints are prefixed with `/api`.

### 1.1 Jobs

| Method | Path | Request | Response | Core Method | Blocking | Notes |
|--------|------|---------|----------|-------------|----------|-------|
| `GET` | `/jobs` | — | `Job[]` | `Slurm.queue()` | subprocess (squeue) | Returns all user jobs. Map `BaseJob` fields to response schema. |
| `GET` | `/jobs/{job_id}` | — | `Job` | `Slurm.retrieve(job_id)` | subprocess (sacct) | Single job status. |
| `POST` | `/jobs` | `JobSubmitRequest` | `Job` | `Slurm.submit(job)` | subprocess (sbatch) | Build `Job`/`ShellJob` from request body, submit. Returns with `job_id` assigned. |
| `DELETE` | `/jobs/{job_id}` | — | `204 No Content` | `Slurm.cancel(job_id)` | subprocess (scancel) | Cancel a running/pending job. |
| `GET` | `/jobs/{job_id}/logs` | — | `{ stdout: str, stderr: str }` | `Slurm.get_job_output(job_id)` | file I/O | Returns log file contents. |

#### Request/Response Schemas

```python
class JobSubmitRequest(BaseModel):
    name: str
    command: list[str] | None = None
    script_path: str | None = None
    script_vars: dict[str, str] | None = None
    resources: JobResourceSchema | None = None
    environment: JobEnvironmentSchema | None = None
    log_dir: str | None = None
    work_dir: str | None = None

class JobResponse(BaseModel):
    name: str
    job_id: int | None
    status: JobStatus
    depends_on: list[str]
    command: list[str] | None = None
    script_path: str | None = None
    resources: JobResourceSchema
    partition: str | None
    nodes: int | None
    gpus: int | None
    elapsed_time: str | None
```

**CAUTION**: `BaseJob.status` property triggers a `sacct` subprocess on every access for non-terminal jobs. Response serialization MUST access `_status` directly or use a pre-fetched value to avoid N+1 subprocess calls.

### 1.2 Workflows

| Method | Path | Request | Response | Core Method | Blocking | Notes |
|--------|------|---------|----------|-------------|----------|-------|
| `GET` | `/workflows` | — | `Workflow[]` | Scan workflow directory | file I/O | List YAML files from configured workflow dir. Parse each with `WorkflowRunner.from_yaml()`. |
| `GET` | `/workflows/{name}` | — | `Workflow` | `WorkflowRunner.from_yaml(path)` | file I/O | Parse specific YAML file. |
| `POST` | `/workflows/validate` | `{ yaml: str }` | `{ valid: bool, errors?: str[] }` | `WorkflowRunner.from_yaml()` + `Workflow.validate()` | file I/O | Write temp file, parse, validate. Return errors if any. |
| `POST` | `/workflows/{name}/run` | `WorkflowRunRequest` | `WorkflowRun` | `WorkflowRunner.run()` | **BLOCKING LOOP** | Must run in background task. See §3.1. |
| `GET` | `/workflows/runs` | `?name=<optional>` | `WorkflowRun[]` | In-memory run registry | — | New state: track active/completed workflow runs. |
| `POST` | `/workflows/upload` | `{ yaml: str, filename: str }` | `Workflow` | File write + parse | file I/O | Save YAML to workflow dir, parse and validate. |

#### Request/Response Schemas

```python
class WorkflowRunRequest(BaseModel):
    from_job: str | None = None
    to_job: str | None = None

class WorkflowRunResponse(BaseModel):
    id: str                          # UUID for this run
    workflow_name: str
    started_at: datetime
    completed_at: datetime | None
    status: Literal["running", "completed", "failed", "cancelled"]
    job_statuses: dict[str, JobStatus]
```

**NEW STATE REQUIRED**: The Python core has no concept of a "workflow run" as a trackable entity. `WorkflowRunner.run()` is fire-and-forget blocking. The backend must:
1. Generate a unique run ID
2. Store run state in memory (or SQLite)
3. Update job statuses as the workflow progresses
4. Expose via `GET /workflows/runs`

### 1.3 Resources

| Method | Path | Request | Response | Core Method | Blocking | Notes |
|--------|------|---------|----------|-------------|----------|-------|
| `GET` | `/resources` | `?partition=<optional>` | `ResourceSnapshot[]` | `ResourceMonitor.get_partition_resources()` | subprocess (sinfo + squeue) | See §3.3 for signal handler caveat. |

**CAUTION**: `ResourceMonitor.__init__()` installs `signal.signal(SIGTERM/SIGINT)` handlers, which must be on the main thread and will overwrite asyncio's handlers. **Do NOT construct `ResourceMonitor` from async context.** Instead, extract the `sinfo`/`squeue` parsing logic into a standalone function or call with `signal_handlers=False` (requires core change).

#### Workaround (no core changes)

Create a thin wrapper that directly calls `sinfo` and `squeue` subprocess commands and parses the output, mirroring `ResourceMonitor.get_partition_resources()` but without constructing the monitor class.

### 1.4 History

| Method | Path | Request | Response | Core Method | Blocking | Notes |
|--------|------|---------|----------|-------------|----------|-------|
| `GET` | `/history` | `?limit=50` | `JobHistoryEntry[]` | `JobHistory.get_recent_jobs(limit)` | SQLite read | Returns `list[dict]`, needs mapping to response schema. |
| `GET` | `/history/stats` | `?from=<date>&to=<date>` | `JobStats` | `JobHistory.get_job_stats(from, to)` | SQLite read | Returns `dict`, needs mapping. |

#### Response Schemas

```python
class JobHistoryEntryResponse(BaseModel):
    job_id: int
    job_name: str
    command: str | None
    status: JobStatus
    submitted_at: str
    completed_at: str | None
    workflow_name: str | None
    partition: str | None
    nodes: int | None
    gpus: int | None

class JobStatsResponse(BaseModel):
    total: int
    completed: int
    failed: int
    cancelled: int
    avg_runtime_seconds: float | None
```

**NOTE**: `JobHistory.get_job_stats()` returns `{ total_jobs, jobs_by_status, avg_duration_seconds, total_gpu_hours }`. The frontend expects `{ total, completed, failed, cancelled, avg_runtime_seconds }`. Backend must map `jobs_by_status` dict to flat fields.

---

## 2. WebSocket Channels

### 2.1 Event Stream

| Path | Direction | Message Format | Purpose |
|------|-----------|----------------|---------|
| `/ws/events` | Server → Client | `WSEvent<T>` | Multiplexed channel for all real-time events |

#### Event Types

```python
class WSEvent(BaseModel):
    type: Literal["job_state_change", "workflow_progress", "resource_update", "log_line"]
    timestamp: str  # ISO 8601
    data: dict

# job_state_change
class JobStateChangeData(BaseModel):
    job_id: int
    job_name: str
    previous_status: JobStatus
    new_status: JobStatus
    workflow_name: str | None

# resource_update
# data = ResourceSnapshot.model_dump()

# workflow_progress
# data = WorkflowRunResponse.model_dump()
```

#### Implementation Strategy

Create a `WebSocketCallback(Callback)` that implements:
- `on_job_submitted` → emit `job_state_change` (UNKNOWN → PENDING)
- `on_job_running` → emit `job_state_change` (PENDING → RUNNING)
- `on_job_completed` → emit `job_state_change` (→ COMPLETED)
- `on_job_failed` → emit `job_state_change` (→ FAILED)
- `on_job_cancelled` → emit `job_state_change` (→ CANCELLED)
- `on_workflow_started` → emit `workflow_progress`
- `on_workflow_completed` → emit `workflow_progress`
- `on_resources_available` / `on_resources_exhausted` → emit `resource_update`

Additionally, run a background polling task for resource updates at configurable intervals (default: 15s).

### 2.2 Log Stream (per-job)

| Path | Direction | Message Format | Purpose |
|------|-----------|----------------|---------|
| `/ws/logs/{job_id}` | Server → Client | `WSEvent<LogLineData>` | Stream stdout/stderr for a specific job |

```python
class LogLineData(BaseModel):
    job_id: int
    stream: Literal["stdout", "stderr"]
    line: str
    timestamp: str
```

#### Implementation Strategy

`Slurm.tail_log(follow=True)` writes to Rich Console — unusable. Instead:
1. Locate log files via `Slurm.get_job_output_detailed(job_id, skip_content=True)` to get file paths
2. Open files and seek to end
3. Poll for new lines (like `tail -f`) in a background task
4. Push each new line as a `WSEvent<LogLineData>` over the WebSocket
5. Stop when job reaches terminal state (check via `Slurm.retrieve()`)

### 2.3 General Log Stream (unused singleton)

| Path | Direction | Notes |
|------|-----------|-------|
| `/ws/logs` | — | Declared in frontend `ws.ts` as singleton but not actively used by any page. Can be deferred. |

---

## 3. Async / Blocking Considerations

### 3.1 Workflow Execution (Critical)

`WorkflowRunner.run()` blocks for the entire workflow duration (minutes to hours). It uses an internal `ThreadPoolExecutor(max_workers=8)`.

**Strategy**: `asyncio.to_thread()` + in-memory run registry

```python
# Pseudocode
async def run_workflow(name: str, options: WorkflowRunRequest):
    run_id = str(uuid4())
    run_registry[run_id] = WorkflowRunResponse(
        id=run_id, workflow_name=name, status="running", ...
    )

    async def execute():
        try:
            runner = WorkflowRunner.from_yaml(path, callbacks=[ws_callback])
            results = await asyncio.to_thread(
                runner.run, from_job=options.from_job, to_job=options.to_job
            )
            run_registry[run_id].status = "completed"
        except Exception:
            run_registry[run_id].status = "failed"

    asyncio.create_task(execute())
    return run_registry[run_id]
```

### 3.2 Job Operations (Moderate)

`Slurm.submit()`, `retrieve()`, `cancel()`, `queue()` each make one subprocess call. These are fast (< 1s typically) but still blocking.

**Strategy**: Wrap all Slurm method calls in `asyncio.to_thread()`:

```python
@router.get("/jobs")
async def list_jobs():
    slurm = Slurm()
    jobs = await asyncio.to_thread(slurm.queue)
    return [serialize_job(j) for j in jobs]
```

### 3.3 Resource Monitoring (Moderate)

`ResourceMonitor.get_partition_resources()` makes 2 subprocess calls but the constructor installs signal handlers.

**Strategy**: Either:
- **(A)** Extract `sinfo`/`squeue` parsing into a standalone function (requires small core refactor)
- **(B)** Construct `ResourceMonitor` at startup on the main thread, cache the instance, call `get_partition_resources()` via `to_thread()`
- **(C)** Reimplement the parsing in the web module (duplicates ~40 lines)

Recommendation: **(B)** for v1, **(A)** as a follow-up refactor.

### 3.4 BaseJob.status Side Effect

`BaseJob.status` triggers `sacct` subprocess on every access for non-terminal jobs.

**Strategy**: Create a `serialize_job()` helper that:
1. Reads `job._status` directly (the private attribute)
2. Maps to the response schema without triggering the property

---

## 4. Security Requirements

### 4.1 YAML `python:` Args (Critical — RCE)

`WorkflowRunner._render_jobs_with_args()` evaluates `python:` prefixed YAML args via `eval()`/`exec()`. If the web UI allows users to upload or edit workflow YAML, this is a **remote code execution vulnerability**.

**Mitigation options (pick one)**:
1. **Disable** `python:` args for web-uploaded workflows (recommended for v1)
2. **Sandbox** via `RestrictedPython` or subprocess isolation
3. **Allowlist** safe functions (e.g., `datetime.now()`, `os.getenv()`)

### 4.2 Authentication & Authorization

For v1 (single-user, local deployment):
- No authentication required
- Bind to `127.0.0.1` by default
- Add `--host` / `--port` CLI flags for explicit network exposure

For v2 (multi-user):
- Token-based auth (JWT or API key)
- User isolation: filter jobs by `$USER`
- Rate limiting on submit/cancel endpoints

### 4.3 CORS

- Development: Allow `http://localhost:3000` (Vite dev server)
- Production: Same-origin (frontend served by FastAPI StaticFiles)

### 4.4 Input Validation

- All path parameters: validate `job_id` is positive integer
- Workflow names: alphanumeric + hyphens + underscores only
- YAML upload: size limit (e.g., 1MB), reject `python:` args
- Command arrays: no shell injection (already handled by `sbatch` file-based submission)

---

## 5. Error Handling

### 5.1 HTTP Error Responses

```python
class ErrorResponse(BaseModel):
    detail: str
    code: str  # machine-readable error code

# Standard mappings:
# - Job not found → 404 {"detail": "Job 12345 not found", "code": "job_not_found"}
# - Workflow not found → 404 {"detail": "...", "code": "workflow_not_found"}
# - Validation error → 422 (FastAPI default)
# - SLURM command failed → 502 {"detail": "sbatch failed: ...", "code": "slurm_error"}
# - Workflow already running → 409 {"detail": "...", "code": "workflow_running"}
```

### 5.2 WebSocket Error Handling

- Send error events: `{"type": "error", "data": {"message": "...", "code": "..."}}`
- Close with appropriate WebSocket close code on fatal errors
- Graceful degradation: frontend already handles reconnection with exponential backoff

### 5.3 SLURM Unavailability

All SLURM operations depend on `sbatch`/`sacct`/`squeue`/`sinfo` being available. If SLURM commands fail:
- Return `502 Bad Gateway` with the subprocess error message
- Cache last known resource state for `/resources` endpoint (serve stale data with warning header)

---

## 6. Application Structure

```
src/srunx/web/
├── __init__.py
├── app.py              # FastAPI app factory, lifespan, static files mount
├── config.py           # Web-specific config (host, port, workflow_dir, cors)
├── deps.py             # Dependency injection (Slurm client, History, etc.)
├── serializers.py      # Job/Workflow → Response schema mapping (avoids status side-effect)
├── state.py            # In-memory workflow run registry
├── routers/
│   ├── __init__.py
│   ├── jobs.py         # /api/jobs/*
│   ├── workflows.py    # /api/workflows/*
│   ├── resources.py    # /api/resources
│   └── history.py      # /api/history/*
├── ws/
│   ├── __init__.py
│   ├── manager.py      # WebSocket connection manager (broadcast to all clients)
│   ├── events.py       # /ws/events endpoint
│   ├── logs.py         # /ws/logs/{job_id} endpoint
│   └── callback.py     # WebSocketCallback(Callback) — bridge core events to WS
└── frontend/           # React build output (served via StaticFiles)
    └── dist/
```

---

## 7. Entry Point & CLI

```python
# src/srunx/web/app.py
def create_app() -> FastAPI:
    app = FastAPI(title="srunx", version="0.1.0")
    # Mount routers, CORS, static files, lifespan
    return app

def main():
    """Entry point for `srunx-web` command."""
    import uvicorn
    uvicorn.run("srunx.web.app:create_app", factory=True, host="127.0.0.1", port=8000)
```

```toml
# pyproject.toml addition
[project.scripts]
srunx-web = "srunx.web.app:main"
```

---

## 8. Dependencies (pyproject.toml)

```toml
[project.optional-dependencies]
web = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.34.0",
    "websockets>=15.0",
]
```

Install: `uv sync --extra web`

**Note**: `websockets` is needed by uvicorn for WebSocket support. FastAPI's WebSocket support is built-in but requires an ASGI server with WS capability.

---

## 9. Frontend Type ↔ Python Model Alignment

### Discrepancies Found

| Frontend Type | Python Model | Discrepancy |
|---------------|-------------|-------------|
| `ResourceSnapshot.gpus_total` | `ResourceSnapshot.total_gpus` | **Field name mismatch** — frontend uses `gpus_total`, Python uses `total_gpus` |
| `ResourceSnapshot.nodes_down` | `ResourceSnapshot.nodes_down` | OK |
| `JobResource.cpus_per_task` | `JobResource.cpus_per_task` | OK — but Python also has `ntasks_per_node` which frontend lacks |
| `ContainerResource.gpu` | `ContainerResource.nv` + `rocm` | **Semantic mismatch** — frontend has single `gpu: boolean`, Python has separate `nv`/`rocm` booleans plus `cleanenv`, `fakeroot`, `writable_tmpfs`, `overlay`, `env` |
| `WorkflowRun` | (does not exist in Python) | **Frontend-only type** — backend must create this |
| `JobHistoryEntry` | `JobHistory.get_recent_jobs()` returns `dict` | **No Pydantic model** — backend must create response model |
| `JobStats` | `JobHistory.get_job_stats()` returns `dict` | **Key name mismatch** — Python: `total_jobs`/`jobs_by_status`/`avg_duration_seconds`, Frontend: `total`/`completed`/`failed`/`cancelled`/`avg_runtime_seconds` |

### Required Backend Actions

1. **ResourceSnapshot serialization**: Alias `total_gpus` → `gpus_total` in response (or use Pydantic `alias`)
2. **ContainerResource**: Simplify to `gpu: bool` in response, mapping `nv or rocm` → `gpu`
3. **WorkflowRun**: New Pydantic model in backend
4. **JobHistoryEntry/JobStats**: New response models with field mapping from dict keys

---

## 10. Summary of New Code Required in Core (Optional Refactors)

These are not mandatory but would significantly simplify the web backend:

| Change | Location | Impact | Priority |
|--------|----------|--------|----------|
| Extract `get_partition_resources()` as standalone function | `monitor/resource_monitor.py` | Avoid signal handler issue | High |
| Add `BaseJob.status_value` property that reads `_status` without refresh | `models.py` | Safe serialization | High |
| Disable `python:` args via parameter flag | `runner.py` | Security | High |
| Add `Workflow.to_dict()` method | `models.py` | Clean serialization | Low |
| Make `JobHistory` return Pydantic models instead of dicts | `history.py` | Type safety | Low |
