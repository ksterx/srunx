# Implementation Plan

## Task Overview

実装は以下のレイヤ順で積み上げる:

**(A) DB migration + models** → **(B) Sweep ドメイン (expand / orchestrator / state service / reconciler / aggregator)** → **(C) Runner 改修 (workflow_run_id 注入 + state service 統合)** → **(D) Active Watch Poller 改修 (state service 経由へ移行)** → **(E) Notifications 拡張** → **(F) CLI** → **(G) Web API** → **(H) MCP** → **(I) Web UI** → **(J) ドキュメント + E2E smoke**

下位から上位への一方向依存。既存の非 sweep ワークフロー実行パスは (C)(D) の state service 統合で自動的に after hooks を得るだけで挙動不変。

## PR 分割戦略

合計 ~55 タスクを 2 PR に分割:

| PR | 範囲 | 含むタスク |
|---|---|---|
| **PR 1: Backend foundation** | DB マイグレーション、sweep ドメイン、runner/poller 改修、通知拡張、CLI、Web API、MCP | A, B, C, D, E, F, G, H |
| **PR 2: Web UI + docs + E2E** | React フロント、ドキュメント、E2E smoke、final regression | I, J |

各 PR 終端で `uv run pytest && uv run mypy . && uv run ruff check .` が通ること。

## Steering Document Compliance

`CLAUDE.md` の配置規約 (`src/srunx/sweep/`, `src/srunx/db/repositories/`)、Python 3.12+ 型ヒント、Pydantic v2、`anyio`、`uv`、ruff/mypy/pytest に準拠。

## Atomic Task Requirements

- 各タスクは **1〜3 ファイル** を touch
- **15〜30 分** で完了可能
- **single purpose / single testable outcome**
- 既存コードへの **_Leverage_** と **_Requirements_** を必ず参照

## Cross-section dependencies (important)

- **C (runner changes) must land before B.19 (orchestrator run loop)** — orchestrator invokes `WorkflowRunner.run(workflow_run_id=...)` which is added in task 23. Implementer MUST complete tasks 23-26 before starting task 19 even though B section precedes C in the document.
- **Task 14 (`WorkflowRunStateService`) must land before task 24 (runner refactor) and task 27 (poller refactor)** — both rely on the service as the centralized status update entry point.
- **Task 2a (`Migration.requires_fk_off`) must land before task 2b (V3 registration)**.

## Tasks

### A. DB migration + models (PR 1)

- [ ] 1. Define `SCHEMA_V3` SQL constant in `src/srunx/db/migrations.py`
  - File: `src/srunx/db/migrations.py` (modify)
  - Add `SCHEMA_V3` multi-statement string per design.md section "SCHEMA_V3":
    - `sweep_runs` CREATE TABLE (full DDL)
    - `workflow_runs` ADD COLUMN sweep_run_id + index
    - `events_v3` + `watches_v3` table rebuild preserving existing CHECK allowlist values (`events`: `'job.submitted', 'job.status_changed', 'workflow_run.status_changed', 'resource.threshold_crossed', 'scheduled_report.due'` + new `'sweep_run.status_changed'`; `watches`: `'job','workflow_run','resource_threshold','scheduled_report'` + new `'sweep_run'`)
    - watches table: **no** endpoint_id/preset columns; only `kind, target_ref, filter, created_at, closed_at` (per SCHEMA_V1 L130-137)
    - events table: column name is `observed_at` (not `created_at`)
  - **Critical**: copy V1 events/watches column definitions verbatim from `migrations.py:152-168` — only CHECK constraint values change
  - Purpose: Define schema evolution DDL
  - _Leverage: existing `SCHEMA_V1`, `SCHEMA_V2_DASHBOARD_INDEXES`_
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 2a. Extend `Migration` dataclass + `apply_migrations` to support FK-off migrations
  - File: `src/srunx/db/migrations.py` (modify)
  - Add `requires_fk_off: bool = False` field to `Migration` dataclass (default preserves existing semantics)
  - In `apply_migrations()`: for migrations with `requires_fk_off=True`, execute outside the BEGIN IMMEDIATE wrap and toggle `PRAGMA foreign_keys=OFF/ON` around it (table rebuild requires this; current `executescript` runs in autocommit mode but doesn't manage FK pragma)
  - Attach `requires_fk_off=False` to existing V1 and V2 migrations explicitly (no behavior change)
  - Purpose: Enable safe CHECK-constraint-widening migrations via table rebuild
  - _Leverage: existing `apply_migrations()` at migrations.py L272_
  - _Requirements: 5.3, 5.6_

- [ ] 2b. Register V3 migration with `requires_fk_off=True`
  - File: `src/srunx/db/migrations.py` (modify)
  - Append `Migration(version=3, name="v3_sweep_runs", sql=SCHEMA_V3, requires_fk_off=True)` to `MIGRATIONS`
  - Purpose: Activate V3 schema
  - _Requirements: 5.6_

- [ ] 2c. Test `apply_migrations` with existing V1/V2 + new V3
  - File: `tests/db/test_migration_v3_apply.py` (new)
  - Tests:
    - V1 existing + V2 applied → apply V3 → all tables + indexes exist
    - Apply V3 twice → idempotent (no double-apply error)
    - V1 behavior unchanged when V3 not yet applied
    - `PRAGMA foreign_keys` is `1` after migration completes (restored)
  - _Requirements: 5.6_

- [ ] 3. Add `SweepRun` Pydantic row model in `src/srunx/db/models.py`
  - File: `src/srunx/db/models.py` (modify)
  - Add `SweepStatus = Literal["pending","running","draining","completed","failed","cancelled"]` and `SweepSubmissionSource = Literal["cli","web","mcp"]`
  - Add `class SweepRun(BaseModel)` with all columns per design.md `SweepRun` definition
  - Extend `EventKind` Literal to include `"sweep_run.status_changed"`
  - Add `WatchKind` Literal with `"sweep_run"`
  - Purpose: Application-level type model for sweep_runs rows
  - _Leverage: `WorkflowRun` model pattern in same file_
  - _Requirements: 5.1_

- [ ] 4. Add `sweep_run_id` to `WorkflowRun` Pydantic model + `_COLUMNS`
  - File: `src/srunx/db/models.py` (modify) + `src/srunx/db/repositories/workflow_runs.py` (modify)
  - Add `sweep_run_id: int | None = None` field to `WorkflowRun`
  - Add `"sweep_run_id"` to `WorkflowRunRepository._COLUMNS` and ensure `_row_to_model` picks it up
  - Purpose: Expose FK column to application code (poller uses it for sweep aggregation)
  - _Leverage: existing `WorkflowRun` + repository_
  - _Requirements: 5.2_

- [ ] 5. Create `SweepRunRepository` in `src/srunx/db/repositories/sweep_runs.py`
  - File: `src/srunx/db/repositories/sweep_runs.py` (new)
  - Subclass `BaseRepository`, set `JSON_FIELDS=("matrix","args")`, `DATETIME_FIELDS=("started_at","completed_at","cancel_requested_at")`, `_COLUMNS=(...)` per SCHEMA_V3
  - Methods: `create(...)`, `get(id)`, `list_all(limit=200)`, `list_incomplete()` (status IN pending/running/draining), `update_status(id, status, *, error=None, completed_at=None)`, `request_cancel(id)`
  - Purpose: CRUD for sweep_runs
  - _Leverage: `WorkflowRunRepository` as template_
  - _Requirements: 5.4_

- [ ] 6. Implement `SweepRunRepository.transition_cell` atomic method
  - File: `src/srunx/db/repositories/sweep_runs.py` (modify)
  - Signature: `transition_cell(*, conn: sqlite3.Connection, workflow_run_id: int, from_status: str, to_status: str, error: str | None = None, completed_at: str | None = None) -> bool`
  - **`conn` is required** — caller is responsible for the enclosing BEGIN IMMEDIATE TX. This method never opens its own TX; it runs under the caller's.
  - Atomic steps inside caller's TX:
    1. `UPDATE workflow_runs SET status=?, completed_at=?, error=? WHERE id=? AND status=?` via `conn.execute`; capture `cursor.rowcount`
    2. If rowcount==0 → return False (another actor won the race; caller should not continue with follow-on sweep updates)
    3. SELECT sweep_run_id from workflow_runs for this id; if NULL → return True (no sweep counter update needed)
    4. `UPDATE sweep_runs SET cells_<from>=cells_<from>-1, cells_<to>=cells_<to>+1 WHERE id=?` (both columns in single UPDATE)
    5. Return True
  - Purpose: Optimistic-locked atomic cell transition + sweep counter sync
  - _Leverage: `BaseRepository` connection handling_
  - _Requirements: 5.5, R4.3_

- [ ] 7. Unit tests for `SweepRunRepository`
  - File: `tests/db/test_sweep_run_repository.py` (new)
  - Tests:
    - `create + get` round trip
    - `transition_cell` idempotency: 2nd call with same from returns False
    - `transition_cell` counter correctness after running → completed
    - `transition_cell` with no parent sweep_run_id returns True but no sweep update
    - `list_incomplete` filters correctly
    - `request_cancel` sets cancel_requested_at and leaves status unchanged
  - _Requirements: 5.4, 5.5_

- [ ] 8. Migration integration test: V1/V2 → V3 idempotency + constraint preservation
  - File: `tests/db/test_migration_v3.py` (new)
  - Tests:
    - Starting from fresh V1 DB, apply V3 → tables exist, FK/UNIQUE/INDEX via `PRAGMA index_list` / `PRAGMA foreign_key_list` match expected
    - Apply V3 twice → no error (idempotent)
    - V1 `events.kind='resource_threshold'` row survives V3 migration
    - V1 `watches.kind='workflow_run'` row survives V3 migration
  - _Requirements: 5.3, 5.6_

### B. Sweep domain (PR 1)

- [ ] 9. Create `src/srunx/sweep/__init__.py` + `SweepSpec` / `CellSpec` models
  - File: `src/srunx/sweep/__init__.py` (new)
  - Add Pydantic models `SweepSpec` (matrix, fail_fast, max_parallel), `CellSpec` (workflow_run_id, effective_args, cell_index)
  - Purpose: Shared domain types
  - _Requirements: R2, R4_

- [ ] 10. Implement `expand_matrix` pure function
  - File: `src/srunx/sweep/expand.py` (new)
  - `expand_matrix(matrix: dict[str, list[Any]], base_args: dict) -> list[dict]` — cross product via `itertools.product`
  - Validation: raise `WorkflowValidationError` on empty matrix, empty axis values, non-scalar values (not in `str|int|float|bool`), axis named `deps`, cell_count > 1000
  - Preserve insertion order (dict + itertools.product is order-preserving)
  - Purpose: Pure matrix expansion logic
  - _Requirements: R2.1, R2.3, R2.4, R2.5, R2.8, R2.10_

- [ ] 11. Implement `merge_sweep_specs` function
  - File: `src/srunx/sweep/expand.py` (modify)
  - `merge_sweep_specs(yaml_sweep: SweepSpec | None, cli_sweep_axes: dict[str, list], cli_arg_overrides: dict, cli_fail_fast: bool | None, cli_max_parallel: int | None) -> SweepSpec | None`
  - Axis-level merge: CLI replaces YAML axis if same key; CLI-only axes are added
  - Raise `WorkflowValidationError` on `--arg KEY` + `--sweep KEY` collision (R3.6)
  - Raise if final max_parallel missing/invalid (R2.6)
  - Purpose: CLI/YAML sweep spec merging
  - _Requirements: R3.1, R3.2, R3.4, R3.6, R3.7_

- [ ] 12. Implement `parse_arg_flags` / `parse_sweep_flags` CLI parsers
  - File: `src/srunx/sweep/expand.py` (modify)
  - `parse_arg_flags(raw: list[str]) -> dict[str, str]` — split `KEY=VALUE` at first `=`, last-wins on duplicates (R1.2), raise on missing `=` (R3.8)
  - `parse_sweep_flags(raw: list[str]) -> dict[str, list[str]]` — split axis at first `=`, values split by `,` (no escape), empty value preserved (R3.9)
  - Purpose: CLI flag tokenization
  - _Requirements: R1.2, R3.8, R3.9, R3.10_

- [ ] 13. Unit tests for `expand_matrix` + merge + parsers
  - File: `tests/sweep/test_expand.py` (new)
  - Tests covering every validation rule in tasks 10-12 (cross product, empty axis, non-scalar, deps collision, 1001 cells rejected, merge axis-level, collision detection, KEY=VAL=VAL split, empty elements, last-wins)
  - _Requirements: R2, R3_

- [ ] 14. Implement `WorkflowRunStateService` in `src/srunx/sweep/state_service.py`
  - File: `src/srunx/sweep/state_service.py` (new)
  - Class `WorkflowRunStateService` with classmethod `update(*, conn: sqlite3.Connection, workflow_run_id: int, from_status: str, to_status: str, error: str | None = None, completed_at: str | None = None) -> bool`
  - **`conn` required**: caller holds the BEGIN IMMEDIATE TX. Service runs under it.
  - Implementation (always under caller TX, always idempotent via optimistic lock + event UNIQUE):
    1. Call `SweepRunRepository.transition_cell(conn=conn, ...)` — this performs the `UPDATE workflow_runs ... WHERE id=? AND status=?` + optional sweep counter updates
    2. If `transition_cell` returned False → return False (no-op, another actor won)
    3. **Always fire `workflow_run.status_changed` event**: `EventRepository.insert(conn=conn, kind='workflow_run.status_changed', source_ref=f'workflow_run:{id}', payload={from_status, to_status, error})` — the event `(kind, source_ref, payload_hash)` UNIQUE index catches duplicates from parallel observers
    4. `NotificationService.fan_out(conn=conn, event_id=...)` in same TX
    5. If the workflow_run belongs to a sweep (sweep_run_id non-NULL): call `evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=...)`
    6. Return True
  - **Critical: always emit the workflow_run.status_changed event**, including non-sweep cases. This centralizes event emission (currently only poller fires these; runner path adds no event). The unified path:
    - preserves existing non-sweep Web UI / CLI notifications (poller still emits, but service dedup protects against duplicates)
    - Adds event emission on the runner-internal transitions too, which the UNIQUE index harmlessly dedups against poller observations
  - Purpose: Single entry point for workflow_run status transitions with always-fire event semantics
  - _Leverage: `SweepRunRepository.transition_cell`, `EventRepository.insert`, `NotificationService.fan_out`_
  - _Requirements: Design Integration Points, R6.8 (non-sweep event preservation)_

- [ ] 15. Verify or extend `NotificationService.fan_out` / `EventRepository.insert` to accept `conn`
  - File: `src/srunx/notifications/service.py` (modify if needed) + `src/srunx/db/repositories/events.py` (modify if needed)
  - If either method currently opens its own TX or does not accept `conn`, extend their signatures to accept `conn: sqlite3.Connection` kwarg (keep backward compat: None → open own TX)
  - Purpose: Thread caller's TX through event INSERT + fan_out so they commit atomically with status UPDATE
  - _Requirements: Design concurrency model_

- [ ] 16. Implement `evaluate_and_fire_sweep_status_event`
  - File: `src/srunx/sweep/aggregator.py` (new)
  - Signature: `evaluate_and_fire_sweep_status_event(*, conn: sqlite3.Connection, sweep_run_id: int) -> None`
  - **`conn` required**, runs under caller TX
  - Implementation per design.md "evaluate_and_fire_sweep_status_event" pseudocode:
    1. SELECT sweep_runs row (status, counters, cancel_requested_at)
    2. Compute target status per R4.6 rules (cancel_requested_at takes precedence over failed)
    3. Idempotency guard: if target == current → return
    4. `UPDATE sweep_runs SET status=?, completed_at=? WHERE id=? AND status=?` (optimistic lock)
    5. If rowcount==0 → return (another actor won)
    6. `EventRepository.insert(conn=conn, kind='sweep_run.status_changed', source_ref=f'sweep_run:{id}', payload=...)` — deterministic payload_hash
    7. `NotificationService.fan_out(conn=conn, event_id=...)`
  - Purpose: Sweep status transition + event fan-out, all under caller TX
  - _Leverage: `EventRepository.insert(conn=...)`, `NotificationService.fan_out(conn=...)` (extended in task 15)_
  - _Requirements: R6.3, R6.4_

- [ ] 16. Unit tests for `evaluate_and_fire_sweep_status_event`
  - File: `tests/sweep/test_aggregator.py` (new)
  - Tests:
    - pending → running fires event with `from=pending, to=running`
    - all cells complete → fires event with `to=completed`
    - some failed, some completed → fires event with `to=failed, representative_error=<earliest failed>`
    - cancel_requested_at set + cells terminal → fires `to=cancelled` (precedence over failed)
    - idempotent: calling twice with same state → only 1 event insert (UNIQUE index catches 2nd)
    - draining → no event fired
  - _Requirements: R6_

- [ ] 16b. Implement `NotificationService.create_watch_for_sweep_run` helper
  - File: `src/srunx/notifications/service.py` (modify)
  - Add method `create_watch_for_sweep_run(conn, sweep_run_id: int, endpoint_id: int | None = None, preset: str = "terminal") -> int`
  - Creates `watches` row with `kind='sweep_run', target_ref=f'sweep_run:{sweep_run_id}'`. If `endpoint_id` is non-None, also creates a `subscriptions` row linking it to this watch with the given preset. If None → watch-only (no subscription).
  - Purpose: Centralized helper to create sweep-level watch + optional subscription
  - _Leverage: existing `create_watch_for_workflow_run` pattern_
  - _Requirements: R6.1, R6.2_

- [ ] 17. Implement `SweepOrchestrator` skeleton
  - File: `src/srunx/sweep/orchestrator.py` (new)
  - Class `SweepOrchestrator(__init__(workflow_yaml_path, workflow_data, args_override, sweep_spec, submission_source, callbacks=None, endpoint_id=None, preset="terminal"))`
  - Method stubs: `_expand_cells()`, `_materialize(cells) -> int`, `_run_cell(sem, cell)`, `_on_cell_done(cell, final_status, error)`, `_drain()`, `request_cancel()`, `run()`, `arun()`
  - Stubs raise `NotImplementedError` with a comment referencing the task that fills it in (18, 19, 20)
  - Purpose: Orchestrator class shell for subsequent tasks
  - _Requirements: R4_

- [ ] 18. Implement `SweepOrchestrator._expand_cells` and `_materialize` (happy path)
  - File: `src/srunx/sweep/orchestrator.py` (modify)
  - `_expand_cells`: call `expand_matrix` on `sweep_spec.matrix` + `workflow_data["args"]` merged with `args_override`; return list of cell dicts
  - `_materialize` (happy path only, error handling in task 18b):
    - Single BEGIN IMMEDIATE TX on a fresh connection
    - INSERT sweep_runs row with status='pending', cell_count=N
    - INSERT N workflow_runs rows with `status='pending'`, `args=<cell_effective_args JSON>`, `sweep_run_id=<parent>`
    - For each cell: call `NotificationService.create_watch_for_workflow_run(conn, workflow_run_id, endpoint_id=None)` for watch-only
    - If `self.endpoint_id is not None`: call `NotificationService.create_watch_for_sweep_run(conn, sweep_run_id, endpoint_id=..., preset=...)` to create sweep-level watch + subscription
    - Commit, return sweep_run_id
  - Purpose: Load-time materialization atomic happy path
  - _Leverage: `NotificationService.create_watch_for_workflow_run`, `create_watch_for_sweep_run` (task 16b)_
  - _Requirements: R4.1, R6.1, R6.2_

- [ ] 18b. Implement `_materialize` error handling and failed sweep recording
  - File: `src/srunx/sweep/orchestrator.py` (modify)
  - Wrap `_materialize` (task 18) in try/except for DB errors
  - On failure: after implicit rollback of the failed TX, open a separate TX and INSERT `sweep_runs(status='failed', cell_count=0, error=<original exception repr>, ...)` for audit visibility (R4.7)
  - Raise `SweepExecutionError` after the fallback insert
  - Unit test: inject a DB error during step 2 (N workflow_runs INSERT) → assert sweep_runs happy-path row is NOT present, failed-row IS present
  - Purpose: Failure observability for load-time errors
  - _Requirements: R4.7_

- [ ] 19. Implement `SweepOrchestrator.run` with anyio semaphore
  - File: `src/srunx/sweep/orchestrator.py` (modify)
  - `run()` → sync wrapper around `anyio.run(self.arun)` for CLI
  - `arun()`: effective_parallel = min(max_parallel, cell_count); `async with anyio.create_task_group() as tg: sem = anyio.Semaphore(effective_parallel); for cell in cells: tg.start_soon(self._run_cell, sem, cell)`
  - `_run_cell`: `async with sem: await anyio.to_thread.run_sync(functools.partial(self._run_cell_sync, cell))` where `_run_cell_sync` calls `WorkflowRunner.run(workflow_run_id=cell.workflow_run_id)`
  - Before each cell start: call `WorkflowRunStateService.update(cell.id, 'pending', 'running')` to get pending→running transition in DB (and trigger sweep event via aggregator)
  - After cell completion: compute final status from runner return/exception, call `WorkflowRunStateService.update(cell.id, 'running', <final>, error=...)`
  - Respect `_should_drain()` / `self._cancelled` flags to skip starting new cells
  - Purpose: Concurrent cell execution with bounded parallelism
  - _Leverage: `WorkflowRunner.run` (modified in task 26)_
  - _Requirements: R4.2, R4.4, R4.5, R4.9, R4.11_

- [ ] 20. Implement `SweepOrchestrator._drain` and `request_cancel`
  - File: `src/srunx/sweep/orchestrator.py` (modify)
  - `_drain`: atomic SQL `UPDATE workflow_runs SET status='cancelled' WHERE sweep_run_id=? AND status='pending'` + adjust sweep counters (cells_pending -= K, cells_cancelled += K) in same TX; set sweep status to 'draining' via `SweepRunRepository.update_status`
  - `request_cancel`: set sweep_runs.cancel_requested_at = now, trigger `_drain`
  - When fail_fast=true and cell fails: call `_drain` from `_on_cell_done`
  - Purpose: Graceful cancellation / fail-fast
  - _Requirements: R4.5, R4.8_

- [ ] 21. Implement `SweepReconciler` in `src/srunx/sweep/reconciler.py`
  - File: `src/srunx/sweep/reconciler.py` (new)
  - Function `scan_and_resume()` per design.md pseudocode
  - For each incomplete sweep: compute headroom, re-spawn `SweepOrchestrator` for pending cells if status != 'draining'
  - If all cells already terminal → force `evaluate_and_fire_sweep_status_event` to finalize sweep status
  - Purpose: Crash recovery at lifespan startup
  - _Requirements: R4.10_

- [ ] 22. Unit tests for `SweepOrchestrator` + `SweepReconciler`
  - File: `tests/sweep/test_orchestrator.py` (new)
  - Use in-memory SQLite + `fake_slurm` to avoid real SLURM calls
  - Tests:
    - 4-cell sweep completes successfully (counters: pending 4→0, running 4→0, completed 0→4)
    - 1 cell fails with fail_fast=false → other 3 complete, final status=failed
    - 1 cell fails with fail_fast=true → pending cells marked cancelled, running cells continue
    - user cancel → future cells cancelled, final status=cancelled even if some cells later fail (R4.6 precedence)
    - max_parallel > cell_count → no error, effective_parallel clamped
    - materialize rollback on injected DB failure → sweep_runs row recorded as failed per R4.7
  - File: `tests/sweep/test_reconciler.py` (new)
  - Tests:
    - reconciler finds sweep with status=running + 2 pending cells → orchestrator resumes
    - reconciler sees all cells terminal → forces final status transition
    - draining sweep → reconciler skips re-spawning
  - _Requirements: R4_

### C. Runner changes (PR 1)

- [ ] 23. Add `workflow_run_id` kwarg to `WorkflowRunner.run`
  - File: `src/srunx/runner.py` (modify)
  - Add `workflow_run_id: int | None = None` kwarg to `run()` signature
  - When non-None: skip `create_cli_workflow_run()` call; use provided id throughout
  - Purpose: Allow sweep orchestrator to inject pre-materialized workflow_run_id
  - _Leverage: existing `run()` DB write at L718_
  - _Requirements: Design Runner 変更_

- [ ] 24. Route runner's `mark_workflow_run_status` calls through `WorkflowRunStateService`
  - File: `src/srunx/runner.py` (modify)
  - Locate all `mark_workflow_run_status(...)` calls in `run()` (running/completed/failed transitions)
  - Replace with `WorkflowRunStateService.update(conn=..., workflow_run_id=..., from_status=..., to_status=..., error=..., completed_at=...)`
  - Runner opens a short BEGIN IMMEDIATE TX for each transition and passes the `conn` to the service
  - **Regression gate**: Before marking this task complete, run `uv run pytest tests/test_runner.py tests/test_workflows.py` — all existing non-sweep tests MUST pass. Any breakage blocks this task.
  - Test expectation: for non-sweep runs (sweep_run_id=NULL), service still fires `workflow_run.status_changed` events (new behavior — slightly more events than before, all idempotent via UNIQUE index). Add regression tests verifying: (1) existing `workflow_runs.status` transitions still work; (2) events now appear for CLI-originated runs too (new — this is acceptable since the dedup index protects against duplicates with poller-observed transitions).
  - Purpose: Single entry point for status transitions
  - _Requirements: Design Integration Points, R10.3_

- [ ] 25. Add `args_override` kwarg to `WorkflowRunner.from_yaml`
  - File: `src/srunx/runner.py` (modify)
  - Add `args_override: dict[str, Any] | None = None` kwarg to `from_yaml(...)`
  - After YAML parse, merge `args_override` into `data["args"]` (override wins) before `_evaluate_variables`
  - Store merged args in `self.args`
  - Purpose: Hub function for args injection used by CLI/Web/MCP
  - _Requirements: R1.1, R1.5, R1.6_

- [ ] 26. Unit tests for runner changes
  - File: `tests/test_runner_sweep_integration.py` (new)
  - Tests:
    - `from_yaml(args_override={"lr": 0.01})` overrides YAML args
    - `run(workflow_run_id=<id>)` skips `create_cli_workflow_run`
    - `run()` without workflow_run_id creates one (regression check)
    - args_override merges with YAML args (not replace)
  - _Requirements: R1, Design_

### D. Active Watch Poller changes (PR 1)

- [ ] 27. Route poller's workflow_run status updates through `WorkflowRunStateService`
  - File: `src/srunx/pollers/active_watch_poller.py` (modify)
  - In `_process_workflow_watches`, replace any `WorkflowRunRepository.update_status` + direct `workflow_run.status_changed` event insert, with `WorkflowRunStateService.update(run_id, from, to, conn=...)`
  - Ensure watch close happens after state service call (not before)
  - Watch close logic: after `WorkflowRunStateService.update` returns True, if to_status is terminal → `WatchRepository.close(watch_id)`
  - Purpose: Sweep aggregation picks up poller-observed transitions
  - _Leverage: existing `_process_workflow_watches` flow_
  - _Requirements: Design active_watch_poller integration_

- [ ] 28. Register `SweepReconciler.scan_and_resume` in Web lifespan
  - File: `src/srunx/web/app.py` (modify)
  - In FastAPI lifespan startup: after DB init, before `PollerSupervisor.start_all()`, call `SweepReconciler.scan_and_resume()`
  - Respect `SRUNX_DISABLE_POLLER` (disable reconciler too — consistent with poller behavior)
  - Purpose: Crash recovery at startup
  - _Leverage: existing lifespan pattern_
  - _Requirements: R4.10_

- [ ] 29. Integration test for poller + sweep aggregation
  - File: `tests/integration/test_sweep_poller_integration.py` (new)
  - Use in-memory DB + fake SLURM returning controlled status sequences
  - Test: poller observes child workflow_run going to `completed` → sweep counter updates + aggregator fires `sweep_run.status_changed` event
  - Test: all child cells completed via poller → final sweep status=completed + event
  - _Requirements: R4.3, R6.3_

### E. Notifications (PR 1)

- [ ] 30. Extend `should_deliver` for `sweep_run.status_changed`
  - File: `src/srunx/notifications/presets.py` (modify)
  - Add `_TERMINAL_SWEEP_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})`
  - Add elif branch for `event_kind == "sweep_run.status_changed"`: mirror workflow_run pattern for terminal / running_and_terminal / all presets
  - Purpose: Preset filtering for sweep events
  - _Leverage: existing workflow_run branch_
  - _Requirements: R6.5_

- [ ] 31. Add sweep message formatter to Slack webhook adapter
  - File: `src/srunx/notifications/adapters/slack_webhook.py` (modify)
  - Add `_format_sweep_run_event(event)` that composes Slack message: title, sweep name, to_status, cell counts, representative_error (if present). Use existing `sanitize_slack_text` for safety.
  - Add dispatch branch in event → formatter switch
  - Purpose: Human-readable Slack notification for sweep events
  - _Leverage: existing `_format_workflow_run_event` pattern_
  - _Requirements: R6.6_

- [ ] 32. Unit tests for notification extensions
  - File: `tests/notifications/test_sweep_notification.py` (new)
  - Test `should_deliver` truth table for each preset × each to_status
  - Test `_format_sweep_run_event` produces expected Slack blocks
  - _Requirements: R6_

### F. CLI (PR 1)

- [ ] 33. Add `--arg` / `--sweep` / `--fail-fast` / `--max-parallel` CLI options
  - File: `src/srunx/cli/workflow.py` (modify)
  - Add Typer options to `run_command`, `execute_yaml` (callback), and `_execute_workflow` signature
  - Pass through raw option values to `_execute_workflow`
  - Purpose: CLI flag surface
  - _Leverage: existing option patterns_
  - _Requirements: R3.1, R3.2, R3.3, R3.4_

- [ ] 34. Wire CLI sweep dispatch in `_execute_workflow`
  - File: `src/srunx/cli/workflow.py` (modify)
  - Use `parse_arg_flags` / `parse_sweep_flags` to tokenize, then `merge_sweep_specs` to combine YAML `sweep:` block + CLI flags
  - If final sweep_spec is non-None → instantiate `SweepOrchestrator(...).run()`; else existing `WorkflowRunner.from_yaml(..., args_override=...).run()`
  - Handle SIGINT: install signal handler that calls `orchestrator.request_cancel()` once, then falls back to default on 2nd SIGINT
  - Purpose: CLI → orchestrator / runner routing
  - _Requirements: R3, R4.8_

- [ ] 35. CLI integration tests
  - File: `tests/cli/test_workflow_sweep.py` (new)
  - Tests using Typer CliRunner + fake_slurm:
    - `srunx flow run w.yaml --arg lr=0.01` → args override applied to single run
    - `srunx flow run w.yaml --sweep lr=0.001,0.01,0.1` → 3 cells execute
    - `--arg lr=0.01 --sweep lr=0.001,0.01` → exits non-zero with validation error
    - `--sweep lr=a,,b` → empty element preserved
    - YAML has `sweep:` block + CLI has `--sweep` for different axis → axes merged
  - _Requirements: R3_

### G. Web API (PR 1)

- [ ] 36. Extend `WorkflowRunRequest` with `args_override` + `sweep`
  - File: `src/srunx/web/routers/workflows.py` (modify)
  - Add `args_override: dict[str, Any] = Field(default_factory=dict)` and `sweep: SweepSpecRequest | None = None`
  - Add `SweepSpecRequest(BaseModel)` with `matrix: dict[str, list[Any]]`, `fail_fast: bool = False`, `max_parallel: int = 4` (R7.9 server default)
  - Apply `_reject_python_args` to `args_override` and matrix values
  - Purpose: API schema for sweep requests
  - _Leverage: existing `WorkflowRunRequest`_
  - _Requirements: R7.1, R7.7, R7.9_

- [ ] 37. Wire Web `POST /workflows/{name}/run` to `SweepOrchestrator`
  - File: `src/srunx/web/routers/workflows.py` (modify)
  - When `body.sweep is not None`: construct `SweepOrchestrator(...)`, call `await orchestrator.arun()` (or spawn as background task for 202 semantics per existing pattern)
  - Return `{"sweep_run_id": sweep_run.id, ...}` in 202 response
  - When sweep is None: existing path, unchanged
  - Extend `GET /workflows/runs` response to include `sweep_run_id` field
  - Purpose: Web API sweep entry
  - _Requirements: R7.1, R7.6_

- [ ] 38. Create `src/srunx/web/routers/sweep_runs.py` with GET endpoints
  - File: `src/srunx/web/routers/sweep_runs.py` (new)
  - Endpoints:
    - `GET /sweep_runs` → `SweepRunRepository.list_all(limit=200)`
    - `GET /sweep_runs/{id}` → `get(id)` or 404
    - `GET /sweep_runs/{id}/cells` → JOIN workflow_runs WHERE sweep_run_id=id, ordered by status/started_at
  - Register router in `src/srunx/web/app.py`
  - Purpose: Read-side API for sweeps
  - _Requirements: R7.2, R7.3, R7.4_

- [ ] 39. Add `POST /sweep_runs/{id}/cancel` endpoint
  - File: `src/srunx/web/routers/sweep_runs.py` (modify)
  - Call `SweepRunRepository.request_cancel(id)` → fetch active orchestrator from in-process registry if present, call `request_cancel()`
  - If no in-process orchestrator (crash recovery scenario): next poller cycle + reconciler will pick up cancel_requested_at
  - Return 202 with current state
  - Purpose: Cancellation entry
  - _Requirements: R7.5_

- [ ] 40. Web API integration tests
  - File: `tests/web/test_sweep_runs_api.py` (new)
  - Tests:
    - POST workflow run with sweep body creates sweep_run + N workflow_runs
    - GET /sweep_runs lists new sweep
    - GET /sweep_runs/{id}/cells returns cells with effective_args
    - POST /sweep_runs/{id}/cancel transitions to draining
    - python: rejection in args_override returns 422
  - _Requirements: R7_

### H. MCP (PR 1)

- [ ] 41. Extend `run_workflow` MCP tool with `args` + `sweep` kwargs
  - File: `src/srunx/mcp/server.py` (modify)
  - Add `args: dict[str, Any] | None = None` and `sweep: dict[str, Any] | None = None` keyword args (placed at end for backward compat)
  - Reject `python:` values in both args and sweep.matrix
  - Dispatch to `SweepOrchestrator` or existing path accordingly
  - Include `sweep_run_id` in response when sweep mode
  - Purpose: MCP sweep support
  - _Requirements: R9_

- [ ] 42. MCP integration tests
  - File: `tests/test_mcp_sweep.py` (new)
  - Tests:
    - `run_workflow(yaml_path=..., args={"lr": 0.01})` works
    - `run_workflow(yaml_path=..., sweep={"matrix": {...}, ...})` works
    - `run_workflow(yaml_path=..., args={"cmd": "python: os.system('x')"})` rejected
  - _Requirements: R9_

### I. Web UI (PR 2)

- [ ] 43. Add `SweepRun` / `SweepSpec` types to frontend
  - File: `src/srunx/web/frontend/src/lib/types.ts` (modify)
  - Add TS interface `SweepRun`, `SweepSpec`, `CellRow` matching backend API
  - Extend `WorkflowRun` interface to include `sweep_run_id?: number | null`
  - Purpose: Frontend type safety
  - _Requirements: R7, R8_

- [ ] 44. Extend API client with sweep endpoints
  - File: `src/srunx/web/frontend/src/lib/api.ts` (modify)
  - Add `listSweepRuns`, `getSweepRun`, `listSweepRunCells`, `cancelSweepRun` functions
  - Purpose: API client wiring
  - _Requirements: R7_

- [ ] 45. Enhance `WorkflowRunDialog.tsx` with single/list toggle per arg field
  - File: `src/srunx/web/frontend/src/components/WorkflowRunDialog.tsx` (modify)
  - Per arg field: add toggle button (single / list). Single mode renders text input, list mode renders comma-separated input
  - Collect list-mode fields into `sweep.matrix`
  - Compute cell_count = `matrix.values().reduce((a, v) => a * v.length, 1)`; show preview under the form
  - If cell_count > 10 → show confirm dialog before submit
  - Purpose: UX for sweep setup
  - _Requirements: R8.1, R8.2, R8.3, R8.4_

- [ ] 46. Add Advanced section with `fail_fast` / `max_parallel`
  - File: `src/srunx/web/frontend/src/components/WorkflowRunDialog.tsx` (modify)
  - Collapsible "Advanced" section with checkbox `fail_fast` (default off) and number input `max_parallel` (default 4, min 1)
  - Include in submit body when sweep mode
  - Purpose: Advanced sweep options
  - _Requirements: R8.5_

- [ ] 47. Create `SweepRunsPage.tsx`
  - File: `src/srunx/web/frontend/src/pages/SweepRunsPage.tsx` (new)
  - Fetch `/api/sweep_runs`, render table: name / status / progress (cells_completed / cell_count) / created_at / actions
  - Add route in router setup + sidebar link
  - Purpose: Sweep list view
  - _Requirements: R8.7_

- [ ] 48. Create `SweepRunDetailPage.tsx`
  - File: `src/srunx/web/frontend/src/pages/SweepRunDetailPage.tsx` (new)
  - Fetch `/api/sweep_runs/{id}` and `/cells`, render two sections:
    - Top: meta (matrix JSON rendered as pretty table / fail_fast / max_parallel / status / cells_*)
    - Bottom: cells table with rows = cells, columns = axis values + status + started_at + completed_at + link-to-workflow-run
  - Cell row click → navigate to existing `/workflow_runs/:id`
  - Add cancel button for active sweeps
  - Purpose: Sweep detail + drilldown
  - _Requirements: R8.8, R8.9_

- [ ] 49. Frontend unit tests
  - File: `src/srunx/web/frontend/src/pages/__tests__/SweepRunsPage.test.tsx`, etc.
  - Basic render + interaction tests (mock API client)
  - _Requirements: R8_

- [ ] 50. Playwright E2E smoke test for sweep flow
  - File: `src/srunx/web/frontend/e2e/sweep-builder.spec.ts` (new)
  - Scenario:
    1. Open workflow run dialog
    2. Enable list mode on `lr` field, input `0.001,0.01,0.1`
    3. Verify cell_count preview shows 3
    4. Submit → navigate to SweepRunsPage, new row appears
    5. Click sweep row → SweepRunDetailPage shows 3 cell rows
  - Requires fake SLURM backend in test setup
  - _Requirements: R8_

### J. Docs + E2E smoke + final regression (PR 2)

- [ ] 51. Update `CLAUDE.md` with sweep docs
  - File: `CLAUDE.md` (modify)
  - Add "Parameter Sweeps" subsection under "Workflow Definition" with example YAML, CLI usage, Web UI note
  - Document `sweep_runs` table and V3 migration
  - Purpose: Developer-facing docs
  - _Requirements: R10.1_

- [ ] 52. Update `srunx flow run --help` examples
  - File: `src/srunx/cli/workflow.py` (modify)
  - Ensure option help text includes usage examples for `--arg` and `--sweep`
  - Add an epilog in Typer app with sweep examples
  - Purpose: CLI help surface
  - _Requirements: R10.2_

- [ ] 53. Add integration smoke: 9-cell sweep E2E
  - File: `tests/integration/test_sweep_end_to_end.py` (new)
  - Scenario: 3x3 matrix via fake_slurm, 1 cell forced to fail, fail_fast=false
  - Verify: cells_completed=8, cells_failed=1, sweep.status=failed, exactly 2 sweep_run.status_changed events (running + failed), exactly 1 Slack delivery (mocked adapter)
  - Verify: each cell's watch is watch-only (endpoint_id=NULL), no per-cell delivery
  - _Requirements: R4, R6_

- [ ] 54. Backward compat regression: sweep-less YAML still works
  - File: `tests/integration/test_sweep_backward_compat.py` (new)
  - Run existing workflow YAML (no `sweep:` block) through CLI and Web API
  - Verify: 1 workflow_run created, no sweep_run row, existing Slack notifications fire as before
  - _Requirements: R2.9, R10.3_

- [ ] 55. Final quality gate: `pytest && mypy . && ruff check .`
  - Run `uv run pytest && uv run mypy . && uv run ruff check .`
  - Fix any regressions / type / lint issues
  - Verify existing test count (1370+) all pass
  - _Requirements: R10.4_
