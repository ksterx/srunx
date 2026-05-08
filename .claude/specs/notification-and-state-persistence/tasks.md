# Implementation Plan

> **Historical naming note** (added during PR #203, the #193 oversized-module
> split): this spec was authored when the SSH-transport SLURM client was
> ``SlurmSSHAdapter`` in ``src/srunx/web/ssh_adapter.py``. It has since been
> renamed to ``SlurmSSHClient`` and moved to ``src/srunx/slurm/clients/ssh.py``.
> The original names are preserved below as historical context; new work
> should target the current paths.

## Task Overview

実装は以下のレイヤ順で積み上げる:

**(A0) 共通基盤抽象**(SLURM Protocol) → **(A) DB レイヤ** → **(A.3) 既存 history.py 呼び出しサイト移行** → **(B) Notifications ドメイン** → **(C) Pollers** → **(D) JobMonitor SSOT** → **(E) Web ルータ + lifespan + DB provider** → **(F) フロントエンド** → **(G) 統合テスト** → **(H) ドキュメント** → **(I) 最終 cleanup**

下位から上位への一方向依存。既存バグ修正(R5.1: Web submit が履歴に残らない)は (E) の `jobs.py` 改修内で自然に閉じる。

## PR 分割戦略

合計 ~90 タスクを1 PR にまとめるとレビュー困難・branch 不安定になるため、3 PR に分割する:

| PR | 範囲 | 含むタスク |
|---|---|---|
| **PR 1: DB Foundation + history migration** | DB 層 + 既存 `history.py` 呼び出しサイトの全移行(shim 込み)、SLURM Protocol 抽象 | A0, A, A.1, A.2, A.3, および conftest 共通 fixture |
| **PR 2: Notifications + Pollers + Lifecycle** | notifications ドメイン / 3 つの poller / DB connection provider / adapter registry / JobMonitor SSOT hook / web lifespan 配線 | B, C, D, E の lifespan/provider 部分のみ |
| **PR 3: Web routes + Frontend + E2E + Docs** | 新規 router 群、既存 `jobs.py` / `workflows.py` 改修、frontend 全更新、E2E、ドキュメント | E の routers、F、G、H、I |

各 PR の終端で `uv run pytest && uv run mypy . && uv run ruff check .` が通ることを条件とする。

## Steering Document Compliance

`CLAUDE.md` のディレクトリ配置(`src/srunx/db/`, `src/srunx/notifications/`, `src/srunx/pollers/`)、Python 3.12+ 型ヒント(`str | None`)、Pydantic v2、`anyio`、`uv` 使用、ruff/mypy/pytest に準拠する。

## Logging Convention

新規モジュールは `loguru.logger` を直接 import せず、`from srunx.logging import get_logger; logger = get_logger(__name__)` を使う(既存慣習に合わせる)。構造化ログは `logger.bind(cycle_id=..., counts=...).info("poller cycle completed")` のように bind で付随データを付ける。

## Atomic Task Requirements

- 各タスクは **1〜3 ファイル** を touch する
- **15〜30 分**で完了可能
- **single purpose / single testable outcome**
- 既存コードへの **_Leverage_** と **_Requirements_** を必ず参照

## Tasks

### A0. SLURM Protocol 共通抽象(PR 1)

- [ ] 0a. Define `SlurmClientProtocol` in `src/srunx/client_protocol.py`
  - File: `src/srunx/client_protocol.py`
  - `Protocol` with `async queue_by_ids(job_ids: list[int]) -> dict[int, JobStatusInfo]` signature (returns mapping of job_id to current state)
  - `JobStatusInfo` TypedDict/dataclass: `status, started_at, completed_at, duration_secs, nodelist` 等
  - Purpose: Both `Slurm`(local)と `SlurmSSHAdapter`(web)が同じ呼び出し口を持つための契約
  - _Leverage: 既存 `src/srunx/client.py` (`Slurm`), `src/srunx/web/ssh_adapter.py` (`SlurmSSHAdapter`)_
  - _Requirements: 10.2_

- [ ] 0b. Implement `Slurm.queue_by_ids()` in `src/srunx/client.py`
  - File: `src/srunx/client.py`(modify)
  - Add method that calls `squeue` filtered by job id list, parses into `dict[int, JobStatusInfo]`
  - Reuse existing squeue-parsing helpers
  - Purpose: ローカル SLURM 経由の batch 取得
  - _Leverage: 既存 `Slurm.queue()` の squeue 呼び出しロジック_
  - _Requirements: 10.2_

- [ ] 0c. Implement `SlurmSSHAdapter.queue_by_ids()` in `src/srunx/web/ssh_adapter.py`
  - File: `src/srunx/web/ssh_adapter.py`(modify)
  - SSH 経由で `squeue --jobs=<id,id,...>` を実行、パース
  - Purpose: SSH 経由の batch 取得
  - _Leverage: 既存 `SlurmSSHAdapter` の SSH 呼び出し基盤_
  - _Requirements: 10.2_

- [ ] 0d. Unit tests for `queue_by_ids` in `tests/test_client_protocol.py`
  - File: `tests/test_client_protocol.py`
  - Test: mocked squeue returns parsed dict correctly
  - Test: empty job_ids list returns empty dict
  - Test: missing/terminal jobs are represented with their final status (from sacct fallback or marked as unknown)
  - _Requirements: 10.2_

### A. DB レイヤ(PR 1)

- [ ] 1. Create `src/srunx/db/__init__.py` as empty package marker
  - File: `src/srunx/db/__init__.py`
  - Empty file (re-exports added in later tasks as needed)
  - Purpose: Initialize db package
  - _Requirements: 5.5_

- [ ] 2. Add XDG-compliant DB path resolution to `src/srunx/db/connection.py`
  - File: `src/srunx/db/connection.py`
  - Function `get_db_path() -> Path`: `$XDG_CONFIG_HOME/srunx/srunx.db` or fallback `~/.config/srunx/srunx.db`
  - Create parent directory with mode 0700 if missing
  - Purpose: Provide single authoritative DB path helper
  - _Leverage: `src/srunx/config.py`(`get_config_dir` がある場合は流用)_
  - _Requirements: 5.5_

- [ ] 3. Add `open_connection()` and `transaction()` context manager to `src/srunx/db/connection.py`
  - File: `src/srunx/db/connection.py`(continue from task 2)
  - `open_connection()`: `sqlite3.connect(path)` + PRAGMA `foreign_keys=ON`, `journal_mode=WAL`, `busy_timeout=5000`, set `row_factory=sqlite3.Row`, `chmod 0600` on first creation
  - `transaction(conn, mode='DEFERRED'|'IMMEDIATE')`: contextmanager wrapping `BEGIN ... COMMIT/ROLLBACK`
  - Purpose: Standardize DB connection and transaction handling
  - _Requirements: 5.5, Non-Functional Reliability, Security_

- [ ] 4. Write schema SQL constant `SCHEMA_V1` in `src/srunx/db/migrations.py`
  - File: `src/srunx/db/migrations.py`
  - Define `SCHEMA_V1: str` containing DDL for all 10 tables (schema_version, workflow_runs, jobs, workflow_run_jobs, job_state_transitions, resource_snapshots, endpoints, watches, subscriptions, events, deliveries) in correct FK order
  - Include all CHECK constraints, UNIQUE indexes, partial indexes as specified in design.md Data Models section
  - Purpose: Ship the entire v1 schema as one atomic migration
  - _Requirements: 3.8, 5.5, 6.1, 7.1, 7.2, 7.4_

- [ ] 5. Add `apply_migrations(conn)` to `src/srunx/db/migrations.py`
  - File: `src/srunx/db/migrations.py`(continue from task 4)
  - `Migration` dataclass with `version`, `name`, `sql`
  - `MIGRATIONS: list[Migration]` containing `(1, 'v1_initial', SCHEMA_V1)`
  - `apply_migrations(conn)`: for each migration, check `schema_version` table existence + row existence; if missing, run SQL inside `BEGIN IMMEDIATE` transaction and insert row
  - Purpose: Idempotent schema application
  - _Requirements: 5.4, 5.5_

- [ ] 6. Add `bootstrap_from_config(conn, config)` to `src/srunx/db/migrations.py`
  - File: `src/srunx/db/migrations.py`(continue from task 5)
  - Check `schema_version` for `name='bootstrap_slack_webhook_url'`; no-op if present
  - Read `config.notifications.slack_webhook_url`; if empty/None, record `schema_version` row and return
  - If non-empty, inside `BEGIN IMMEDIATE`: INSERT into `endpoints` (kind='slack_webhook', name='default', config JSON), then INSERT into `schema_version`; on failure, ROLLBACK and log warning (no schema_version row written)
  - Purpose: One-time migration of legacy webhook URL to endpoints table
  - _Leverage: `src/srunx/config.py`_
  - _Requirements: Migration NFR_

- [ ] 7. Add `init_db()` function to `src/srunx/db/connection.py`
  - File: `src/srunx/db/connection.py`(continue from task 3)
  - Steps: ensure parent dir (0700), create connection (chmod 0600 on first creation), apply PRAGMAs, call `apply_migrations(conn)`, delete `~/.srunx/history.db` if exists (on OSError rename to `.broken` and log warning)
  - Purpose: Single entrypoint for DB initialization from lifespan
  - _Leverage: `src/srunx/db/migrations.py`_
  - _Requirements: 5.4, 5.5, Security NFR_

- [ ] 8. Unit tests for `db/connection.py` in `tests/db/test_connection.py`
  - File: `tests/db/test_connection.py`
  - Test: `get_db_path()` with/without `XDG_CONFIG_HOME` env var
  - Test: `open_connection()` sets all PRAGMAs, `row_factory`
  - Test: `init_db()` creates file with mode 0600, removes legacy `~/.srunx/history.db`, applies schema
  - Use `tmp_path` and monkeypatch for env isolation
  - Purpose: Verify DB bootstrapping edge cases
  - _Requirements: 5.5, Security NFR_

- [ ] 9. Unit tests for `db/migrations.py` in `tests/db/test_migrations.py`
  - File: `tests/db/test_migrations.py`
  - Test: `apply_migrations` idempotent(2 回呼び出しで行数不変)
  - Test: `bootstrap_from_config` with None URL records schema_version once
  - Test: `bootstrap_from_config` with valid URL creates endpoint and records schema_version
  - Test: `bootstrap_from_config` on INSERT failure does NOT record schema_version
  - Purpose: Guard migration correctness and once-only semantics
  - _Requirements: Migration NFR_

### A.1 Pydantic モデル層(PR 1)

- [ ] 10. Create `src/srunx/db/models.py` with core Pydantic models
  - File: `src/srunx/db/models.py`
  - Define: `Endpoint`, `Watch`, `Subscription`, `Event`, `Delivery`, `WorkflowRun`, `WorkflowRunJob`, `JobStateTransition`, `ResourceSnapshot`, `Job` as `pydantic.BaseModel` v2
  - Use `datetime | None` for timestamps, `dict | None` for JSON columns
  - Purpose: Typed row representations shared by repositories
  - _Requirements: R1-R7, R10_

### A.2 Repository 群(PR 1)

- [ ] 11. Create `src/srunx/db/repositories/__init__.py` as empty package marker
  - File: `src/srunx/db/repositories/__init__.py`
  - Purpose: Initialize repositories package
  - _Requirements: Structure (CLAUDE.md conventions)_

- [ ] 12. Create `BaseRepository` in `src/srunx/db/repositories/base.py`
  - File: `src/srunx/db/repositories/base.py`
  - `BaseRepository(conn)`: store connection, provide `_row_to_model(row, Model)` helper that parses JSON columns and datetime strings
  - ISO 8601 UTC serialization helper `now_iso() -> str` returning `datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')`
  - Purpose: Shared repository utilities and timestamp policy enforcement
  - _Requirements: R3 Performance NFR(timestamp format)_

- [ ] 13. Create `JobRepository` in `src/srunx/db/repositories/jobs.py`
  - File: `src/srunx/db/repositories/jobs.py`
  - Methods: `record_submission(job, submission_source, workflow_run_id)`, `update_status(job_id, status, started_at, completed_at, duration_secs)`, `update_completion(...)`, `get(job_id)`, `list(limit, offset)`, `count_by_status_in_range(from_at, to_at, statuses)`
  - Port existing `history.py:record_job` / `update_job_completion` logic; add new columns
  - Purpose: Centralize jobs table CRUD
  - _Leverage: `src/srunx/history.py` の既存実装をロジックの下敷きにする_
  - _Requirements: 5.1, 5.2, 5.3, 6.3_

- [ ] 14. Create `WorkflowRunRepository` in `src/srunx/db/repositories/workflow_runs.py`
  - File: `src/srunx/db/repositories/workflow_runs.py`
  - Methods: `create(workflow_name, yaml_path, args, triggered_by)`, `get(id)`, `list()`, `list_incomplete()`, `update_status(id, status, error=None)`, `update_error(id, error)`
  - Purpose: Replace in-memory `RunRegistry` with durable storage
  - _Requirements: 2.1, 2.7, 2.9_

- [ ] 15. Create `WorkflowRunJobRepository` in `src/srunx/db/repositories/workflow_run_jobs.py`
  - File: `src/srunx/db/repositories/workflow_run_jobs.py`
  - Methods: `create(workflow_run_id, job_id, job_name, depends_on)`, `list_by_run(workflow_run_id)`, `update_job_id(wrj_id, job_id)` (for delayed association if needed)
  - Purpose: Manage job-to-run membership
  - _Requirements: 2.1_

- [ ] 16. Create `JobStateTransitionRepository` in `src/srunx/db/repositories/job_state_transitions.py`
  - File: `src/srunx/db/repositories/job_state_transitions.py`
  - Methods: `insert(job_id, from_status, to_status, source)`, `latest_for_job(job_id) -> JobStateTransition | None`, `history_for_job(job_id)`, `count_by_status_in_range(...)`
  - Purpose: SSOT for state transitions, supports dedup via `latest_for_job`
  - _Requirements: 6.1, 6.2, 6.4_

- [ ] 17. Create `ResourceSnapshotRepository` in `src/srunx/db/repositories/resource_snapshots.py`
  - File: `src/srunx/db/repositories/resource_snapshots.py`
  - Methods: `insert(snapshot)`, `list_range(from_at, to_at, partition=None)`, `delete_older_than(days)`
  - Purpose: Time-series GPU utilization storage
  - _Leverage: `src/srunx/monitor/types.py` (`ResourceSnapshot`)_
  - _Requirements: 7.1, 7.3, 7.5_

- [ ] 18. Create `EndpointRepository` in `src/srunx/db/repositories/endpoints.py`
  - File: `src/srunx/db/repositories/endpoints.py`
  - Methods: `create(kind, name, config)`, `get(id)`, `list(include_disabled=True)`, `update(id, ...)`, `disable(id)`, `enable(id)`, `delete(id)`
  - Webhook URL regex validation done in service layer, not here
  - Purpose: Endpoints table CRUD
  - _Requirements: 4.1, 4.5, 4.6_

- [ ] 19. Create `WatchRepository` in `src/srunx/db/repositories/watches.py`
  - File: `src/srunx/db/repositories/watches.py`
  - Methods: `create(kind, target_ref, filter=None)`, `get(id)`, `list_open()`, `close(id)`
  - Purpose: Watch CRUD with open/closed semantics
  - _Requirements: 1.5, 10.6_

- [ ] 20. Create `SubscriptionRepository` in `src/srunx/db/repositories/subscriptions.py`
  - File: `src/srunx/db/repositories/subscriptions.py`
  - Methods: `create(watch_id, endpoint_id, preset)`, `get(id)`, `list_by_watch(watch_id)`, `delete(id)`
  - Purpose: Subscription CRUD with ON DELETE CASCADE semantics
  - _Requirements: 4.4_

- [ ] 21. Create `EventRepository` with payload_hash computation in `src/srunx/db/repositories/events.py`
  - File: `src/srunx/db/repositories/events.py`
  - Methods: `insert(event)` — computes `payload_hash` as SHA-256 hex of logical key string per design.md idempotency policy; uses `INSERT OR IGNORE` on UNIQUE violation, returns the inserted row id or None
  - `get(id)`, `list_recent(limit)`
  - `_compute_payload_hash(kind, source_ref, payload) -> str` as static helper (testable)
  - Purpose: Event insertion with producer-side dedup
  - _Requirements: 3.1, 10.3, 10.4, Error Handling #4_

- [ ] 22. Create `DeliveryRepository` with claim/retry in `src/srunx/db/repositories/deliveries.py`
  - File: `src/srunx/db/repositories/deliveries.py`
  - Methods: `insert(event_id, subscription_id, endpoint_id, idempotency_key)` using `INSERT OR IGNORE`, `reclaim_expired_leases()`, `claim_one(worker_id, lease_duration_secs=300)` using SELECT then UPDATE pattern inside `BEGIN IMMEDIATE`, `mark_delivered(id)`, `mark_retry(id, error, backoff_secs)`, `mark_abandoned(id, error)`, `count_stuck_pending(older_than_sec=300)`
  - Retry schedule helper `_backoff_secs(attempt_count, base=10, factor=2, cap=3600)`
  - Purpose: Outbox CRUD with lease mechanics
  - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, Observability NFR_

- [ ] 23. Unit tests for `JobRepository` in `tests/db/repositories/test_jobs.py`
  - File: `tests/db/repositories/test_jobs.py`
  - Test: `record_submission` inserts with correct `submission_source`, `workflow_run_id`
  - Test: `update_status` updates terminal fields correctly
  - Test: `count_by_status_in_range` returns expected counts
  - _Requirements: 5.1, 5.2, 5.3, 6.3_

- [ ] 24. Unit tests for `WorkflowRunRepository` in `tests/db/repositories/test_workflow_runs.py`
  - File: `tests/db/repositories/test_workflow_runs.py`
  - Test: `create` → `list_incomplete` returns pending/running runs only
  - Test: `update_status` transitions through pending → running → completed/failed/cancelled
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.7_

- [ ] 25. Unit tests for `EventRepository.payload_hash` in `tests/db/repositories/test_events.py`
  - File: `tests/db/repositories/test_events.py`
  - Test: `_compute_payload_hash` deterministic for same (kind, source_ref, logical key)
  - Test: `_compute_payload_hash` differs across event kinds
  - Test: `insert` dedup via UNIQUE — second insert returns None / no new row
  - _Requirements: 3.1, 10.3, Error Handling #4_

- [ ] 26. Unit tests for `DeliveryRepository` in `tests/db/repositories/test_deliveries.py`
  - File: `tests/db/repositories/test_deliveries.py`
  - Test: `claim_one` returns row and marks status=sending
  - Test: concurrent claim from 2 connections — only one gets the row
  - Test: `reclaim_expired_leases` converts stale sending rows back to pending
  - Test: `mark_retry` increments `attempt_count` and sets `next_attempt_at` per exponential backoff
  - Test: `mark_abandoned` sets terminal status
  - Test: `INSERT OR IGNORE` prevents duplicates with same `(endpoint_id, idempotency_key)`
  - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6, 3.8_

- [ ] 27. Unit tests for `WatchRepository`, `SubscriptionRepository`, `EndpointRepository` in `tests/db/repositories/test_endpoints_watches_subs.py`
  - File: `tests/db/repositories/test_endpoints_watches_subs.py`
  - Test: Endpoint disable/enable cycle
  - Test: Watch `close`, `list_open` filters
  - Test: Endpoint deletion CASCADE removes subscriptions
  - _Requirements: 1.5, 4.1, 4.5, 4.6, 10.6_

### A.3 既存 history.py 呼び出しサイト移行(PR 1)

**目的**: `~/.srunx/history.db` への旧来の読み書きを全て `src/srunx/db/repositories/jobs.py`(`JobRepository`)経由に置き換える。移行漏れがあると旧 DB が再作成されて SSOT が割れる(Codex Critical #1 対応)。

- [ ] 27a. Add `get_job_repo()` helper in `src/srunx/db/__init__.py`
  - File: `src/srunx/db/__init__.py`(modify)
  - Export a convenience function that opens a connection and returns a `JobRepository` bound to it (context-manager style)
  - Purpose: 既存 call site 置換を短い1行差分に収める
  - _Requirements: 5.1, 5.2, 5.3, 5.5_

- [ ] 27b. Replace `history` usage in `src/srunx/client.py`
  - File: `src/srunx/client.py`(modify, 1 file)
  - Lines `151-156`(submit 時の history 書き込み)と `337-341`(完了時)を `JobRepository.record_submission(..., submission_source='cli')` / `JobRepository.update_status(...)` に置換
  - Purpose: CLI submit/retrieve 経路を新 DB に寄せる
  - _Leverage: `src/srunx/db/repositories/jobs.py`_
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 27c. Replace `history` usage in `src/srunx/runner.py`
  - File: `src/srunx/runner.py`(modify, 1 file)
  - Lines `721-724` 周辺の history 呼び出しを `JobRepository` 経由に置換
  - `WorkflowRunner` が生成するジョブは引き続き `triggered_by='cli'` + `submission_source='workflow'` で記録(CLI 起動 workflow run 自体の DB 永続化は Phase 2 だが、ジョブ単位の履歴は Phase 1 で揃える)
  - _Leverage: `src/srunx/db/repositories/jobs.py`_
  - _Requirements: 5.2, 5.3_

- [ ] 27d. Replace `history` usage in `src/srunx/cli/main.py`
  - File: `src/srunx/cli/main.py`(modify, 1 file)
  - Lines `1191-1281` 周辺の history コマンド実装(`srunx history` 等)を `JobRepository.list/get` 経由に置換
  - 表示ロジック / フォーマットは変更せず、データソースのみ差し替える
  - _Leverage: `src/srunx/db/repositories/jobs.py`_
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 27e. Replace `history` usage in `src/srunx/web/routers/history.py`
  - File: `src/srunx/web/routers/history.py`(modify, 1 file)
  - Lines `18-48` の API 実装を `JobRepository` 経由に置換
  - レスポンス shape は変更しないようにする(frontend 互換維持)
  - _Leverage: `src/srunx/db/repositories/jobs.py`_
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 27f. Replace `history` usage in `src/srunx/web/deps.py`
  - File: `src/srunx/web/deps.py`(modify)
  - Lines `60-61` の `get_history` dependency を `get_job_repo` ベースに書き換え
  - 既存の dependency injection を使っている側のシグネチャも合わせて一括更新
  - _Leverage: `src/srunx/db/repositories/jobs.py`_
  - _Requirements: 5.1, 5.2, 5.3_

- [ ] 27g. Mark `src/srunx/history.py` as deprecated shim
  - File: `src/srunx/history.py`(modify)
  - Replace all top-level functions with deprecation warnings that raise `DeprecationWarning` and delegate to `JobRepository` equivalents via a lazy-opened connection (backward-compat shim for any missed call site)
  - Add module docstring: "Deprecated. Use `srunx.db.repositories.jobs.JobRepository` instead. This module will be removed in a future version."
  - Purpose: Any missed caller surfaces as a warning, not a silent split-brain
  - _Requirements: 5.1, 5.4_

- [ ] 27h. Grep-verify no remaining `srunx.history` imports outside shim and tests
  - Run: `grep -rn "from srunx.history\|import srunx.history" src/ tests/`
  - Expected result: only `src/srunx/history.py`(shim 自身)と `tests/` 内の互換性テストのみ参照している状態
  - If any other site is found: **stop and file a new task in this document**(do NOT modify the site inside this task — keeps task atomic)
  - Purpose: Gating verification of 27b-g completeness; detects missed call sites
  - _Requirements: 5.1_

- [ ] 27i. Shared pytest DB fixture in `tests/conftest.py`
  - File: `tests/conftest.py`(modify — extend existing file)
  - Fixture `tmp_srunx_db`(scope='function'):
    - Creates a temporary directory via `tmp_path`
    - Monkeypatches `XDG_CONFIG_HOME` to that dir
    - Calls `init_db()` to create and migrate the DB
    - Yields connection + path
    - Closes connection after test
  - **File-backed tmp SQLite, NOT `:memory:`**(WAL + multi-connection と矛盾するため、Codex Medium 指摘対応)
  - Purpose: All new tests (db/, notifications/, pollers/, e2e/) share a single consistent DB bootstrap
  - _Leverage: `tmp_path`, `monkeypatch` built-in fixtures_
  - _Requirements: test 基盤整合_

### B. Notifications ドメイン(PR 2)

- [ ] 28. Create `src/srunx/notifications/__init__.py` as empty marker
  - File: `src/srunx/notifications/__init__.py`
  - Purpose: Package initialization
  - _Requirements: Structure (CLAUDE.md conventions)_

- [ ] 29. Extract `sanitize_slack_text` to `src/srunx/notifications/sanitize.py`
  - File: `src/srunx/notifications/sanitize.py`
  - Copy body of `SlackCallback._sanitize_text` as a standalone `sanitize_slack_text(text: str) -> str`
  - Preserve exact current replacement table (HTML entities, markdown escapes, control chars)
  - Purpose: Share sanitizer between CLI callback and new delivery adapter
  - _Leverage: `src/srunx/callbacks.py:_sanitize_text`_
  - _Requirements: Security NFR_

- [ ] 30. Update `src/srunx/callbacks.py` to use `notifications.sanitize`
  - File: `src/srunx/callbacks.py`(modify)
  - Replace `_sanitize_text` body with `from srunx.notifications.sanitize import sanitize_slack_text; return sanitize_slack_text(text)`
  - Keep method signature for backward compat
  - Purpose: Eliminate duplication while preserving CLI behavior
  - _Leverage: `src/srunx/notifications/sanitize.py`_
  - _Requirements: Security NFR_

- [ ] 31. Create preset filter in `src/srunx/notifications/presets.py`
  - File: `src/srunx/notifications/presets.py`
  - `should_deliver(preset: str, event_kind: str, to_status: str | None) -> bool`
  - Truth table: `terminal` → terminal status_changed only; `running_and_terminal` → RUNNING + terminal; `all` → all; `digest` → returns False (digest is Phase 2, flag only for now)
  - Pure function, zero dependencies
  - Purpose: Centralize preset matching logic
  - _Requirements: 1.2, 1.3, 1.4, 1.6_

- [ ] 32. Unit tests for `presets.py` in `tests/notifications/test_presets.py`
  - File: `tests/notifications/test_presets.py`
  - Test all 4 presets × 5 event kinds truth table
  - Test `terminal` does not fire for `job.submitted` or RUNNING
  - _Requirements: 1.2, 1.3, 1.4, 1.6_

- [ ] 33. Create `DeliveryAdapter` protocol in `src/srunx/notifications/adapters/base.py`
  - File: `src/srunx/notifications/adapters/__init__.py`, `src/srunx/notifications/adapters/base.py`
  - `class DeliveryAdapter(Protocol): kind: str; def send(event: Event, endpoint_config: dict) -> None`
  - Custom exception `DeliveryError`
  - Purpose: Abstract contract for delivery channels
  - _Requirements: 3.3, Phase 1 scope_

- [ ] 34. Create `SlackWebhookDeliveryAdapter` in `src/srunx/notifications/adapters/slack_webhook.py`
  - File: `src/srunx/notifications/adapters/slack_webhook.py`
  - Class with `kind='slack_webhook'` and `send(event, endpoint_config)`:
    - Parse `webhook_url` from endpoint_config
    - Build Slack blocks from event kind+payload using `sanitize_slack_text`
    - Instantiate `slack_sdk.WebhookClient(webhook_url)` and call `send(text=..., blocks=...)`
    - Check response status; raise `DeliveryError` on non-OK
  - Purpose: Concrete Slack delivery implementation
  - _Leverage: `src/srunx/notifications/sanitize.py`, `slack_sdk.WebhookClient`_
  - _Requirements: 3.3, 8.5, Security NFR_

- [ ] 35. Unit tests for `slack_webhook` adapter in `tests/notifications/adapters/test_slack_webhook.py`
  - File: `tests/notifications/adapters/test_slack_webhook.py`
  - Mock `slack_sdk.WebhookClient.send`; verify payload structure per event kind
  - Test sanitization applied to job name containing `<`, `&`, control chars
  - Test non-OK response raises `DeliveryError`
  - _Requirements: 3.3, 8.5, Security NFR_

- [ ] 36. Create `NotificationService` in `src/srunx/notifications/service.py`
  - File: `src/srunx/notifications/service.py`
  - Class `NotificationService(watch_repo, subscription_repo, event_repo, delivery_repo, endpoint_repo, now_iso_fn)`
  - Methods:
    - `fan_out(event, conn)`: query open watches matching `event.source_ref`; for each, query subscriptions with endpoint NOT disabled; apply `should_deliver` preset filter; compute `idempotency_key`; insert via `delivery_repo.insert` (INSERT OR IGNORE)
    - `create_watch_for_job(job_id, endpoint_id, preset) -> int`
    - `create_watch_for_workflow_run(run_id, endpoint_id | None, preset | None) -> int`(endpoint/preset optional to support auto-watch)
  - `_idempotency_key(event)` deterministic per design policy
  - Purpose: Orchestrate event → deliveries fan-out in a single transaction
  - _Leverage: repositories from tasks 18-22_
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 2.8, 4.4, 4.6, 10.5_

- [ ] 37. Unit tests for `NotificationService` in `tests/notifications/test_service.py`
  - File: `tests/notifications/test_service.py`
  - Test: `fan_out` matches open watches only
  - Test: `fan_out` skips disabled endpoints
  - Test: `fan_out` applies preset filter
  - Test: `fan_out` uses deterministic `idempotency_key` — re-calling with same event produces same deliveries (UNIQUE prevents duplicates)
  - Test: `create_watch_for_workflow_run` without endpoint_id creates watch with no subscription
  - _Requirements: 1.1-1.6, 2.8, 4.6, 10.5_

- [ ] 37a. Create adapter registry in `src/srunx/notifications/adapters/registry.py`
  - File: `src/srunx/notifications/adapters/registry.py`
  - `ADAPTERS: dict[str, DeliveryAdapter] = { 'slack_webhook': SlackWebhookDeliveryAdapter() }` module-level singleton
  - `get_adapter(kind: str) -> DeliveryAdapter` lookup with `KeyError` on unknown
  - Purpose: Centralize adapter discovery for `DeliveryPoller`(Codex Low 対応)
  - _Leverage: `notifications/adapters/slack_webhook.py`_
  - _Requirements: 3.3, 4.1_

### C. Pollers(PR 2)

- [ ] 38. Create `src/srunx/pollers/__init__.py` as empty marker
  - File: `src/srunx/pollers/__init__.py`
  - Purpose: Package initialization
  - _Requirements: Structure (CLAUDE.md conventions)_

- [ ] 39. Create `reload_guard.py` in `src/srunx/pollers/reload_guard.py`
  - File: `src/srunx/pollers/reload_guard.py`
  - Pure functions: `is_reload_mode(env=os.environ, argv=sys.argv) -> bool`; `should_start_pollers(env, argv) -> bool`
  - `is_reload_mode`: True if `UVICORN_RELOAD` env set, or `--reload` in argv
  - `should_start_pollers`: False if reload mode or `SRUNX_DISABLE_POLLER=='1'`
  - Purpose: Testable guard function for poller startup
  - _Requirements: 8.1, 8.2, 8.3_

- [ ] 40. Unit tests for `reload_guard.py` in `tests/pollers/test_reload_guard.py`
  - File: `tests/pollers/test_reload_guard.py`
  - Test all combinations: env var set/unset, `--reload` in argv or not, `SRUNX_DISABLE_POLLER=1/0/unset`
  - Purpose: Cover guard matrix fully
  - _Requirements: 8.1, 8.2, 8.3_

- [ ] 41. Create `PollerSupervisor` in `src/srunx/pollers/supervisor.py`
  - File: `src/srunx/pollers/supervisor.py`
  - `Poller` Protocol with `name`, `interval_seconds`, `async run_cycle()`
  - `PollerSupervisor(pollers, shutdown_event=anyio.Event())`
  - `start_all()`: `anyio.create_task_group()`; per poller, run `_run_with_backoff(poller)` loop that catches exceptions and applies `1→2→4→...→60s` backoff; each cycle checks `shutdown_event` between sleeps
  - `shutdown(grace_seconds=5.0)`: set event; `anyio.move_on_after(grace_seconds)`; on timeout cancel scope
  - Purpose: Long-running task lifecycle with crash recovery and grace shutdown
  - _Requirements: 8.3, 8.4_

- [ ] 42. Unit tests for `PollerSupervisor` in `tests/pollers/test_supervisor.py`
  - File: `tests/pollers/test_supervisor.py`
  - Test: failing poller restarts after backoff; web-level failure not propagated
  - Test: shutdown signal halts pollers within grace period
  - Test: two pollers run concurrently without interference
  - Use dummy pollers that raise/sleep to simulate conditions
  - _Requirements: 8.3, 8.4_

- [ ] 43a. Create `ActiveWatchPoller` skeleton with open-watch loading in `src/srunx/pollers/active_watch_poller.py`
  - File: `src/srunx/pollers/active_watch_poller.py`
  - Constructor takes `slurm_client: SlurmClientProtocol`, repos bundle, `NotificationService`, `interval_seconds=15`, `logger`
  - `async run_cycle()` skeleton: load open watches via `WatchRepository.list_open()`, split by kind (`job` vs `workflow_run`), batch-query `slurm_client.queue_by_ids(job_ids)` via `anyio.to_thread.run_sync` for job watches
  - Emit structured log at cycle start/end with counts (open_watches, transitions_detected, events_emitted, elapsed_ms) — Observability NFR
  - Purpose: Foundation for producer loop; no transition logic yet
  - _Leverage: `src/srunx/client.py` (`Slurm`), `src/srunx/db/repositories/watches.py`_
  - _Requirements: 10.1, 10.2, 10.7, Observability NFR_

- [ ] 43b. Add job transition detection and fan-out to `ActiveWatchPoller`
  - File: `src/srunx/pollers/active_watch_poller.py`(continue from task 43a)
  - For each job whose SLURM status differs from `JobStateTransitionRepository.latest_for_job(job_id)`, within `transaction(conn, 'IMMEDIATE')`:
    - Insert `job_state_transitions` row (`source='poller'`)
    - Call `JobRepository.update_status(job_id, status, started_at, completed_at, duration_secs)` (R5.1 completion persistence)
    - Insert `events(kind='job.status_changed', ...)`; rely on `INSERT OR IGNORE` to skip duplicates (producer-side dedup)
    - If inserted (non-None id), call `NotificationService.fan_out(event, conn)`
    - If terminal (COMPLETED/FAILED/CANCELLED/TIMEOUT), close associated watches
  - Purpose: Core transition-to-delivery producer logic for jobs
  - _Leverage: repositories from A.2, `NotificationService` from task 36_
  - _Requirements: 1.2-1.5, 6.1, 6.2, 10.3, 10.5, 10.6_

- [ ] 43c. Add workflow_run aggregation logic to `ActiveWatchPoller`
  - File: `src/srunx/pollers/active_watch_poller.py`(continue from task 43b)
  - For each open `workflow_run` watch:
    - Query `WorkflowRunJobRepository.list_by_run(run_id)` for child jobs
    - Aggregate child job statuses into workflow-level status per R2 rules (all COMPLETED → completed; any FAILED/TIMEOUT → failed; any CANCELLED → cancelled; otherwise running)
    - If aggregated status differs from current `workflow_runs.status`, within `transaction` run `WorkflowRunRepository.update_status(...)`, insert `events(kind='workflow_run.status_changed', ...)`, fan-out, close watch if terminal
  - Purpose: Workflow-level status detection layered on top of job transitions
  - _Leverage: WorkflowRunRepository, WorkflowRunJobRepository, NotificationService_
  - _Requirements: 2.3, 2.4, 2.5, 10.4_

- [ ] 44. Create `DeliveryPoller` in `src/srunx/pollers/delivery_poller.py`
  - File: `src/srunx/pollers/delivery_poller.py`
  - Constructor takes `delivery_repo`, `endpoint_repo`, `event_repo`, `adapter_registry: dict[str, DeliveryAdapter]`, `worker_id`, `interval_seconds=10`, `max_retries=5`, `logger`
  - `async run_cycle()`:
    1. `delivery_repo.reclaim_expired_leases()`
    2. Loop: `delivery = delivery_repo.claim_one(worker_id)`; break if None
    3. Fetch `event` via `event_repo.get`, `endpoint` via `endpoint_repo.get`, pick adapter by `endpoint.kind`
    4. `await anyio.to_thread.run_sync(adapter.send, event, endpoint.config)`
    5. On success: `mark_delivered(id)`
    6. On `DeliveryError`: if `attempt_count+1 >= max_retries`, `mark_abandoned(id, error)`; else `mark_retry(id, error, backoff_secs)`
  - Emit structured log per cycle with counts (claimed, delivered, retried, abandoned, elapsed_ms) — Observability NFR
  - Purpose: Consumer side — deliver pending deliveries with retry/abandon semantics
  - _Leverage: repositories from task 22, adapters from task 34_
  - _Requirements: 3.2-3.8, 8.5, Observability NFR_

- [ ] 45. Create `ResourceSnapshotter` in `src/srunx/pollers/resource_snapshotter.py`
  - File: `src/srunx/pollers/resource_snapshotter.py`
  - Constructor `(resource_monitor, snapshot_repo, interval_seconds=300, logger)`
  - `async run_cycle()`: call `anyio.to_thread.run_sync(resource_monitor.get_current_snapshot)`; insert via `snapshot_repo.insert`
  - Emit structured log per cycle with (partition, gpus_total, gpus_available, elapsed_ms) — Observability NFR
  - Purpose: Periodically capture cluster GPU state
  - _Leverage: `src/srunx/monitor/resource_monitor.py`_
  - _Requirements: 7.1, 7.2, Observability NFR_

- [ ] 46. Integration tests for pollers in `tests/pollers/test_poller_integration.py`
  - File: `tests/pollers/test_poller_integration.py`
  - With tmp DB and mocked `Slurm`:
    - `ActiveWatchPoller.run_cycle()` detects PENDING→RUNNING, emits event, fan-outs to deliveries
    - Terminal transition closes watch and updates `jobs.status`
    - `DeliveryPoller` with failing adapter retries 5 times and abandons
  - _Requirements: R1, R3, R10_

### D. JobMonitor SSOT 更新(PR 2)

- [ ] 47. Update `src/srunx/monitor/job_monitor.py` to write transitions
  - File: `src/srunx/monitor/job_monitor.py`(modify)
  - Inject optional `JobStateTransitionRepository` in `__init__`; `None` → no-op (preserves existing tests)
  - **Exact hook location**: `job_monitor.py:110-115` — the point where both `previous_status` and `current_status` are simultaneously in scope(NOT `_notify_transition()` which loses `from_status`, nor `watch_continuous()` which is too coarse)
  - Call `repo.insert(job_id, from_status=previous_status, to_status=current_status, source='cli_monitor')` when `previous_status != current_status`
  - Purpose: CLI observations contribute to SSOT without changing user-facing behavior
  - _Leverage: existing JobMonitor internals_
  - _Requirements: 6.1, 10.9_

- [ ] 48. Unit test for JobMonitor SSOT writes in `tests/monitor/test_job_monitor_ssot.py`
  - File: `tests/monitor/test_job_monitor_ssot.py`
  - Inject in-memory repo; run JobMonitor against mocked SLURM with synthetic transitions; verify `insert` calls
  - _Requirements: 6.1_

### E. Web ルータ(PR 3、ただし 48a/48b/59a/59b は PR 2 先行)

- [ ] 48a. Add DB connection provider in `src/srunx/web/deps.py`
  - File: `src/srunx/web/deps.py`(modify)
  - Add FastAPI dependency `get_db_conn()` (contextmanager-style or generator) that opens a new sqlite3 connection with PRAGMAs applied for each request, and closes it at request end
  - **Important**: `sqlite3.Connection` is threadlocal default; web request handlers MUST NOT share connections with lifespan poller tasks. Each request gets its own connection. Pollers own their own connection(s) separately.
  - Purpose: Connection ownership strategy(Codex High #3 対応)
  - _Leverage: `src/srunx/db/connection.py:open_connection`_
  - _Requirements: 5.5, Non-Functional Reliability_

- [ ] 48b. Add repository dependency factories in `src/srunx/web/deps.py`
  - File: `src/srunx/web/deps.py`(continue from 48a)
  - Add `get_job_repo(conn=Depends(get_db_conn))`, `get_workflow_run_repo(conn=...)`, `get_endpoint_repo(conn=...)`, etc. for each repository used by routers
  - Purpose: Clean DI for router handlers
  - _Requirements: 5.5_

- [ ] 49. Add `default_endpoint_name` and `default_preset` to `SrunxConfig`
  - File: `src/srunx/config.py`(modify)
  - Add to `NotificationDefaults` (or equivalent existing nested model): `default_endpoint_name: str | None = None`, `default_preset: str = 'terminal'`
  - Mark `slack_webhook_url` as deprecated in docstring (kept for migration only)
  - Purpose: Persist user-selected defaults for submit dialog
  - _Requirements: 9.3, Migration NFR_

- [ ] 50. Create `web/routers/endpoints.py` with CRUD endpoints
  - File: `src/srunx/web/routers/endpoints.py`
  - `GET /api/endpoints`, `POST /api/endpoints`, `PATCH /api/endpoints/{id}` (disable/enable/rename), `DELETE /api/endpoints/{id}`
  - Validate Slack webhook URL regex in `POST` body; return 422 on invalid
  - Purpose: Endpoint management API surface
  - _Leverage: `src/srunx/db/repositories/endpoints.py`_
  - _Requirements: 4.1, 4.2, 4.5, 4.6, 9.1, 9.2, Security NFR_

- [ ] 51. Create `web/routers/subscriptions.py` with CRUD endpoints
  - File: `src/srunx/web/routers/subscriptions.py`
  - `GET /api/subscriptions?watch_id=`, `POST /api/subscriptions`, `DELETE /api/subscriptions/{id}`
  - Purpose: Subscription management API surface
  - _Leverage: `src/srunx/db/repositories/subscriptions.py`_
  - _Requirements: 4.4_

- [ ] 52. Create `web/routers/watches.py` read-only listing
  - File: `src/srunx/web/routers/watches.py`
  - `GET /api/watches?open=true|false`
  - Purpose: Observability into current watches
  - _Leverage: `src/srunx/db/repositories/watches.py`_
  - _Requirements: Observability NFR_

- [ ] 53. Create `web/routers/deliveries.py` observability endpoints
  - File: `src/srunx/web/routers/deliveries.py`
  - `GET /api/deliveries?subscription_id=&status=`, `GET /api/deliveries/stuck`
  - Purpose: Visibility into delivery outbox state
  - _Leverage: `src/srunx/db/repositories/deliveries.py`_
  - _Requirements: Observability NFR, Usability NFR_

- [ ] 54a. Update `JobSubmitRequest` schema in `web/routers/jobs.py`
  - File: `src/srunx/web/routers/jobs.py`(modify, lines ~73-79)
  - Remove `notify_slack: bool = False` field
  - Add `notify: bool = False`, `endpoint_id: int | None = None`, `preset: str = 'terminal'`
  - Add Pydantic validator: if `notify` is True, `endpoint_id` MUST be present
  - Purpose: Request schema aligned with new notification model
  - _Requirements: 1.1, 4.3, 9.3, 9.4_

- [ ] 54b. Modify `web/routers/jobs.py` submit handler body
  - File: `src/srunx/web/routers/jobs.py`(continue from 54a, lines ~116-143)
  - **Differencing starts at line 116** (after `result = await anyio.to_thread.run_sync(...)` success)
  - Remove direct Slack-send block (lines ~125-143)
  - New flow:
    1. `JobRepository.record_submission(job, submission_source='web', workflow_run_id=None)` (bug fix R5.1)
    2. If `req.notify`: `watch_id = NotificationService.create_watch_for_job(job_id, req.endpoint_id, req.preset)`
    3. Insert `job.submitted` event via `EventRepository` (inside same `BEGIN IMMEDIATE` transaction as step 2)
    4. If `req.preset == 'all'`, `NotificationService.fan_out` runs; otherwise `events` is recorded but no deliveries generated
  - Inject `get_db_conn`, `get_job_repo`, `NotificationService` factory via `Depends()`
  - Purpose: Integrate notification creation into Web submit and fix history bug
  - _Leverage: repos from A.2, `NotificationService` from task 36, DI from 48a/48b_
  - _Requirements: 1.1, 1.2, 5.1, 5.2, 5.3_

- [ ] 55a. Rewrite `web/routers/workflows.py` POST /workflows/runs — creation + auto-watch
  - File: `src/srunx/web/routers/workflows.py`(modify)
  - Replace the initial handler block:
    1. `WorkflowRunRepository.create(status='pending', triggered_by='web')` returning `run_id`
    2. `WatchRepository.create(kind='workflow_run', target_ref=f'workflow_run:{run_id}')` (auto-watch, always)
    3. If request body includes `notify=True` + `endpoint_id` + `preset`: `SubscriptionRepository.create(watch_id, endpoint_id, preset)`
  - Purpose: Durable workflow run creation + auto-watch for resume
  - _Leverage: `WorkflowRunRepository`, `WatchRepository`, `SubscriptionRepository`_
  - _Requirements: 2.1, 2.8, 10.5_

- [ ] 55b. Per-job submit loop with history + membership writes
  - File: `src/srunx/web/routers/workflows.py`(continue from 55a)
  - In the per-job submit loop:
    1. Submit job to SLURM via adapter, get `job_id`
    2. `JobRepository.record_submission(job, submission_source='workflow', workflow_run_id=run_id)` (order matters for FK)
    3. `WorkflowRunJobRepository.create(workflow_run_id=run_id, job_id=job_id, job_name, depends_on)`
    4. On first successful submit, `WorkflowRunRepository.update_status(run_id, 'running')` + insert `workflow_run.status_changed` event
  - Purpose: Persist membership with correct FK order
  - _Leverage: `JobRepository`, `WorkflowRunJobRepository`, `WorkflowRunRepository`, `EventRepository`_
  - _Requirements: 2.1, 2.2, 2.6, 5.3_

- [ ] 55c. Remove `_monitor_run` and in-memory `run_registry` sites from `web/routers/workflows.py`
  - File: `src/srunx/web/routers/workflows.py`(continue from 55b)
  - **Targeted line ranges(Codex High #4 対応)**: 324-357, 503-543, 626-628, 785-789, 832-901
  - Delete `_monitor_run` function and any `asyncio.create_task`/`BackgroundTasks` that spawn it
  - Replace references to in-memory `run_registry` with `WorkflowRunRepository` calls
  - **`cancel_run()`**(存在する場合): `WorkflowRunRepository.update_status(run_id, 'cancelled')` + `SLURM cancel` + events 挿入に置換
  - Purpose: Eliminate duplicate monitoring loop and in-memory state fully
  - _Leverage: `WorkflowRunRepository`, `WorkflowRunJobRepository`, `EventRepository`_
  - _Requirements: 2.5, 2.5 (full coverage)_

- [ ] 56. Update `web/routers/workflows.py` GET /runs and status endpoints
  - File: `src/srunx/web/routers/workflows.py`(continue from task 55)
  - `GET /runs`: return `WorkflowRunRepository.list()` results, including `running` runs post-restart (R2.9)
  - Remove references to in-memory `run_registry`
  - Update response model to use integer `run_id` and new `status` enum (`pending/running/completed/failed/cancelled`)
  - Purpose: Expose durable run state to Web UI
  - _Requirements: 2.7, 2.9, API contract NFR_

- [ ] 57. Remove `web/state.py` RunRegistry usage
  - File: `src/srunx/web/state.py`(delete or stub out), and any import sites
  - Delete `RunRegistry` class and global `run_registry` singleton
  - Replace remaining call sites (found via grep) with repository calls
  - Purpose: Eliminate in-memory workflow state
  - _Leverage: `WorkflowRunRepository`_
  - _Requirements: 2.5_

- [ ] 58. Update `web/ssh_adapter.py` docstring to clarify recording responsibility
  - File: `src/srunx/web/ssh_adapter.py`(modify)
  - Update `SlurmSSHAdapter.submit_job` docstring to explicitly state that history recording is the caller's (router's) responsibility (clarifies the R5.1 bug fix boundary)
  - No logic change; pure docs improvement
  - Purpose: Prevent future regression where someone re-adds history.record_job inside the adapter
  - _Requirements: 5.1_

- [ ] 59a. Wire DB init + bootstrap into `web/app.py` lifespan
  - File: `src/srunx/web/app.py`(modify)
  - In `lifespan` startup:
    1. `init_db()`(creates and migrates DB, sets 0600 perms)
    2. Open a dedicated lifespan connection via `open_connection()` for bootstrap
    3. `bootstrap_from_config(conn, get_config())` then close
  - Store the config / supervisor references on `app.state` for access from handlers
  - Purpose: DB boot + migration on web start
  - _Leverage: tasks 7, 6_
  - _Requirements: 5.5, Migration NFR_

- [ ] 59b. Build poller list and start `PollerSupervisor` in `web/app.py`
  - File: `src/srunx/web/app.py`(continue from 59a)
  - After DB init:
    1. `should_start = should_start_pollers(os.environ, sys.argv)`
    2. If `should_start`, enumerate pollers per env var:
       - `ActiveWatchPoller` unless `SRUNX_DISABLE_ACTIVE_WATCH_POLLER=1`
       - `DeliveryPoller` unless `SRUNX_DISABLE_DELIVERY_POLLER=1`
       - `ResourceSnapshotter` unless `SRUNX_DISABLE_RESOURCE_SNAPSHOTTER=1`
    3. Construct `PollerSupervisor(pollers)` and `await supervisor.start_all()`
    4. Register `app.state.supervisor = supervisor`
  - In shutdown: `await app.state.supervisor.shutdown(grace_seconds=5.0)`
  - Purpose: Start background pollers with individual env guards
  - _Leverage: tasks 39, 41, 43-45, adapter registry task 37a_
  - _Requirements: 8.1-8.5, 7.2_

- [ ] 59c. Register new routers in `web/app.py`
  - File: `src/srunx/web/app.py`(continue from 59b, lines ~247-253 area)
  - Add `app.include_router(...)` calls for `endpoints`, `subscriptions`, `watches`, `deliveries` routers created in tasks 50-53
  - Purpose: Expose new API endpoints(Codex Medium #11 対応)
  - _Leverage: tasks 50-53_
  - _Requirements: 4.1, 4.4, Observability NFR_

- [ ] 60. API integration tests for new endpoints in `tests/web/test_notifications_api.py`
  - File: `tests/web/test_notifications_api.py`
  - Use FastAPI TestClient with `tmp_srunx_db` fixture from `tests/conftest.py`(task 27i)
  - **NOTE(Codex Medium)**: Use file-backed tmp SQLite, NOT `:memory:`(multi-connection + WAL と矛盾するため)
  - Test: CRUD endpoint flow for `/api/endpoints`, `/api/subscriptions`
  - Test: submit with `notify=true` creates watch/subscription
  - Test: workflow run POST creates workflow_run + auto-watch
  - Test: `GET /runs` returns persisted runs
  - _Leverage: task 27i shared fixture_
  - _Requirements: R1, R2, R4, R9_

### F. Frontend(PR 3)

- [ ] 61. Update `lib/types.ts` for new workflow run shape
  - File: `src/srunx/web/frontend/src/lib/types.ts`(modify)
  - Change `WorkflowRun.id` from string to number; change `status` enum to `'pending'|'running'|'completed'|'failed'|'cancelled'`
  - Add `Endpoint`, `Subscription`, `Delivery` types
  - Purpose: Align TS types with new API contract
  - _Requirements: API contract NFR_

- [ ] 61a. Update `lib/api.ts` client functions
  - File: `src/srunx/web/frontend/src/lib/api.ts`(modify, lines ~73-95, 177-205)
  - `jobs.submit(...)` signature: remove `notifySlack: boolean`, add `notify: boolean`, `endpointId: number | null`, `preset: string`
  - Workflow run endpoints: update response type to new integer run_id / new status enum
  - Add endpoint CRUD functions: `endpoints.list()`, `endpoints.create()`, `endpoints.update()`, `endpoints.delete()`
  - Add subscription CRUD functions
  - Add deliveries query functions for NotificationsCenter
  - Purpose: Frontend API client aligned with new backend(Codex High #7 対応)
  - _Leverage: tasks 50-53 API contracts_
  - _Requirements: API contract NFR, 4.1, 4.4_

- [ ] 61b. Update `pages/WorkflowDetail.tsx` for new run shape
  - File: `src/srunx/web/frontend/src/pages/WorkflowDetail.tsx`(modify, lines ~36-52, 103-149)
  - Replace string UUID `run_id` with integer
  - Replace old status names (`syncing`, `submitting`) with new(`pending`, `running`, `completed`, `failed`, `cancelled`)
  - Update any status rendering / color mapping / icon logic to match new enum
  - Purpose: Workflow detail page aligned with new API(Codex High #7 対応)
  - _Leverage: task 61 types_
  - _Requirements: 2.7, 2.9, API contract NFR_

- [ ] 62. Rewrite `NotificationsTab.tsx` with endpoint list CRUD
  - File: `src/srunx/web/frontend/src/pages/settings/NotificationsTab.tsx`(modify)
  - Remove single webhook URL input
  - Add: endpoints table (kind/name/status), add form with Slack URL validation, row actions (enable/disable/delete)
  - Add: default endpoint and default preset selectors bound to `SrunxConfig`
  - Purpose: Multi-endpoint management UI
  - _Leverage: task 50 API_
  - _Requirements: 9.1, 9.2, 9.3_

- [ ] 63. Update submit UI with notification controls
  - File: `src/srunx/web/frontend/src/components/FileExplorer.tsx`(modify — submit flow currently lives here at lines ~246-520 with `notifySlack` toggle)
  - Replace current `notifySlack` boolean with: notify toggle, endpoint select (disabled if no endpoints from `/api/endpoints`), preset select with values `terminal`/`running_and_terminal`/`all`
  - Show guidance banner when no endpoints exist (R9.4) — disable notify toggle
  - Read defaults (default_endpoint_name, default_preset) from `SrunxConfig` via config API
  - Update `jobs.submit(...)` call signature to pass `{notify, endpoint_id, preset}` instead of `notifySlack` boolean
  - Purpose: Per-submit notification configuration matching new backend API
  - _Leverage: `/api/endpoints` (task 50), `SrunxConfig` defaults (task 49)_
  - _Requirements: 4.3, 9.3, 9.4_

- [ ] 64. Create `NotificationsCenter.tsx` dashboard
  - File: `src/srunx/web/frontend/src/pages/NotificationsCenter.tsx`
  - Sections: subscription list, recent deliveries table (filter by status), stuck pending count
  - Use `/api/deliveries` and `/api/subscriptions`
  - Purpose: Observability UI for notifications
  - _Requirements: Observability NFR, Usability NFR_

- [ ] 65. Update Sidebar / routing to include NotificationsCenter
  - File: `src/srunx/web/frontend/src/components/Sidebar.tsx`(modify), routing config
  - Add nav entry linking to NotificationsCenter
  - Purpose: Expose new dashboard page
  - _Requirements: Observability NFR_

- [ ] 66. Frontend E2E test update for workflow run lifecycle
  - File: Existing Playwright specs under `src/srunx/web/frontend/e2e/`(modify 1-2 files)
  - Update assertions from UUID string to integer, from old status names to new
  - Add coverage for endpoint CRUD and submit-with-notification flow
  - _Requirements: API contract NFR, R9_

### G. 統合テスト(PR 3)

- [ ] 67. E2E test: submit → state transition → delivery in `tests/e2e/test_submit_to_slack.py`
  - File: `tests/e2e/test_submit_to_slack.py`
  - Local HTTP mock Slack; start FastAPI TestClient with real tmp DB
  - Sequence: add endpoint → POST submit (notify=true, preset=terminal) → manually drive `ActiveWatchPoller.run_cycle` with mocked SLURM states → `DeliveryPoller.run_cycle` → verify mock received expected payload
  - _Requirements: R1, R3_

- [ ] 68. E2E test: workflow run resume in `tests/e2e/test_workflow_resume.py`
  - File: `tests/e2e/test_workflow_resume.py`
  - Start app, create workflow run with `running` status + open auto-watch, shut down
  - Restart app; confirm `/runs` returns the run and `ActiveWatchPoller` picks it up on first cycle
  - _Requirements: 2.7, 2.9_

- [ ] 69. E2E test: --reload guard in `tests/e2e/test_reload_guard.py`
  - File: `tests/e2e/test_reload_guard.py`
  - Simulate env with `UVICORN_RELOAD=1`; verify `should_start_pollers` returns False
  - Smoke-run lifespan with guard active; verify no `events` or `deliveries` are written over 5s
  - _Requirements: 8.1, 8.2, 8.3_

- [ ] 70. E2E test: config.json webhook migration in `tests/e2e/test_bootstrap_migration.py`
  - File: `tests/e2e/test_bootstrap_migration.py`
  - Seed `config.json` with `notifications.slack_webhook_url`; run `bootstrap_from_config`; verify endpoint created and `schema_version` row present
  - Run a second time; verify no additional endpoint row
  - Test failure path: pre-create conflicting endpoint name, verify bootstrap does NOT record `schema_version`
  - _Requirements: Migration NFR_

### H. Documentation(PR 3)

- [ ] 71. Update `CLAUDE.md` with new DB location and notification model
  - File: `CLAUDE.md`(modify)
  - Add: new DB path `~/.config/srunx/srunx.db`, removal of `~/.srunx/history.db`
  - Add: notification model (endpoints / watches / subscriptions / deliveries) summary under Architecture
  - Add: env vars `SRUNX_DISABLE_POLLER`, `SRUNX_DISABLE_ACTIVE_WATCH_POLLER`, `SRUNX_DISABLE_DELIVERY_POLLER`, `SRUNX_DISABLE_RESOURCE_SNAPSHOTTER`
  - Purpose: Keep project guidance current
  - _Requirements: Usability NFR (documentation)_

- [ ] 72. Add migration note to README or changelog (if exists)
  - File: Check `README.md` or `CHANGELOG.md`(modify if exists; skip if neither)
  - Note breaking change: old `~/.srunx/history.db` deleted; Slack webhook URL moved from config.json to DB
  - _Requirements: Migration NFR_

### I. Final Cleanup(各 PR の終端で都度実施)

- [ ] 73. Run quality gates and fix failures scoped to this feature
  - Run: `uv run pytest && uv run mypy . && uv run ruff check .`
  - Fix any test / type / lint failures introduced by tasks 1-72
  - Do not expand scope to pre-existing issues in unrelated modules
  - Rerun until all three commands pass
  - Purpose: Pre-commit quality gate per CLAUDE.md convention
  - _Requirements: All_
