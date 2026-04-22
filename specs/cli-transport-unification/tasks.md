# Tasks: CLI Transport 統一リファクタリング

## Prerequisites
- [ ] Spec approved: `specs/cli-transport-unification/spec.md`
- [ ] Plan approved: `specs/cli-transport-unification/plan.md`
- [ ] 作業は Phase 1 → Phase 8 の順序で進める (各 Phase の Gate を満たしてから次へ)
- [ ] Nice to Have (REQ-N1 / REQ-N2) は本イテレーションでは**実装しない** (明示的に out of scope)

## Phase 1: Protocol / 例外階層 (足場)

後続 phase が依存する型・例外を純追加する。既存 CLI 挙動は一切変わらない。

- [ ] T-1.1: `LogChunk` Pydantic モデルを `src/srunx/client_protocol.py` に追加 (REQ-3)
      Files: `src/srunx/client_protocol.py`
      完了条件: `stdout: str`, `stderr: str`, `stdout_offset: int = Field(ge=0)`, `stderr_offset: int = Field(ge=0)` を持ち、負値を reject する unit test が pass。フィールド名は WebUI wire と一致 (`stdout_offset` / `stderr_offset`)。

- [ ] T-1.2: `JobOperationsProtocol` を `src/srunx/client_protocol.py` に追加 (REQ-2, REQ-3)
      Files: `src/srunx/client_protocol.py`
      完了条件: `submit` / `cancel` / `status` / `queue` / `tail_log_incremental` の 5 メソッドを持つ `typing.Protocol`、`@runtime_checkable` 付与。戻り値は全て Pydantic モデル (`RunnableJobType` / `BaseJob` / `LogChunk`)。stdout / TTY 操作はシグネチャに含めない。

- [ ] T-1.3: Transport 例外階層を `src/srunx/exceptions.py` に追加 (REQ-4)
      Files: `src/srunx/exceptions.py`
      完了条件: `TransportError` (base) / `TransportConnectionError` / `TransportAuthError` / `TransportTimeoutError` / `JobNotFound` / `SubmissionError` / `RemoteCommandError` の 7 クラスを追加。`SubmissionError` と `TransportError` の排他セマンティクスを docstring に明記。

- [ ] T-1.4: `runtime_checkable` 付与後の既存テスト回帰確認 (REQ-2)
      Files: `tests/`
      完了条件: `uv run pytest` が全 pass。この時点で `isinstance(Slurm(), JobOperationsProtocol)` は False (未実装) で良い。

### Phase 1 Gate
- [ ] `uv run mypy .` / `uv run ruff check .` / `uv run pytest` が全 pass
- [ ] CLI 挙動は未変化 (`srunx submit echo hi` が従来通り動く)

---

## Phase 2: `Slurm` / `SlurmSSHAdapter` LSP 整流

Protocol に準拠させる。既存メソッド名は alias で温存し、WebUI / MCP 経路を壊さない。

### `Slurm` 側

- [ ] T-2.1: `Slurm.status(job_id) -> BaseJob` を `retrieve` への thin alias として追加 (REQ-2)
      Files: `src/srunx/client.py`
      完了条件: `status = retrieve` または `def status(self, job_id): return self.retrieve(job_id)`。既存 `retrieve` は caller 互換のため残す。

- [ ] T-2.2: `Slurm.tail_log_incremental(job_id, stdout_offset, stderr_offset) -> LogChunk` を実装 (REQ-3)
      Files: `src/srunx/client.py`
      完了条件: local log file を `open` / `seek(offset)` / `read` して `LogChunk` を返す。未読ファイルは空 stdout/stderr + 同一 offset を返す。

- [ ] T-2.3: `Slurm` が 3 Protocol (`JobOperationsProtocol` / `SlurmClientProtocol` / `WorkflowJobExecutorProtocol`) を満たすことを assert する unit test (REQ-2, AC-2.1)
      Files: `tests/test_client.py` (既存) もしくは新規
      完了条件: `assert isinstance(Slurm(), JobOperationsProtocol)` 等が True。

### `SlurmSSHAdapter` 側

- [ ] T-2.4: `SlurmSSHAdapter.submit(job: RunnableJobType) -> RunnableJobType` を新規実装 (REQ-4, AC-4.1)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: Jinja render → sftp upload → sbatch → `job.job_id` をセットして返す。既存 `submit_job(script_content)` は温存。

- [ ] T-2.5: `SlurmSSHAdapter.cancel(job_id)` を新規実装、`cancel_job = cancel` alias を温存 (REQ-4)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: `cancel(未知_id)` で `JobNotFound` を raise (AC 互換)。旧名 `cancel_job` は backcompat alias として残す。

- [ ] T-2.6: `SlurmSSHAdapter.status(job_id: int) -> BaseJob` を新規実装、`get_job_status` alias を温存 (REQ-4, AC-4.2)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: 戻り値が `BaseJob`。未知 ID は `JobNotFound`。`BaseJob.status` の lazy refresh が SSH で local sacct に fallback しないよう対処 (P-14, frozen snapshot もしくは後続 T-2.11 で対処)。

- [ ] T-2.7: `SlurmSSHAdapter.queue(user: str | None) -> list[BaseJob]` を新規実装、`list_jobs` alias を温存 (REQ-4, AC-4.3)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: `user=None` は profile の username を使う。`user="alice"` は明示指定。ゼロ件は空 list を返し例外にしない。

- [ ] T-2.8: `SlurmSSHAdapter.tail_log_incremental(job_id, stdout_offset, stderr_offset) -> LogChunk` を新規実装 (REQ-3, AC-4.4)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: SSH 越しの `wc -c` + `tail -c +offset` で incremental 取得。stdout 直接出力はしない (pure function)。

- [ ] T-2.9: paramiko 系例外を transport 例外にラップ (REQ-4, AC-4.5)
      Files: `src/srunx/web/ssh_adapter.py`
      完了条件: `paramiko.AuthenticationException` → `TransportAuthError`、`paramiko.SSHException` / 接続失敗 → `TransportConnectionError`、`socket.timeout` → `TransportTimeoutError`。paramiko 固有例外が adapter 外部に漏れない。

- [ ] T-2.10: `SlurmSSHAdapter` が 3 Protocol を満たすことを assert する unit test (REQ-2, AC-2.1)
      Files: `tests/test_ssh_adapter.py` 等
      完了条件: `assert isinstance(SlurmSSHAdapter(...), JobOperationsProtocol)` 等が True。

- [ ] T-2.11: `BaseJob.status` lazy refresh の SSH fallback 防止対応 (REQ-3, P-14)
      Files: `src/srunx/models.py` or `src/srunx/web/ssh_adapter.py`
      完了条件: SSH adapter が返す `BaseJob` に対して CLI が属性アクセスしても local `sacct` 呼出が発生しないこと。`JobStatusInfo` 使用 or `model_config = ConfigDict(frozen=True)` 等の選択肢から実装方針を決めて docstring に明記。

### Phase 2 Gate
- [ ] AC-2.1 / AC-4.1 / AC-4.2 / AC-4.3 / AC-4.4 / AC-4.5 の unit test が全 pass
- [ ] 既存 CLI テストが**無修正で** pass (新メソッド追加 + alias 温存のみなので非破壊)
- [ ] `uv run pytest && uv run mypy . && uv run ruff check .` が通る

---

## Phase 3: Migration V5 + Repository + 全 caller 修正 (単一 PR)

**重要制約**: 本 Phase 全体を**単一 PR** で出す。schema 変更と repository API 変更と全 caller 修正を同じ PR にまとめないと、途中 commit で `job_id` 参照がビルドエラーになる。

### Schema

- [ ] T-3.1: `SCHEMA_V5` を `src/srunx/db/migrations.py` に追加 — `jobs` rebuild (REQ-5, AC-5.1, AC-5.2, AC-5.4)
      Files: `src/srunx/db/migrations.py`
      完了条件: `_apply_fk_off_migration` template (`requires_fk_off=True`) を用いて `jobs_v5` CREATE → `INSERT...SELECT` → `DROP` → `RENAME`。新規カラム `transport_type` (NOT NULL DEFAULT `'local'` CHECK IN local/ssh) / `profile_name` (NULL) / `scheduler_key` (NOT NULL DEFAULT `'local'`) + 三項 CHECK 制約 + `UNIQUE(scheduler_key, job_id)`。既存行は全て `('local', NULL, 'local')` backfill。

- [ ] T-3.2: `SCHEMA_V5` に `workflow_run_jobs` rebuild を追加 (REQ-5, AC-5.5)
      Files: `src/srunx/db/migrations.py`
      完了条件: `job_id` カラムを `jobs_row_id` に改名、FK を `jobs(id)` (AUTOINCREMENT PK) へ retarget。backfill は `LEFT JOIN jobs ON old.job_id = jobs.job_id` で `jobs.id` を取得。

- [ ] T-3.3: `SCHEMA_V5` に `job_state_transitions` rebuild を追加 (REQ-5, AC-5.5)
      Files: `src/srunx/db/migrations.py`
      完了条件: `workflow_run_jobs` と同じパターンで FK 先を `jobs.id` に retarget。

- [ ] T-3.4: `watches.target_ref` / `events.source_ref` を `job:N` → `job:local:N` に一括 UPDATE (REQ-5, REQ-8, AC-5.3)
      Files: `src/srunx/db/migrations.py`
      完了条件: 同一 migration transaction 内で UPDATE。適用後に `target_ref NOT LIKE 'job:local:%' AND NOT LIKE 'job:ssh:%:%'` が 0 件。

- [ ] T-3.5: pre-V5 open watch の強制クローズ (REQ-5, P-15 相当)
      Files: `src/srunx/db/migrations.py`
      完了条件: migration 最後に `UPDATE watches SET status='closed', closed_at=<now>, closed_reason='v5_migration' WHERE status='open'`。pre-V5 の WebUI 経由 SSH ジョブが backfill で `'local'` 扱いされて poller が local SLURM に誤問い合わせする事故を防ぐ。

- [ ] T-3.6: profile_name CHECK 制約の追加 (REQ-5, P-12)
      Files: `src/srunx/db/migrations.py`
      完了条件: `jobs.profile_name` に `:` を含まない CHECK 制約、または migration と同時に `add_profile` 側で `:` を reject する validation を実装。

### Repository / Models

- [ ] T-3.7: `src/srunx/db/models.py` の Pydantic row モデルに新列反映 (REQ-5)
      Files: `src/srunx/db/models.py`
      完了条件: `JobRow` に `transport_type` / `profile_name` / `scheduler_key` 追加。`WorkflowRunJobRow` / `JobStateTransitionRow` は `job_id` → `jobs_row_id`。

- [ ] T-3.8: `JobRepository` の signature 書き換え (REQ-5)
      Files: `src/srunx/db/repositories/jobs.py`
      完了条件: `record_submission` / `get` / `update_status` / `update_completion` / `delete` を `(scheduler_key, job_id)` 複合キーまたは `jobs_row_id` ベースに変更。`transport_type` / `profile_name` を保存。

- [ ] T-3.9: `WorkflowRunJobRepository.upsert` / `get` の signature 書き換え (REQ-5)
      Files: `src/srunx/db/repositories/workflow_run_jobs.py`
      完了条件: `job_id` → `jobs_row_id` へ追従。全 `WHERE job_id = ?` → `WHERE jobs_row_id = ?`。

- [ ] T-3.10: `JobStateTransitionRepository.latest_for_job` / `history_for_job` の signature 書き換え (REQ-5)
      Files: `src/srunx/db/repositories/job_state_transitions.py`
      完了条件: 同上。

- [ ] T-3.11: `cli_helpers.record_submission_from_job` に `transport_type` / `profile_name` / `scheduler_key` kwargs 追加 (REQ-5)
      Files: `src/srunx/db/cli_helpers.py`
      完了条件: default は local (既存挙動と互換)。明示指定で SSH 経路の記録が可能。

- [ ] T-3.12: `Slurm.submit()` / `SlurmSSHAdapter.submit()` 内部で `record_submission_from_job` を呼ぶ (REQ-5, DB 記録責務の一元化)
      Files: `src/srunx/client.py`, `src/srunx/web/ssh_adapter.py`
      完了条件: CLI 側での double-record を防止。adapter は自分の `transport_type='ssh'` / `profile_name` / `scheduler_key='ssh:<profile>'` を認識して渡す。

### Caller 修正 (同一 PR 内)

- [ ] T-3.13: Web routers の repository call site を新 signature に追従 (REQ-5)
      Files: `src/srunx/web/routers/jobs.py`, `src/srunx/web/routers/workflows.py`, `src/srunx/web/routers/sweep_runs.py`
      完了条件: 全ての `JobRepository.get(job_id)` 等の呼出が新 signature に合う。wire API は unchanged。

- [ ] T-3.14: Poller の repository call site 追従 (REQ-5)
      Files: `src/srunx/pollers/active_watch_poller.py`
      完了条件: signature 互換を Phase 3 で完了。本格 transport-aware 化は Phase 6。

- [ ] T-3.15: Notifications / CLI notification_setup の追従 (REQ-5)
      Files: `src/srunx/cli/notification_setup.py`, `src/srunx/notifications/service.py`
      完了条件: repository 呼出を新 signature へ。

- [ ] T-3.16: Repository / cli_helper テストの修正 (REQ-10, AC-10.1)
      Files: `tests/` 配下の該当ファイル
      完了条件: signature 変更に追従して全 pass。新列の assertion も追加。

### Migration 検証

- [ ] T-3.17: Migration V5 happy path の integration test (REQ-5, AC-5.1〜5.6)
      Files: `tests/db/test_migrations.py` 等
      完了条件: in-memory SQLite に V4 schema + fixture data → `apply_migrations` → 以下を assert:
      - `PRAGMA table_info(jobs)` に `transport_type` / `profile_name` / `scheduler_key` が含まれ NOT NULL (AC-5.1)
      - `SELECT COUNT(*) FROM jobs WHERE scheduler_key IS NULL` = 0 (AC-5.2)
      - `watches` の legacy 2 セグメント形式が 0 件 (AC-5.3)
      - 同じ `job_id=12345` が `scheduler_key='local'` と `'ssh:dgx'` で同時挿入可能 (AC-5.4)
      - `PRAGMA foreign_key_list` の FK 先が `jobs.id` (AC-5.5)
      - `submission_source` カラム diff なし (AC-5.6)

- [ ] T-3.18: Migration V5 rollback atomicity test (REQ-5, R-1)
      Files: `tests/db/test_migrations.py`
      完了条件: `jobs_v5` 作成後の `INSERT...SELECT` に例外注入 → 元 schema / rows / indexes / FK が完全に残ることを assert。

- [ ] T-3.19: Migration V5 の open watch 強制クローズ test (REQ-5)
      Files: `tests/db/test_migrations.py`
      完了条件: V4 DB に open watch 2 件を含む fixture → V5 migration 後に全 open watch が `status='closed'`, `closed_reason='v5_migration'` であることを assert。

### Phase 3 Gate
- [ ] AC-5.1 / AC-5.2 / AC-5.3 / AC-5.4 / AC-5.5 / AC-5.6 の integration test が全 pass
- [ ] Repository / CLI helper テストが修正込みで pass (AC-10.1)
- [ ] Web routers / poller / notifications の既存機能が無修正 wire API で動く
- [ ] 単一 PR として commit が完結 (途中 commit で `job_id` 参照エラー出ない)

---

## Phase 4: `resolve_transport` / `TransportRegistry`

CLI が使う transport 解決レイヤを新設。この時点では CLI 側はまだ呼ばない。

- [ ] T-4.1: `src/srunx/transport/__init__.py` 新規作成 (REQ-1)
      Files: `src/srunx/transport/__init__.py`
      完了条件: module を成立させる。公開 API (`resolve_transport`, `ResolvedTransport`, `TransportHandle`, `TransportRegistry`) を re-export。

- [ ] T-4.2: `TransportHandle` dataclass を新規実装 (REQ-1, REQ-8)
      Files: `src/srunx/transport/registry.py`
      完了条件: `scheduler_key` / `profile_name` / `transport_type` / `job_ops` / `queue_client` / `executor_factory` / `submission_context` フィールドを持つ `@dataclass(frozen=True)`。

- [ ] T-4.3: `ResolvedTransport` dataclass を新規実装 (REQ-1, REQ-7)
      Files: `src/srunx/transport/registry.py`
      完了条件: `label` / `source` (Literal) / `handle` フィールド + `handle` へのショートカット property。

- [ ] T-4.4: `resolve_transport()` context manager を新規実装 (REQ-1, REQ-7)
      Files: `src/srunx/transport/registry.py`
      完了条件: 優先順 (`--profile` > `--local` > `$SRUNX_SSH_PROFILE` > local fallback)。`--profile` + `--local` 併用を `ClickException` で起動時拒否 (AC-1.2)。`__exit__` で SSH 接続を close。

- [ ] T-4.5: `resolve_transport()` の SSH import を conditional に (R-3)
      Files: `src/srunx/transport/registry.py`
      完了条件: local fallback のみ使う経路で `SlurmSSHAdapter` / paramiko の import が発生しない (CLI 起動時間回帰防止)。

- [ ] T-4.6: `TransportRegistry` 新規実装 (REQ-8)
      Files: `src/srunx/transport/registry.py`
      完了条件: `__init__(local_client, profile_loader, db_connection_factory)` / `resolve(scheduler_key) -> TransportHandle | None` / `known_scheduler_keys() -> set[str]` / `close()` を持つ。`resolve("ssh:nonexistent")` は `None` を返す (AC-8.5)。

- [ ] T-4.7: `resolve_transport` 優先順序の table-driven unit test (REQ-1, AC-1.1〜1.4)
      Files: `tests/transport/test_registry.py`
      完了条件: 5 パターン (flag/env/default/conflict/local 上書き) を網羅。

- [ ] T-4.8: `TransportRegistry.resolve` failure policy の unit test (REQ-8, AC-8.5)
      Files: `tests/transport/test_registry.py`
      完了条件: 未知 profile で `None` 返却、caller が cycle 全体を落とさず warning + skip できることを mock で検証。

### CLI 共通オプション

- [ ] T-4.9: `src/srunx/cli/transport_options.py` 新規作成 (REQ-6)
      Files: `src/srunx/cli/transport_options.py`
      完了条件: `ProfileOpt` / `LocalOpt` / `ScriptOpt` / `QuietOpt` を Annotated 型エイリアスで定義。`--profile` に `-p` 短縮形は割り当てない (P-8)。

- [ ] T-4.10: `emit_transport_banner(resolved, quiet)` 補助関数を実装 (REQ-7, AC-7.3)
      Files: `src/srunx/cli/transport_options.py`
      完了条件: `Console(stderr=True)` で 1 行出力。`source == "default"` では無出力 (AC-10.2 対応)。`quiet=True` で常に無出力。

### Phase 4 Gate
- [ ] AC-1.2 / AC-8.5 の unit test が pass
- [ ] `resolve_transport` table-driven test 全 pass
- [ ] 既存 CLI 挙動未変化 (この phase ではまだ CLI 側から呼ばない)

---

## Phase 5a: CLI 書き換え — main.py コア

`submit` / `cancel` / `status` / `list` / `logs` / `template_apply` を transport 対応に。

- [ ] T-5a.1: `submit` コマンドに `--profile` / `--local` / `--quiet` / `--script` オプション追加 (REQ-1, REQ-6, AC-6.4, AC-6.5)
      Files: `src/srunx/cli/main.py`
      完了条件: `ProfileOpt` / `LocalOpt` / `QuietOpt` / `ScriptOpt` を import して適用。`--script` と command list は Typer callback で排他検査 (AC-6.5)。

- [ ] T-5a.2: `submit` を `resolve_transport()` 経由に書き換え (REQ-1, REQ-6)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:564` 付近の `Slurm()` 直接 new を `with resolve_transport(...) as rt:` に置換。`rt.job_ops.submit(job)` を呼ぶ。`--script` 指定時は `ShellJob` を構築。

- [ ] T-5a.3: `status` コマンドに transport オプション追加 + 書き換え (REQ-1, REQ-6, AC-6.2)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:613` 付近を `resolve_transport()` 経由に。`srunx status --profile foo 12345` が SSH 経由で `BaseJob` 表示 (AC-6.2)。

- [ ] T-5a.4: `list` コマンドに transport オプション追加 + 書き換え (REQ-1, REQ-6, REQ-7, AC-1.3, AC-1.4, AC-7.1, AC-7.2)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:655` 付近を `resolve_transport()` 経由に。`--format json` のとき banner を stdout に絶対に出さない (stderr のみ)。`SRUNX_SSH_PROFILE=foo srunx list` が SSH 経由 (AC-1.3)、`--local` で env を上書き (AC-1.4)。

- [ ] T-5a.5: `cancel` コマンドに transport オプション追加 + 書き換え (REQ-1, REQ-6, AC-6.1)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:734` 付近を `resolve_transport()` 経由に。`srunx cancel --profile foo 12345` が SSH 経由で `scancel` (AC-6.1)。

- [ ] T-5a.6: `logs` コマンドに transport オプション追加 + 書き換え (REQ-1, REQ-6)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:830` 付近を `resolve_transport()` 経由に。follow 実装は `rt.job_ops.tail_log_incremental` を CLI レイヤでループ呼出 (REQ-3、Protocol は pure)。

- [ ] T-5a.7: `template_apply` を `resolve_transport()` 経由に書き換え (REQ-1)
      Files: `src/srunx/cli/main.py`
      完了条件: `main.py:1276` 付近の `Slurm()` 直接 new を置換。

- [ ] T-5a.8: `DebugCallback.on_job_submitted` (line 57) の特殊ケース対応 (REQ-1)
      Files: `src/srunx/cli/main.py`
      完了条件: callback コンテキストの `Slurm().default_template` は `resolve_transport()` 経由ではなく、module-level 定数 or `Slurm._get_default_template()` class method に書き換え。通常 CLI transport 解決経路ではないことを docstring で明記。

- [ ] T-5a.9: **(別 commit)** `main.py:660` の "No jobs in queue" 出力を `format == "json"` 判定後に移す (R-11)
      Files: `src/srunx/cli/main.py`
      完了条件: **Phase 5a 本体とは別 commit**。PR description に "incidental fix" と明記。JSON 出力に pre-print が混ざらなくなる。

- [ ] T-5a.10: CLI E2E — default path golden test (REQ-10, AC-1.1, AC-10.2)
      Files: `tests/cli/test_main_golden.py`
      完了条件: `srunx submit python foo.py` (フラグなし・env なし) の stdout / stderr / exit code が V4/V5 前後で **bytewise 完全一致** (banner が出ないこと含む)。

- [ ] T-5a.11: CLI E2E — conflict test (REQ-1, AC-1.2)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `srunx submit --profile foo --local echo hi` が exit ≠ 0、stderr に「`--profile` と `--local` は同時指定できません」メッセージ。

- [ ] T-5a.12: CLI E2E — env priority test (REQ-1, AC-1.3, AC-1.4)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `SRUNX_SSH_PROFILE=foo srunx list` が SSH mock 経由、`--local` で env を上書きして local 経由。

- [ ] T-5a.13: CLI E2E — JSON purity test (REQ-7, AC-7.1, AC-7.2)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `srunx list --format json --quiet` / `srunx list --format json` の stdout が `jq .` を通る。後者は stderr に banner。

- [ ] T-5a.14: CLI E2E — banner test (REQ-7, AC-7.3)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `srunx submit --profile foo echo hi` の stderr に `→ transport: ssh:foo (from --profile)` が 1 行だけ。

- [ ] T-5a.15: CLI E2E — script mode test (REQ-6, AC-6.4, AC-6.5)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `srunx submit --script train.sh --profile foo` が ShellJob として SSH 投入 (mock)。`srunx submit --script train.sh python foo.py` は exit ≠ 0。

- [ ] T-5a.16: CLI E2E — cancel / status via SSH (REQ-6, AC-6.1, AC-6.2)
      Files: `tests/cli/test_main_transport.py`
      完了条件: `srunx cancel --profile foo 12345` / `srunx status --profile foo 12345` が SSH mock 経由で動く。

### Phase 5a Gate
- [ ] AC-1.1〜1.4 / AC-6.1 / AC-6.2 / AC-6.4 / AC-6.5 / AC-7.1〜7.3 / AC-10.2 の test が pass
- [ ] Golden test (T-5a.10) が完全一致で pass
- [ ] `srunx ssh` サブツリーはこの時点では未変更 (Phase 7 で deprecation)

---

## Phase 5b: CLI 書き換え — monitor + workflow

- [ ] T-5b.1: `monitor jobs` コマンドに transport オプション追加 (REQ-1, REQ-6)
      Files: `src/srunx/cli/monitor.py`
      完了条件: `ProfileOpt` / `LocalOpt` / `QuietOpt` を適用。`monitor.py:116` / `monitor.py:369` の `Slurm()` 2 箇所を `resolve_transport()` 経由に置換。`JobMonitor(client=rt.queue_client)` で明示注入。

- [ ] T-5b.2: `flow run` コマンドに transport オプション追加 (REQ-1, REQ-6)
      Files: `src/srunx/cli/workflow.py`
      完了条件: `--profile` / `--local` / `--quiet` を `_execute_workflow` に追加。

- [ ] T-5b.3: `flow run` で `resolve_transport()` から `executor_factory` / `submission_context` を取得 (REQ-6, AC-6.3)
      Files: `src/srunx/cli/workflow.py`
      完了条件: sweep / 非 sweep の注入点 (line 376, 483, 495) を両方 `executor_factory=rt.executor_factory, submission_context=rt.submission_context` に書き換え。`srunx flow run --profile foo wf.yaml` が SSH 経由で workflow 実行 (AC-6.3, mock)。

- [ ] T-5b.4: `flow run --profile` の `ShellJob.script_path` guard 実装 (REQ-6, P-11, R-6)
      Files: `src/srunx/cli/workflow.py`
      完了条件: `rt.submission_context.mounts` の `local` root 配下にない場合 `ClickException` で exit ≠ 0。WebUI / MCP sweep と同じガード。

- [ ] T-5b.5: `flow run --profile` で mounts なしの場合の warning 出力 (REQ-6)
      Files: `src/srunx/cli/workflow.py`
      完了条件: profile に `mounts` 設定なし → warning を stderr に出す (path は remote 前提)。

- [ ] T-5b.6: CLI E2E — `monitor jobs --profile` (REQ-6)
      Files: `tests/cli/test_monitor_transport.py`
      完了条件: `srunx monitor jobs --profile foo 12345` が SSH mock 経由で動く。

- [ ] T-5b.7: CLI E2E — `flow run --profile` dry-run (REQ-6, AC-6.3)
      Files: `tests/cli/test_workflow_transport.py`
      完了条件: mock SSH で `srunx flow run --profile foo wf.yaml` が executor_factory 経由で動く。

- [ ] T-5b.8: CLI E2E — `flow run` script_path guard (REQ-6)
      Files: `tests/cli/test_workflow_transport.py`
      完了条件: `ShellJob.script_path` が mount 外の場合 exit ≠ 0。

### Phase 5b Gate
- [ ] AC-6.3 の test が pass
- [ ] Phase 5a の golden test が依然 pass (回帰なし)

---

## Phase 6: Poller transport-aware 化

- [ ] T-6.1: `_parse_target_ref(ref)` 実装 (REQ-8, AC-8.2, AC-8.3, AC-8.4)
      Files: `src/srunx/pollers/active_watch_poller.py`
      完了条件: `rsplit` ベースの末尾 job_id 切り出し実装 (P-5)。
      - `"job:local:12345"` → `("local", 12345)` (AC-8.2)
      - `"job:ssh:dgx:12345"` → `("ssh:dgx", 12345)` (AC-8.3)
      - `"job:12345"` (legacy 2 セグメント) → `None` (AC-8.4)
      - malformed → `None`

- [ ] T-6.2: `ActiveWatchPoller.__init__` に `registry: TransportRegistry` を導入 (REQ-8)
      Files: `src/srunx/pollers/active_watch_poller.py`
      完了条件: 既存 `slurm_client` 引数を `registry` に置換。`AWP(registry=registry)` で構築できる。

- [ ] T-6.3: `run_cycle` で watches を `scheduler_key` で group-by (REQ-8, AC-8.1)
      Files: `src/srunx/pollers/active_watch_poller.py`
      完了条件: 各 group ごとに `registry.resolve(scheduler_key).queue_by_ids(ids)` を呼ぶ。戻り値を merge して後段の `_process_job_watches` に渡す。

- [ ] T-6.4: poller の failure isolation (REQ-8, AC-8.5, R-4)
      Files: `src/srunx/pollers/active_watch_poller.py`
      完了条件: 未知 scheduler_key (`registry.resolve()` が `None`) は warning log + skip、次 group へ進む。`TransportConnectionError` catch も同様に group 単位で skip。

- [ ] T-6.5: SSH connect_timeout を 10s に短縮 (Performance)
      Files: `src/srunx/pollers/active_watch_poller.py` or registry
      完了条件: poller 起動時に `SSHSlurmClient.connect_timeout=10` を設定。最悪 1 cycle 時間を ~20s 以内に抑える。

- [ ] T-6.6: `events._extract_source_id` を 3+ セグメント対応 (REQ-8)
      Files: `src/srunx/db/repositories/events.py`
      完了条件: 新文法 `job:local:N` / `job:ssh:profile:N` をパース、legacy 2 セグメントは `None`。

- [ ] T-6.7: Slack webhook adapter の `_id_from_source_ref` を 3+ セグメント対応 (REQ-8)
      Files: `src/srunx/notifications/adapters/slack_webhook.py`
      完了条件: 同上パーサ。

- [ ] T-6.8: `notifications/service.py` の `source_ref` 書込を新文法に (REQ-8)
      Files: `src/srunx/notifications/service.py`
      完了条件: `source_ref = f"job:{scheduler_key}:{job_id}"` 形式。

- [ ] T-6.9: Web routers の `target_ref` 書込を新文法に (REQ-8, R-9)
      Files: `src/srunx/web/routers/jobs.py` (L173, L177)
      完了条件: 呼出側に `scheduler_key` を伝搬して `job:local:N` / `job:ssh:profile:N` を書く。

- [ ] T-6.10: CLI notification_setup の `target_ref` 書込を新文法に (REQ-8)
      Files: `src/srunx/cli/notification_setup.py` (L133, L151)
      完了条件: 同上。

- [ ] T-6.11: `NotificationWatchCallback` / `attach_notification_watch` に `scheduler_key` / `profile_name` kwargs 追加 (REQ-8)
      Files: `src/srunx/callbacks.py`, `src/srunx/notifications/service.py`
      完了条件: watches.target_ref 構築時に scheduler_key を伝搬。

- [ ] T-6.12: Web app lifespan の `ActiveWatchPoller` 構築変更 (REQ-8)
      Files: `src/srunx/web/app.py` (lifespan)
      完了条件: `ActiveWatchPoller(slurm_client=adapter)` を `ActiveWatchPoller(registry=registry)` に置換。`TransportRegistry` を lifespan で 1 instance 構築、`app.state` に保持、shutdown で `registry.close()`。

- [ ] T-6.13: `_parse_target_ref` の unit test (REQ-8, AC-8.2, AC-8.3, AC-8.4)
      Files: `tests/pollers/test_active_watch_poller.py`
      完了条件: 4 パターン (`job:local:N` / `job:ssh:foo:N` / `job:N` / malformed) 全 pass。

- [ ] T-6.14: Poller 1 cycle で複数 scheduler_key に `queue_by_ids` が呼ばれる integration test (REQ-8, AC-8.1)
      Files: `tests/pollers/test_active_watch_poller.py`
      完了条件: `scheduler_key='local'` と `scheduler_key='ssh:dgx'` の両方を含む watches を DB に insert、`run_cycle` mock で両 transport に呼出が入ることを assert。

- [ ] T-6.15: Poller failure policy test (REQ-8, AC-8.5)
      Files: `tests/pollers/test_active_watch_poller.py`
      完了条件: `ssh:foo` watch を残したまま profile 削除 → cycle 全体 crash せず、warning log + 次 group 進行。

- [ ] T-6.16: 既存 poller テスト (`test_active_watch_poller.py`) の修正 + pass 確認 (REQ-2, AC-2.2)
      Files: `tests/pollers/test_active_watch_poller.py`
      完了条件: `Slurm` / `SlurmSSHAdapter` のどちらを `SlurmClientProtocol` として注入しても pass。

### Phase 6 Gate
- [ ] AC-2.2 / AC-8.1〜8.5 の test が pass
- [ ] Web app lifespan が `SRUNX_DISABLE_POLLER=1` / `UVICORN_RELOAD=1` 経路で regression なし

---

## Phase 7: `srunx ssh` deprecation + Web 整合確認

- [ ] T-7.1: `srunx ssh submit` に deprecation warning 追加 (REQ-9, AC-9.1)
      Files: `src/srunx/ssh/cli/commands.py`
      完了条件: コマンド最初で `typer.echo("WARNING: ...", err=True)` を 1 行追加。新 CLI (`srunx submit --script <path> --profile <name>`) を案内。ロジック温存。exit code 0 で従来通り動作 (AC-9.1)。

- [ ] T-7.2: `srunx ssh logs` に deprecation warning 追加 (REQ-9)
      Files: `src/srunx/ssh/cli/commands.py`
      完了条件: 同上パターン。新 CLI (`srunx logs --profile <name> <job_id>`) を案内。

- [ ] T-7.3: `srunx ssh test` / `ssh sync` / `ssh profile *` は変更しない確認 (REQ-9, AC-9.2, AC-9.3)
      Files: `src/srunx/ssh/cli/commands.py`
      完了条件: これらのコマンドに warning を追加しないことを docstring / code review で確認。保守用として残る。

- [ ] T-7.4: Web app dev reload 経路の回帰確認 (REQ-8)
      Files: `src/srunx/web/app.py`
      完了条件: `SRUNX_DISABLE_POLLER=1` / `SRUNX_DISABLE_ACTIVE_WATCH_POLLER=1` / `UVICORN_RELOAD=1` で startup / shutdown が通る (手動確認 or integration test)。

- [ ] T-7.5: MCP サーバー (`src/srunx/mcp/server.py`) 変更なし確認 (REQ-8)
      Files: `src/srunx/mcp/server.py`
      完了条件: 既に transport 抽象に乗っているため Phase 1 では触らない。tool 契約 unchanged を code review で確認。

- [ ] T-7.6: WebUI `/api/jobs/{id}/logs` の wire 契約 unchanged 確認 (REQ-3)
      Files: `src/srunx/web/routers/jobs.py`
      完了条件: Phase 2 で `tail_log_incremental` が Protocol 化されたことで `Slurm` / `SlurmSSHAdapter` 両方が同じ wire 型 (`LogChunk`) を返す。router 側は無変更で OK。

- [ ] T-7.7: Deprecation warning の CLI E2E test (REQ-9, AC-9.1, AC-9.2, AC-9.3)
      Files: `tests/ssh/test_commands_deprecation.py`
      完了条件:
      - `srunx ssh submit foo.sh` の stderr に warning、exit 0 (AC-9.1)
      - `srunx ssh profile list` に warning なし (AC-9.2)
      - `srunx ssh sync` に warning なし (AC-9.3)

### Phase 7 Gate
- [ ] AC-9.1 / AC-9.2 / AC-9.3 の test が pass
- [ ] Web / MCP / WebUI の wire 契約が unchanged

---

## Phase 8: 後方互換検証 + ドキュメント微修正

- [ ] T-8.1: V4 DB fixture 作成 (REQ-10, AC-10.3)
      Files: `tests/fixtures/v4_db/`
      完了条件: V4 schema + 代表的行 (jobs / workflow_runs / watches 等) を含む SQLite fixture。

- [ ] T-8.2: V4 → V5 auto migration integration test (REQ-10, AC-10.3)
      Files: `tests/db/test_migrations.py`
      完了条件: V4 DB fixture を配置 → srunx 起動 → V5 migration 自動適用 → 既存 CLI (`srunx list`) が動作することを assert。

- [ ] T-8.3: 全 AC の verification matrix 最終確認 (REQ-10, AC-10.1)
      Files: (test suite 全体)
      完了条件: Phase 1〜7 の全 AC test が最終 green。`uv run pytest` で全 pass。

- [ ] T-8.4: Quality gate 通過 (REQ-10)
      Files: N/A
      完了条件: `uv run pytest && uv run mypy . && uv run ruff check .` が全 pass。

- [ ] T-8.5: Runbook docstring の整備 (Phase 1 では md ファイル新規作成しない)
      Files: 該当する module の docstring (`src/srunx/transport/registry.py` / `src/srunx/cli/transport_options.py` 等)
      完了条件: manual verification 手順 (実 SSH 環境、V4 DB 自動 migration、deprecation warning 目視) を docstring コメントとして記述。**新規 `.md` は作成しない** (CLAUDE.md の "never create docs" ルール遵守)。

- [ ] T-8.6: 手動検証 (実 SSH 環境、推奨)
      Files: N/A
      完了条件: 実 SSH (dgx サーバー等) で以下を目視確認:
      - `srunx submit --profile dgx python train.py` → 投入成功
      - `srunx status --profile dgx <id>` → 状態表示
      - `srunx logs --profile dgx <id>` → ログ取得
      - V4 DB を `~/.config/srunx/srunx.db` に置いて srunx 起動 → auto-migration
      - `srunx ssh submit` で deprecation warning 表示確認

- [ ] T-8.7: Reviewer subagent による second opinion (CLAUDE.md Review Gate)
      Files: N/A
      完了条件: 全 Phase 終了後、reviewer subagent に回して second opinion を取得、合意形成。

### Phase 8 Gate
- [ ] AC-10.1 / AC-10.2 / AC-10.3 が全 pass
- [ ] Quality gate (`pytest` / `mypy` / `ruff`) 通過
- [ ] Reviewer second opinion 完了

---

## Verification Checklist

### Acceptance Criteria (全 AC)

#### REQ-1
- [ ] AC-1.1: `srunx submit echo hi` (フラグなし) が従来 CLI 互換 (T-5a.10)
- [ ] AC-1.2: `srunx submit --profile foo --local` が起動時エラー (T-5a.11, T-4.7)
- [ ] AC-1.3: `SRUNX_SSH_PROFILE=foo srunx list` が SSH 経由 (T-5a.12)
- [ ] AC-1.4: `--local` が env を上書き (T-5a.12, T-4.7)

#### REQ-2 / REQ-3
- [ ] AC-2.1: `Slurm` / `SlurmSSHAdapter` が `JobOperationsProtocol` を満たす (T-2.3, T-2.10)
- [ ] AC-2.2: poller テストで両実装が `SlurmClientProtocol` として注入可能 (T-6.16)
- [ ] AC-3.1: `tail_log_incremental` が純粋関数 (T-2.2, T-2.8)
- [ ] AC-3.2: follow loop が CLI レイヤにある (T-5a.6)

#### REQ-4
- [ ] AC-4.1: `SlurmSSHAdapter.submit(Job)` が `RunnableJobType` 返却 (T-2.4)
- [ ] AC-4.2: `status(未知)` が `JobNotFound` (T-2.6)
- [ ] AC-4.3: `queue(user="alice")` が `list[BaseJob]` (T-2.7)
- [ ] AC-4.4: `tail_log_incremental` が `LogChunk` (T-2.8)
- [ ] AC-4.5: paramiko 例外が transport 例外にラップ (T-2.9)

#### REQ-5
- [ ] AC-5.1: V5 後の jobs table_info に新列 (T-3.17)
- [ ] AC-5.2: `scheduler_key IS NULL` = 0 (T-3.17)
- [ ] AC-5.3: legacy 2 セグメント残存なし (T-3.17)
- [ ] AC-5.4: 同一 job_id を複数 scheduler_key で保持可能 (T-3.17)
- [ ] AC-5.5: FK 先が `jobs.id` (T-3.17)
- [ ] AC-5.6: `submission_source` diff なし (T-3.17)

#### REQ-6
- [ ] AC-6.1: `srunx cancel --profile foo` が SSH 経由 (T-5a.16)
- [ ] AC-6.2: `srunx status --profile foo` が SSH 経由 (T-5a.16)
- [ ] AC-6.3: `srunx flow run --profile foo` が SSH 経由 (T-5b.7)
- [ ] AC-6.4: `--script --profile foo` が ShellJob/SSH (T-5a.15)
- [ ] AC-6.5: `--script` + command list が起動時エラー (T-5a.15)

#### REQ-7
- [ ] AC-7.1: `--format json --quiet` が純粋 JSON (T-5a.13)
- [ ] AC-7.2: `--format json` (quiet なし) の stdout が純粋 JSON、stderr に banner (T-5a.13)
- [ ] AC-7.3: `--profile foo` の stderr に banner 1 行 (T-5a.14)

#### REQ-8
- [ ] AC-8.1: 複数 scheduler_key group-by (T-6.14)
- [ ] AC-8.2: `_parse_target_ref("job:local:12345")` (T-6.13)
- [ ] AC-8.3: `_parse_target_ref("job:ssh:dgx:12345")` (T-6.13)
- [ ] AC-8.4: `_parse_target_ref("job:12345")` が None (T-6.13)
- [ ] AC-8.5: 未知 profile で registry が None 返却、poller skip (T-4.8, T-6.15)

#### REQ-9
- [ ] AC-9.1: `ssh submit` に warning + exit 0 (T-7.7)
- [ ] AC-9.2: `ssh profile list` に warning なし (T-7.7)
- [ ] AC-9.3: `ssh sync` に warning なし (T-7.7)

#### REQ-10
- [ ] AC-10.1: CLI behavioral + repository テスト全 pass (T-8.3)
- [ ] AC-10.2: default path の golden 一致 (T-5a.10)
- [ ] AC-10.3: V4 → V5 auto migration → CLI 動作 (T-8.2, T-8.6)

### Quality Gates
- [ ] `uv run pytest` 全通過
- [ ] `uv run mypy .` 全通過
- [ ] `uv run ruff check .` 全通過
- [ ] Reviewer subagent second opinion 完了 (T-8.7)

### Out of Scope 確認
- [ ] REQ-N1 (`SRUNX_DEBUG_TRANSPORT=1` トレース) は実装していない (Phase 2+)
- [ ] REQ-N2 (`srunx config show` に transport 候補表示) は実装していない (Phase 2+)
- [ ] active profile config fallback は実装していない (Phase 2+)
- [ ] profile-per-poller は実装していない (Phase 2+)
- [ ] `srunx ssh submit` の即時削除はしていない (deprecation のみ)
- [ ] WebUI / MCP の wire 契約変更なし
- [ ] `submission_source` に transport 軸を混ぜていない
