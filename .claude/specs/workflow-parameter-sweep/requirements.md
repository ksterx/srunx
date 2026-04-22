# Requirements Document

## Introduction

srunx のワークフローは YAML の `args` を Jinja2 で各ジョブフィールドに展開できるが、同じワークフローをパラメータ違いで複数回実行するには YAML を手動でコピーする必要がある。ML 実験の研究者が lr / seed / batch_size などを振る、リソーススケーリングを比較するといった頻出ユースケースで、命名規約と保守コストが増え続ける。

本 spec では、既存の `args` を活用しつつ **matrix 宣言による cross product 展開** を追加する。具体的には:

- **単発引数上書き**: `srunx flow run w.yaml --arg lr=0.01` のように args を CLI から上書きする。
- **Matrix スイープ**: YAML または CLI に `matrix: { lr: [0.001, 0.01, 0.1], seed: [1, 2, 3] }` を書くと、ロード時に 9 セルへ展開する。各セルは独立した workflow_run として materialize される。
- **Web UI 統合**: Run ダイアログの args フォーム上で単一値 / リスト値のトグルが可能になり、リスト化された軸が自動的に matrix の軸になる。

全セルは load-time に `workflow_runs` として永続化されるため、Web UI のランタイム表示・セル単位の retry / logs・再起動耐性 (R2 系) はそのまま機能する。親 sweep は `sweep_runs` テーブルで束ねて 1 レベル上の集約 (status / 成功失敗数 / 代表エラー) を保持し、通知は親 sweep_run 単位で 1 通にまとめる (セル 100 個で Slack 100 通を防ぐ)。

## Alignment with Product Vision

srunx は個人ユーザーがローカルから SLURM クラスタを操作する CLI + Web ツール。チーム共有運用や platform 化は想定しない。本 spec もこの前提に従い、以下の設計判断を採る:

- **後方互換最優先**: `sweep:` ブロックを書かない既存ワークフローは 100 % 従来通り動く (matrix なし = `args` を 1 回だけ展開、`workflow_runs` を 1 行作る)。DB の既存テーブルへの破壊的変更は極小化する。
- **load-time 展開**: 起動時に全セルを materialize する。runtime に動的にセルを追加するパスは Phase 1 スコープ外 (追加したい場合は同一 matrix で親 sweep に追加 submit するルートで対応)。
- **SLURM array には collapse しない**: matrix はリソース軸 (`gpus_per_node`, `memory_per_node` 等) も動かす想定。各セル = 独立 sbatch で統一する。array collapse は将来の最適化余地として残す。
- **階層は 1 段のみ**: `sweep_runs` → `workflow_runs` → `jobs` の 3 階層で固定。sweep の sweep (nested) は Phase 1 スコープ外。
- **既存通知基盤に乗る**: `events` / `watches` / `subscriptions` / `deliveries` の 5 概念は拡張せず、新イベント kind `sweep_run.status_changed` と新 watch kind `sweep_run` を追加するだけで実現する。

Phase 1 では Slack webhook endpoint のみ対応 (既存制約に揃える)。

## Requirements

### Requirement 1: 単発引数上書き (CLI --arg / Web args_override / MCP args)

**User Story:** 研究者として、YAML をコピーせずに一度だけ違うパラメータでワークフローを投げたい。CLI / Web / MCP のいずれからも同じセマンティクスで上書きできてほしい。

#### Acceptance Criteria

1. WHEN ユーザーが `srunx flow run w.yaml --arg lr=0.01 --arg seed=42` を実行する THEN システム SHALL YAML 内の `args.lr` / `args.seed` を対応する値で上書きし、上書き後の args で通常の `_render_jobs_with_args_and_deps` を実行する。
2. WHEN CLI で同じキーが `--arg` に複数回指定される THEN システム SHALL **最後の指定値を採用する**(エラーにしない)。
3. IF `--arg` の値として `python:` プレフィックス付き文字列が渡される AND 呼び出し経路が CLI THEN システム SHALL 既存の `_evaluate_variables` 経由で AST-safe 評価を行う(既存 CLI 仕様の維持)。
4. IF `--arg` の値として `python:` プレフィックス付き文字列が渡される AND 呼び出し経路が Web API または MCP THEN システム SHALL 422 / 入力エラーで拒否する(既存 `_reject_python_args` と同等のガードを適用)。
5. WHEN Web API `POST /api/workflows/{name}/run` のリクエストボディに `args_override: {key: value, ...}` が含まれる THEN システム SHALL `WorkflowRunner.from_yaml(..., args_override=...)` に渡して YAML の `args` とマージする(override が勝つ)。
6. WHEN MCP `run_workflow` ツールに `args: {key: value, ...}` が渡される THEN システム SHALL CLI と同様に args を上書きする(MCP 経路では `python:` を 422 相当で拒否する)。
7. IF `--arg` / `args_override` / `args` で指定されたキーが YAML の `args` に存在しない THEN システム SHALL 新規キーとして追加する(エラーにしない)。Jinja2 レンダリング時に未使用なら副作用なし。

### Requirement 2: Matrix スイープの YAML 宣言

**User Story:** 研究者として、YAML にパラメータリストを書くだけで cross product の全組み合わせをワンショットで投げたい。matrix の軸はリソースを含めて自由に動かせるべき。

#### Acceptance Criteria

1. WHEN YAML の最上位に以下の構造の `sweep:` ブロックがある:
   ```yaml
   sweep:
     matrix:
       <axis_name>: [<value1>, <value2>, ...]
       <axis_name>: [...]
     fail_fast: <bool, default false>
     max_parallel: <int, required>
   ```
   THEN システム SHALL `matrix` の直積(cross product)を展開してセル数 N を決定する。
2. WHEN matrix の軸名が YAML の `args` と衝突する THEN システム SHALL 各セルの実効 args を「YAML `args` をベースに、matrix 軸の値で上書き」したものとする(`deps.<parent>.<key>` より優先度低、`args` 評価ロジック上で matrix 値は通常の args として扱われる)。
3. WHEN 軸名が既存 Jinja2 予約名 (`deps`) と衝突する THEN システム SHALL 422 / WorkflowValidationError で拒否する。
4. WHEN matrix 軸の値リストが空 `[]` である THEN システム SHALL WorkflowValidationError で拒否する(セル 0 になる無意味な展開を未然に防ぐ)。
5. WHEN matrix 軸の値が scalar (str / int / float / bool) 以外 (dict / list / null) である THEN システム SHALL WorkflowValidationError で拒否する(Phase 1 では scalar のみ)。
6. WHEN YAML / CLI 経由で sweep が起動される AND `max_parallel` が未指定または 1 未満 THEN システム SHALL WorkflowValidationError で拒否する(無制限 submit を未然に防ぐ、必須化方針)。Web API 経由のみ、リクエストボディで省略された場合サーバ側で 4 をデフォルト補完する(R7.9, R8.5 に準拠、Web UI は必ずプリフィル値を表示)。
7. WHEN `fail_fast` が未指定 THEN システム SHALL `false` をデフォルトとする。
8. WHEN matrix の cell_count が 1000 を超える THEN システム SHALL WorkflowValidationError で拒否する(運用上の安全弁。根拠: SLURM デフォルト `MaxSubmitJobs=4096` の 1/4 以下として保守的に 1000 を採用、Phase 2 で config 経由で可変化する)。
9. IF YAML に `sweep:` ブロックがない THEN システム SHALL 現行動作(単一 workflow_run, matrix 未展開)を完全に維持する。
10. WHEN `sweep.matrix` が空 dict `{}` (軸ゼロ) THEN システム SHALL WorkflowValidationError で拒否する("matrix must declare at least one axis" メッセージを含める)。
11. WHEN `sweep.matrix` が存在する AND 全軸の要素数が 1 (cross product=1) THEN システム SHALL **sweep モードとして扱い** `sweep_runs` を 1 行、`workflow_runs` を 1 行 materialize する(matrix 宣言の再現性を履歴に残すため、cell_count=1 でも single workflow_run への fallback は行わない)。

### Requirement 3: CLI からの matrix 指定

**User Story:** 研究者として、YAML を修正せずに CLI から軸を追加してスイープ起動したい。既存の YAML 定義の上書きもしたい。

#### Acceptance Criteria

1. WHEN ユーザーが `srunx flow run w.yaml --sweep lr=0.001,0.01,0.1` を実行する THEN システム SHALL lr 軸を持つ matrix として展開する(YAML に `sweep:` がなければ新規作成、あれば **軸単位で上書き**)。
2. WHEN ユーザーが `--sweep` を複数回指定する (`--sweep lr=0.001,0.01 --sweep seed=1,2`) THEN システム SHALL 各軸を組み合わせた cross product に展開する。
3. WHEN ユーザーが `--fail-fast` を指定する THEN システム SHALL `sweep.fail_fast` を `true` に上書きする。
4. WHEN ユーザーが `--max-parallel N` を指定する THEN システム SHALL `sweep.max_parallel` を N に上書きする。CLI で指定しない場合は YAML の値を採用、YAML にもない場合は R2.6 により拒否。
5. WHEN `--sweep` の値に `,` を含むエスケープが必要な値が現れる THEN システム SHALL Phase 1 では **エスケープ未対応** とし、複雑な値は YAML 側で定義する方針を CLI --help に明記する。
6. WHEN ユーザーが `--arg` と `--sweep` を同一キーで同時指定する THEN システム SHALL WorkflowValidationError で拒否する(単一値と軸値は同時に定義できない)。
7. WHEN CLI から matrix が指定される AND YAML に `sweep.matrix` が存在する THEN システム SHALL **軸単位で merge**(CLI `--sweep lr=...` が YAML の lr 軸を置換、YAML にしかない軸はそのまま残す)。
8. WHEN `--arg` / `--sweep` の形式が `KEY=VALUE` でない THEN システム SHALL WorkflowValidationError で拒否する("KEY=VALUE format expected" メッセージ)。`=` が複数出現する場合は最初の `=` で分割する(後続の `=` は値に含める)。
9. WHEN `--sweep KEY=v1,,v3` のように値に空要素が含まれる THEN システム SHALL 空文字列 `""` を要素値として扱う(トリム / 除去しない)。
10. WHEN `--arg KEY=VALUE` の VALUE を内部型に変換する THEN システム SHALL **文字列としてのみ扱う**(int / float / bool への自動キャストはしない)。YAML 側で `lr: 0.01` と書いた場合と数値型の扱いが一致しないため、ドキュメントで明記する。Jinja2 レンダリング先が数値を期待する場合は Jinja2 フィルタ (`{{ lr | float }}`) で明示変換する設計を推奨する。

### Requirement 4: Sweep 実行モデル (セル materialize と並列制御)

**User Story:** srunx 開発者として、セルを load-time に DB に永続化し、`max_parallel` で同時実行セル数を制御したい。各セルは既存 `WorkflowRunner` の実行パスを再利用し、セル単位の failure が他セルに波及しないでほしい。

Sweep は次の状態機械を持つ:

```
pending → running → completed
                 → draining → failed    (fail_fast 発火 or 実行中にキャンセル要求)
                 → draining → cancelled (ユーザー cancel 要求)
                 → failed              (render/submit 段階の失敗、セル起動前)
```

`draining` は「新規セルは起動しないが、既に実行中のセルが完走するのを待つ」中間状態。親 sweep のステータス確定は全実行中セルが終端に到達した後に行う。

#### Acceptance Criteria

1. WHEN sweep が起動される THEN システム SHALL 以下の順で処理する:
   (a) matrix 展開 → N 個のセル args を決定 (失敗時: R4.7 に従い abort、sweep_runs 行は作成しない)
   (b) `sweep_runs` テーブルに 1 行作成 (status='pending', cell_count=N)
   (c) `workflow_runs` テーブルに N 行作成 (各行 `sweep_run_id` = 親 sweep id, status='pending', `args` = セル毎の実効 args)
   (d) ここで DB コミット (sweep_runs + N 個の workflow_runs が同一トランザクション内で永続化される)
   (e) sweep_runs.status を 'running' に更新し `sweep_run.status_changed(to=running)` イベント発火
   (f) `anyio.Semaphore(max_parallel)` で制御しながら N セルを起動
2. WHEN セル N を起動する THEN システム SHALL そのセル専用の `WorkflowRunner` を生成し、セルの実効 args を与え、**事前に materialize 済みの `workflow_run_id` を注入**して実行する。既存 `WorkflowRunner.run()` が内部で `create_cli_workflow_run()` を呼ぶ挙動は、`workflow_run_id` が注入されている場合は skip する(既存 API 拡張)。
3. WHEN セルが完了・失敗・キャンセル・タイムアウトで終了する THEN システム SHALL 同一トランザクション内で以下を atomic に行う:
   (a) `UPDATE workflow_runs SET status=?, completed_at=?, error=? WHERE id=? AND status=?`(現在 status を WHERE 節で指定し optimistic locking で二重更新を抑止)
   (b) UPDATE が 1 行影響した場合のみ `UPDATE sweep_runs SET cells_<from>=cells_<from>-1, cells_<to>=cells_<to>+1 WHERE id=?`
   (c) R6.3 の遷移ルールに合致すれば同一トランザクション内で `events(kind='sweep_run.status_changed', ...)` を insert + NotificationService.fan_out を実行
4. WHEN `fail_fast=false` の sweep 中にいずれかのセルが失敗する THEN システム SHALL 他セルの実行を**継続**する。
5. WHEN `fail_fast=true` の sweep 中にいずれかのセルが失敗する THEN システム SHALL 以下の順で処理する:
   (a) sweep_runs.status を 'draining' に遷移 (sweep_run.status_changed イベントは発火しない — draining は外部向けステータスではなく内部遷移)
   (b) `workflow_runs` テーブルで `sweep_run_id=? AND status='pending'` の未起動セルを atomic UPDATE で 'cancelled' に遷移し、同時に `sweep_runs.cells_pending -= K, cells_cancelled += K`
   (c) 実行中セルには SLURM scancel を発行せず継続(強制 kill はしない)
   (d) 全実行中セルが終端状態に到達した後、R4.6 のルールで最終 status を確定
6. WHEN 全セルが終端状態に到達する THEN システム SHALL 以下のルールで最終 sweep status を決定する(優先度順で評価):
   - ユーザー cancel 要求 (R4.8) を既に受けている → `cancelled` (**user cancel は failed を上書き**)
   - 全セル completed → `completed`
   - 1 セル以上 failed / timeout → `failed`
   - 1 セル以上 cancelled あり、failed / timeout なし → `cancelled`
   - 全セル cancelled → `cancelled`
7. IF matrix 展開 (R4.1.a) または workflow_runs materialize (R4.1.c) でエラーが発生する THEN システム SHALL 同一トランザクションを rollback する(sweep_runs 行も残さない)。その後、別トランザクションで `sweep_runs` に status='failed', error=<原因>, cell_count=0 の行を書き込む(失敗の可視性確保のため)。R4.1.b の行は残さない。
8. WHEN sweep が cancelled される(Web UI `POST /api/sweep_runs/{id}/cancel` または CLI Ctrl+C) THEN システム SHALL:
   (a) sweep_runs.status を 'draining' に遷移
   (b) `workflow_runs` の未起動セル (`status='pending'`) を atomic UPDATE で 'cancelled' に遷移し、sweep カウンタを同期
   (c) 実行中セルは継続(Phase 1 では強制 kill しない)
   (d) 全セル終端到達後、R4.6 の優先度ルールにより sweep_runs.status を 'cancelled' で確定し、`sweep_run.status_changed(to=cancelled)` を発火
9. WHEN セル内の Runner が `WorkflowRunner.run()` の既存 fail-fast 挙動で内部ジョブ失敗を検出する THEN システム SHALL そのセルを 'failed' 扱いで終了させ、他セルには影響させない(セル間の独立性を担保)。
10. WHEN Web server が再起動される AND `sweep_runs.status IN ('pending', 'running', 'draining')` の行がある THEN システム SHALL 起動時に reconciler が走り、`workflow_runs WHERE sweep_run_id=? AND status='pending'` を発見した場合は親 sweep の `max_parallel` に従ってセル起動を再開する。既に実行中のセル(`status='running'`) は既存 `active_watch_poller` が引き続き SLURM 状態から遷移を検出する(crash 回復)。
11. WHEN `max_parallel > cell_count` THEN システム SHALL エラーにせず、実効同時実行数を `min(max_parallel, cell_count)` にクランプする(警告ログのみ)。

### Requirement 5: DB スキーマ拡張

**User Story:** srunx 開発者として、sweep の状態を SQLite に永続化し、再起動耐性とセル単位の独立性を既存パターンで表現したい。

#### Acceptance Criteria

1. WHEN DB マイグレーション V3 が適用される THEN システム SHALL 新規テーブル `sweep_runs` を以下のカラムで作成する:
   - `id` INTEGER PRIMARY KEY AUTOINCREMENT
   - `name` TEXT NOT NULL (YAML の `name` から取得)
   - `workflow_yaml_path` TEXT NULL (Web UI 経由時のみ。CLI 直接実行時は NULL 可)
   - `status` TEXT NOT NULL CHECK (status IN ('pending','running','draining','completed','failed','cancelled'))
   - `matrix` TEXT NOT NULL (JSON, **sweep 起動時点の matrix 宣言を snapshot**。展開後の per-cell args は子 `workflow_runs.args` 各行に保存)
   - `args` TEXT NULL (JSON, matrix 軸以外の base args)
   - `fail_fast` INTEGER NOT NULL DEFAULT 0 CHECK (fail_fast IN (0,1))
   - `max_parallel` INTEGER NOT NULL
   - `cell_count` INTEGER NOT NULL
   - `cells_pending` INTEGER NOT NULL DEFAULT 0
   - `cells_running` INTEGER NOT NULL DEFAULT 0
   - `cells_completed` INTEGER NOT NULL DEFAULT 0
   - `cells_failed` INTEGER NOT NULL DEFAULT 0
   - `cells_cancelled` INTEGER NOT NULL DEFAULT 0
   - `submission_source` TEXT NOT NULL CHECK (submission_source IN ('cli','web','mcp'))
   - `started_at` TEXT NOT NULL
   - `completed_at` TEXT NULL
   - `cancel_requested_at` TEXT NULL (ユーザー cancel 要求時刻、R4.6 の優先度判定に使用)
   - `error` TEXT NULL (render/submit 段階の failure 理由、または代表エラー)
2. WHEN DB マイグレーション V3 が適用される THEN システム SHALL `workflow_runs` テーブルに `sweep_run_id INTEGER NULL REFERENCES sweep_runs(id) ON DELETE SET NULL` カラムと、クエリ用 index (`CREATE INDEX idx_workflow_runs_sweep_run_id ON workflow_runs(sweep_run_id)`) を追加する。
3. WHEN DB マイグレーション V3 が適用される THEN システム SHALL `events.kind` CHECK 制約および `watches.kind` CHECK 制約を table rebuild で拡張し、`sweep_run.status_changed` / `sweep_run` をそれぞれ許可値に含める。table rebuild 時には以下の既存制約を **すべて同一マイグレーション内で復元**する:
   - `events` UNIQUE `(kind, source_ref, payload_hash)` 制約
   - `deliveries` UNIQUE `(endpoint_id, idempotency_key)` 制約
   - 全外部キー制約
   - 全 INDEX
   マイグレーション後、`PRAGMA index_list` / `PRAGMA foreign_key_list` で制約保持を検証するテストを追加する。
4. WHEN `sweep_runs` への全操作は THEN システム SHALL `SweepRunRepository`(新設、`BaseRepository` 継承)経由で行う。既存 `WorkflowRunRepository` のパターン (JSON_FIELDS / DATETIME_FIELDS / `_row_to_model`) に揃える。
5. WHEN セル(子 `workflow_runs`) の status 変更に連動して sweep 集計を更新する THEN システム SHALL `SweepRunRepository.transition_cell(workflow_run_id, from_status, to_status)` の atomic メソッドで行う。このメソッドは単一トランザクション内で (a) `UPDATE workflow_runs SET status=? WHERE id=? AND status=?`(optimistic locking)、(b) 1 行影響した場合のみ sweep カウンタを更新、(c) 0 行の場合は何もしない(二重呼び出し安全)を実行する。
6. IF 既存の `~/.config/srunx/srunx.db` が V1 または V2 スキーマで存在する THEN システム SHALL `apply_migrations` が V3 を冪等に追加する(破壊なし、データ保持。V2=v2_dashboard_indexes は既存の Migration で、V3=v3_sweep_runs が本 spec で新規追加される)。
7. WHEN `sweep_runs` 行が削除される (将来の retention 対応時を想定) THEN システム SHALL ON DELETE SET NULL により子 `workflow_runs` は残るが `sweep_run_id` は NULL になる(履歴の独立性を保つ)。

### Requirement 6: 通知統合 (sweep 単位集約 + 子セル通知抑止)

**User Story:** 研究者として、100 セル sweep で 100 通 Slack が飛ぶのは避けたい。sweep 全体の進行は 1 通で受け取り、必要なときだけ個別セルを開いて詳細を見たい。

#### Acceptance Criteria

1. WHEN sweep が起動される AND ユーザーが通知をオンにしていた THEN システム SHALL 親 `sweep_runs.id` に対する **watch + subscription を 1 組だけ作成**する(watch.kind='sweep_run', target_ref='sweep_run:<id>')。
2. WHEN sweep 配下の各セル(子 `workflow_run`)が作成される THEN システム SHALL 各セルに対して watch を作成する **が、subscription は作成しない**(既存 `NotificationService.create_watch_for_workflow_run` の watch-only 呼び出しパターンをそのまま利用。これにより `NotificationService.fan_out` はセル単位では配送対象を見つけない)。
3. WHEN 子セル (workflow_run) の status が変化する AND その親 sweep_run が存在する THEN システム SHALL **R4.3 の atomic transition 処理の一部として同一トランザクション内で** sweep status 遷移を判定し、以下のタイミングで `sweep_run.status_changed` イベントを発火する:
   - sweep_runs.status='pending' → 'running' (最初のセルが running に入った瞬間、すなわち `cells_pending` が `cell_count-1` に減り `cells_running` が 1 になった時)
   - 全セルが終端到達 (`cells_pending + cells_running = 0`) 時点で R4.6 ルールにより最終 status を確定し `to_status` を載せた event を 1 回発火
   - `draining` 状態への内部遷移では event を発火しない(外部向けステータスではない)

   **責務の所在**: この集計は専用の aggregator poller を新設せず、`active_watch_poller` の子セル status 観測ハンドラ内に組み込む(別 poller は Phase 1 では導入しない)。CLI 直接実行経路で poller が動いていない場合は、sweep コントローラ(Phase 1 で CLI 経路も同プロセス内で走る)が同じロジックを呼ぶ共通ヘルパ関数 `evaluate_and_fire_sweep_status_event(sweep_run_id)` を介して発火する。
4. WHEN `sweep_run.status_changed` イベントが発火される THEN システム SHALL payload に以下を含める:
   - `to_status`: 現在の sweep status (`running` / `completed` / `failed` / `cancelled`)
   - `from_status`: 遷移前の status (`pending` / `running` / `draining`)
   - `cell_count`: 総セル数
   - `cells_completed` / `cells_failed` / `cells_cancelled` / `cells_running` / `cells_pending`: 各 status のカウント
   - `representative_error`: sweep 配下の `status='failed'` な workflow_run のうち、`completed_at` が最も **早い** 行の `error` フィールド。タイが発生した場合は `workflow_run.id` 昇順で決定的に解決する (null 可)
   - `sweep_run_id` / `name`
5. WHEN `sweep_run.status_changed` イベントが subscription にマッチする THEN システム SHALL `should_deliver(preset, 'sweep_run.status_changed', to_status)` の分岐を以下で実装:
   - `preset='terminal'`: to_status が {completed, failed, cancelled} のいずれかなら True
   - `preset='running_and_terminal'`: 上記 + to_status=='running' も True
   - `preset='all'`: 常に True
   - `preset='digest'`: Phase 1 は常に False(既存仕様に揃える)
6. WHEN Slack webhook adapter が `sweep_run.status_changed` を受ける THEN システム SHALL sweep 専用のメッセージフォーマット(sweep 名 / status / セル成功失敗数 / 代表エラー)で送信する。
7. WHEN ユーザーが「セル単位でも通知が欲しい」オプションを将来指定できるようにする THEN システム SHALL Phase 1 では対応しないが、DB スキーマと API 構造はそれを阻害しない(セル watch に subscription を後から足せる設計)。
8. WHEN sweep 配下のセル `workflow_run.status_changed` イベントが発火される THEN システム SHALL そのセルに subscription がないため delivery は作られない(セル単位通知は発生しない)。watch-only 行は `workflow_runs` の履歴表示や再起動後の再開に使われる。

### Requirement 7: Web API エンドポイント

**User Story:** Web UI 開発者として、sweep の作成・一覧・詳細・セル一覧を取得する REST API が欲しい。既存 `workflow_runs` の API パターンに揃えて使いやすくしてほしい。

#### Acceptance Criteria

1. WHEN クライアントが `POST /api/workflows/{name}/run` に `args_override: {...}` と `sweep: { matrix: ..., fail_fast: bool, max_parallel: int }` を含めてリクエストする THEN システム SHALL sweep モードで起動し、`sweep_run_id` を含めた 202 Accepted を返す(ワークフロー名自体は既存の YAML 参照パスをそのまま使う)。
2. WHEN クライアントが `GET /api/sweep_runs` を呼ぶ THEN システム SHALL 全 sweep_runs を新しい順で返す(ページング不要、Phase 1 は最大 200 件で top limit)。
3. WHEN クライアントが `GET /api/sweep_runs/{id}` を呼ぶ THEN システム SHALL 親 sweep_run の全フィールドを返す。
4. WHEN クライアントが `GET /api/sweep_runs/{id}/cells` を呼ぶ THEN システム SHALL `workflow_runs WHERE sweep_run_id={id}` の行を status 順 → 作成順で返す。レスポンスには各セルの `workflow_run_id`, `args`(セル実効値), `status`, `started_at`, `completed_at`, `error` を含める。
5. WHEN クライアントが `POST /api/sweep_runs/{id}/cancel` を呼ぶ THEN システム SHALL R4.8 のキャンセル動作を実行する(未起動セル cancel、実行中セル継続、sweep_runs.status='cancelled')。
6. WHEN 既存 `GET /api/workflows/runs` が呼ばれる THEN システム SHALL 各 `workflow_run` に `sweep_run_id` フィールドを含める(後方互換のため null の場合は省略可または明示的 null で返す)。
7. WHEN sweep 作成時に Python プレフィックス `python:` の値が `args_override` または `sweep.matrix` に含まれる THEN システム SHALL 422 で拒否する(既存ガード適用)。
8. WHEN Web 経由の sweep 作成時に `submission_source='web'` を `sweep_runs` に記録する。MCP 経由なら `submission_source='mcp'`。CLI 経由で起動したものは 'cli'。
9. WHEN Web API のリクエストボディで `sweep.max_parallel` が省略される THEN システム SHALL サーバ側で 4 をデフォルト補完する(Web UI は必ずプリフィル値を表示するため実運用では省略はクライアントバグ扱い、サーバ側のデフォルトは保険)。

### Requirement 8: Web UI (Run ダイアログ拡張と Sweep ビュー)

**User Story:** Web UI ユーザーとして、sweep の起動・監視・詳細ドリルダウンを GUI で完結させたい。個別セルのログ閲覧も直感的にしたい。

#### Acceptance Criteria

1. WHEN ユーザーが Workflow Run ダイアログを開く THEN システム SHALL `args` フォームの各フィールドに「単一値 / リスト」トグルを表示する。単一値モードは従来通り、リストモードはカンマ区切り入力を受ける。
2. WHEN 1 つ以上のフィールドがリストモードに切り替わる THEN システム SHALL それらを `sweep.matrix` 軸として扱い、cell 数 (cross product) をリアルタイムにプレビュー表示する。
3. WHEN `cell_count` が 10 を超える THEN システム SHALL "このスイープは N セルを submit します" の確認ダイアログを表示する(Phase 1 では閾値 10 を固定値、Phase 2 で UI 設定可能化を検討)。
4. WHEN リストモードが 1 つもなく単一値のみ THEN システム SHALL 従来の単発 workflow_run として submit する(sweep_run は作成しない)。
5. WHEN ユーザーが Advanced セクションを展開する THEN システム SHALL `fail_fast` (チェックボックス, 默认 off) と `max_parallel` (数値入力, デフォルト 4) を編集可能にする。
6. WHEN ユーザーが sweep を submit する AND `endpoints` が 1 つ以上登録されている THEN システム SHALL 通知トグル + preset 選択を既存 UI と同じく表示し、sweep レベル watch + subscription を作成する。
7. WHEN ユーザーが左サイドバーから Sweep Runs ページに遷移する THEN システム SHALL `GET /api/sweep_runs` の結果を一覧テーブル(name / status / progress / created_at)で表示する。
8. WHEN ユーザーが sweep 行をクリックする THEN システム SHALL 詳細ページに遷移し、上段に sweep メタ(matrix / fail_fast / max_parallel / status / cell counts)、下段にセルテーブル(axis 値列 × status 列 × actions 列)を表示する。
9. WHEN ユーザーがセル行をクリックする THEN システム SHALL 該当 `workflow_run` の既存詳細ページにドリルダウンする(sweep 固有の新規ページではなく、既存の `workflow_run` 詳細ページを再利用)。
10. WHEN 既存の Run Workflow UI は(sweep なし submit) THEN システム SHALL 100 % 従来通り動作する(後方互換)。

### Requirement 9: MCP ツール互換性

**User Story:** AI エージェントユーザーとして、MCP 経由で sweep を起動したい。既存 `run_workflow` ツールの互換性は維持してほしい。

#### Acceptance Criteria

1. WHEN MCP `run_workflow` ツールに `args: dict | None = None` キーワード引数が渡される THEN システム SHALL CLI の `--arg` と同等に args を上書きする(`python:` 値は拒否)。
2. WHEN MCP `run_workflow` ツールに `sweep: dict | None = None` キーワード引数が渡される THEN システム SHALL YAML 同様の構造 (`{matrix: {...}, fail_fast: bool, max_parallel: int}`) で受け取り、matrix 展開を行う。
3. WHEN 既存の位置引数のみで `run_workflow` が呼ばれる THEN システム SHALL `args=None, sweep=None` として従来動作を維持する(破壊的変更なし)。
4. WHEN sweep モードで MCP 経由実行された THEN システム SHALL レスポンスに `sweep_run_id` を含める(既存 `workflow_run_id` と並存)。

### Requirement 10: ドキュメント + 後方互換性保証

**User Story:** srunx 利用者として、新機能の使い方が CLI help とドキュメントに反映されていてほしい。既存ワークフローが壊れないことを確約してほしい。

#### Acceptance Criteria

1. WHEN `CLAUDE.md` の「Workflow Definition」セクションが更新される THEN システム SHALL `sweep:` 構文、`--arg` / `--sweep` CLI フラグ、`sweep_runs` テーブル、親子階層の説明を追記する。
2. WHEN `srunx flow run --help` が呼ばれる THEN システム SHALL `--arg KEY=VALUE`, `--sweep KEY=v1,v2,...`, `--fail-fast`, `--max-parallel N` の各フラグと使い方を表示する。
3. WHEN 既存の sweep 未使用 YAML ワークフローが `srunx flow run` で実行される THEN システム SHALL Phase 1 以前と完全に同一の挙動で完走する(single workflow_run、sweep_run なし、通知も従来通り)。
4. WHEN 既存のテストスイート `uv run pytest` が実行される THEN システム SHALL すべて PASS する(matrix 機能追加による regression なし)。

## Non-Functional Requirements

### Performance

- **matrix 展開**: cross product 計算は O(N) (全セル生成)。N ≤ 1000 の上限(R2.8)内であれば in-memory 展開で十分。
- **DB 書き込み**: N 個の `workflow_runs` 行 + 1 個の `sweep_runs` 行を同一トランザクションで作成(`BEGIN IMMEDIATE`)。N=1000 で約 100ms 以内を目標。
- **並列 sbatch**: `max_parallel` で制御。SLURM の `MaxSubmitJobs` 超過時のリトライは Phase 1 では行わず、セル単位 'failed' として記録する(将来の改善項目)。
- **sweep ステータス集計**: 各セル status 変化ごとに O(1) のカウンタ UPDATE。aggregator poller の負荷は既存 `active_watch_poller` + ε。

### Security

- **`python:` プレフィックスの境界保持**: Web / MCP 経由の `args_override` / `sweep.matrix` 値は既存 `_reject_python_args` 同等のガードを適用。CLI のみ許可(既存仕様)。
- **matrix 値のサニタイズ**: YAML に書かれた値 / CLI `--sweep` の値は既存の Jinja2 `StrictUndefined` レンダリングに通るため、テンプレート injection はすでに制限されている。
- **DB ファイルパーミッション**: 既存 R5 (notification spec) に準拠(`~/.config/srunx/srunx.db` は 0600)。

### Reliability

- **部分失敗の独立性**: 1 セルの SLURM submit 失敗 / ジョブ失敗は他セルに波及しない(R4.4)。
- **起動前エラーの全体 abort**: matrix 展開 / workflow_runs materialize 段階の失敗は sweep 全体を abort する(R4.7)。
- **Crash 回復**: Web server 再起動後、`sweep_runs.status='running'` かつ `closed_at IS NULL` な子セル watch が残っていれば、`active_watch_poller` が既存パスで自動再開する(既存 R10 と同じ機構)。
- **idempotency**: `sweep_run.status_changed` イベントも既存 `events` の UNIQUE `(kind, source_ref, payload_hash)` に乗るため、重複発火しても重複配送は自動抑止。

### Usability

- **デフォルト値**:
  - `fail_fast`: false
  - `max_parallel`: 必須(不親切だが submit 量を明示的にユーザーに決めさせる設計)
  - Web UI デフォルト初期値は 4
  - `preset`: 既存通り `terminal`
- **プレビュー**: Web UI で cell_count が閾値(10)を超える場合は確認ダイアログ(R8.3)。
- **ドキュメント**: CLAUDE.md に例付きで 1 セクション追記(R10.1)。

### Observability

- **poller ログ**: sweep 集計 poller は各 cycle で処理した sweep 数と status 遷移をログ出力する(既存 `active_watch_poller` のパターンを踏襲)。
- **Web UI での進捗可視化**: sweep 詳細ページでセル progress bar(`cells_completed / cell_count`)を表示。

### Retention / Growth

- **Phase 1 での pruning**: 行わない。sweep_runs テーブルも `resource_snapshots` と同様、個人ローカル前提なので数年分許容。Phase 2 で `delete_older_than(days)` を追加する設計余地あり(ON DELETE SET NULL で子セルは残る)。

### Migration / Compatibility

- **DB マイグレーション**: V1/V2 → V3 は `apply_migrations` に新規 Migration(version=3, name="v3_sweep_runs") を追加。冪等、破壊的変更なし(既存テーブルの CHECK 制約を table rebuild で拡張、データ保持)。
- **既存 YAML ワークフロー**: `sweep:` を書かないワークフローは 100% 従来通り(R2.9, R10.3)。
- **既存 API クライアント**: `GET /api/workflows/runs` のレスポンスに `sweep_run_id` フィールドが増えるのみ(既存フィールドは不変)。
- **既存 CLI / MCP**: `--arg` / `--sweep` / `args` / `sweep` は新規オプション、既存呼び出しには影響なし。
