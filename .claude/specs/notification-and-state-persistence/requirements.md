# Requirements Document

## Introduction

srunx の通知システムと状態永続化を刷新する。現状は以下の欠陥を抱えており、ユーザーが SLURM ジョブを安心して管理できる状態ではない:

- **submit-only 通知**: Web UI で Slack 通知をオンにして submit しても、「SUBMITTED」が1回飛ぶだけで、RUNNING/COMPLETED/FAILED は通知されない。
- **in-memory ワークフロー状態**: `web/state.py` の `RunRegistry` が in-memory のため、Web server 再起動で実行中ワークフローの状態が消失する。Web UI の Runs ページがサーバー再起動後に空になる。
- **Web submit が履歴に記録されない**: `SlurmSSHAdapter.submit_job` が `history.record_job` を呼んでいないバグにより、Web 経由の submit が SQLite 履歴 DB に残らない。
- **状態遷移通知の重複**: `JobMonitor._previous_states` が in-memory のため、再起動後に全ジョブの状態遷移が再通知される(または通知が欠落する)。
- **リソース時系列データ欠落**: `ResourceSnapshot` が都度生成されるのみで蓄積されず、GPU 需要のトレンド分析ができない。

本 spec では、通知配送を耐久性のある outbox パターンで実現し、Watch/Subscription/Endpoint/Event/Delivery の5概念をデータモデルの核とする。同時に、ワークフロー実行状態・ジョブ状態遷移・リソース時系列を同じ SQLite DB に統合し、再起動耐性と観測可能性を獲得する。

## Alignment with Product Vision

srunx は個人ユーザーが自分のローカルマシンから SLURM クラスタを操作するツール。チーム共有運用や platform 化は想定しない。本 spec もこの前提に従い、以下の設計判断を採用する:

- **単一プロセス前提**: `uvicorn --workers 1` 固定、独立デーモンは導入しない。
- **ローカル SQLite のみ**: RDBMS 運用は負担過多なので採用しない。
- **XDG 準拠**: DB と設定ファイルを `~/.config/srunx/` に統一する(既存 `~/.srunx/history.db` は後方互換性なしで削除)。
- **将来の platform 化への拡張余地を確保**: データモデルは Watch/Subscription/Endpoint/Event/Delivery の5概念を分離し、lease 機構も完全実装する(将来独立プロセスへ切り出す際のリファクタを最小化)。

Phase 1 では Slack webhook を唯一の endpoint 種別として実装する。他の種別(generic webhook, email, slack bot)は Phase 2+ で拡張する。

**CLI のスコープ**: Phase 1 では CLI からの submit 時通知は**現状動作を変更しない**(既存の `--slack` フラグと `SlackCallback` 直接生成パスはそのまま残す)。CLI を subscription API に寄せて Web UI と統合するのは Phase 2 で扱う。これは CLI 利用者への regression 回避と Phase 1 PR サイズ抑制のため。

## Requirements

### Requirement 1: ジョブのライフサイクル通知(Web UI)

**User Story:** Web UI からジョブを submit したユーザーとして、SUBMITTED に加えて RUNNING/COMPLETED/FAILED/CANCELLED/TIMEOUT の状態遷移も通知されてほしい。そうすれば別画面を見続けなくてもジョブの進行を追える。

#### Acceptance Criteria

1. WHEN ユーザーが Web UI の submit 時に通知をオンにする THEN システム SHALL そのジョブを対象とする watch と、選択された endpoint を結ぶ subscription を作成する。
2. WHEN ジョブ submit が成功する AND watch が作成されている THEN システム SHALL `job.submitted` イベントを `events` に記録し、subscription の preset が `all` の場合のみ配送する(デフォルト `terminal` では配送しない)。
3. WHEN SLURM 上でジョブの状態が PENDING→RUNNING に遷移する THEN システム SHALL 対応する watch の subscription に従って通知を配送する(preset が `running_and_terminal` または `all` の場合)。
4. WHEN SLURM 上でジョブが COMPLETED/FAILED/CANCELLED/TIMEOUT のいずれかに到達する THEN システム SHALL 対応する watch の subscription に従って通知を配送する(preset が `terminal` / `running_and_terminal` / `all` のいずれの場合も)。
5. WHEN ジョブが終端状態(COMPLETED/FAILED/CANCELLED/TIMEOUT)に到達する THEN システム SHALL 対応する watch の `closed_at` をセットし以降の通知判定から除外する。
6. IF ユーザーが subscription の preset を `terminal` に設定する THEN システム SHALL RUNNING 遷移では通知せず終端状態のみ通知する。
7. WHEN 同一 subscription に対して同じ state transition イベントが重複して観測される THEN システム SHALL idempotency_key により重複配送を抑止する。

### Requirement 2: ワークフロー実行の通知と永続化

**User Story:** Web UI からワークフローを起動したユーザーとして、ワークフロー全体の開始・完了・失敗・キャンセルを通知されてほしい。また Web server を再起動しても実行中のワークフロー状態が失われないでほしい。

#### Acceptance Criteria

1. WHEN ユーザーがワークフローを起動する THEN システム SHALL `workflow_runs` テーブルにレコードを作成し(`status='pending'`)、所属ジョブを `workflow_run_jobs` テーブルに登録する。
2. WHEN ワークフローの最初のジョブ submit が成功する THEN システム SHALL `workflow_runs.status` を `running` に更新し `workflow_run.status_changed` イベント(`from=pending, to=running`)を発火する。
3. WHEN ワークフローの全ジョブが COMPLETED に到達する THEN システム SHALL `workflow_runs.status` を `completed` に更新し `workflow_run.status_changed` イベント(`to=completed`)を発火する。
4. WHEN ワークフロー実行中にいずれかのジョブが FAILED または TIMEOUT で終了する THEN システム SHALL `workflow_runs.status` を `failed` に更新し `workflow_run.status_changed` イベント(`to=failed`, error 詳細付き)を発火する。
5. WHEN ユーザーがワークフローをキャンセルする OR いずれかのジョブが CANCELLED で終了する THEN システム SHALL `workflow_runs.status` を `cancelled` に更新し `workflow_run.status_changed` イベント(`to=cancelled`)を発火する。
6. IF ジョブ submit 自体が失敗する(sbatch エラー等) THEN システム SHALL `workflow_runs.status` を `failed` に更新し、`error` カラムに原因を記録する。
7. WHEN Web server が shutdown する AND workflow_runs に `running` 状態のレコードがある THEN システム SHALL 次回 Web server 起動時に `/runs` API エンドポイントがそれらを返し、Active Watch Poller(R10)が monitoring を再開する。
8. IF ユーザーが workflow_run 単位の watch を作成している THEN システム SHALL `workflow_run.status_changed` イベントに対して subscription に従って通知を配送する。
9. WHEN Web UI の `/runs` ページが開かれる THEN システム SHALL 再起動前の実行中 run を含め、全 workflow_runs を DB から読み込んで表示する(in-memory 消失しない)。

### Requirement 3: 耐久性のある通知配送(Outbox)

**User Story:** srunx ユーザーとして、Slack 側の一時的な障害やネットワーク断があっても、ジョブ完了通知が失われないでほしい。

#### Acceptance Criteria

1. WHEN ジョブまたはワークフローの状態変化が検出される THEN システム SHALL `events` テーブルに1行挿入してからマッチする subscription の fan-out を行う。
2. WHEN イベントが subscription にマッチする THEN システム SHALL `deliveries` テーブルに `pending` 状態の行を1件以上挿入する(配送前)。
3. WHEN poller が `deliveries` の pending 行を処理する THEN システム SHALL まず lease 取得(`leased_until`, `worker_id` を更新)の transaction を commit し、その後に外部送信を実行する(claim と送信は別 transaction)。
4. IF 外部送信が失敗する THEN システム SHALL `attempt_count` を増やし、exponential backoff に基づいた `next_attempt_at` を設定し、`last_error` にエラー内容を記録する。
5. WHEN lease 期限(`leased_until`)が現在時刻を過ぎた delivery が存在する THEN システム SHALL 次の poll cycle で自動的に再取得して処理を継続する(crash 回復)。
6. WHEN `attempt_count` が設定された最大リトライ回数(デフォルト 5)に到達しても失敗が続く THEN システム SHALL `status` を `abandoned` に変更し、以降の処理対象から除外する。
7. WHEN 外部送信が成功する THEN システム SHALL `status` を `delivered` に更新し `delivered_at` を記録する。
8. IF 同じ `(endpoint_id, idempotency_key)` の組み合わせで delivery を挿入しようとする THEN システム SHALL UNIQUE 制約により重複挿入を拒否し、重複配送を起こさない。

### Requirement 4: 通知購読とエンドポイントの管理

**User Story:** ユーザーとして、複数の Slack webhook を登録し、ジョブごとに使い分けたい(例: 短時間ジョブは個人 DM、長時間ジョブはチームチャンネル)。

#### Acceptance Criteria

1. WHEN ユーザーが Settings > Notifications 画面で新しい endpoint を追加する THEN システム SHALL `endpoints` テーブルに新規レコードを作成する(kind, name, 設定 JSON を保存)。
2. WHEN ユーザーが endpoint の webhook URL を入力する THEN システム SHALL Slack webhook URL の正規表現(`^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$`)で検証し、不正な形式ならエラーを返す。
3. WHEN ユーザーがジョブ submit 時に通知をオンにする THEN システム SHALL 使用する endpoint を(複数登録がある場合)選択できる UI を提示する。
4. WHEN ユーザーが subscription を作成する THEN システム SHALL watch×endpoint×preset の組み合わせを `subscriptions` テーブルに保存する。
5. IF endpoint が削除される THEN システム SHALL 参照している subscriptions も ON DELETE CASCADE で削除される。
6. WHEN ユーザーが endpoint を「無効化」する THEN システム SHALL `disabled_at` をセットし、以降の配送対象から除外するが、過去の delivery 履歴は保持する。

### Requirement 5: 統合ジョブ履歴(バグ修正含む)

**User Story:** srunx ユーザーとして、Web UI から submit したジョブも CLI から submit したジョブも、同一の履歴 DB に記録されてほしい。現状 Web 経由の submit が欠落しているバグを修正してほしい。

#### Acceptance Criteria

1. WHEN ユーザーが Web UI からジョブを submit する THEN システム SHALL `jobs` テーブルに記録する(現状は記録されないバグ)。
2. WHEN ジョブがどのルート(CLI / Web / workflow)から submit されたかを記録する THEN システム SHALL `jobs.submission_source` カラム(`'cli'|'web'|'workflow'`)に値を保存する。
3. WHEN ワークフロー実行中にジョブが submit される THEN システム SHALL `jobs.workflow_run_id` に `workflow_runs.id` への参照を保存する。
4. IF 既存 `~/.srunx/history.db` が存在する THEN システム SHALL 起動時にそれを削除する(後方互換性は持たない。無視ではなく削除を選ぶのは、古いファイルが残ると以後のユーザー混乱源になるため)。
5. WHEN DB ファイルパスが決定される THEN システム SHALL `$XDG_CONFIG_HOME/srunx/srunx.db` を使用し、`$XDG_CONFIG_HOME` が未設定の場合は `~/.config/srunx/srunx.db` にフォールバックする(XDG Base Directory Specification 準拠)。

### Requirement 6: ジョブ状態遷移の single source of truth

**User Story:** srunx 開発者として、ジョブ状態遷移の正しい履歴が1箇所にまとまっていてほしい。現状 4箇所(history.jobs、RunRegistry、JobMonitor._previous_states、SLURM 本体)に分散しているため、dedup とトレンド分析が不可能。

#### Acceptance Criteria

1. WHEN `JobMonitor` がジョブの状態変化を検出する THEN システム SHALL `job_state_transitions` テーブルに `from_status`, `to_status`, `observed_at`, `source` を記録する。
2. WHEN poller が状態遷移イベントを生成する THEN システム SHALL `job_state_transitions` の最新行と比較して重複を抑止する(`_previous_states` の in-memory dedup を置き換え)。
3. WHEN `ScheduledReporter._get_historical_counts` 相当の処理が必要になる THEN システム SHALL `job_state_transitions` と `jobs` への SQL クエリで履歴集計を回答可能にする(既存の `sacct` パスは Phase 1 中は共存を許容)。
4. WHEN Web server が再起動される THEN システム SHALL `job_state_transitions` の最新状態を各ジョブの「最後に観測された state」として読み込み、再起動後の初回 poll で重複通知を起こさない。

### Requirement 7: リソース時系列スナップショット

**User Story:** srunx ユーザーとして、GPU の空き状況の履歴を後から参照したい。「先週の水曜 3pm は GPU が空いていたか」のような問いに答えたい。

#### Acceptance Criteria

1. WHEN `ResourceMonitor` または `ScheduledReporter` が `ResourceSnapshot` を生成する THEN システム SHALL `resource_snapshots` テーブルに1行挿入する(timestamp, partition, GPU/node metrics)。
2. WHEN Web server lifespan が起動する AND `SRUNX_DISABLE_RESOURCE_SNAPSHOTTER` が未設定 THEN システム SHALL 定期 ResourceSnapshotter タスクを起動し、デフォルト 5 分間隔でクラスタ全体のスナップショットを取得して `resource_snapshots` に挿入する(Phase 1 で `resource_snapshots` が実質的に蓄積されることを保証)。
3. WHEN partition を指定しないクラスタ全体のスナップショットが取得される THEN システム SHALL `partition` カラムを NULL で保存する。
4. WHEN `gpu_utilization` が必要となる THEN システム SHALL 生成カラム(`GENERATED ALWAYS AS`)で計算する。`gpus_total = 0` の場合の値は **NULL** とする(ゼロ除算による実装固有エラーを避け、集計時に IS NOT NULL で除外可能にするため)。
5. WHEN 蓄積件数が増えすぎる THEN システム SHALL デフォルト保持期間 90 日を超える古いスナップショットを削除する pruning 関数 `delete_older_than(days)` を提供する(Phase 1 では関数提供のみ、自動スケジュール実行は Phase 2 で導入)。

### Requirement 8: Poller の実行モデル

**User Story:** srunx 開発者として、Web server を起動すれば自動的に通知 poller が立ち上がり、開発中(`--reload`)でも二重起動事故を起こさないでほしい。

#### Acceptance Criteria

1. WHEN FastAPI lifespan が起動する AND 環境変数 `SRUNX_DISABLE_POLLER` が unset または `'0'` AND uvicorn が `--reload` フラグなしで起動されている THEN システム SHALL `DeliveryPoller` を1つだけ起動する。
2. IF 環境変数 `SRUNX_DISABLE_POLLER` が `'1'` にセットされている OR uvicorn が `--reload` フラグ付きで起動されている THEN システム SHALL `DeliveryPoller` を起動しない(開発時の二重配送事故を防止)。
3. WHEN uvicorn の `--reload` 状態を判定する THEN システム SHALL 環境変数 `UVICORN_RELOAD`(uvicorn 本体が設定)または sys.argv の検査により判定し、判定ロジックをユニットテスト可能な関数として切り出す。
4. WHEN poller のメインループが例外で終了する THEN システム SHALL supervisor ラッパが exception をログ出力し、exponential backoff(base 1秒、最大 60 秒)で poller ループを再起動する。この再起動は web 全体の lifespan を巻き込まない。
5. WHEN lifespan shutdown が発火する THEN システム SHALL poller に shutdown シグナルを送り、最大5秒の grace period 内で in-flight delivery を1件完走させ、残りの lease は放棄する(次回起動時に lease expiry で自動回収される)。
6. WHEN Slack webhook 送信が行われる THEN システム SHALL `anyio.to_thread.run_sync` 経由で sync な slack-sdk 呼び出しを実行し、イベントループをブロックしない。

### Requirement 9: Notifications 設定 UI の拡張

**User Story:** Web UI ユーザーとして、Settings > Notifications 画面で endpoint を複数追加・削除でき、デフォルトの preset を設定できるようにしてほしい。

#### Acceptance Criteria

1. WHEN ユーザーが Settings > Notifications 画面を開く THEN システム SHALL 登録済みの endpoint 一覧と、それぞれの kind / name / 状態(有効/無効)を表示する。
2. WHEN ユーザーが新しい endpoint を追加する THEN システム SHALL kind = `slack_webhook` のみを Phase 1 で選択可能とし、webhook URL の入力欄と検証を提供する。
3. WHEN ユーザーがジョブ submit 画面で通知トグルをオンにする THEN システム SHALL デフォルト endpoint と preset を `terminal` で初期選択する(設定画面でデフォルトを変更可能)。
4. IF endpoint が1つも登録されていない状態でユーザーが submit 時に通知をオンにする THEN システム SHALL 「endpoint を先に Settings で追加してください」の案内を表示し、通知トグルを有効化できないようにする。

### Requirement 10: Active Watch Poller(イベント生産者)

**User Story:** srunx 開発者として、作成された watch が実際に SLURM を監視して状態遷移を検出し、`events` テーブルにイベントを生み出す「生産者」の責務が明確であってほしい。生産者が曖昧だと outbox(consumer 側)だけ堅牢でも、通知が永久に発火しない。

**Note:** R3 / R8 は consumer 側(fan-out と delivery)の責務、R10 は producer 側の責務を定義する。

#### Acceptance Criteria

1. WHEN Web server lifespan が起動する AND `SRUNX_DISABLE_POLLER` が unset AND `--reload` モードではない THEN システム SHALL `ActiveWatchPoller` を1つ起動する(`DeliveryPoller` とは別タスク、同一 lifespan 内で supervisor ラップ)。
2. WHEN `ActiveWatchPoller` が poll cycle を実行する THEN システム SHALL `watches` テーブルの `closed_at IS NULL` な行をすべて読み込み、対応する SLURM ジョブ状態(job watch)またはワークフロー run 状態(workflow_run watch)を SLURM / DB から取得する。
3. WHEN `ActiveWatchPoller` がジョブの現在状態を `job_state_transitions` の最新行と比較する AND 状態が変化している THEN システム SHALL `job_state_transitions` に新規行を挿入し、対応する `events`(`job.status_changed`)を1件挿入する(同一 transaction 内)。
4. WHEN `ActiveWatchPoller` がワークフロー run の現在状態を評価する AND workflow_runs のステータスが変化している THEN システム SHALL `workflow_runs` を更新し、対応する `events`(`workflow_run.status_changed`)を1件挿入する(同一 transaction 内)。
5. WHEN `ActiveWatchPoller` が新規 `events` を挿入する THEN システム SHALL 同一 transaction 内でマッチする subscription を fan-out し、`deliveries` に `pending` 行を作成する(events と deliveries の結合書き込み原子性を保つ)。
6. WHEN ジョブまたはワークフロー run が終端状態に到達する AND 対応する watch が存在する THEN システム SHALL `watches.closed_at` をセットし、以降の poll cycle で当該 watch を処理対象から除外する。
7. WHEN `ActiveWatchPoller` のポーリング間隔が設定される THEN システム SHALL デフォルト 15 秒で実行する(設定変更可能)。
8. WHEN Web server が再起動される AND `watches.closed_at IS NULL` の行が残っている THEN システム SHALL 再起動後の初回 poll cycle で `job_state_transitions` の最新行を「最後に観測された state」として扱い、すでに観測済みの transition は重複通知しない。
9. WHEN 既存の CLI 側 `JobMonitor.watch_continuous()` が呼ばれる THEN システム SHALL Phase 1 では現状のまま動作させる(CLI は `ActiveWatchPoller` を使わない。両者の統合は Phase 2)。

## Non-Functional Requirements

### Performance

- **DeliveryPoller ポーリング間隔**: デフォルト 10 秒。設定変更可能。
- **ActiveWatchPoller ポーリング間隔**: デフォルト 15 秒。設定変更可能。SLURM への `squeue` 発行頻度を考慮し DeliveryPoller より長めに。
- **ResourceSnapshotter 間隔**: デフォルト 5 分。1 日あたり 288 行、90 日で約 26,000 行(クラスタ全体 1 partition の場合)。個人ローカル SQLite で問題ない規模。
- **DB 書き込みバッチング**: `events` と `deliveries` は同一 transaction で書き込む(`BEGIN IMMEDIATE` + 短時間コミット)。
- **DB ロック保持時間**: 外部送信中は transaction を保持しない。`claim` と `send` は別 transaction に分離する。

### Security

- **Webhook URL の検証**: Slack webhook URL のパターンに合致することを UI とバックエンド双方で検証する(現状のフロント検証を踏襲しつつバックエンドでも再検証)。
- **通知メッセージのサニタイズ**: 現状の `SlackCallback._sanitize_text` を継承し、ジョブ名などに含まれる制御文字・markdown 記号・HTML をエスケープする。
- **SQL injection 防止**: 全 DB アクセスはパラメータ化クエリ経由とし、文字列連結は禁止。
- **DB ファイルパーミッション**: `~/.config/srunx/srunx.db` は 0600(所有者のみ読み書き)。

### Reliability

- **At-least-once 配送**: exactly-once は狙わない。`idempotency_key` で受信側の重複抑止可能性を担保する。
- **Crash 回復**: Web server の crash・再起動・kill -9 後、次回起動時の poll cycle で `leased_until` が過去の delivery を自動回収する。
- **トランザクション境界**: `claim` と外部送信は別 transaction。ネットワーク I/O 中に DB トランザクションを保持しない。
- **WAL + busy_timeout**: SQLite を `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON` で開く。
- **最大リトライ**: デフォルト 5 回。exponential backoff の base 10 秒、最大 1 時間。

### Usability

- **設定の場所**: `~/.config/srunx/config.json`(既存)と `~/.config/srunx/srunx.db`(新規)が同ディレクトリにまとまる。
- **エラー表示**: 通知配送失敗時、該当 delivery の `last_error` を Web UI の Notifications 画面で確認できる(Phase 1 では最低限、該当 subscription に紐づく最新の failed delivery のエラー文を表示する API を提供)。
- **デフォルト値の合理性**: submit 時の通知デフォルト preset は `terminal`(RUNNING は opt-in)。「届きすぎ」を避けるための選択。
- **ドキュメント**: `CLAUDE.md` に新しい DB 位置と通知モデルの概要を追記する(実装タスクに含める)。

### Observability

- **Poller ヘルス**: poller は起動時・各 cycle 完了時に構造化ログを出力し、処理件数・失敗件数・平均レイテンシを記録する。
- **Stuck delivery の可視化**: `pending` 状態で `next_attempt_at < now - 5min` の delivery 件数を取得する API を Phase 1 で提供する(Web UI への組み込みは Phase 2 で対応)。

### Retention / Growth

- **`deliveries` / `events` の成長**: Phase 1 では pruning を自動実行しない。個人ローカル前提かつ `delivered` / `abandoned` 行のサイズが小さいため、数年単位で許容範囲。Phase 2 で `resource_snapshots` と同じ `delete_older_than(days)` パターンを適用する。

### Migration / Compatibility

- **既存 DB の扱い**: `~/.srunx/history.db` が存在する場合は起動時に削除する(R5.4)。後方互換性は持たない(個人ローカル前提のため移行パスは不要)。
- **既存設定の扱い(webhook)**: `config.json` の `notifications.slack_webhook_url` が設定されている場合、起動時に1回限り自動で `endpoints` テーブルに `kind=slack_webhook`, `name='default'` として移行する。移行後は `config.json` の `notifications.slack_webhook_url` フィールドを **deprecated** として扱い、**以降はバックエンドも UI も `endpoints` テーブルを唯一の真実の源とする**(config.json 側に値が残っていても無視し、Settings UI で変更した場合は DB のみを書き換える)。
- **通知設定の Single Source of Truth**: 移行完了後、通知の配送先情報(webhook URL, email アドレス等)は `endpoints` テーブルのみに保持する。`SrunxConfig` / config.json / 環境変数 / submit dialog が独立に Slack URL を持つ split-brain 状態を解消する。
- **v2 スキーマ変更への配慮**: 将来 platform 化に備えて、`deliveries.leased_until` / `worker_id` は Phase 1 から実装する(カラム予約だけで済ませない)。
