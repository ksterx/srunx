# 仕様書：定期レポート機能 (Scheduled Reporting)

## 概要

SLURM環境のジョブ投入状況とリソース利用状況を、指定したスケジュールで定期的にSlackなどに送信する機能を実装する。

既存の`srunx watch`コマンドに統合し、新しい`--schedule`オプションで定期レポートモードを有効化する。

## 目的

- 定期的なクラスタ状態の可視化
- ジョブキューの滞留状況の早期発見
- GPU/ノードリソースの利用傾向把握
- チーム全体への情報共有の自動化

## 機能要件

### 1. コマンド統合

既存の`srunx watch`コマンドに統合：

```bash
# 既存: リアルタイム監視（閾値まで待つ）
srunx watch --resources --min-gpus 4

# 既存: 継続監視（状態変化を通知）
srunx watch --resources --min-gpus 4 --continuous --notify $SLACK_WEBHOOK

# 新規: 定期レポート（スケジュール実行）
srunx watch --schedule 1h --notify $SLACK_WEBHOOK
srunx watch --schedule "0 9 * * *" --notify $SLACK_WEBHOOK
```

### 2. スケジュール設定

#### 2.1 インターバル形式
```bash
# 1時間ごと
srunx watch --schedule 1h --notify $SLACK_WEBHOOK

# 30分ごと
srunx watch --schedule 30m --notify $SLACK_WEBHOOK

# 毎日（24時間ごと）
srunx watch --schedule 1d --notify $SLACK_WEBHOOK
```

**サポート単位**:
- `s`: 秒
- `m`: 分
- `h`: 時間
- `d`: 日

#### 2.2 Cron形式
```bash
# 毎時00分
srunx watch --schedule "0 * * * *" --notify $SLACK_WEBHOOK

# 毎日9:00
srunx watch --schedule "0 9 * * *" --notify $SLACK_WEBHOOK

# 月〜金の9:00と18:00
srunx watch --schedule "0 9,18 * * 1-5" --notify $SLACK_WEBHOOK
```

**Cron形式**: `分 時 日 月 曜日`

### 3. レポート内容

#### 3.1 ジョブ統計
```
📊 Job Queue Status
━━━━━━━━━━━━━━━━━━━━
⏳ PENDING:     12 jobs
🔄 RUNNING:     8 jobs
✅ COMPLETED:   45 jobs (last 24h)
❌ FAILED:      2 jobs (last 24h)
🚫 CANCELLED:   1 job (last 24h)
━━━━━━━━━━━━━━━━━━━━
📈 Total Active: 20 jobs
```

#### 3.2 リソース統計
```
🎮 GPU Resources (partition: gpu)
━━━━━━━━━━━━━━━━━━━━
💾 Total GPUs:    32
⚡ In Use:        24 (75.0%)
✨ Available:     8 (25.0%)
━━━━━━━━━━━━━━━━━━━━
🖥️  Nodes:
  • Total:  8 nodes
  • Idle:   2 nodes
  • Down:   0 nodes
```

#### 3.3 ユーザー別統計（オプション）
```
👤 Your Jobs (user: researcher)
━━━━━━━━━━━━━━━━━━━━
⏳ PENDING:   3 jobs
🔄 RUNNING:   2 jobs
```

### 4. CLI インターフェース

#### 4.1 コマンド拡張

既存の`srunx watch`コマンドに新しいオプションを追加：

```bash
srunx watch [OPTIONS]
```

#### 4.2 新規オプション（定期レポート用）

**必須**:
- `--schedule TEXT`: スケジュール設定（interval形式 or cron形式）
  - Interval: `1h`, `30m`, `1d` など
  - Cron: `"0 * * * *"`, `"0 9 * * *"` など
- `--notify URL`: Slack webhook URL（既存オプションを必須化）

**任意**:
- `--include TEXT`: レポート内容 (デフォルト: jobs,resources,user)
  - `jobs`: ジョブ統計
  - `resources`: リソース統計
  - `user`: ユーザー別統計
  - カンマ区切りで複数指定可能
- `--partition TEXT`: 対象パーティション（リソース統計用、既存）
- `--user TEXT`: 対象ユーザー（デフォルト: 現在のユーザー）
- `--timeframe TEXT`: 完了/失敗ジョブの集計期間 (デフォルト: 24h)
- `--daemon / --no-daemon`: デーモンモードで実行（デフォルト: True）

#### 4.3 モード判定

```python
if schedule is not None:
    # 定期レポートモード
    run_scheduled_reporting()
elif resources:
    # 既存のリソース監視モード
    run_resource_monitoring()
else:
    # 既存のジョブ監視モード（将来実装）
    run_job_monitoring()
```

#### 4.4 使用例

```bash
# 基本的な使用（1時間ごと、全情報）
srunx watch --schedule 1h --notify $SLACK_WEBHOOK

# 毎日9:00にジョブ統計のみ
srunx watch --schedule "0 9 * * *" --notify $SLACK_WEBHOOK --include jobs

# パーティション指定でリソース監視
srunx watch --schedule 30m --notify $SLACK_WEBHOOK --include resources --partition gpu

# 完全な例
srunx watch \
  --schedule 2h \
  --notify $SLACK_WEBHOOK \
  --include jobs,resources,user \
  --partition gpu \
  --user researcher \
  --timeframe 48h \
  --daemon

# 既存機能（リソース監視）は変更なし
srunx watch --resources --min-gpus 4
srunx watch --resources --min-gpus 4 --continuous --notify $SLACK_WEBHOOK
```

### 5. プログラマティックAPI

```python
from srunx import Slurm
from srunx.callbacks import SlackCallback
from srunx.monitor.scheduler import ScheduledReporter
from srunx.monitor.types import ReportConfig

client = Slurm()
callback = SlackCallback(webhook_url)

# インターバル形式
config = ReportConfig(
    interval="1h",
    include=["jobs", "resources", "user"],
    partition="gpu",
    timeframe="24h"
)

reporter = ScheduledReporter(
    client=client,
    callback=callback,
    config=config
)

# バックグラウンドで開始
reporter.start()

# 停止
reporter.stop()
```

```python
# Cron形式
config = ReportConfig(
    cron="0 9 * * *",
    include=["jobs", "resources"]
)

reporter = ScheduledReporter(
    client=client,
    callback=callback,
    config=config
)

# ブロッキングモードで実行
reporter.run()
```

## 技術設計

### 6. アーキテクチャ

#### 6.1 コンポーネント構成
```
src/srunx/monitor/
├── scheduler.py          # ScheduledReporter クラス
├── report_types.py       # ReportConfig, JobStats, ResourceStats
└── report_callback.py    # ReportCallback (Slackレポート生成)

src/srunx/cli/
└── main.py              # CLI: srunx watch の拡張
```

#### 6.2 クラス設計

**ScheduledReporter**:
```python
class ScheduledReporter:
    """定期レポート実行クラス"""

    def __init__(
        self,
        client: Slurm,
        callback: Callback,
        config: ReportConfig
    ):
        """Initialize scheduler"""

    def start(self) -> None:
        """バックグラウンドでスケジューラー開始"""

    def stop(self) -> None:
        """スケジューラー停止"""

    def run(self) -> None:
        """ブロッキングモードで実行"""

    def _generate_report(self) -> Report:
        """レポート生成（内部メソッド）"""

    def _send_report(self, report: Report) -> None:
        """レポート送信（内部メソッド）"""
```

**ReportConfig**:
```python
@dataclass
class ReportConfig:
    """レポート設定"""
    interval: str | None = None
    cron: str | None = None
    include: list[str] = field(default_factory=lambda: ["jobs", "resources", "user"])
    partition: str | None = None
    user: str | None = None
    timeframe: str = "24h"

    def __post_init__(self):
        """Validate: interval XOR cron"""
        if (self.interval is None) == (self.cron is None):
            raise ValueError("Exactly one of interval or cron must be specified")
```

**JobStats**:
```python
@dataclass
class JobStats:
    """ジョブ統計"""
    pending: int
    running: int
    completed: int  # within timeframe
    failed: int     # within timeframe
    cancelled: int  # within timeframe
    total_active: int
```

**ResourceStats**:
```python
@dataclass
class ResourceStats:
    """リソース統計"""
    partition: str | None
    total_gpus: int
    gpus_in_use: int
    gpus_available: int
    utilization: float  # percentage
    nodes_total: int
    nodes_idle: int
    nodes_down: int
```

**Report**:
```python
@dataclass
class Report:
    """生成されたレポート"""
    timestamp: datetime
    job_stats: JobStats | None = None
    resource_stats: ResourceStats | None = None
    user_stats: JobStats | None = None
```

**ReportCallback**:
```python
class ReportCallback(Callback):
    """レポート専用コールバック"""

    def on_scheduled_report(self, report: Report) -> None:
        """定期レポート送信"""
```

### 7. 依存ライブラリ

**APScheduler**: Pythonスケジューリングライブラリ
```toml
[project]
dependencies = [
    # ... existing
    "apscheduler>=3.10.0",
]
```

**機能**:
- Interval triggers
- Cron triggers
- Thread-based execution
- Graceful shutdown

### 8. エラーハンドリング

#### 10.1 SLURM障害時
- エラーログ記録
- レポート送信をスキップ
- 次回スケジュールは継続

#### 10.2 Slack送信失敗時
- リトライ（最大3回、exponential backoff）
- 失敗時はログに記録
- スケジューラーは継続

#### 10.3 設定エラー
- 起動時に検証
- 不正な設定で即座にエラー終了
- 明確なエラーメッセージ

### 9. テスト設計

#### 10.1 ユニットテスト
```python
# tests/test_scheduler.py
- test_interval_parsing()
- test_cron_parsing()
- test_report_generation()
- test_job_stats_calculation()
- test_resource_stats_calculation()
- test_timeframe_filtering()

# tests/test_report_callback.py
- test_slack_message_format_jobs()
- test_slack_message_format_resources()
- test_slack_message_format_combined()
- test_sanitization()
```

#### 10.2 インテグレーションテスト
```python
# tests/test_scheduled_reporter.py
- test_scheduler_start_stop()
- test_interval_execution()
- test_cron_execution()
- test_report_sent_to_callback()
- test_error_recovery()
```

### 10. ドキュメント更新

#### 10.1 README.md
```markdown
## Scheduled Reporting

Send periodic reports of job queue and resource status to Slack:

```bash
# Every hour
srunx watch --schedule 1h --notify $SLACK_WEBHOOK

# Daily at 9:00 AM
srunx watch --schedule "0 9 * * *" --notify $SLACK_WEBHOOK

# Custom report content
srunx watch --schedule 30m --notify $SLACK_WEBHOOK --include jobs,resources --partition gpu
```
```

#### 10.2 CLAUDE.md
- CLI Commands セクションに追加
- Monitoring セクションに使用例追加

## 実装計画

### Phase 1: Core Infrastructure
1. ReportConfig, JobStats, ResourceStats データモデル作成
2. ScheduledReporter クラス実装
3. APScheduler統合
4. 基本的なinterval/cron対応

### Phase 2: Report Generation
1. ジョブ統計生成機能
2. リソース統計生成機能
3. ユーザー別統計生成機能
4. Timeframeフィルタリング

### Phase 3: Callback Integration
1. ReportCallback実装
2. Slackメッセージフォーマット作成
3. エラーハンドリングとリトライ

### Phase 4: CLI
1. `srunx report schedule` コマンド実装
2. 引数パース
3. Daemon mode実装

### Phase 5: Testing & Documentation
1. ユニットテスト作成
2. インテグレーションテスト
3. ドキュメント更新
4. 使用例追加

## バージョン

- **Target Version**: 0.8.0
- **Breaking Changes**: なし（新機能追加のみ）

## セキュリティ考慮事項

1. **Webhook URL保護**
   - 環境変数推奨
   - 設定ファイルでの平文保存は警告

2. **情報漏洩防止**
   - ユーザー名のサニタイゼーション
   - ジョブ名のサニタイゼーション（既存機能利用）

3. **リソース制限**
   - 最小interval: 1分（過剰な頻度を防止）
   - タイムアウト設定

## 将来の拡張

- 複数webhook対応（チーム別通知）
- レポートテンプレートのカスタマイズ
- メール通知サポート
- Discord/Teams統合
- グラフ生成（matplotlib）
- 履歴データの保存とトレンド分析
