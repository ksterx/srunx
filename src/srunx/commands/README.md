# srunx コマンドリファレンス

このドキュメントは、srunxのすべてのCLIコマンドの詳細な使用方法を説明します。実際のコード実装に基づいた正確な情報を提供します。

## 目次

- [基本コマンド](#基本コマンド)
  - [submit - ジョブ投入](#submit---ジョブ投入)
  - [status - ステータス確認](#status---ステータス確認)
  - [list - ジョブ一覧](#list---ジョブ一覧)
  - [cancel - ジョブキャンセル](#cancel---ジョブキャンセル)
- [ワークフローコマンド](#ワークフローコマンド)
  - [flow run - ワークフロー実行](#flow-run---ワークフロー実行)
  - [flow validate - ワークフロー検証](#flow-validate---ワークフロー検証)
- [設定コマンド](#設定コマンド)
  - [config show - 設定表示](#config-show---設定表示)
  - [config paths - 設定パス表示](#config-paths---設定パス表示)
  - [config init - 設定初期化](#config-init---設定初期化)
- [SSHコマンド](#sshコマンド)
  - [ssh submit - リモートジョブ投入](#ssh-submit---リモートジョブ投入)
  - [ssh test - 接続テスト](#ssh-test---接続テスト)
  - [ssh profile - プロファイル管理](#ssh-profile---プロファイル管理)

---

## 基本コマンド

### submit - ジョブ投入

SLURMクラスタにジョブを投入します。

#### 基本構文

```bash
srunx submit <command> [options]
```

#### 必須引数

- `<command>` - 実行するコマンド（複数の引数を指定可能）

#### オプション

##### ジョブ基本設定

- `--name <name>`, `--job-name <name>` - ジョブ名（デフォルト: "job"）
- `--log-dir <dir>` - ログディレクトリ
- `--work-dir <dir>`, `--chdir <dir>` - 作業ディレクトリ

##### リソース設定

- `-N <num>`, `--nodes <num>` - ノード数（デフォルト: 1）
- `--gpus-per-node <num>` - ノードあたりGPU数（デフォルト: 0）
- `--ntasks-per-node <num>` - ノードあたりタスク数（デフォルト: 1）
- `--cpus-per-task <num>` - タスクあたりCPU数（デフォルト: 1）
- `--memory <size>`, `--mem <size>` - ノードあたりメモリ（例: '32GB', '1TB'）
- `--time <time>`, `--time-limit <time>` - 時間制限（例: '1:00:00', '30:00', '1-12:00:00'）
- `--nodelist <nodes>` - 使用する特定のノード（例: 'node001,node002'）
- `--partition <partition>` - SLURMパーティション（例: 'gpu', 'cpu'）

##### 環境設定

- `--conda <env>` - Conda環境名
- `--venv <path>` - 仮想環境のパス
- `--container <image>` - コンテナイメージまたは設定
- `--env <KEY=VALUE>` - 環境変数（複数指定可能）

##### 実行オプション

- `--dry-run` - 実行せずに投入内容を表示
- `--wait` - ジョブ完了まで待機
- `--slack` - Slackに通知を送信
- `--template <path>` - カスタムSLURMスクリプトテンプレート
- `--verbose`, `-v` - 詳細出力を表示

#### 使用例

##### 基本的なジョブ投入

```bash
# シンプルなPythonスクリプト実行
srunx submit python train.py

# ジョブ名を指定
srunx submit python train.py --name my_training_job

# 複数の引数を持つコマンド
srunx submit python train.py --epochs 100 --batch-size 32
```

##### GPU使用

```bash
# 1ノード、2GPUでジョブを実行
srunx submit python train.py --gpus-per-node 2

# 複数ノードでGPUを使用
srunx submit python distributed_train.py --nodes 4 --gpus-per-node 2
```

##### メモリと時間制限

```bash
# メモリと時間を指定
srunx submit python train.py --memory 64GB --time 4:00:00

# 長時間ジョブ（1日12時間）
srunx submit python long_job.py --time 1-12:00:00
```

##### 環境設定

```bash
# Conda環境を使用
srunx submit python train.py --conda ml_env

# 仮想環境を使用
srunx submit python train.py --venv /path/to/venv

# コンテナを使用
srunx submit python train.py --container /path/to/container.sqsh

# 環境変数を設定
srunx submit python train.py \
  --env CUDA_VISIBLE_DEVICES=0,1 \
  --env WANDB_PROJECT=my_project
```

##### 特定のノードやパーティションを指定

```bash
# 特定のパーティションを使用
srunx submit python train.py --partition gpu

# 特定のノードを指定
srunx submit python train.py --nodelist node001,node002
```

##### ジョブ完了まで待機

```bash
# ジョブが完了するまで待機
srunx submit python train.py --wait

# 完了まで待機し、Slack通知も送信
srunx submit python train.py --wait --slack
```

##### ドライラン（実行せずに確認）

```bash
# 実際には投入せず、内容を確認
srunx submit python train.py --nodes 2 --gpus-per-node 1 --dry-run
```

##### 包括的な例

```bash
# すべてのオプションを組み合わせた例
srunx submit python train.py \
  --name bert_training \
  --nodes 2 \
  --gpus-per-node 4 \
  --cpus-per-task 8 \
  --memory 128GB \
  --time 8:00:00 \
  --partition gpu \
  --conda ml_env \
  --env CUDA_VISIBLE_DEVICES=0,1,2,3 \
  --env WANDB_PROJECT=nlp_experiments \
  --wait \
  --slack
```

---

### status - ステータス確認

ジョブのステータスを確認します。

#### 基本構文

```bash
srunx status <job_id>
```

#### 必須引数

- `<job_id>` - 確認するジョブID

#### 使用例

```bash
# ジョブステータスを確認
srunx status 12345

# 出力例:
# Job ID: 12345
# Status: RUNNING
# Name: my_training_job
# Command: python train.py
```

---

### list - ジョブ一覧

ユーザーのジョブキューを一覧表示します。

#### 基本構文

```bash
srunx list
```

#### 使用例

```bash
# 自分のジョブを一覧表示
srunx list

# 出力例（テーブル形式）:
# ┏━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
# ┃ Job ID ┃ Name            ┃ Status  ┃ Nodes ┃ Time     ┃
# ┡━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
# │ 12345  │ my_training_job │ RUNNING │ 1     │ 2:00:00  │
# │ 12346  │ preprocess      │ PENDING │ 1     │ 1:00:00  │
# └────────┴─────────────────┴─────────┴───────┴──────────┘
```

---

### cancel - ジョブキャンセル

実行中のジョブをキャンセルします。

#### 基本構文

```bash
srunx cancel <job_id>
```

#### 必須引数

- `<job_id>` - キャンセルするジョブID

#### 使用例

```bash
# ジョブをキャンセル
srunx cancel 12345

# 出力例:
# ✅ Job 12345 cancelled successfully
```

---

## ワークフローコマンド

### flow run - ワークフロー実行

YAMLファイルで定義されたワークフローを実行します。

#### 基本構文

```bash
srunx flow run <yaml_file> [options]
```

#### 必須引数

- `<yaml_file>` - YAMLワークフロー定義ファイルのパス

#### オプション

- `--dry-run` - 実行せずにワークフロー構造を表示
- `--slack` - Slackに通知を送信
- `--debug` - レンダリングされたSLURMスクリプトを表示
- `--from <job_name>` - 指定したジョブから実行開始（それ以前の依存関係を無視）
- `--to <job_name>` - 指定したジョブまで実行（それ以降を無視）
- `--job <job_name>` - 特定のジョブのみを実行（すべての依存関係を無視）

#### 使用例

##### 基本的なワークフロー実行

```bash
# YAMLワークフローを実行
srunx flow run workflow.yaml

# Slack通知付きで実行
srunx flow run workflow.yaml --slack
```

##### ドライラン

```bash
# 実行せずにワークフロー構造を確認
srunx flow run workflow.yaml --dry-run

# 出力例:
# 🔍 Dry run mode - showing workflow structure:
# Workflow: ml_pipeline
# Executing all jobs: 4 jobs
#   - preprocess: python preprocess.py
#   - train: python train.py
#   - evaluate: python evaluate.py
#   - notify: python notify.py
```

##### デバッグモード

```bash
# 各ジョブのSLURMスクリプトを表示
srunx flow run workflow.yaml --debug
```

##### 部分実行

```bash
# 特定のジョブから実行（それ以前の依存関係を無視）
srunx flow run workflow.yaml --from train

# 特定のジョブまで実行
srunx flow run workflow.yaml --to evaluate

# 範囲を指定して実行
srunx flow run workflow.yaml --from preprocess --to evaluate

# 特定のジョブのみを実行（依存関係を完全に無視）
srunx flow run workflow.yaml --job train
```

##### 包括的な例

```bash
# デバッグモードとSlack通知を有効にして実行
srunx flow run ml_pipeline.yaml --debug --slack
```

#### YAMLワークフロー例

```yaml
name: ml_pipeline

# テンプレート変数（オプション）
args:
  experiment_name: "bert-fine-tuning"
  dataset_path: "/data/nlp/imdb"
  output_dir: "/outputs/{{ experiment_name }}"
  batch_size: 32

jobs:
  - name: preprocess
    command:
      - "python"
      - "preprocess.py"
      - "--dataset"
      - "{{ dataset_path }}"
      - "--output"
      - "{{ output_dir }}/preprocessed"
    resources:
      nodes: 1
      memory_per_node: "16GB"

  - name: train
    command: ["python", "train.py"]
    depends_on: [preprocess]
    resources:
      nodes: 1
      gpus_per_node: 2
      memory_per_node: "32GB"
      time_limit: "8:00:00"
    environment:
      conda: ml_env
      env_vars:
        CUDA_VISIBLE_DEVICES: "0,1"

  - name: evaluate
    command: ["python", "evaluate.py"]
    depends_on: [train]
    resources:
      nodes: 1

  - name: notify
    command: ["python", "notify.py"]
    depends_on: [train, evaluate]
```

---

### flow validate - ワークフロー検証

YAMLワークフローファイルを実行せずに検証します。

#### 基本構文

```bash
srunx flow validate <yaml_file>
```

#### 必須引数

- `<yaml_file>` - YAMLワークフロー定義ファイルのパス

#### 使用例

```bash
# ワークフローを検証
srunx flow validate workflow.yaml

# 出力例（成功時）:
# ✅ Workflow validation successful
#    Workflow: ml_pipeline
#    Jobs: 4
```

---

## 設定コマンド

### config show - 設定表示

現在のsrunx設定を表示します。

#### 基本構文

```bash
srunx config show
```

#### 使用例

```bash
# 設定を表示
srunx config show

# 出力例（テーブル形式）:
# ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
# ┃ Section     ┃ Key             ┃ Value       ┃
# ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
# │ General     │ log_dir         │ logs        │
# │             │ work_dir        │ .           │
# │ Resources   │ nodes           │ 1           │
# │             │ gpus_per_node   │ 0           │
# │             │ memory_per_node │ None        │
# │ Environment │ conda           │ None        │
# └─────────────┴─────────────────┴─────────────┘
```

---

### config paths - 設定パス表示

設定ファイルのパスを優先順位順に表示します。

#### 基本構文

```bash
srunx config paths
```

#### 使用例

```bash
# 設定ファイルのパスを表示
srunx config paths

# 出力例:
# Configuration file paths (in order of precedence):
# 1. /home/user/.config/srunx/config.json - ❌ not found
# 2. /etc/srunx/config.json - ❌ not found
```

---

### config init - 設定初期化

設定ファイルを初期化（作成）します。

#### 基本構文

```bash
srunx config init [options]
```

#### オプション

- `--force` - 既存の設定ファイルを上書き

#### 使用例

```bash
# 設定ファイルを作成
srunx config init

# 出力例:
# ✅ Configuration file created: /home/user/.config/srunx/config.json
# Edit this file to customize your defaults

# 既存ファイルを上書き
srunx config init --force
```

---

## SSHコマンド

### ssh submit - リモートジョブ投入

SSH経由でリモートSLURMサーバーにジョブを投入します。

#### 基本構文

```bash
srunx ssh submit <script_path> [options]
```

または、typerを使わない従来の方法:

```bash
srunx ssh <script_path> [options]
```

#### 必須引数

- `<script_path>` - 投入するsbatchスクリプトファイルのパス

#### 接続オプション

以下の3つの接続方法があります（優先順位順）:

##### 1. SSHコンフィグホストを使用

- `--host <host>`, `-H <host>` - `~/.ssh/config`のホスト名

```bash
srunx ssh submit train.sh --host dgx-server
```

##### 2. 保存済みプロファイルを使用

- `--profile <profile>`, `-p <profile>` - 保存済みプロファイル名

```bash
srunx ssh submit train.sh --profile ml-cluster
```

##### 3. 直接接続パラメータを指定

- `--hostname <hostname>` - サーバーのホスト名
- `--username <username>` - SSHユーザー名
- `--key-file <path>` - SSH秘密鍵ファイルのパス
- `--port <port>` - SSHポート（デフォルト: 22）

```bash
srunx ssh submit train.sh \
  --hostname dgx.example.com \
  --username researcher \
  --key-file ~/.ssh/dgx_key
```

##### その他の接続オプション

- `--config <path>` - 設定ファイルのパス（デフォルト: `~/.config/srunx/config.json`）
- `--ssh-config <path>` - SSHコンフィグファイルのパス（デフォルト: `~/.ssh/config`）

#### ジョブオプション

- `--job-name <name>` - ジョブ名
- `--poll-interval <seconds>`, `-i <seconds>` - ステータス確認間隔（デフォルト: 10秒）
- `--timeout <seconds>` - 監視タイムアウト（デフォルト: 無制限）
- `--no-monitor` - ジョブ監視を行わない
- `--no-cleanup` - アップロードしたスクリプトファイルを削除しない

#### 環境変数オプション

- `--env <KEY=VALUE>` - 環境変数を設定（複数指定可能）
- `--env-local <KEY>` - ローカル環境変数を転送（複数指定可能）

自動検出される環境変数:
- `HF_TOKEN`, `HUGGING_FACE_HUB_TOKEN`
- `WANDB_API_KEY`, `WANDB_ENTITY`, `WANDB_PROJECT`
- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`
- `CUDA_VISIBLE_DEVICES`
- `HF_HOME`, `HF_HUB_CACHE`, `TRANSFORMERS_CACHE`, `TORCH_HOME`
- `SLURM_LOG_DIR`

#### 通知オプション

- `--slack` - Slackに通知を送信

#### その他のオプション

- `--verbose`, `-v` - 詳細ログを有効化

#### 使用例

##### SSHコンフィグホストを使用

```bash
# 基本的な使用
srunx ssh submit train.py --host dgx-server

# ジョブ名を指定
srunx ssh submit experiment.sh --host dgx-server --job-name ml-experiment-001

# 環境変数を指定
srunx ssh submit train.py --host dgx-server \
  --env CUDA_VISIBLE_DEVICES=0,1 \
  --env-local WANDB_API_KEY
```

##### プロファイルを使用

```bash
# プロファイルで投入
srunx ssh submit train.py --profile ml-cluster

# Slack通知付き
srunx ssh submit experiment.sh --profile ml-cluster --slack
```

##### 直接接続

```bash
# 直接接続パラメータを指定
srunx ssh submit script.py \
  --hostname dgx.example.com \
  --username researcher \
  --key-file ~/.ssh/dgx_key
```

##### カスタムポーリングとタイムアウト

```bash
# 30秒ごとにステータス確認、2時間でタイムアウト
srunx ssh submit long_job.sh --host server \
  --poll-interval 30 \
  --timeout 7200
```

##### バックグラウンド投入

```bash
# 監視せずに投入のみ
srunx ssh submit background_job.sh --host server --no-monitor
```

##### デバッグ用

```bash
# アップロードファイルを残す（デバッグ用）
srunx ssh submit debug_script.py --host server --no-cleanup
```

##### 包括的な例

```bash
# すべてのオプションを使用した例
srunx ssh submit train_bert.py \
  --host dgx-server \
  --job-name bert-large-training \
  --env CUDA_VISIBLE_DEVICES=0,1,2,3 \
  --env WANDB_PROJECT=nlp_experiments \
  --env-local WANDB_API_KEY \
  --poll-interval 60 \
  --slack \
  --verbose
```

---

### ssh test - 接続テスト

SSH接続とSLURMの利用可能性をテストします。

#### 基本構文

```bash
srunx ssh test [options]
```

#### 接続オプション

`ssh submit`と同じ接続オプションを使用します:

- `--host <host>`, `-H <host>` - SSHコンフィグホスト
- `--profile <profile>`, `-p <profile>` - プロファイル名
- `--hostname <hostname>` - ホスト名（直接接続）
- `--username <username>` - ユーザー名（直接接続）
- `--key-file <path>` - 秘密鍵ファイル（直接接続）
- `--port <port>` - ポート（直接接続）
- `--config <path>` - 設定ファイルパス
- `--ssh-config <path>` - SSHコンフィグファイルパス

#### その他のオプション

- `--verbose`, `-v` - 詳細ログを有効化

#### 使用例

```bash
# SSHコンフィグホストでテスト
srunx ssh test --host dgx-server

# プロファイルでテスト
srunx ssh test --profile ml-cluster

# 直接接続でテスト
srunx ssh test \
  --hostname dgx.example.com \
  --username researcher \
  --key-file ~/.ssh/dgx_key

# 詳細ログ付き
srunx ssh test --host dgx-server --verbose
```

#### 出力例

```
Testing SSH connection to:
  Hostname: dgx.example.com
  Username: researcher
  Port: 22
  Key file: /home/user/.ssh/dgx_key

┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Check          ┃ Status       ┃ Details                        ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ SSH Connection │ ✅ Connected │ Host: dgx.example.com, User... │
└────────────────┴──────────────┴────────────────────────────────┘

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Success                               ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ Connection test successful!       │
│ SSH connection is working.            │
└───────────────────────────────────────┘
```

---

### ssh profile - プロファイル管理

SSH接続プロファイルを管理します。

#### サブコマンド一覧

- `list` - すべてのプロファイルを一覧表示
- `add` - 新しいプロファイルを追加
- `remove` - プロファイルを削除
- `set` - デフォルトプロファイルを設定
- `show` - プロファイルの詳細を表示
- `update` - プロファイルを更新
- `env` - 環境変数管理
  - `set` - 環境変数を設定
  - `unset` - 環境変数を削除
  - `list` - 環境変数を一覧表示

---

#### profile list - プロファイル一覧

すべての保存済みプロファイルを一覧表示します。

##### 基本構文

```bash
srunx ssh profile list [options]
```

##### オプション

- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# プロファイル一覧を表示
srunx ssh profile list

# 出力例:
# ┏━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
# ┃ Name     ┃ Connection               ┃ Description     ┃ Status ┃
# ┡━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
# │ dgx      │ SSH Config: dgx1         │ Main DGX server │ Current│
# │ ml-gpu   │ researcher@10.0.1.100:22 │ ML GPU cluster  │        │
# └──────────┴──────────────────────────┴─────────────────┴────────┘
```

---

#### profile add - プロファイル追加

新しいプロファイルを追加します。

##### 基本構文

```bash
srunx ssh profile add <name> [options]
```

##### 必須引数

- `<name>` - プロファイル名

##### オプション（2つの方法のいずれか）

**方法1: SSHコンフィグホストを使用**

- `--ssh-host <host>` - `~/.ssh/config`のホスト名

**方法2: 直接接続パラメータを指定**（すべて必須）

- `--hostname <hostname>` - サーバーのホスト名
- `--username <username>` - SSHユーザー名
- `--key-file <path>` - SSH秘密鍵ファイルのパス
- `--port <port>` - SSHポート（デフォルト: 22）

**共通オプション**

- `--proxy-jump <host>` - ProxyJumpホスト
- `--description <text>` - プロファイルの説明
- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# SSHコンフィグホストを使用してプロファイル追加
srunx ssh profile add dgx \
  --ssh-host dgx1 \
  --description "Main DGX server"

# 直接接続パラメータでプロファイル追加
srunx ssh profile add ml-gpu \
  --hostname 10.0.1.100 \
  --username researcher \
  --key-file ~/.ssh/ml_key \
  --description "ML GPU cluster"

# ポート番号を指定
srunx ssh profile add custom-port \
  --hostname server.example.com \
  --username user \
  --key-file ~/.ssh/key \
  --port 2222

# ProxyJumpを使用
srunx ssh profile add bastion \
  --hostname internal.example.com \
  --username user \
  --key-file ~/.ssh/key \
  --proxy-jump bastion.example.com
```

---

#### profile remove - プロファイル削除

プロファイルを削除します。

##### 基本構文

```bash
srunx ssh profile remove <name> [options]
```

##### 必須引数

- `<name>` - 削除するプロファイル名

##### オプション

- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# プロファイルを削除
srunx ssh profile remove old-server

# 出力例:
# ✅ Profile 'old-server' removed successfully
```

---

#### profile set - デフォルトプロファイル設定

デフォルトプロファイルを設定します。

##### 基本構文

```bash
srunx ssh profile set <name> [options]
```

##### 必須引数

- `<name>` - デフォルトに設定するプロファイル名

##### オプション

- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# デフォルトプロファイルを設定
srunx ssh profile set dgx

# 出力例:
# ✅ Current profile set to 'dgx'
# ┏━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃ Current Profile: dgx   ┃
# ┡━━━━━━━━━━━━━━━━━━━━━━━━┩
# │ SSH Host: dgx1         │
# │ Description: Main DGX  │
# └────────────────────────┘
```

---

#### profile show - プロファイル詳細表示

プロファイルの詳細を表示します。

##### 基本構文

```bash
srunx ssh profile show [name] [options]
```

##### オプション引数

- `[name]` - プロファイル名（省略時は現在のプロファイル）

##### オプション

- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# 特定のプロファイルを表示
srunx ssh profile show dgx

# 現在のプロファイルを表示
srunx ssh profile show

# 出力例:
# ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃ Profile: dgx (Current)            ┃
# ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
# │ Connection Details:               │
# │   SSH Config Host: dgx1           │
# │                                   │
# │ Description:                      │
# │   Main DGX server                 │
# │                                   │
# │ Environment Variables:            │
# │   WANDB_PROJECT=my_project        │
# │   WANDB_API_KEY=***HIDDEN***      │
# └───────────────────────────────────┘
```

---

#### profile update - プロファイル更新

既存のプロファイルを更新します。

##### 基本構文

```bash
srunx ssh profile update <name> [options]
```

##### 必須引数

- `<name>` - 更新するプロファイル名

##### オプション

更新したい項目のみを指定:

- `--ssh-host <host>` - SSHコンフィグホスト名
- `--hostname <hostname>` - サーバーのホスト名
- `--username <username>` - SSHユーザー名
- `--key-file <path>` - SSH秘密鍵ファイルのパス
- `--port <port>` - SSHポート
- `--proxy-jump <host>` - ProxyJumpホスト
- `--description <text>` - プロファイルの説明
- `--config <path>` - 設定ファイルのパス

##### 使用例

```bash
# 説明を更新
srunx ssh profile update dgx --description "Updated description"

# ホスト名とポートを更新
srunx ssh profile update ml-gpu \
  --hostname new-hostname.example.com \
  --port 2222

# SSH秘密鍵を変更
srunx ssh profile update dgx --key-file ~/.ssh/new_key
```

---

#### profile env - 環境変数管理

プロファイルの環境変数を管理します。

##### env set - 環境変数設定

プロファイルに環境変数を設定します。

###### 基本構文

```bash
srunx ssh profile env set <profile_name> <key> <value> [options]
```

###### 必須引数

- `<profile_name>` - プロファイル名
- `<key>` - 環境変数名
- `<value>` - 環境変数の値

###### オプション

- `--config <path>` - 設定ファイルのパス

###### 使用例

```bash
# 環境変数を設定
srunx ssh profile env set dgx WANDB_PROJECT my_project

# API キーを設定（自動的に***HIDDEN***で表示される）
srunx ssh profile env set dgx WANDB_API_KEY your_api_key_here

# 出力例:
# ✅ Environment variable set for profile 'dgx'
# WANDB_API_KEY=***HIDDEN***
```

---

##### env unset - 環境変数削除

プロファイルから環境変数を削除します。

###### 基本構文

```bash
srunx ssh profile env unset <profile_name> <key> [options]
```

###### 必須引数

- `<profile_name>` - プロファイル名
- `<key>` - 削除する環境変数名

###### オプション

- `--config <path>` - 設定ファイルのパス

###### 使用例

```bash
# 環境変数を削除
srunx ssh profile env unset dgx WANDB_PROJECT

# 出力例:
# ✅ Environment variable 'WANDB_PROJECT' removed from profile 'dgx'
```

---

##### env list - 環境変数一覧

プロファイルの環境変数を一覧表示します。

###### 基本構文

```bash
srunx ssh profile env list <profile_name> [options]
```

###### 必須引数

- `<profile_name>` - プロファイル名

###### オプション

- `--config <path>` - 設定ファイルのパス

###### 使用例

```bash
# 環境変数を一覧表示
srunx ssh profile env list dgx

# 出力例:
# ┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
# ┃ Variable         ┃ Value         ┃
# ┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
# │ WANDB_PROJECT    │ my_project    │
# │ WANDB_API_KEY    │ ***HIDDEN***  │
# │ CUDA_VISIBLE_... │ 0,1,2,3       │
# └──────────────────┴───────────────┘
```

---

## 環境変数

srunxは以下の環境変数をサポートしています:

### 一般的な環境変数

- `SLURM_LOG_DIR` - SLURMログのデフォルトディレクトリ（デフォルト: `logs`）
- `SLACK_WEBHOOK_URL` - Slack通知用のWebhook URL

### SSH経由で自動転送される環境変数

以下の環境変数は、SSH経由でジョブを投入する際に自動的に検出・転送されます:

**Hugging Face関連**
- `HF_TOKEN`
- `HUGGING_FACE_HUB_TOKEN`
- `HF_HOME`
- `HF_HUB_CACHE`
- `TRANSFORMERS_CACHE`

**Weights & Biases関連**
- `WANDB_API_KEY`
- `WANDB_ENTITY`
- `WANDB_PROJECT`

**AI API関連**
- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`

**GPU/システム関連**
- `CUDA_VISIBLE_DEVICES`
- `TORCH_HOME`

**SLURM関連**
- `SLURM_LOG_DIR`

---

## 設定ファイル

### srunx設定ファイル

設定ファイルは以下の場所から読み込まれます（優先順位順）:

1. `~/.config/srunx/config.json`
2. `/etc/srunx/config.json`

### SSH設定ファイル

- `~/.ssh/config` - 標準SSH設定ファイル
- `~/.config/srunx/config.json` - SSHプロファイルストレージ（環境変数を含む）

### 設定ファイル例

```json
{
  "log_dir": "logs",
  "work_dir": ".",
  "resources": {
    "nodes": 1,
    "gpus_per_node": 0,
    "ntasks_per_node": 1,
    "cpus_per_task": 1,
    "memory_per_node": null,
    "time_limit": "1:00:00",
    "partition": null
  },
  "environment": {
    "conda": null,
    "venv": null,
    "container": null
  }
}
```

---

## トラブルシューティング

### SSH接続のテスト

```bash
# 直接SSHで接続できるか確認
ssh your-hostname

# srunxの接続テストを使用
srunx ssh test --host your-hostname

# 詳細ログを有効にして問題を診断
srunx ssh test --host your-hostname --verbose
```

### ジョブが失敗した場合

```bash
# ジョブステータスを確認
srunx status <job_id>

# SSH経由のジョブは自動的にログを表示（失敗時）
# または --no-cleanup オプションでファイルを保持
srunx ssh submit script.sh --host server --no-cleanup
```

### プロファイル管理

```bash
# すべてのプロファイルを確認
srunx ssh profile list

# 特定のプロファイルの詳細を確認
srunx ssh profile show <profile_name>

# 現在のプロファイルを確認
srunx ssh profile show
```

---

## まとめ

このドキュメントでは、srunxのすべてのCLIコマンドについて、実際のコード実装に基づいた正確な使用方法を説明しました。各コマンドの詳細なオプションと実用的な例を参考に、効率的にSLURMジョブを管理してください。

より詳しい情報は、メインの[README.md](../../../README.md)を参照してください。
