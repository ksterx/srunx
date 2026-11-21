# 設計: クラスタ別設定とファイル配置の再編

状態: 実装済み
互換性: 考慮しない（旧パス・旧形式の移行処理は持たない）

## 目的

- 同じプロジェクトのジョブを、`--cluster` を変えるだけで複数の Slurm クラスタへ投げ分けられるようにする。
- 設定・状態・作業データの置き場所を整理し、パス解決と設定保存をそれぞれ一箇所に集約する。

## 非目的

- 資源指定の共通化（`gpus_per_node` をクラスタごとの `--gres` 形式へ変換する等）。値はそのまま渡す。
- `--cluster auto`（空き状況による自動選択）と複数クラスタへの同時投入。
- Slurm 以外のスケジューラ（PBS 等）。

## 1. 用語

| 用語 | 意味 |
|---|---|
| cluster | ジョブの投入先。名前で識別し、接続方法（`local` / `ssh`）とクラスタ別既定値を持つ。旧 SSH プロファイルを置き換える |
| project | `.srunx/config.toml` を持つディレクトリ。ジョブ投入の文脈 |
| mount | project のローカルパスとクラスタ上のパスの対応。同期とパス変換に使う |
| job ref | `(cluster, job_id)` の組。ジョブを一意に指す |

## 2. ファイル配置

### ローカル

| パス | 中身 |
|---|---|
| `/etc/srunx/config.toml` | システム共通の設定 |
| `$XDG_CONFIG_HOME/srunx/config.toml` | ユーザー設定（共通既定値、クラスタ定義、通知など） |
| `$XDG_CONFIG_HOME/srunx/templates/` | ユーザー定義テンプレート |
| `$XDG_STATE_HOME/srunx/srunx.db` | 履歴・通知・ワークフロー実行 |
| `$XDG_STATE_HOME/srunx/locks/` | 同期ロック |

`XDG_*` が未設定・空・相対パスのときは仕様どおりの既定値（`~/.config`、`~/.local/state`）を使う。

HOME が NFS 上にある共有ログインノードでは SQLite（WAL）が正しく動かないため、ユーザー設定の `state_dir` で DB とロックの置き場所をローカルディスクへ変えられる。

### プロジェクト

| パス | 中身 | Git |
|---|---|---|
| `<root>/.srunx/config.toml` | プロジェクト設定 | 管理する |
| `<root>/.srunx/workflows/*.yaml` | ワークフロー | 管理する |

srunx は `.srunx/.gitignore` を自動生成しない。将来 `.srunx/` に生成物を置く場合は、その生成物だけを無視対象として列挙する。

### リモート

リモートのパスはリモート側の環境（`$HOME`、`$XDG_*`）を SSH で取得して解決する。ローカルの `Path.home()` や環境変数は使わない。

| パス | 中身 |
|---|---|
| `$XDG_CONFIG_HOME/srunx/secrets.env` | `srunx secret` で登録したシークレット |
| `$XDG_DATA_HOME/srunx/workspaces/<name>-<hash>/` | mount 未指定時の同期先の既定 |
| `$XDG_DATA_HOME/srunx/snapshots/` | スナップショットの既定（`snapshot_root` で変更可） |
| `<mount.remote>/.srunx-owner.json`、`.srunx-manifest.json` | 同期の所有者マーカーとマニフェスト（現状維持） |

`<hash>` はローカルの project root の実体パスから計算し、`<name>` は project 設定の `name` を使う。同名プロジェクトの衝突を避けるため。

### パス解決

`srunx/common/paths.py` に集約する。

- `config_dir()`、`state_dir()`、`system_config_path()`：ローカルの基点。読み取り時にディレクトリを作らない。
- `find_project_root(start)`：§4 の探索。
- `RemoteDirs.fetch(conn)`：リモートの `$HOME` / `$XDG_*` を一度取得し、そこから各パスを計算する。

## 3. 設定スキーマ

### ユーザー設定（`config.toml`）

```toml
default_cluster = "gpu"
state_dir = "/local/ksterx/srunx"  # 任意。DB とロックの置き場所（既定 $XDG_STATE_HOME/srunx）

[defaults]                         # すべてのクラスタに共通
log_dir = "logs"

[defaults.resources]
time_limit = "01:00:00"
nodes = 1

[defaults.environment]
conda = "ml"

[clusters.local]
transport = "local"

[clusters.gpu]
transport = "ssh"
host = "gpu-login"                 # ~/.ssh/config のエイリアス。hostname/user/port/key の直接指定も可
snapshot_root = "/data/ksterx/snapshots"   # 任意

[clusters.gpu.defaults.resources]
partition = "gpu"
gpus_per_node = 8

# 個人ごとに異なる同期先はユーザー側で project×cluster 単位に上書きできる
[projects.myproj.clusters.gpu]      # キーは project 設定の name
remote_dir = "/home/ksterx/work/myproj"

[notifications]
# 現行と同等
```

### プロジェクト設定（`.srunx/config.toml`）

```toml
name = "myproj"                    # 必須。ユーザー設定の projects.<name> と workspace 名に使う
cluster = "gpu"                    # 既定の投げ先

[defaults.resources]
time_limit = "02:00:00"

[clusters.gpu]
remote_dir = "/home/shared/myproj" # 省略時はリモートの workspaces 既定
sync = true                        # 共有ストレージで rsync 不要なら false
exclude = ["data/", "*.ckpt"]

[[clusters.gpu.mounts]]            # project root 以外の追加 mount
name = "datasets"
local = "../datasets"
remote = "/data/datasets"
sync = false

[clusters.gpu.defaults.resources]
time_limit = "04:00:00"

[clusters.hr.defaults.resources]
partition = "a100-long"
```

プロジェクト設定で書けるのは `name`、`cluster`、`defaults`、`clusters.<name>.{remote_dir, sync, exclude, mounts, defaults}` だけ。`transport`、`host` など接続に関わる項目は書けない（書けばエラー）。リポジトリを clone しただけで未知のホストへ接続させないため。

プロジェクト設定が、そのユーザーの設定にないクラスタの節を含んでいても無視する（共有リポジトリで他人のクラスタ名が書かれていても投入を妨げない）。そのクラスタを投げ先として選んだときだけエラーにする。

### mount の扱い

- project root ↔ `remote_dir` を「主 mount」とする。`remote_dir` は主 mount の簡易表現。
- 追加 mount は `mounts` で列挙する。
- mount はパス変換（スクリプトパス、作業ディレクトリ）に使い、`sync = true` のものだけ投入前に rsync する。
- 所有者マーカーの確認と、同期から投入までのロック保持は現状を維持する。
- `remote_dir` / `mounts.remote` は絶対パスのみ。場所の制限は設けない。代わりに、`(cluster, remote パス)` に初めて同期するときは同期先を表示して確認を求め、承認済みの組を `state_dir` に記録する。非対話環境では未承認なら停止し、`--yes` で承認できる。

## 4. プロジェクト探索

1. `--project <path>` があればそれを使う。
2. なければ cwd の実体パスから親へたどり、最初に見つかった `.srunx/config.toml` を採用する。
3. 次のいずれかに到達したら停止する：Git リポジトリのルート（そのディレクトリ自体は調べる）、`$HOME`（調べない）、ファイルシステムのルート。
4. 見つからなければ project なしとして扱う。

Web UI と MCP はサーバーの cwd から推測せず、リクエストで project を明示する。

## 5. 解決順

### クラスタ

`--cluster` > `SRUNX_CLUSTER` > project の `cluster` > ユーザーの `default_cluster` > エラー

暗黙に `local` を選ばない。

### ジョブパラメータ

下ほど優先する。

1. srunx 組込み既定値
2. システム設定 `defaults`
3. ユーザー設定 `defaults`
4. ユーザー設定 `clusters.<c>.defaults`
5. project `defaults`
6. project `clusters.<c>.defaults`
7. スクリプトの `#SBATCH` ／ workflow・sweep の job 定義
8. CLI 引数 ／ API リクエストの明示値

- 1〜6 は「既定値」、7〜8 は「そのジョブの指定」。
- 既定値を環境変数で与える仕組み（`SRUNX_DEFAULT_*` など）は持たない。環境変数で切り替えるのは `SRUNX_CLUSTER` だけ。
- Slurm 自身の `SBATCH_*` 環境変数は srunx では解釈せず、sbatch にそのまま任せる。
- ユーザー設定の `projects.<name>.clusters.<c>` は project 設定の同キーより優先する（個人パスの上書き用）。
- 値ごとに出所（層とファイルパス）を保持し、`--dry-run` と `srunx config show` で表示する。

### 明示的な解除

上位層で `partition = ""` のように空にしても「未指定に戻す」ことにはならない。解除は同じテーブル内の `unset` で明示する。

```toml
[clusters.hr.defaults.resources]
unset = ["partition", "account"]   # 下位層から来た値を捨て、未指定にする
```

同じテーブルで同じキーを設定と `unset` の両方に書けばエラー。CLI・API では明示解除を専用の値（例：`--partition=`）ではなく `--unset partition` で表す。

### 省略と明示の区別

CLI、MCP、Web の入口は、省略された引数を既定値で埋めない。「省略」「明示値」「明示解除」を区別した入力をそのまま resolver に渡す。ジョブのドメインモデルは設定ファイルを読まず、resolver が完成値を渡す。

## 6. `#SBATCH` の扱い

- `srunx sbatch` はスクリプトの `#SBATCH` 行のうち、次の項目を解析する（長い形・短い形の両方）：`--nodes/-N`、`--ntasks/-n`、`--ntasks-per-node`、`--cpus-per-task/-c`、`--gpus-per-node`、`--gres`、`--mem`、`--time/-t`、`--partition/-p`、`--account/-A`、`--qos/-q`、`--job-name/-J`、`--output/-o`、`--error/-e`、`--chdir/-D`、`--nodelist/-w`、`--clusters/-M`。それ以外の行は解析せずそのまま sbatch に任せる。
- 解析結果は層 7 として合成する。sbatch へは、層 1〜6 の値のうちスクリプトが指定していない項目と、層 8 の値だけを引数で渡す。スクリプトの指定は上書きしない。
- 警告とエラーを分ける。
  - 警告：スクリプトの値がクラスタ別既定値と異なる（例：`#SBATCH -p gpu` だが投げ先 hr の既定は `a100`）。
  - エラー：投げ先で成立しない指定。具体的には `--clusters` / `-M` がどこにあってもエラー（srunx の記録と実際の投入先が食い違うため）。
- `--gres` の現行の限定的な変換（`gpu:N` → `gpus_per_node`）は廃止し、文字列のまま渡す。

## 7. CLI

```bash
srunx sbatch [--cluster NAME] [--project PATH] [--dry-run] script.sh [sbatch 引数...]
srunx status|cancel|logs [--cluster NAME] JOB_ID
srunx cluster list | show NAME | add | remove | test NAME
srunx config show [--cluster NAME]      # 合成結果と出所
```

- `-C` は短縮形として使わない（sbatch の `--constraint` と衝突する）。
- `srunx ssh use` と current profile の仕組みは廃止する。一時的な切替は `SRUNX_CLUSTER` で行う。
- 現 `srunx ssh profile *` は `srunx cluster *` に置き換える。

### `srunx sbatch` の流れ

1. project とクラスタを決める。
2. 設定を合成し、`#SBATCH` を解析する。
3. 警告・エラーを判定する。`--dry-run` ならここで合成結果と出所、実行予定の sbatch コマンドと同期対象を表示して終了する。
4. `sync = true` の mount を rsync する（ロック取得 → 所有者確認 → 同期 → 投入 → ロック解放）。
5. 投入し、履歴に `(cluster, job_id)` で記録する。

## 8. ジョブ参照

- DB の `scheduler_key` は `local`（transport = "local" のクラスタ）/ `ssh:<cluster>` を使う。接続方式を符号化したままにすることで、監視側が設定を引かずに経路を決められる。`transport = "local"` のクラスタは 1 つまでに制限するので、`(scheduler_key, job_id)` はそのまま job ref になる。`UNIQUE(scheduler_key, job_id)` は維持する。
- `status` / `cancel` / `logs` は、`--cluster` があればそれを使う。なければ履歴を job_id で引き、候補が 1 件ならそのクラスタ、0 件または複数件ならエラーとして `--cluster` を求める。
- 監視、通知、workflow、sweep の参照も同じ job ref を使う。
- クラスタ名は `[A-Za-z0-9_-]{1,64}`。`:` は使えない。
- クラスタの接続先を変えた場合、同名で履歴は引き継がれる。別物として扱いたい場合は名前を変える（`srunx cluster add` は既存名に履歴があれば警告する）。

## 9. Web UI と MCP

- サーバープロセス全体で接続先を切り替える現在の仕組み（active profile、共有 adapter）を廃止する。
- リクエストごとに `project` と `cluster` を受け取り、resolver の結果と接続を固定した context を作る。バックグラウンド処理（workflow 実行、監視）にはこの context を渡す。接続は `TransportRegistry` でクラスタ名ごとにプールする。
- workflow / sweep の 1 回の実行は 1 クラスタに限る。
- 設定画面は「合成後の有効値（出所付き）」と「編集対象ファイル」を分ける。保存は編集したキーだけを書き、継承値を書き込まない。編集開始時からファイルが変わっていれば保存を拒否する。

## 10. 設定保存

- 書き込みは `srunx/common/config_store.py` の一箇所に限る。CLI、Web、MCP すべてが通る。
- 手順：対象ファイル専用のロックを取る → 再読込 → 指定キーだけを更新 → スキーマ検証 → 同じディレクトリの一時ファイルに書いて原子的に置換。
- `tomlkit` を使い、コメントと書式を保持する。モデル全体を再出力しない。
- 「値の設定」「キーの削除（継承に戻す）」「明示解除」を別の操作として提供する。
- 壊れたファイルを空として扱って上書きしない。読み込みエラーで停止する。
- SQLite はこの仕組みに含めない。

## 11. 決定事項

1. 同期先の場所は制限しない。初回同期時に確認を求める（§3 mount の扱い）。
2. 明示解除は `unset = [...]`（§5）。
3. `#SBATCH` の解析対象は §6 の一覧で固定する。
4. ユーザー設定の `projects` のキーは project 設定の `name`。`name` は必須。
5. 既定値を与える環境変数は持たない。`SRUNX_CLUSTER` のみ。
6. DB とロックの置き場所は `state_dir` で変更できる。

## 12. 実装順

1. 本仕様の確定。
2. `paths.py`、TOML スキーマ、`config_store.py`、出所を保持する読み込み。
3. 共通 resolver と投入計画。入口の既定値埋めとドメインモデルの暗黙読込を除去。
4. クラスタ識別子、DB、`TransportRegistry`、監視を更新。CLI で解決 → 同期 → 投入 → 操作まで通す。
5. Web UI、MCP、workflow、sweep をリクエスト単位の context に対応させる。

検証の重点：同一 job_id の複数クラスタ、同時保存、明示 `0` / `false`、同期失敗時の未投入、`--dry-run` で何も変更しないこと、プロジェクト探索の境界。

## 13. 実装メモ

- `defaults` テーブルの中身は `resources.*`（nodes, gpus_per_node, ntasks_per_node, cpus_per_task, memory_per_node, time_limit, nodelist, partition）、`environment.*`（conda, venv, container, env_vars）、`log_dir`、`work_dir`。
- `remote_dir` を省略した場合の同期先は、`srunx cluster test` などで取得・キャッシュしたリモート環境（`<state_dir>/remote-env.json`）から計算する。未取得なら投入時に取得する。
- Web UI で `?cluster=` を省略した API 呼び出しは、プロジェクトの `cluster` → `default_cluster` を使う（サーバープロセスの `$SRUNX_CLUSTER` は無視）。UI 自体は常に選択中のクラスタを付けて呼ぶ。
- Web UI からの同期はユーザーの明示操作なので、初回同期の承認を兼ねる。
