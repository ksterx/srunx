# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Package Management
- `uv sync` - Install dependencies
- `uv add <package>` - Add new dependency
- `uv run <command>` - Run commands in virtual environment

### CLI Usage

#### Job Management (SLURM-aligned commands)
Command names mirror SLURM's CLI where it makes sense (`sbatch` / `squeue` /
`scancel` / `sinfo`) so a SLURM user can map their muscle memory directly.
srunx-specific commands that don't map to a SLURM binary:

- `history` — srunx's own submission history (SQLite-backed). Named
  `history` rather than `sacct` because it shares no backend with real
  `sacct`; only jobs submitted via srunx are listed.
- `gpus` — GPU aggregate summary across partitions.
- `tail` / `watch` — log tailing and cluster watching.

- `uv run srunx sbatch <script>` - Submit a sbatch script (positional, like real sbatch)
- `uv run srunx sbatch --wrap "cmd ..."` - Wrap a command into a SLURM job (mutually exclusive with the positional script)
- `uv run srunx sbatch --cluster <name> ...` - Submit to a configured cluster (local or SSH)
- `uv run srunx squeue` - List active jobs (all users by default — matches native `squeue`). Default columns: Job ID, User, Name, Status, CPUs, GPUs, Elapsed, NodeList
- `uv run srunx squeue -j <job_id>` - Filter to a specific job (replaces the old `srunx status` for active jobs)
- `uv run srunx squeue -u <user>` - Filter to a single user
- `uv run srunx squeue --show-partition --show-limit --show-nodes` - Opt-in columns (each flag adds one)
- `uv run srunx squeue -a` - Shortcut for all opt-in columns at once
- `uv run srunx squeue -i <seconds>` - Live refresh: re-query and redraw in place every N seconds (like native `squeue -i`). Ctrl+C exits
- `uv run srunx squeue --format json` - JSON always includes every field regardless of show flags
- `srunx sbatch` exports `$SRUNX_CODE_DIR` (SSH: remote root of the mount holding the script, else the project's primary mount; local cluster: project root; `--snapshot`: the snapshot dir; unset without a project) so scripts can use the same paths across clusters and snapshots; per-cluster paths go in `[clusters.<c>.defaults.environment.env_vars]`
- `uv run srunx scancel <job_id>` - Cancel a job (without `--cluster`, the job's cluster is looked up in srunx history; 0 or >1 matches is an error — same for `tail`; the banner then says `via job history`). `tail` finds logs of finished jobs via `sacct` (`StdOut`/`StdErr` when the Slurm version has them, otherwise a file named after the job ID under `WorkDir`)
- `uv run srunx sinfo` - Partition / state / nodelist listing (same columns as native SLURM `sinfo`)
- `uv run srunx sinfo --cluster <name>` - Query a remote cluster via SSH adapter (#139)
- `uv run srunx sinfo --partition gpu --format json` - Partition rows as JSON
- `uv run srunx gpus` - GPU aggregate summary (what the old `srunx sinfo` showed)
- `uv run srunx gpus --cluster <name> --partition gpu` - GPU summary scoped to one partition on a remote cluster
- `uv run srunx history` - DB-backed submission history (srunx's own SQLite). Only jobs submitted via srunx are listed
- `uv run srunx history -j <job_id>` - Filter history to a specific job (replaces `srunx status` for finished jobs)
- `uv run srunx history --cluster <name>` - Scope history to a single cluster's jobs (`scheduler_key` filter: `local` / `ssh:<cluster>`)
- `uv run srunx sacct` - Real SLURM `sacct` wrapper (cluster accounting DB — includes manual sbatch jobs, needs slurmdbd). Default columns: Job ID, User, Name, Partition, State, ExitCode, Elapsed
- `uv run srunx sacct -a -S now-1day` - All users over the last day (like native `sacct -a -S now-1day`)
- `uv run srunx sacct -j <job_id>` / `-u <user>` / `-s FAILED,TIMEOUT` / `-p <partition>` - Standard sacct filters
- `uv run srunx sacct --show-steps` - Include `.batch` / `.extern` sub-step rows
- `uv run srunx tail <job_id>` - Show the last 10 lines of the job log (matches native `tail`; use `-n N` for a different cap, `--all` to dump the whole file)
- `uv run srunx tail <job_id> --follow` - Stream job logs. Works over SSH via periodic `tail_log_incremental` polls; tune with `--interval <seconds>` (default 2s). Ctrl+C exits

`sbatch` accepts the standard SLURM short flags (`-J` / `-N` / `-c` /
`-t` / `-p` / `-w` / `-D`) and `--gres=gpu:N`. `-n` is also accepted, but
(per the parity section below) as a real-sbatch passthrough for
`--ntasks`, not a modeled srunx option. srunx-specific extensions
(`--cluster` / `--project` / `--conda` / `--venv` / `--container` / `--template` / etc.)
layer on top. `status` was intentionally dropped — use `squeue -j <id>` for
active jobs or `history -j <id>` for finished jobs.

##### `sbatch` flag parity with real SLURM

`srunx sbatch`'s own flag spellings now match real `sbatch` exactly — same
spelling, same meaning, everywhere:

- **`-Q` is `--quiet`, `-q` is `--qos`** (a value option), matching real
  `sbatch`/`squeue`/`scancel`. srunx never defines its own `-q`.
- **Native sbatch options srunx doesn't model itself pass straight
  through** — `--array`, `--qos`, `--dependency`, `--output`, `--error`,
  `--exclusive`, `--mail-type`, and the rest of the table in
  `src/srunx/cli/_helpers/sbatch_passthrough.py` (`SBATCH_OPTIONS`) are
  recognized in all their real forms (`--array=1-10`, `--array 1-10`,
  `-a 1-10`, `-a1-10`) and forwarded to the actual `sbatch` invocation.
  Options with an *optional* argument (`--exclusive`, `--nice`, ...) only
  accept a value via `=` (matching `getopt_long`'s optional-argument
  rule) — a bare `--exclusive script.sh` cannot swallow the script path.
  **Long-option abbreviations are not recognized here** (unlike real
  `sbatch`'s `getopt_long`) — use `--sbatch-arg=--arr=1-10` to opt in to
  an abbreviation explicitly.
- **`--sbatch-arg <token>`** is the escape hatch for any sbatch option
  the table above doesn't cover yet — pass one raw `-`-prefixed token,
  repeatable.
- **A handful of sbatch spellings are rejected outright**, by exact match
  and by prefix-match *within the reject list* (so a hand-written
  `--sbatch-arg=--pars` is caught too; an abbreviation typed directly as
  `srunx sbatch --pars` is never recognized by the table in the first place
  and Click rejects it as an unknown option — same exit 2, different message):
  `--parsable`, `--test-only`, `--quiet`/`-Q`, `--wrap`, `--help`/`-h`,
  `--usage`, `--version`/`-V`. These would either corrupt srunx's job-ID
  parsing (`--test-only`, `--parsable`), silently swallow the job-ID
  output (`--quiet`), or bypass srunx's own script/`--wrap` handling.
- **Passthrough always wins over srunx's own generated flags** — e.g.
  `--log-dir L --output O` expands to
  `--output=L/... --error=L/... --output=O`; sbatch's own "last wins"
  rule means the user's `--output=O` sticks (and `--error` still comes
  from `--log-dir`). Same precedence applies against `--wrap`'s
  template-rendered `#SBATCH` directives.
- **`-v`/`--verbose` and `--wait` are srunx's own flags**, not real
  sbatch's `--verbose`/`--wait` — `srunx sbatch -v` controls srunx's own
  output, and sbatch's `-W`/`--wait` (block until the job completes,
  scheduler-side) isn't exposed at all (different meaning from srunx's
  own `--wait`, which polls after submission).
- **Known limitation**: array job IDs (e.g. `123_4`) are parsed with
  `int()` in `srunx.slurm.clients._ssh_queries`, which silently drops the
  `_4` suffix (`int("123_4")` is `1234`, Python's digit-separator
  syntax) — `squeue`/`history` mis-track array jobs submitted via
  `--array`. Not fixed by this passthrough; tracked separately.

##### Script defaults (`#SBATCH` vs config)
`srunx sbatch script.sh` parses the script's `#SBATCH` directives (the set in
`DIRECTIVES` in `src/srunx/cli/_helpers/sbatch_resolve.py`) and forwards a
configured default only for options the script does not set and the user did
not type. Built-in defaults are never forwarded (Slurm has its own). A warning
is printed when the script contradicts a *cluster-specific* default
(`[clusters.<c>.defaults]`). `--gres` is passed through verbatim (and covers
`gpus_per_node`). `--clusters` / `-M` is rejected anywhere (CLI, passthrough,
or `#SBATCH`) — srunx's own `--cluster` picks the target. `--unset KEY`
(repeatable) drops an inherited default for one submission; `--dry-run` shows
the selected cluster and each value's source.

##### Auto-sync + in-place execution

When the positional script lives under one of the current project's mounts on
the target SSH cluster (`mount.local`), srunx:

1. **Auto-rsyncs** that mount to the remote (`mount.remote`) under a
   per-mount file lock (default ON; opt out with `--no-sync` or
   `[sync] auto = false`). The first sync to a remote path that has not been
   approved before asks for confirmation; `--yes`/`-y` approves it
   non-interactively (approved targets are recorded in the state dir, see
   `src/srunx/sync/approval.py`).
2. Translates the script path to its remote equivalent and invokes
   `sbatch` **directly on the remote file** — no tmp copy, no
   ``-o $SLURM_LOG_DIR/%x_%j.log`` auto-injection, your script's own
   `#SBATCH` directives win.
3. ``cd``s into the script's mount-translated parent directory before
   sbatch, so relative paths inside the script (e.g.
   ``#SBATCH --output=./logs/%j.out``) resolve where you'd expect.

Generated artifacts (``--wrap``, ``--template``, workflow ShellJobs
with Jinja substitution) always go through the historical
``$SRUNX_TEMP_DIR`` upload path because the rendered bytes have no
canonical home in the mount.

Mounts come from the project config: the project root maps to
`[clusters.<c>] remote_dir` (default: the remote
`$XDG_DATA_HOME/srunx/workspaces/<name>-<hash>`), plus any extra
`[[clusters.<c>.mounts]]` (`name` / `local` / `remote` / `sync` / `exclude`).

Sync settings live under `[sync]` in the user (or system) config
(`$XDG_CONFIG_HOME/srunx/config.toml`):

```toml
[sync]
auto = true
lock_timeout_seconds = 120
warn_dirty = true
require_clean = false
owner_check = true
verify_remote_hash = false
```

Per-invocation overrides: `srunx sbatch --sync` / `--no-sync` /
`--force-sync`. (The old `SRUNX_SYNC_*` env vars no longer exist.)

##### Configuration (TOML, layered)
Files (see `src/srunx/common/paths.py`; `srunx config paths` lists them):

- System `/etc/srunx/config.toml` and user `$XDG_CONFIG_HOME/srunx/config.toml`:
  `default_cluster`, optional `state_dir`, `[defaults]`, `[clusters.<c>]`
  (connection + `[clusters.<c>.defaults]`), `[notifications]`, `[sync]`, and
  personal per-project overrides `[projects.<name>.clusters.<c>]`.
- Project `<root>/.srunx/config.toml` (meant to be committed): required
  `name`, optional `cluster`, `[defaults]`, `[clusters.<c>]` with
  `remote_dir` / `sync` / `exclude` / `[[mounts]]` / `defaults`. Connection
  settings are not allowed. Discovered by walking up from cwd, stopping at a git
  root or `$HOME`; `--project PATH` overrides. Project workflows live in
  `<root>/.srunx/workflows/`.
- State (`$XDG_STATE_HOME/srunx`, or `state_dir`): `srunx.db`, `locks/`,
  remote-env cache, approved sync targets.

Job-default precedence (low → high): built-in, system `defaults`, user
`defaults`, user `clusters.<c>.defaults`, project `defaults`, project
`clusters.<c>.defaults`, script `#SBATCH` / workflow job fields, CLI flags.
Any table may contain `unset = [...]` to clear an inherited key.
`SRUNX_DEFAULT_*` env vars no longer exist.

- `uv run srunx config show [--cluster C] [--project PATH]` - Effective config with each value's source
- `uv run srunx config paths` - Files read, in precedence order, plus the state dir
- `uv run srunx config init [--force]` / `config init-project [PATH] [--name N]`
- `uv run srunx config set KEY VALUE [--cluster C] [--project]` - `resources.*` / `environment.*` / `log_dir` / `work_dir` go under `defaults` (or `clusters.<c>.defaults` with `--cluster`); other keys are top-level or `clusters.<c>.<key>`. `--project` edits the project file (`--project-path` to pick it)
- `uv run srunx config delete KEY` / `config unset KEY` (both take `--cluster` / `--project` / `--project-path`) - Remove a key from one file / add it to that table's `unset`

##### Cluster selection (unified CLI)
`--quiet`'s short flag is `-Q` (not `-q`) to match real sbatch/squeue/scancel,
where `-q` is short for `--qos` (a value-taking option). On `sbatch`, `-q <value>`
already works as the `--qos` passthrough (see the sbatch section above). On the
other commands (`squeue` / `scancel` / `sinfo` / …), which have no `--qos`, `-q`
is simply an unknown option and exits 2.

All job-management commands above accept `--cluster <name>` / `--quiet`
(no `-C` short form: `-C` is sbatch's `--constraint`). Resolution order:
1. `--cluster <name>`
2. `$SRUNX_CLUSTER` (the only selection env var)
3. the project's `cluster` (`.srunx/config.toml`)
4. the user config's `default_cluster`
5. otherwise an error — there is no implicit local fallback

A one-line banner on stderr always names the cluster and why it was chosen —
e.g. `● Connected to user@host (cluster: <name> · via --cluster)` for SSH, or
`● Local SLURM (cluster: <name> · via default_cluster)`. `--quiet` suppresses
it. MCP never resolves implicitly: every tool takes an explicit `cluster`.

`--profile` / `--local` / `$SRUNX_SSH_PROFILE` / `srunx ssh ...` are gone.
History is keyed by (cluster, job_id); the DB scheduler key is `local` or
`ssh:<cluster>`.

#### Watching / monitoring
- `uv run srunx watch jobs <job_id>` - Block until a job reaches a terminal state
- `uv run srunx watch jobs <job_id> --continuous` - Stream state transitions (no timeout)
- `uv run srunx watch jobs --all` - Watch every user job
- `uv run srunx watch jobs <job_id> --interval 30` - Custom polling interval
- `uv run srunx watch resources --min-gpus 4` - Block until N GPUs are free
- `uv run srunx watch resources --min-gpus 2 --continuous` - Stream GPU availability changes
- `uv run srunx watch resources --min-gpus 4 --partition gpu` - Scoped to one partition
- `uv run srunx watch cluster --schedule 1h --notify $WEBHOOK` - Periodic cluster reports

#### Clusters
- `uv run srunx cluster list` - List configured clusters
- `uv run srunx cluster show [NAME] [--project PATH]` - Connection, defaults, and the project's mounts on it
- `uv run srunx cluster add NAME --local | --host ALIAS | --hostname H [--username U --key-file K --port P --proxy-jump J --description D --snapshot-root R] [--default]` - Add a cluster to the user config
- `uv run srunx cluster update NAME ...` - Change connection settings
- `uv run srunx cluster update NAME --snapshot-root <path>` - Set where `--snapshot` stores per-commit snapshots on the cluster (absolute or `~`-relative, charset `[A-Za-z0-9._/~+-]`, no `..`; empty string resets to the default `~/.local/share/srunx/snapshots`). Must lie outside every mount (`cluster sync --delete` mirrors mounts)
- `uv run srunx cluster remove NAME` - Remove a cluster from the user config
- `uv run srunx cluster test [NAME]` - Check reachability (and cache the remote `$HOME` / XDG dirs)
- `uv run srunx cluster secret set NAME KEY [--from-env VAR]` - Store a secret (value via hidden getpass prompt, or `--from-env VAR`; NO inline value argument)
- `uv run srunx cluster secret list [NAME]` - List secret KEY names (values are never printed)
- `uv run srunx cluster secret unset NAME KEY` - Remove a secret
- `uv run srunx cluster sync [NAME] [--mount M] [--project PATH]` - Sync a project mount (default: the one owning cwd)
- `uv run srunx cluster sync --dry-run` / `--pull` / `-e PATTERN` / `--yes`
- `uv run srunx cluster sync --delete` - Mirror: also delete destination files missing from the source

##### Sync is additive by default
`RsyncClient.push()` defaults to `delete=False`, so **no** caller inherits
mirror semantics by accident — a sync adds and updates, and leaves
destination-only files (checkpoints, job logs, job outputs) alone. Mirroring
is opt-in per call site: `srunx cluster sync --delete`, the Web
file/template/job sync routes (`delete=True` explicit), and MCP
`sync_files(delete=True)`. This is the standing fix for the incident where
auto-sync silently pruned remote-only outputs; see
`src/srunx/sync/mount_helpers.py`.

The trade-off is that a locally deleted file stays on the cluster, where a job
can still import it. MCP exposes `inspect_mount` for that — read-only, reports
cluster-only paths. It is a separate tool rather than a flag on `sync_files`
because an inspection has to be documented as unconditionally safe: folding it
into `sync_files` meant one `delete` argument carrying both "show me what a
mirror would remove" (harmless) and "remove it" (destructive), and an agent
reading the data-loss warning avoids the argument entirely — losing the safe
inspection with it.

`inspect_mount` narrows the raw candidates using a **manifest** — a record of
what srunx uploaded, kept at `<mount.remote>/.srunx-manifest.json` (mode 0600;
paths stored in rsync's escaped form so they compare against a deletion
preview). A path that is in the record but no longer local is stale code;
output a job wrote at a path of its own was never uploaded, so it is not in the
record and cannot be reported, which holds even when the mount's excludes miss
an output directory. That is the whole point: excludes separate the two only as
well as they are maintained, and a real mount here had four stale scripts
buried among 39 artifacts because `dist/` was missing from them.

Only path names are recorded, not contents, so a path that is *both* recorded
and job-written stays ambiguous — a job that overwrote a file srunx sent keeps
its path, and a file deleted in the gap between srunx's inventory and rsync's
own walk is recorded without having been sent. Proving otherwise needs a
content hash per file on every sync, which is out of scope; the report is for a
human to read, and srunx never deletes on its own.

The set recorded is the inventory taken **before** the transfer, not after.
Scanning afterwards is wrong in both directions: a file created mid-sync would
be recorded though rsync never sent it (and could then name a job's output for
deletion), while a file deleted mid-sync would be dropped though rsync did send
it — leaving it on the cluster, permanently unrecordable, with the report still
calling itself complete.

The record fails **closed**: no record yet, an unreadable one, or a changed
exclude filter all report `stale_uploads_known: false` rather than an empty
list, because "cannot tell" presented as "clean" is how a stale module keeps
running unnoticed. Written only after a successful sync (and after hash
verification, like the owner marker), accumulating on an additive sync and
replaced by a mirror. See `src/srunx/sync/manifest.py`, whose module docstring
also records the gaps left open on purpose.

##### Commit snapshots (`sbatch --snapshot`, `snapshot create`, `snapshot list`, `snapshot prune`)
Auto-sync runs the script from a mutable mount at job *start*, so a later sync
changes the code under pending, running, and requeued jobs. `--snapshot` runs
the job from a read-only, per-commit git worktree on the cluster instead
(SSH clusters only; issue #247).

- `uv run srunx sbatch --snapshot job.sbatch --cluster <name>` - Snapshot HEAD of the repo in cwd and submit the script from it
- `uv run srunx sbatch --snapshot-rev <REV> job.sbatch --cluster <name>` - Snapshot another revision (implies `--snapshot`)
- `uv run srunx snapshot create [REV] --cluster <name>` - Ensure the snapshot exists; stdout is only the absolute snapshot path (status on stderr), no lease is written
- `uv run srunx snapshot list [--cluster <name>] [-f/--format table|json] [--size]` - Every repo and snapshot under the cluster's snapshot root: state (`complete` / `incomplete` / `missing`), last-requested time (marker mtime), each lease (state, job id, profile, cluster, live job state) and queued jobs that reference the snapshot path. A scheduler failure only shows job states as `unknown` with a warning (exit stays 0). JSON carries every field
- `uv run srunx snapshot prune [--older-than 7d] [-n/--dry-run] [-f/--force] [--include-unbound] [--cluster <name>]` - Remove snapshots no pending, running or requeue-able job can use. Prints one `keep` / `remove` line per snapshot with the reason, then asks `Remove <n> snapshot(s)?` unless `-f`; `-n` prints the plan only

Behaviour:
- Layout: `<snapshot_root>/<repo_id>/{repo.git, trees/<sha>/, trees/<sha>.complete, leases/<id>.json, .lock}`. `repo_id` is `<repo name>-<12 hex of the root commit>`, shared by every linked worktree of one repo. The commit is pushed (only missing objects) to the bare repo with the remote git binary the probe validated (>= 2.17); the cluster needs no GitHub access.
- The script path is the one in the commit (the local file need not exist, and job name / `#SBATCH` directives are read from the commit). Default job cwd is the snapshot dir matching your local cwd, else the snapshot top; `-D/--chdir` still wins. Job env gains `SRUNX_CODE_DIR` (snapshot path) and `SRUNX_SNAPSHOT_SHA`; these override a same-named `--env`. `jobs.metadata` records `{"snapshot": {repo_id, sha, path, lease_id}}`.
- No auto-sync, no mount lock, the script need not be under a mount. Rejected with exit 2: `--wrap`, `--template`, a local cluster, an explicit `--sync`, `--force-sync`, a missing positional script. `--no-sync` is accepted. `--dry-run` validates locally and prints the plan with no remote call.
- Local validation fails closed (exit 2): not a git repo, shallow clone, submodule, LFS filter, script missing / symlink / directory in the commit, and tracked changes whenever the commit equals HEAD (untracked files only warn). Remote, push, lock, and sbatch errors exit 1.
- Lease: a `provisional` lease is written before `sbatch` and rewritten as `bound` (with the job id) after. It is deleted only on a confirmed sbatch rejection (non-zero exit, no job id, and an `sbatch: error:` line in the output; a bare non-zero exit is not evidence); any indeterminate outcome keeps it and warns with its path, so a later prune cannot remove code under a job that may exist.
- **The snapshot is read-only, so the job cannot write into its default cwd.** srunx warns (does not reject) when `--chdir` is missing or relative and `--output` is missing or relative, or `--error` is relative. Pass an absolute `--output` / `--error` (or `--log-dir`) or an absolute `-D`. Relative paths in `-D/-o/-e` resolve inside the snapshot.
- Locking: one `mkdir`-based lock per repo, held for seconds inside a single remote script and released by a shell `trap`. Waiters retry for about 120 s and the lock is **never broken automatically**. After a timeout the error prints the lock path and owner; once you have confirmed the owner is dead (e.g. a killed client or crashed node), remove it manually with `rm -rf <snapshot_root>/<repo_id>/.lock`.
- Reuse trusts a tree that is a real directory at the same HEAD with a regular marker file; it does not re-hash contents. srunx requires every directory from `/` down to the snapshot root, repo dir, `trees/` and `leases/` to be owned by you (ancestors: you or root) and not replaceable by other users (not group/other-writable, except sticky dirs like `/tmp`). A directory you own with no group/other execute bit and no ACL (for example `$HOME` at 0700) shields everything below it, so a group-writable `~/.local` under it is fine; otherwise it exits 1 with code 32 naming the directory. Fix it with `chmod go-w <dir>` or choose a `snapshot_root` under a private directory.
- `chmod -R a-w` only prevents accidental writes; it is not tamper protection. Prefer `snapshot prune` (below) to deleting by hand. If you must, make only the directories writable (`find <tree> -type d -exec chmod u+rwx {} +`) before `rm -rf`; a recursive `chmod -R u+w` would also change files hardlinked from outside the store.

Prune (`snapshot list` / `snapshot prune`, SSH clusters only; a local cluster exits 2):
- Fail closed: a snapshot is removed only when every fact needed to call it dead was positively confirmed; everything else is kept. If the scheduler cannot be queried (`squeue` fails, a section is missing, a row cannot be parsed, no SLURM runner) nothing is deleted and the exit code is 1; an unavailable `sacct` or `scontrol` only makes the affected jobs `unknown` (kept). There is no flag to assume unknown jobs finished.
- Job states come from the cluster through the SSH client's SLURM environment (the discovered SLURM directory is put on `PATH`), judged only by stdout and exit codes (stderr is never inspected): `squeue -h -a -t all` for the current user, `sacct` (`State`, `End`, `Cluster`; `End` as epoch seconds), `scontrol show config` for `MinJobAge` (best effort) and `scontrol show job` for ids nobody else answered. Any `squeue -t all` row, in any state (finished jobs stay in the scheduler's memory and can be requeued), makes the job active; `scontrol` can never make a job terminal. Terminal needs no `squeue` row and only terminal `sacct` rows. A missing clock or `sacct` output section fails the whole query (nothing is deleted, exit 1); an unavailable `sacct` makes the jobs `unknown`, and so does one malformed `sacct` row (nothing in that section is trusted).
- A bound lease's terminal state is used only when the lease records the cluster it was submitted to and that name equals the `Cluster` of the job's `sacct` rows (`lease from another cluster`, `lease cannot be verified against this cluster` otherwise). The cluster is recorded at bind time with one bounded lookup, `squeue -h -j <id> -O Cluster` (answered by slurmctld, no accounting lag) and then `sacct -j <id> -X -n -P -o Cluster`, each wrapped in `timeout 10` when available; it never fails the submission. A lease without a recorded cluster (lookup failed, or written by an older srunx) is never verifiable: its snapshot is kept with `lease cluster unknown` until removed by hand. No time- or profile-based guess is made (a profile is not a cluster, and a job id alone is not an identity). `sbatch --snapshot` rejects `-M/--clusters` (exit 2), including as a `#SBATCH` directive in the script, because the lease would name the wrong cluster.
- A snapshot is removed when its leases are all dead (job ended more than `--older-than` ago) or it has none, and it was last requested (marker mtime) longer ago than `--older-than`; lease-less snapshots (and incomplete trees, marker-only entries) additionally need 7 days (`max(--older-than, 7d)`). Leases without a snapshot are removed once dead. A repo with an unreadable lease (every entry of `leases/` counts, hidden or oddly named files too: a symlink, not valid JSON, mismatched id, or a name other than `<32 hex>.json`) keeps every snapshot in it; a repo whose directories fail the Phase 1 trust checks, or whose `repo.git` is a symlink or not owned by you, is skipped with the reason.
- `--older-than` takes `<N>d`, `<N>h` or `<N>m`; below `1h` exits 2 before any remote call. When `MinJobAge` is readable it must also be at least `MinJobAge + 10 minutes` (exit 2, nothing deleted); when it is not readable only the 1 hour floor applies.
- `--include-unbound`: without it a provisional lease (an `sbatch` whose outcome was unknown) always keeps its snapshot. With it, a provisional lease older than `max(--older-than, 24h)` counts as dead (unless a queued job's working directory or command names the snapshot), with one stderr warning per such lease, because a job may still exist.
- Path-reference guard: for every snapshot, whatever its leases, a `squeue -a -t all` row whose working directory or command contains `<repo_id>/trees/<sha>` keeps it (`referenced by job <id>`). A job submitted outside `sbatch --snapshot` (for example a plain `sbatch` using a `snapshot create` path) has no lease and is protected only by this guard and the 7 day floor for lease-less snapshots; a job that names the snapshot only through an environment variable (neither working directory nor command) is not detected.
- Reuse refreshes the marker: `snapshot create` / `sbatch --snapshot` of an existing snapshot touches `trees/<sha>.complete` under the lock, so its mtime means "last requested". If the touch fails or does not advance, the reuse fails (remote exit 33) rather than handing out a snapshot prune may delete.
- Deletion: the decision is made locally from one read-only inventory; per repo, one remote script takes the Phase 1 lock, requires `repo.git` to be a real directory you own (else every sha is `FAILED repo-git` and nothing is touched), recomputes each snapshot's fingerprint (marker and tree mtime, every entry of `leases/` that names the sha, with mtimes) and removes a sha only if unchanged (otherwise `CHANGED`, reported as kept, not an error). Per sha, in order: rename the tree to `trees/.trash.<sha>.<token>` (if that fails nothing else is touched), remove the marker, delete the `refs/srunx/snapshots/<sha>` ref, delete exactly the expected lease files; each step is checked and a failure reports `FAILED <sha> <step>` and stops that sha. Then `git worktree prune`. The results are printed and flushed before the lock-free trash sweep (only exact `.trash.<40 hex>.<32 hex>` real directories you own; others are reported and left), so a sweep cut short by a timeout still reports the removals (with a warning and exit 1) and the next prune finishes it. A leftover trash directory from an interrupted prune is cleaned by the next real prune, not by `-n`.
- Never touched: `.lock`, `repo.git`, the repo directory, `trees/`, `leases/`, unrecognised entries, anything in a skipped repo. No `git gc` is run (objects of a pruned commit are reclaimed by git's own gc later). Emptied repo directories stay until removed by hand.
- Exit codes: option errors, `--older-than` below the floor or `MinJobAge + 10 min`, a non-SSH cluster, an unknown cluster and a literal root/mount conflict exit 2; inventory failure, scheduler failure, a lock timeout or script failure in any repo (the other repos are still processed) and any `FAILED` sha exit 1; a declined prompt or `-n` exits 0, a closed stdin aborts with 1.

##### Account secrets (`cluster secret`)
Secrets (API keys etc.) are stored in a single **remote** `0600` file per
account (not per cluster), `$HOME/.config/srunx/secrets.env` (parent dir
`~/.config/srunx` is `0700`), as `export KEY='...'` lines. The path is keyed by
remote account (`$HOME`), not by local cluster name — the cluster you
pick selects the account, so the same file is shared by any
cluster pointing at that account (this avoids rename-orphaning and cluster-name
mismatches between machines). The value is captured only via a hidden getpass
prompt or `--from-env VAR` — never an inline command argument — and never
lands in the local config, command-line args, or log output. Writes are
atomic (temp file in the same dir + `mv`) with a symlink/foreign-owner guard.
`secret list` prints KEY names only. The old `srunx ssh env` /
profile-`custom_env_vars` mechanism (which stored env vars in local plaintext
config) has been removed entirely.

On submit over SSH, when the account's secret file exists srunx sources it into
the submission shell — `if [ -r '<path>' ]; then set -a; . '<path>'; set +a; fi`
(non-blocking: always exits 0 so an unreadable/removed file never fails the
actual squeue/scancel/sbatch) — before the job-level `--env` exports (a `--env`
key still wins over a same-named secret). Because the secret must reach the job, `--export=ALL` is added to the
remote `sbatch` whenever job env vars are present **or** the secret file exists;
that deliberately overrides a script's own `#SBATCH --export=NONE`. With neither
job env vars nor a secret file, `--export=ALL` is not added and the script's own
export policy wins.

#### Workflows
- `uv run srunx flow run <yaml_file>` - Execute workflow from YAML
- `uv run srunx flow run <yaml_file> --validate` - Validate without executing (replaces the old `flow validate` subcommand)
- `uv run srunx flow run <yaml_file> --cluster <name>` - Run on an SSH cluster; auto-syncs each touched mount **once** + holds the per-mount lock across every job in the workflow (Phase 2 of #135)
- `uv run srunx flow run <yaml_file> --cluster <name> --no-sync` - Skip rsync but still hold the per-mount lock (race-free submission against existing remote state)
- ShellJobs whose `script_path` lives under a synced mount and whose Jinja-rendered bytes match the source bytes are **executed in place** on the remote — the user's own `#SBATCH --output=` directives win, no tmp copy.

#### `--wrap` (real sbatch parity)
- `uv run srunx sbatch --wrap "cmd1 && cmd2"` runs both commands on the compute node — internally wrapped as `srun bash -c '...'` so shell operators (`&&`, `|`, `>`, `;`) evaluate where the user expects (closes #138).

#### Configuration
- `uv run srunx config show` - Show current configuration
- `uv run srunx config paths` - Show configuration file paths

#### Templates
- `uv run srunx template list` - List available SLURM script templates
- `uv run srunx template show <name>` - Show template contents
- Submit with a specific template: `srunx sbatch --template <name> --wrap "<cmd>"`

### Testing
- `uv run pytest` - Run all tests
- `uv run pytest --cov=srunx` - Run tests with coverage
- `uv run pytest tests/domain/test_jobs.py` - Run specific test file
- `uv run pytest -v` - Run tests with verbose output

### Test layout rules (KEEP THESE TO AVOID FUTURE TECH DEBT)

The `tests/` tree mirrors `src/srunx/`. New tests must follow this 1:1
correspondence so the test for any source module is mechanically findable
without grep:

1. **`tests/X/test_Y.py` tests `src/srunx/X/Y.py`.** No exceptions inside
   subpackages. If `src/srunx/observability/monitoring/job_monitor.py`
   exists, its tests live in `tests/observability/monitoring/test_job_monitor.py`.
2. **Only top-level srunx modules go to `tests/` root.** Currently that's
   `tests/test_callbacks.py` (covers `srunx.callbacks`) and
   `tests/test_utils.py` (covers `srunx.utils`). Don't add new files at
   `tests/` root unless `src/srunx/<name>.py` is also at the top level.
3. **Cross-cutting tests get their own dir, NOT the root.**
   - `tests/integration/` — end-to-end / multi-module scenarios.
   - `tests/architecture/` — codebase-wide invariants (import-layer lint
     etc.).
4. **Every test subpackage has an `__init__.py`.** This prevents pytest
   collection-name collisions when two unrelated subpackages happen to
   share a test filename (e.g. multiple `test_protocols.py`).
5. **No path tricks like `Path(__file__).parent.parent / "src" / ...`.**
   They tie tests to a specific filesystem layout and break the moment
   the test file moves. Use the public API for resource discovery —
   e.g. `from srunx.runtime.templates import get_template_path` for
   built-in templates, `importlib.resources` for package data.
6. **Test isolation in `tests/conftest.py`:** the autouse fixture
   `_isolate_srunx_dirs` (and the `reset_config` fixture) neutralise
   developer-machine state: `XDG_CONFIG_HOME` / `XDG_STATE_HOME` point at
   tmp dirs, the user config gets a single `local` cluster as
   `default_cluster`, and `$SRUNX_CLUSTER` is cleared. When adding a new env-driven
   surface, extend these fixtures rather than relying on
   `@patch.dict(os.environ, {}, clear=True)` (which over-clears and
   clobbers the XDG isolation set by the autouse fixtures).

### Direct Usage Examples

#### Job Submission
- `uv run srunx sbatch --wrap "python train.py" --job-name ml_job --gpus-per-node 1`
- `uv run srunx sbatch train.sh --conda ml_env --nodes 2`
- `uv run srunx sbatch --wrap "python eval.py" --gres=gpu:4`  # SLURM-native --gres form

#### Monitoring Workflows
```bash
# Submit a job and watch until completion
job_id=$(uv run srunx sbatch --wrap "python train.py" --gpus-per-node 2 | grep "Job ID" | awk '{print $3}')
uv run srunx watch jobs $job_id

# Wait for GPUs to become available, then submit
uv run srunx watch resources --min-gpus 4
uv run srunx sbatch --wrap "python train.py" --gpus-per-node 4

# Continuously watch all user jobs with notifications
uv run srunx watch jobs --all --continuous --interval 30

# Send periodic cluster reports
uv run srunx watch cluster --schedule 1h --notify $SLACK_WEBHOOK

# Check current resource availability
uv run srunx gpus --partition gpu

# Inspect partition / state / nodelist (native-sinfo parity)
uv run srunx sinfo --partition gpu
```

#### Clusters
- `uv run srunx cluster add myserver --hostname dgx.example.com --username researcher`
- `uv run srunx sbatch train.sh --cluster myserver --job-name remote_training`

#### Workflows
- `uv run srunx flow run workflow.yaml`

## Architecture Overview

### Current Modular Structure
6-layer architecture: `interfaces → runtime / slurm / observability / integrations → domain → common`.

```
src/srunx/
├── __init__.py, _version.py, py.typed
├── callbacks.py       # Callback base class + NotificationWatchCallback (SlackCallback lives under observability)
├── utils.py           # get_job_status + job_status_msg
│
├── common/            # Shared infrastructure
│   ├── config.py      # TOML config: file schemas, Settings (layered merge + sources), cluster resolution, mounts
│   ├── paths.py       # XDG config / state / project paths + project discovery
│   ├── exceptions.py  # WorkflowError / TransportError / JobNotFoundError / ...
│   ├── logging.py     # configure_{cli,workflow}_logging + get_logger
│   └── _version.py    # hatch-vcs auto-generated
│
├── domain/            # Canonical models (Pydantic v2)
│   ├── jobs.py        # BaseJob / Job / ShellJob / JobResource / JobEnvironment / JobStatus / JobType / RunnableJobType / ContainerResource / DependencyType / JobDependency
│   └── workflow.py    # Workflow
│
├── runtime/           # Execution layer (no I/O primitives; sits above slurm/observability)
│   ├── lifecycle.py   # JobLifecycleSink / CompositeSink / NoOpSink
│   ├── rendering.py   # render_job_script + render_shell_job_script + SubmissionRenderContext + normalize_job_for_submission
│   ├── submission_plan.py  # plan_sbatch_submission / collect_touched_mounts / translate_local_to_remote / resolve_mount_for_path
│   ├── templates.py   # Template registry + get_template_path / list_templates / get_template_info
│   ├── _jinja/        # base.slurm.jinja
│   ├── security/      # mount_paths + python_args guards
│   ├── sweep/         # SweepSpec / SweepOrchestrator / aggregator / reconciler / state_service / expand
│   └── workflow/      # WorkflowRunner + loader / safe_eval / transitions
│
├── slurm/             # SLURM wrappers (local + SSH)
│   ├── local.py       # LocalClient + Slurm wrapper + submit_job/retrieve_job/cancel_job
│   ├── ssh.py         # SlurmSSHAdapter
│   ├── ssh_executor.py # SlurmSSHExecutorPool
│   ├── protocols.py   # Client / JobOperations / WorkflowJobExecutor / JobSnapshot / LogChunk
│   ├── parsing.py     # GPU_TRES_RE, parse_slurm_datetime, parse_slurm_duration
│   └── states.py      # SLURM state normalisation
│
├── transport/         # Cluster → transport resolver (scheduler key `local` / `ssh:<cluster>`)
│   └── registry.py    # TransportHandle / resolve_transport / _build_ssh_handle
│
├── observability/     # Monitoring + notifications + state persistence
│   ├── recorder.py    # DBRecorderSink
│   ├── callbacks.py   # CallbackSink (adapts Callback → JobLifecycleSink)
│   ├── monitoring/    # Job/resource monitoring + pollers + scheduled reports
│   │   ├── base.py, job_monitor.py, resource_monitor.py, resource_source.py, scheduler.py, types.py
│   │   └── pollers/   # supervisor, active_watch_poller, delivery_poller, resource_snapshotter, reload_guard
│   ├── notifications/ # Notification domain (Slack + future channels)
│   │   ├── sanitize.py, presets.py, service.py, legacy_slack.py, formatting.py
│   │   └── adapters/  # DeliveryAdapter + SlackWebhookAdapter + registry
│   └── storage/       # SQLite persistence at <state_dir>/srunx.db
│       ├── connection.py, migrations.py, models.py, cli_helpers.py
│       └── repositories/  # JobRepository, DeliveryRepository, EndpointRepository, ...
│
├── containers/        # Container runtime adapters (ContainerRuntime / PyxisRuntime / ApptainerRuntime / LaunchSpec)
├── ssh/               # SSH connection (ServerProfile view of a cluster) + SSHSlurmClient + ProxyJump
│   ├── core/          # client.py, slurm.py, connection.py, proxy_client.py, file_manager.py, log_reader.py, config.py, ssh_config.py
│   ├── cli/           # secret_impl (backs `srunx cluster secret`)
│   └── helpers/       # proxy_helper
├── sync/              # rsync client + per-mount lock + owner_marker + hash_verify + approval (first-sync target approval)
├── snapshot/          # per-commit read-only git snapshots (local_git, lease, remote scripts, service) + prune (inventory scripts, job_states, pure prune.decide); integrations layer
│
├── cli/               # Typer CLI surface
│   ├── main.py        # Thin Typer root (app, flow_app, sub-app registration)
│   ├── watch.py       # watch app (jobs / resources / cluster)
│   ├── workflow.py    # flow run executor (_execute_workflow)
│   ├── commands/      # Per-command-group modules: jobs (sbatch/squeue/scancel/sinfo/gpus/tail), reports (history), cluster, config, templates, snapshot, ui
│   └── _helpers/      # debug_callback, sbatch_helpers, notification_setup, transport_options
│
├── mcp/               # MCP server for AI agent integration (FastMCP tool surface)
│
└── web/               # FastAPI + React web UI
    ├── app.py, config.py, deps.py, sync_utils.py, serializers.py
    ├── routers/       # workflows, jobs, runs, resources, endpoints, deliveries, subscriptions, watches, templates, sweep_runs, files, config, history
    ├── schemas/       # Pydantic request/response DTOs (workflows, ...)
    ├── services/      # Business logic extracted from routers (workflow_{storage,validation,run_query,run_cancellation,submission}, sweep_submission, _submission_common)
    └── frontend/      # React SPA (components, hooks, pages, lib)
```

### Template System
- Enhanced Jinja2 templates with conditional resource allocation
- `base.slurm.jinja`: Full-featured template with all options; inter-job values are resolved at workflow load time via `exports` / `deps` (no runtime env-file wiring in the template)
- Automatic environment setup integration

### Workflow Definition
Enhanced YAML workflow format with variables and exports:
```yaml
name: ml_pipeline
args:
  base_dir: /data/experiments
  model_name: resnet50

jobs:
  - name: preprocess
    command: ["python", "preprocess.py", "--output", "{{ base_dir }}/data"]
    exports:
      data_path: "{{ base_dir }}/data/processed"
    nodes: 1

  - name: train
    command: ["python", "train.py", "--data", "{{ deps.preprocess.data_path }}"]
    depends_on: [preprocess]
    exports:
      model_path: "{{ base_dir }}/models/best.pt"
    gpus_per_node: 1
    conda: ml_env
    memory_per_node: "32GB"
    time_limit: "4:00:00"

  - name: evaluate
    command: ["python", "evaluate.py", "--model", "{{ deps.train.model_path }}"]
    depends_on: [train]
```

#### Workflow Variables (`args`)
- Defined at workflow level, expanded into job fields via Jinja2 (`{{ var_name }}`) at workflow load time
- Supports `python:` prefix for dynamic evaluation (CLI only, rejected from web API for security)
- Variables can reference each other with automatic dependency resolution

#### Inter-Job Exports (Load-Time)
- Jobs declare static `exports` (dict of KEY=value) — string literals expanded with workflow `args` at load time
- Downstream jobs reference parent values as `{{ deps.<parent_name>.<key> }}`; Jinja (with `StrictUndefined`) substitutes literals into the child's fields at workflow load time, before any job is submitted
- `deps.X` is only populated for names listed in the child's `depends_on`; referencing a non-dep or a missing key fails the workflow at load time
- No runtime env-file mechanism: `$SRUNX_OUTPUTS`, `$SRUNX_OUTPUTS_DIR`, and dynamic `echo "key=value" >> $SRUNX_OUTPUTS` writes are gone
- Values that can only be computed at runtime are out of scope — pass them explicitly (e.g. `--out-file /shared/path/result.json`) or make the path deterministic from `args`

### Parameter Sweeps

srunx supports matrix parameter sweeps for running the same workflow with
different parameter combinations without copying YAML. A sweep expands the
matrix into N workflow_runs (cells) that execute independently, each with
its own materialized sbatch. Sweeps are usable from CLI, Web API, and MCP.

Minimal sweep YAML:

```yaml
name: train
args:
  lr: 0.01
  seed: 1
  dataset: cifar10

sweep:
  matrix:
    lr: [0.001, 0.01, 0.1]
    seed: [1, 2, 3]
  fail_fast: false       # default
  max_parallel: 4        # required

jobs:
  - name: train
    command: ["python", "train.py", "--lr", "{{ lr }}", "--seed", "{{ seed }}"]
    gpus_per_node: 1
```

CLI invocations:

```bash
# YAML-declared sweep
srunx flow run train.yaml

# Ad-hoc sweep (CLI overrides/augments YAML)
srunx flow run --sweep lr=0.001,0.01,0.1 --max-parallel 2 train.yaml

# Single-arg override (no sweep)
srunx flow run --arg dataset=imagenet train.yaml

# Dry-run preview (prints cell args without submitting)
srunx flow run --sweep lr=0.001,0.01 --max-parallel 2 --dry-run train.yaml
```

Constraints:

- `max_parallel` is required (YAML or `--max-parallel`; Web API defaults to 4).
- Matrix values must be scalar (str/int/float/bool); nested structures rejected.
- Cell count capped at 1000 (safety valve; SLURM MaxSubmitJobs is typically ~4096).
- `fail_fast` defaults to false; one cell failing does not abort peers.
- Web UI + MCP sweep submissions can route through the configured SSH adapter
  via a per-sweep `SlurmSSHExecutorPool` (capped at `min(max_parallel, 8)`
  pooled connections); the pool is closed when the orchestrator returns.
  MCP tools always take an explicit `cluster` argument — MCP never reads
  `$SRUNX_CLUSTER`, the project's `cluster`, or `default_cluster`. The
  orchestrator's default `executor_factory=None` preserves local-SLURM
  behaviour bit-for-bit.
- MCP `run_workflow(cluster=..., project=..., mount=...)` applies the same
  ShellJob script-root guard as the Web sweep path (paths outside every
  project mount's `local` root are rejected before render), so the MCP and
  Web security surfaces match. `mount` requires an SSH `cluster` and the
  `project` whose mounts it names.
- MCP-originated sweep cells record `workflow_runs.triggered_by='mcp'`
  (V4 migration widened the CHECK allowlist); parent
  `sweep_runs.submission_source` is `'mcp'` for the same sweep so cell
  and parent origins always agree.

DB schema (V3 migration): `sweep_runs` table + `workflow_runs.sweep_run_id`
FK + widened `events.kind` / `watches.kind` CHECK allowlist. V4 migration
widens `workflow_runs.triggered_by` CHECK to admit `'mcp'` alongside
`'cli'`/`'web'`/`'schedule'`. Each cell has a
per-cell `workflow_runs` row and inherits the parent's `sweep_run_id`. The
parent `sweep_runs` row tracks aggregate counters (cells_pending /
cells_running / cells_completed / cells_failed / cells_cancelled) that the
`WorkflowRunStateService` updates atomically on every cell transition.

Notifications for sweeps: only the parent `sweep_run` gets a
watch+subscription (if `--endpoint` is provided); cells do not produce
individual Slack messages — a single `sweep_run.status_changed` event fires
at first-cell-start and at final terminal.

### Notification + State Persistence (new in 2026-Q2)

srunx stores durable state in a SQLite DB at
**`<state_dir>/srunx.db`** — `$XDG_STATE_HOME/srunx/srunx.db` (default
`~/.local/state/srunx/srunx.db`), or under the user config's `state_dir` when
set (e.g. to keep it off an NFS home). See `get_db_path()` in
`src/srunx/observability/storage/connection.py`. Schema lives in
`src/srunx/observability/storage/migrations.py`.

Tables (abbreviated):
- `jobs` — every SLURM submission, annotated with `submission_source` (`cli` / `web` / `workflow`) and `workflow_run_id`.
- `workflow_runs` + `workflow_run_jobs` — Web UI workflow runs, replacing the former in-memory `RunRegistry`.
- `job_state_transitions` — single source of truth for observed state changes, fed by both `ActiveWatchPoller` (`source='poller'`) and `JobMonitor` (`source='cli_monitor'`).
- `resource_snapshots` — periodic GPU/node stats; `gpu_utilization` is a STORED generated column (NULL when `gpus_total=0`).
- `endpoints` + `watches` + `subscriptions` + `events` + `deliveries` — the notification 5-concept outbox. `events` has a UNIQUE `(kind, source_ref, payload_hash)` dedup index; `deliveries` has UNIQUE `(endpoint_id, idempotency_key)`. `deliveries` uses a SELECT-then-UPDATE claim pattern inside `BEGIN IMMEDIATE` (stock Python `sqlite3` lacks `UPDATE ... LIMIT RETURNING`).

Background pollers (lifespan tasks managed by `PollerSupervisor`):
- `ActiveWatchPoller` (producer) — polls SLURM every 15 s, writes `job_state_transitions`, `jobs` status, `events`, and fan-outs into `deliveries`.
- `DeliveryPoller` (consumer) — claims `pending` deliveries every 10 s, sends via `SlackWebhookAdapter` (or future channels), handles retry/abandon with exponential backoff (base 10 s, cap 1 h, max 5 attempts).
- `ResourceSnapshotter` — every 5 min, writes one `resource_snapshots` row.

All pollers are crash-resilient via a lease mechanism (`leased_until`, `worker_id`) and a `reclaim_expired_leases()` sweep at the start of every `DeliveryPoller` cycle.

**Environment variables** that affect poller startup:
- `SRUNX_DISABLE_POLLER=1` — disable ALL pollers (also applied automatically in `uvicorn --reload` dev mode).
- `SRUNX_DISABLE_ACTIVE_WATCH_POLLER=1` — skip the SLURM → events producer.
- `SRUNX_DISABLE_DELIVERY_POLLER=1` — skip the outbox consumer.
- `SRUNX_DISABLE_RESOURCE_SNAPSHOTTER=1` — skip resource time-series capture.
- `UVICORN_RELOAD` — anything truthy enables dev-mode reload detection in `pollers.reload_guard`.

Notification settings UI lives in `Settings → Notifications`; Phase 1 supports endpoint CRUD for `slack_webhook` only. Webhook URL validation (both UI and backend): `^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$`.

## Dependencies
- **Jinja2**: Template rendering
- **Pydantic**: Data validation and serialization
- **Loguru**: Structured logging
- **PyYAML**: YAML parsing
- **Rich**: Terminal UI and tables
- **slack-sdk**: Slack notifications

## Code Quality and Linting

### Quality Checks
- `uv run mypy .` - Type checking with mypy
- `uv run ruff check .` - Code linting
- `uv run ruff format .` - Code formatting

### Pre-commit Quality Checks
Always run these before committing:
```bash
uv run pytest && uv run mypy . && uv run ruff check .
```

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
