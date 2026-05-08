# Project Synchronization

srunx can sync your local project directory to a remote SLURM server using
rsync. This allows scripts that import local modules, read config files, or
reference data to work correctly on the remote side.

## Prerequisites

- rsync installed on both local and remote machines
- SSH key-based authentication to the remote server
- The `srunx` package installed locally

## Quick Start

Sync a project via `SSHSlurmClient`:

``` python
from srunx.ssh.core.client import SSHSlurmClient

client = SSHSlurmClient(
    hostname="dgx.example.com",
    username="researcher",
    key_filename="~/.ssh/id_rsa",
)

# Sync local project to remote workspace
remote_path = client.sync_project()
# remote_path = "~/.config/srunx/workspace/myproject/"
```

The project is synced to `~/.config/srunx/workspace/{repo_name}/` on the
remote server. The repository name is detected from `git rev-parse --show-toplevel`.

## Using RsyncClient Directly

For more control, use `RsyncClient` directly:

``` python
from srunx.sync import RsyncClient

rsync = RsyncClient(
    hostname="dgx.example.com",
    username="researcher",
    key_filename="~/.ssh/id_rsa",
)

# Push local directory to remote
result = rsync.push("./my_project", "~/work/my_project/")
if result.success:
    print("Sync complete")
else:
    print(f"Failed: {result.stderr}")

# Pull results back from remote
result = rsync.pull("~/work/my_project/outputs/", "./local_outputs/")
```

## Push and Pull

**push()** syncs a local directory to the remote server:

``` python
result = rsync.push(
    local_path="./src",
    remote_path="~/workspace/src/",
    delete=True,        # Remove remote files not in local (default)
    dry_run=False,       # Set True to preview without transferring
)
```

- `delete=True` (default): Remote mirrors local exactly. Files on the
  remote that don't exist locally are deleted.
- Directories automatically get a trailing `/` so rsync copies contents,
  not the directory itself.

**pull()** syncs a remote directory to local:

``` python
result = rsync.pull(
    remote_path="~/workspace/results/",
    local_path="./results/",
    delete=False,        # Don't delete local files not on remote (default)
)
```

## Dry Run

Preview what would be transferred without actually syncing:

``` python
result = rsync.push("./src", "~/workspace/src/", dry_run=True)
print(result.stdout)  # Shows file list that would be transferred
```

## Exclude Patterns

By default, common development artifacts are excluded:

- `.git/`, `__pycache__/`, `.venv/`, `*.pyc`
- `.mypy_cache/`, `.pytest_cache/`, `.ruff_cache/`
- `*.egg-info/`, `.tox/`, `node_modules/`, `.DS_Store`

Add custom excludes at initialization or per-call:

``` python
# At initialization (applies to all push/pull calls)
rsync = RsyncClient(
    hostname="host",
    username="user",
    exclude_patterns=["data/raw/", "*.h5", "wandb/"],
)

# Per-call (merged with instance patterns)
result = rsync.push(
    "./project",
    "~/workspace/project/",
    exclude_patterns=["logs/"],
)
```

## ProxyJump Support

If your SLURM server is behind a jump host, pass `proxy_jump`:

``` python
rsync = RsyncClient(
    hostname="dgx-internal",
    username="researcher",
    key_filename="~/.ssh/id_rsa",
    proxy_jump="gateway.example.com",
)
```

This translates to `rsync -e "ssh -J gateway.example.com" ...`.

You can also specify a custom SSH config file:

``` python
rsync = RsyncClient(
    hostname="dgx",
    username="researcher",
    ssh_config_path="~/.ssh/config",
)
```

## macOS Notes

macOS ships with `openrsync` (rsync 2.6.9 compatible), which does not
support `--protect-args` or `--mkpath`. srunx detects this automatically:

- When `--mkpath` is unavailable, remote directories are created via
  `ssh mkdir -p` before syncing.
- When `--protect-args` is unavailable, it is omitted from the rsync
  command.

For full GNU rsync features, install via Homebrew:

``` bash
brew install rsync
```

## Workflow: Sync and Submit

A typical workflow combines sync with job submission:

``` python
from srunx.ssh.core.client import SSHSlurmClient

with SSHSlurmClient(
    hostname="dgx.example.com",
    username="researcher",
    key_filename="~/.ssh/id_rsa",
) as client:
    # 1. Sync project to remote
    remote_path = client.sync_project()

    # 2. Submit job using the synced project (via the SLURM component)
    job = client.slurm.submit_sbatch_job(
        script_content=f"""#!/bin/bash
#SBATCH --job-name=training
#SBATCH --gpus=2
cd {remote_path}
python train.py
""",
        job_name="training",
    )

    # 3. Monitor job
    if job:
        client.slurm.monitor_job(job)

    # 4. Pull results back
    client._rsync_client.pull(
        f"{remote_path}/outputs/",
        "./outputs/",
    )
```

## Mount Points

Mount points provide named local-to-remote path mappings, stored in your SSH
profile. They are used by the Web UI's file browser and can also serve as
a structured way to manage project sync targets.

**Add a mount:**

``` bash
srunx ssh profile mount add myserver ml-project \
    --local ~/projects/ml-project \
    --remote /home/researcher/projects/ml-project
```

**List mounts for a profile:**

``` bash
srunx ssh profile mount list myserver
```

**Remove a mount:**

``` bash
srunx ssh profile mount remove myserver ml-project
```

Mount configuration is stored in `~/.config/srunx/config.json` alongside
the SSH profile:

``` json
{
  "profiles": {
    "myserver": {
      "hostname": "dgx.example.com",
      "mounts": [
        {
          "name": "ml-project",
          "local": "/home/user/projects/ml-project",
          "remote": "/home/researcher/projects/ml-project"
        }
      ]
    }
  }
}
```

!!! note
    The `local` path is automatically expanded (`~` resolved) and must
    exist on the local filesystem. The `remote` path must be absolute.

Mounts integrate with the Web UI's DAG builder: when you click the file
browser icon on a path field, the configured mounts appear as browsable
project roots. See [Web UI guide](../how-to/webui.md) for details.

## API Summary

| Class / Method | Description |
|----|----|
| `RsyncClient(hostname, username, ...)` | Create an rsync wrapper with SSH connection parameters |
| `RsyncClient.push(local, remote, ...)` | Sync local to remote (`--delete` by default) |
| `RsyncClient.pull(remote, local, ...)` | Sync remote to local (no `--delete` by default) |
| `RsyncClient.get_default_remote_path()` | Returns `~/.config/srunx/workspace/{repo_name}/` |
| `RsyncResult.success` | `True` if rsync exited with code 0 |
| `SSHSlurmClient.sync_project(...)` | Sync project directory and return remote path |
