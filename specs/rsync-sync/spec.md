# Spec: rsync-based File Synchronization

## Overview

Add rsync-based project directory synchronization between local and remote SLURM servers, enabling bidirectional file sync (push/pull). Existing SFTP-based single-file transfers (`upload_file()`, `_write_remote_file()`) remain unchanged.

## Background

The current SSH integration (`SSHSlurmClient`) uses Paramiko SFTP to upload individual script files to remote servers. This has critical limitations:

1. **No directory sync** - Only single script files can be transferred. If a script imports local modules/data, those are not available on the remote server.
2. **No artifact retrieval** - Job outputs (models, results) cannot be downloaded back to local.
3. **No delta transfer** - Every upload transfers the entire file, no matter how small the change.

rsync solves all of these via delta transfers over SSH, with exclude patterns and bidirectional support.

## Requirements

### Must Have

- REQ-1: `RsyncClient` class that wraps `subprocess.run(["rsync", ...])` for file transfers
- REQ-2: `push(local_path, remote_path)` - sync local directory/file to remote server
- REQ-3: `pull(remote_path, local_path)` - sync remote directory/file to local
- REQ-4: SSH connection parameter support: hostname, username, port, key_filename
- REQ-5: ProxyJump support via rsync's `-e "ssh -J ..."` flag
- REQ-6: Exclude pattern support (`.git/`, `__pycache__/`, `.venv/`, `*.pyc`, etc.)
- REQ-7: Default remote sync path: `~/.config/srunx/workspace/{git_repo_name}/` on the remote server (repo name derived from `git rev-parse --show-toplevel` basename). If not inside a git repo, fall back to cwd name
- REQ-8: `SSHSlurmClient.sync_project()` method that uses `RsyncClient.push()` to sync the local project directory to the remote workspace, then returns the remote project path
- REQ-9: `dry_run` mode via `rsync -n` for previewing transfers
- REQ-10: Shell-quote remote paths to prevent injection. Tilde (`~`) must NOT be quoted (to allow shell expansion on the remote side)
- REQ-11: Trailing slash semantics: `push()` of a directory appends trailing `/` to local path (sync contents into remote dir, not nested subdir)

### Nice to Have

- REQ-N1: Progress callback/output integration with `rich` for transfer progress display
- REQ-N2: `.srunxignore` file support (like `.gitignore`) for user-defined exclude patterns
- REQ-N3: `ssh_config_path` support via `-e "ssh -F ..."` flag

## Acceptance Criteria

- AC-1: Given a local project directory, when `push()` is called, then the directory contents are synced to `~/.config/srunx/workspace/{git_repo_name}/` on the remote server via rsync
- AC-2: Given a remote directory with job artifacts, when `pull()` is called, then files are synced to the specified local path
- AC-3: Given a ProxyJump configuration, when `push()` or `pull()` is called, then rsync uses `-e "ssh -J proxy_host"` and the transfer succeeds
- AC-4: Given exclude patterns (default or custom), when syncing, then excluded files/dirs are not transferred
- AC-5: Given `dry_run=True`, when `push()` or `pull()` is called, then rsync runs with `-n` and returns what would be transferred without actually transferring
- AC-6: Given `SSHSlurmClient.sync_project()` is called, then the local project directory is synced to the remote workspace via rsync, and the remote project path is returned for use with `sbatch --chdir`
- AC-7: Given no explicit remote path is specified, then `~/.config/srunx/workspace/{git_repo_name}/` is used and auto-created on the remote server
- AC-8: Given rsync is not installed on the local machine, then a clear error message is raised at `RsyncClient` initialization
- AC-9: Given `push()` is called with default settings, then `--delete` is used (remote mirrors local exactly). This can be disabled via `delete=False` parameter
- AC-10: Given `upload_file()` is called, then it continues to use SFTP as before (no change to existing behavior)
- AC-11: Given a remote path containing `~`, then rsync expands the tilde on the remote side (not suppressed by quoting)

## Out of Scope

- Removing Paramiko or SFTP (still needed for SSH command execution and `_write_remote_file()`)
- Modifying `upload_file()` or `_write_remote_file()` behavior
- Adding rsync to remote server (assumed to be pre-installed)
- Real-time streaming progress bars (nice-to-have, not required)
- Bidirectional auto-sync / file watching
- Password authentication support for rsync (key-based only)
- CLI commands for sync/pull (v1 is API-only; CLI is a future feature)

## Constraints

- Must use system `rsync` binary via `subprocess` - no new Python dependencies
- Must work with existing `SSHSlurmClient` connection parameters (key-based auth only for rsync)
- Must support the same ProxyJump configurations that Paramiko currently handles
- Python 3.12+ (match existing project)
- Remote server must have rsync installed (standard on Linux HPC systems)

## Resolved Questions

- Q1: Remote sync base path → **`~/.config/srunx/workspace/{git_repo_name}/`** (gitリポジトリ名で分離、複数プロジェクト対応)
- Q2: `--delete` flag in `push()` → **On by default** (remote mirrors local exactly, opt-out via `delete=False`)
- Q3: `upload_file()` vs project sync → **Separate responsibilities**. `upload_file()` stays as SFTP for ephemeral temp files. New `sync_project()` uses rsync for directory sync (SRP)
- Q4: `_write_remote_file()` → **Unchanged**. Stays on SFTP. Out of scope
- Q5: Tilde expansion → **Do not quote tilde**. Split path into `~` prefix and rest, quote only the rest
- Q6: Trailing slash → **Append `/` to local dir path** so rsync syncs contents, not nested subdir
