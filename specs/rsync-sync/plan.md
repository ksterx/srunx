# Plan: rsync-based File Synchronization

## Spec Reference

[specs/rsync-sync/spec.md](spec.md) - REQ-1 through REQ-11, AC-1 through AC-11

## Approach

Subprocess-based rsync wrapper as a standalone module (`src/srunx/sync/`), integrated into `SSHSlurmClient` as a new `sync_project()` method. Existing SFTP-based `upload_file()` and `_write_remote_file()` are unchanged.

### Trade-offs Considered

| Option | Pros | Cons |
|--------|------|------|
| **subprocess rsync (chosen)** | No new deps, leverages `~/.ssh/config` natively, delta transfer, ProxyJump via `-e` | Requires rsync on local+remote, no Paramiko auth reuse |
| Paramiko SFTP recursive | Reuses existing connection | No delta transfer, slow for directories, custom implementation |
| sshfs mount | Transparent file access | Heavy setup, not portable, requires FUSE |

### Key Design Decision: Separation of Concerns

Two distinct file transfer responsibilities remain separate:

1. **`upload_file()` (SFTP)** — Ephemeral single-file staging to `SRUNX_TEMP_DIR`. Returns exact remote path. Callers rely on temp naming, chmod, cleanup. **Unchanged.**
2. **`sync_project()` (rsync)** — Project directory sync to `~/.config/srunx/workspace/{repo}/`. Returns remote project root. Used before `sbatch` with `--chdir`. **New.**

This avoids SRP violation and preserves backward compatibility.

## Architecture

### Components

| Component | File Path | Responsibility |
|-----------|-----------|----------------|
| `RsyncClient` | `src/srunx/sync/rsync.py` | rsync subprocess wrapper: push/pull |
| `RsyncResult` | `src/srunx/sync/rsync.py` | Result dataclass: returncode, stdout, stderr, success |
| `sync __init__` | `src/srunx/sync/__init__.py` | Public API exports |
| `SSHSlurmClient` (modified) | `src/srunx/ssh/core/client.py` | New `sync_project()` method using `RsyncClient` |

### Interfaces

```python
@dataclass
class RsyncResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.returncode == 0


class RsyncClient:
    def __init__(
        self,
        hostname: str,
        username: str,
        port: int = 22,
        key_filename: str | None = None,
        proxy_jump: str | None = None,
        ssh_config_path: str | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> None:
        # Validates rsync binary exists via shutil.which()
        ...

    def push(
        self,
        local_path: str | Path,
        remote_path: str | None = None,
        *,
        delete: bool = True,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> RsyncResult:
        # Default remote_path: ~/.config/srunx/workspace/{git_repo_name}/
        # Appends trailing / to local dir path (REQ-11)
        ...

    def pull(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        delete: bool = False,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> RsyncResult: ...

    def _build_ssh_cmd(self) -> list[str]:
        # ["ssh", "-p", port, "-i", key, "-J", proxy, "-F", config]
        ...

    def _build_rsync_cmd(
        self, src: str, dst: str, *, delete: bool, dry_run: bool, excludes: list[str]
    ) -> list[str]:
        # ["rsync", "-az", "--protect-args", "-e", ssh_cmd, *exclude_flags, src, dst]
        ...

    @staticmethod
    def get_default_remote_path() -> str:
        # git rev-parse --show-toplevel | basename → ~/.config/srunx/workspace/{name}/
        # Falls back to Path.cwd().name if not in git repo
        ...

    @staticmethod
    def _format_remote_path(username: str, hostname: str, path: str) -> str:
        # Handles tilde: "user@host:~/.config/..." (tilde unquoted)
        # Quotes non-tilde portion for injection prevention
        ...
```

### Data Flow

```
sync_project() (new):
  SSHSlurmClient.sync_project()
      ↓
  RsyncClient.push(project_root, remote_workspace_path)
      ↓
  _build_ssh_cmd()  ← hostname, port, key, proxy_jump, ssh_config
      ↓
  _build_rsync_cmd()  ← -az --delete --protect-args -e "ssh ..." --exclude
      ↓
  subprocess.run(["rsync", ...])
      ↓
  RsyncResult
      ↓
  return remote_project_path (for sbatch --chdir)

upload_file() (unchanged):
  SSHSlurmClient.upload_file(local_path)
      ↓
  sftp_client.put()  ← single file to SRUNX_TEMP_DIR
      ↓
  return remote_temp_path

pull():
  RsyncClient.pull(remote_path, local_path)
      ↓
  (same rsync pipeline, reversed src/dst, delete=False)
      ↓
  RsyncResult
```

## Integration Points

- **`SSHSlurmClient.__init__`**: Optionally create `RsyncClient` instance with same connection params (only when key-based auth)
- **`SSHSlurmClient.sync_project()`**: New method. Calls `rsync_client.push()` to sync local project dir to `~/.config/srunx/workspace/{repo}/`. Returns remote path
- **`SSHSlurmClient.upload_file()`**: **Unchanged.** Continues using SFTP
- **`SSHSlurmClient._write_remote_file()`**: **Unchanged.** Continues using SFTP
- **`SSHSlurmClient.connect()/disconnect()`**: **Unchanged.** SFTP client stays
- **`get_default_remote_path()`**: Static method on `RsyncClient`. Runs `git rev-parse --show-toplevel` locally, takes basename

## Tilde Expansion Strategy

rsync remote paths use `user@host:path` format. The `~` must not be shell-quoted:

```python
# WRONG: shlex.quote suppresses tilde
f"{user}@{host}:{shlex.quote('~/.config/srunx/workspace/repo/')}"
# → user@host:'~/.config/srunx/workspace/repo/'  ← tilde treated literally

# RIGHT: use --protect-args and leave tilde unquoted
# rsync --protect-args handles whitespace/special chars in filenames
# tilde is expanded by remote shell before rsync receives the path
f"{user}@{host}:~/.config/srunx/workspace/repo/"
```

`--protect-args` (-s) protects filenames from shell interpretation while allowing tilde expansion in the path prefix.

## Dependencies

### Internal
- `srunx.ssh.core.client` (consumer of `RsyncClient`)

### External
- System `rsync` binary (no Python package)
- System `ssh` binary (used by rsync via `-e`)
- System `git` binary (for repo name detection in `get_default_remote_path()`)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| rsync not installed locally | High | Check at `RsyncClient.__init__` with `shutil.which("rsync")`, raise clear error |
| rsync not installed on remote | High | Document requirement; standard on HPC Linux |
| SSH host key mismatch (Paramiko AutoAddPolicy vs system ssh) | Med | Use `-o StrictHostKeyChecking=accept-new` in `_build_ssh_cmd()` |
| Password-only auth won't work with rsync | Med | rsync requires key-based auth. `sync_project()` is unavailable for password-only connections. `upload_file()` (SFTP) still works as fallback |
| `git rev-parse` fails (not a git repo) | Low | Fall back to `Path.cwd().name` |
| Same repo basename from different sources | Low | Acceptable for v1. Future: add remote URL hash for disambiguation |
| Large initial sync | Low | rsync handles this well; `--progress` available for visibility |

## Testing Strategy

- **Unit**: Test `_build_ssh_cmd()` output for all param combinations (port, key, proxy, config)
- **Unit**: Test `_build_rsync_cmd()` output for delete/dry_run/exclude variations
- **Unit**: Test `get_default_remote_path()` with mocked git subprocess (repo found, not found)
- **Unit**: Test `_format_remote_path()` tilde handling and path construction
- **Unit**: Test trailing slash behavior for directory vs file push
- **Unit**: Test `push()`/`pull()` with mocked `subprocess.run` — verify correct command construction and `RsyncResult` mapping
- **Unit**: Test `RsyncResult.success` property
- **Unit**: Test `SSHSlurmClient.sync_project()` with mocked `RsyncClient`
- **Existing**: Verify existing `upload_file()` tests still pass unchanged
