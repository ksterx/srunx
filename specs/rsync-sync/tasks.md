# Tasks: rsync-based File Synchronization

## Prerequisites
- [x] Spec approved: `specs/rsync-sync/spec.md`
- [x] Plan approved: `specs/rsync-sync/plan.md`

## Phase 1: Core Module
- [x] T1.1: Create `src/srunx/sync/__init__.py` with public exports (REQ-1)
      Files: `src/srunx/sync/__init__.py`
- [x] T1.2: Implement `RsyncResult` dataclass with returncode, stdout, stderr, success property (REQ-1)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.3: Implement `RsyncClient.__init__` with rsync binary check (`shutil.which`) and SSH params storage (REQ-1, REQ-4)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.4: Implement `_build_ssh_cmd()` with port, key, ProxyJump, ssh_config, `StrictHostKeyChecking=accept-new` (REQ-4, REQ-5, REQ-N3)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.5: Implement `_build_rsync_cmd()` with `-az --protect-args`, exclude patterns, delete, dry_run flags (REQ-6, REQ-9, REQ-10)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.6: Implement `get_default_remote_path()` static method using `git rev-parse --show-toplevel` basename, fallback to `Path.cwd().name` (REQ-7)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.7: Implement `_format_remote_path()` static method with tilde-safe path formatting (REQ-10, REQ-11)
      Files: `src/srunx/sync/rsync.py`
- [x] T1.8: Define default exclude patterns as class constant (REQ-6)
      Files: `src/srunx/sync/rsync.py`

## Phase 2: Push/Pull Operations
- [x] T2.1: Implement `push()` with default remote path, trailing slash for directories, delete=True default (REQ-2, REQ-7, REQ-11)
      Files: `src/srunx/sync/rsync.py`
      Depends: T1.2-T1.8
- [x] T2.2: Implement `pull()` with delete=False default (REQ-3)
      Files: `src/srunx/sync/rsync.py`
      Depends: T1.2-T1.5, T1.7

## Phase 3: SSHSlurmClient Integration
- [x] T3.1: Add `sync_project()` method to `SSHSlurmClient` that creates `RsyncClient` and calls `push()`, returns remote project path (REQ-8)
      Files: `src/srunx/ssh/core/client.py`
      Depends: T2.1
      Note: Only available for key-based auth connections. upload_file() and _write_remote_file() remain unchanged on SFTP

## Phase 4: Tests
- [x] T4.1: Unit tests for `_build_ssh_cmd()` — all param combinations (port, key, proxy, config, StrictHostKeyChecking)
      Files: `tests/test_rsync.py`
- [x] T4.2: Unit tests for `_build_rsync_cmd()` — delete, dry_run, exclude, protect-args
      Files: `tests/test_rsync.py`
- [x] T4.3: Unit tests for `get_default_remote_path()` — git repo found, not found
      Files: `tests/test_rsync.py`
- [x] T4.4: Unit tests for `_format_remote_path()` — tilde handling, special chars
      Files: `tests/test_rsync.py`
- [x] T4.5: Unit tests for `push()` and `pull()` — mocked subprocess, verify command construction and RsyncResult
      Files: `tests/test_rsync.py`
      Depends: T2.1, T2.2
- [x] T4.6: Unit tests for `SSHSlurmClient.sync_project()` — mocked RsyncClient
      Files: `tests/test_ssh_client.py`
      Depends: T3.1
- [x] T4.7: Verify existing `upload_file()` tests still pass (no regression)
      Files: `tests/test_ssh_client.py`
- [x] T4.8: Run mypy + ruff
      Depends: T4.1-T4.7

## Verification Checklist

### Acceptance Criteria
- [x] AC-1: `push()` syncs directory contents to `~/.config/srunx/workspace/{git_repo_name}/`
- [x] AC-2: `pull()` syncs remote to local
- [x] AC-3: ProxyJump works via `-e "ssh -J ..."`
- [x] AC-4: Exclude patterns filter correctly
- [x] AC-5: `dry_run=True` runs `rsync -n` without transferring
- [x] AC-6: `sync_project()` syncs project and returns remote path for `sbatch --chdir`
- [x] AC-7: Default remote path auto-created
- [x] AC-8: Missing rsync raises clear error at init
- [x] AC-9: `push()` uses `--delete` by default
- [x] AC-10: `upload_file()` still uses SFTP (no regression)
- [x] AC-11: Tilde in remote path is expanded, not quoted

### Quality Gates
- [x] All tests pass
- [x] mypy passes
- [x] ruff passes
- [x] Existing SSH client tests unchanged and passing
