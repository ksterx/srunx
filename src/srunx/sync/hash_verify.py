"""Per-script SHA-256 verification post-rsync (#137 part 5).

After ``rsync`` returns 0 the user has every reason to assume their
script reached the cluster intact. Reality has at least three ways to
break that assumption silently:

* a stray ``--exclude`` rule (project-level or user-level rsyncd config)
  that filters out the specific file we're about to ``sbatch``,
* a path-translation bug between ``mount.local`` and ``mount.remote``,
* an rsync incremental-algorithm hiccup that decided the remote copy
  was "good enough" but actually wasn't.

Per-script hashing (vs. hashing the entire mount tree) keeps the cost
to a single ``sha256sum`` round-trip per submission. The caller passes
in the local script path; we translate it to its remote equivalent
(``mount.local`` prefix → ``mount.remote`` prefix) and compare local
hash with remote hash. Mismatch → abort with a :class:`HashMismatch`
carrying both hashes plus a hint about likely rsync excludes, so the
user can debug the silent-failure cause instead of submitting stale
bytes.

The check is opt-in via ``[sync] verify_remote_hash`` (default
``False``) because it adds an ssh round-trip per submit. Solo-machine
setups with a known-good rsync config don't need it; CI / shared-cluster
setups very much do.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.mount_helpers import build_rsync_client
from srunx.sync.service import SyncAbortedError


class HashMismatch(SyncAbortedError):
    """Raised when the post-rsync hash does not match the local source.

    Subclasses :class:`SyncAbortedError` so existing CLI / Web error
    handling — which already catches "we refused to sync" via that
    base class — surfaces hash mismatches with the same exit code
    and exception channel as owner-marker mismatches and
    require-clean failures.

    Carries both hashes and the local + remote paths so a debugging
    user can ``ls -la`` and ``sha256sum`` each side directly.
    """

    def __init__(
        self,
        *,
        local_path: Path,
        remote_path: str,
        local_hash: str,
        remote_hash: str | None,
    ) -> None:
        if remote_hash is None:
            detail = (
                f"Remote file {remote_path!r} not found after rsync. "
                f"This usually means an rsync exclude rule filtered out "
                f"the file, or the path translation is wrong. "
                f"Local: {local_path} (sha256={local_hash})."
            )
        else:
            detail = (
                f"Hash mismatch for {local_path}:\n"
                f"  local  ({local_path}) sha256={local_hash}\n"
                f"  remote ({remote_path}) sha256={remote_hash}\n"
                f"rsync exited successfully but the remote copy of this "
                f"file does not match. Most likely an rsync exclude rule "
                f"is filtering it out (check ~/.rsyncrc, project "
                f"``.rsync-filter``, and ``mount.exclude_patterns``)."
            )
        super().__init__(detail)
        self.local_path = local_path
        self.remote_path = remote_path
        self.local_hash = local_hash
        self.remote_hash = remote_hash


def local_sha256(path: Path) -> str:
    """Return the SHA-256 hex digest of *path*'s contents.

    Reads the whole file into memory: every caller in scope is a
    sbatch script, which is kB at most. Chunked reads would be
    over-engineering for this size class.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _local_to_remote(path: Path, mount: MountConfig) -> str:
    """Translate a local path under *mount* into its remote equivalent.

    Mirrors :func:`srunx.cli.submission_plan.translate_local_to_remote`,
    duplicated here so the ``srunx.sync`` layer doesn't import upward
    into ``srunx.cli``. Same contract: caller has already verified
    *path* lives under ``mount.local`` (e.g. via the planner).

    Joins with ``/`` explicitly because the remote side is POSIX —
    relying on :class:`Path` would pick up the host separator on
    Windows workstations.
    """
    local_root = Path(mount.local).expanduser().resolve()
    rel = path.resolve().relative_to(local_root)
    remote_root = mount.remote.rstrip("/")
    rel_posix = str(rel).replace("\\", "/")
    if rel_posix in ("", "."):
        return remote_root
    return f"{remote_root}/{rel_posix}"


def verify_paths_match(
    profile: ServerProfile,
    mount: MountConfig,
    local_paths: Sequence[Path],
) -> None:
    """Hash each *local_path* on both ends; raise on any mismatch.

    Per-path semantics:

    * Local hash + matching remote hash → no-op.
    * Local hash + remote returns ``None`` because the remote tooling
      is missing → log debug, skip silently. The rsync that just
      succeeded is the user's main signal of last resort.
    * Local hash + remote returns ``None`` because the file is missing
      → raise :class:`HashMismatch`. This is the most damning silent
      rsync failure and exactly what the verifier exists to catch.
    * Local hash != remote hash → raise :class:`HashMismatch`.

    Network / ssh failures from
    :meth:`RsyncClient.remote_sha256` propagate as
    :class:`RuntimeError`; the caller (``mount_sync_session``)
    deliberately does not swallow them so the user sees the
    underlying connection problem rather than a misleading "hash
    mismatch" or silent "skip".

    The "no tool" case is intentionally non-fatal: forcing every
    cluster admin to install ``sha256sum`` to use srunx would be a
    regression. The check has shipped as opt-in precisely so users
    who care about it can flip it on; users on toolless clusters
    can leave it off without penalty.
    """
    if not local_paths:
        return

    client = build_rsync_client(profile)

    for local_path in local_paths:
        local_hash = local_sha256(local_path)
        remote_path = _local_to_remote(local_path, mount)

        # ssh / network failures propagate from remote_sha256 so the
        # user sees the underlying cause instead of a misleading hash
        # message. Same shape ``check_owner`` uses for marker reads.
        remote_hash = client.remote_sha256(remote_path)

        if remote_hash is None:
            # ``remote_sha256`` returns None for both "file missing"
            # and "no hashing tool". Disambiguate via read_remote_file
            # (which itself uses ``test -f`` so a None return means
            # the file is really gone). The extra round-trip only
            # fires in the rare None branch of an opt-in code path,
            # which is cheaper than enriching the RsyncClient API
            # with a tri-state return.
            exists = client.read_remote_file(remote_path) is not None
            if not exists:
                raise HashMismatch(
                    local_path=local_path,
                    remote_path=remote_path,
                    local_hash=local_hash,
                    remote_hash=None,
                )
            # File exists but we got no hash → no remote tooling.
            # Already logged at debug inside remote_sha256; skip
            # silently per the opt-in contract.
            continue

        if remote_hash != local_hash:
            raise HashMismatch(
                local_path=local_path,
                remote_path=remote_path,
                local_hash=local_hash,
                remote_hash=remote_hash,
            )
