"""Orchestrates snapshot creation and the lease lifecycle around one submission."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from loguru import logger

from srunx.common.exceptions import SbatchRejectedError
from srunx.snapshot.errors import SnapshotRemoteError, SnapshotValidationError
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import SnapshotPlan
from srunx.snapshot.remote import (
    CommitResult,
    ProbeResult,
    RemoteShell,
    RemoteSnapshotStore,
    literal_root_conflict,
)
from srunx.ssh.core.config import ServerProfile


class SnapshotStore(Protocol):
    """The slice of :class:`RemoteSnapshotStore` the service uses."""

    def probe(self, *, repo_id: str, sha: str) -> ProbeResult: ...

    def push(self, *, repo_top: Path, probe: ProbeResult, sha: str) -> None: ...

    def commit(
        self, *, probe: ProbeResult, sha: str, lease: Lease | None
    ) -> CommitResult: ...

    def bind(self, *, repo_dir: str, lease: Lease) -> bool: ...

    def delete_lease(self, *, repo_dir: str, lease_id: str) -> None: ...


@dataclass(frozen=True)
class PreparedSnapshot:
    plan: SnapshotPlan
    root: str
    repo_dir: str
    path: str
    status: Literal["reused", "created", "recovered"]

    @property
    def script_path(self) -> str | None:
        if self.plan.script_rel is None:
            return None
        return f"{self.path}/{self.plan.script_rel}"

    @property
    def submit_cwd(self) -> str:
        if self.plan.cwd_rel:
            return f"{self.path}/{self.plan.cwd_rel}"
        return self.path

    def env_vars(self) -> dict[str, str]:
        return {"SRUNX_CODE_DIR": self.path, "SRUNX_SNAPSHOT_SHA": self.plan.sha}


class LeaseHandle:
    """A provisional lease on a prepared snapshot; ``bind`` ties it to a job."""

    def __init__(
        self,
        *,
        store: SnapshotStore,
        prepared: PreparedSnapshot,
        lease: Lease,
        lease_path: str,
    ) -> None:
        self._store = store
        self.prepared = prepared
        self.lease = lease
        self.lease_path = lease_path
        self.is_bound = False

    def metadata(self) -> dict[str, Any]:
        return {
            "snapshot": {
                "repo_id": self.prepared.plan.repo_id,
                "sha": self.prepared.plan.sha,
                "path": self.prepared.path,
                "lease_id": self.lease.lease_id,
            }
        }

    def bind(self, job_id: int) -> None:
        """Record the job id in the lease. Never raises: the job already exists."""
        bound = self.lease.bound(job_id)
        try:
            present = self._store.bind(repo_dir=self.prepared.repo_dir, lease=bound)
        except Exception as exc:
            logger.warning(
                "Job {} was submitted but its snapshot lease could not be bound "
                "({}); the lease stays provisional at {}",
                job_id,
                exc,
                self.lease_path,
            )
            return
        self.lease = bound
        self.is_bound = True
        if not present:
            logger.error(
                "Job {} was submitted, but its snapshot {} has been removed from "
                "the cluster; the job may fail to find its code",
                job_id,
                self.prepared.path,
            )


class SnapshotService:
    def __init__(
        self,
        *,
        profile_name: str,
        profile: ServerProfile,
        shell: RemoteShell | None = None,
        store: SnapshotStore | None = None,
    ) -> None:
        self._profile_name = profile_name
        self._profile = profile
        self._mounts: Sequence[str] = [m.remote for m in profile.mounts]
        if store is None:
            if shell is None:
                from srunx.sync.mount_helpers import build_rsync_client

                shell = build_rsync_client(profile)
            store = RemoteSnapshotStore(
                shell, snapshot_root=profile.snapshot_root, mounts=self._mounts
            )
        self._store = store

    def _prepare(
        self, plan: SnapshotPlan, *, leased: bool
    ) -> tuple[PreparedSnapshot, CommitResult, Lease | None]:
        probe = self._store.probe(repo_id=plan.repo_id, sha=plan.sha)
        push_error: SnapshotRemoteError | None = None
        if not probe.has_commit:
            try:
                self._store.push(repo_top=plan.repo_top, probe=probe, sha=plan.sha)
            except SnapshotRemoteError as exc:
                # A concurrent push of the same commit makes ours fail with
                # "reference already exists". The locked commit step checks
                # for the commit itself and is authoritative.
                push_error = exc
                logger.debug(
                    "push failed, checking whether the commit arrived: {}", exc
                )
        lease: Lease | None = None
        if leased:
            lease = Lease.new_provisional(
                repo_id=plan.repo_id,
                sha=plan.sha,
                snapshot_path=f"{probe.repo_dir}/trees/{plan.sha}",
                profile=self._profile_name,
            )
        try:
            result = self._store.commit(probe=probe, sha=plan.sha, lease=lease)
        except SnapshotRemoteError as exc:
            if push_error is not None:
                raise push_error from exc
            raise
        prepared = PreparedSnapshot(
            plan=plan,
            root=probe.root,
            repo_dir=probe.repo_dir,
            path=result.path,
            status=result.status,
        )
        # The CLI prints the user-facing status line; keep this for --verbose.
        logger.debug("Snapshot {} {} at {}", plan.sha[:12], result.status, result.path)
        return prepared, result, lease

    def _check_root(self) -> None:
        conflict = literal_root_conflict(self._profile.snapshot_root, self._mounts)
        if conflict:
            raise SnapshotValidationError(conflict)

    def create(self, plan: SnapshotPlan) -> PreparedSnapshot:
        """Ensure the snapshot exists on the cluster; writes no lease."""
        self._check_root()
        prepared, _, _ = self._prepare(plan, leased=False)
        return prepared

    @contextmanager
    def leased_snapshot(self, plan: SnapshotPlan) -> Iterator[LeaseHandle]:
        """Ensure the snapshot and hold a provisional lease across the submission.

        The lease is deleted only when the body raises ``SbatchRejectedError``
        (a job provably does not exist). In every other case where the lease was
        not bound it is kept and its path is logged, because a job may exist.
        """
        self._check_root()
        prepared, result, lease = self._prepare(plan, leased=True)
        assert lease is not None
        lease_path = result.lease_path or RemoteSnapshotStore.lease_path(
            prepared.repo_dir, lease.lease_id
        )
        handle = LeaseHandle(
            store=self._store, prepared=prepared, lease=lease, lease_path=lease_path
        )
        try:
            yield handle
        except SbatchRejectedError:
            try:
                self._store.delete_lease(
                    repo_dir=prepared.repo_dir, lease_id=lease.lease_id
                )
            except Exception as exc:
                logger.warning(
                    "sbatch was rejected but the provisional lease {} could not be "
                    "deleted: {}",
                    lease_path,
                    exc,
                )
            raise
        except BaseException:
            if not handle.is_bound:
                self._warn_kept(lease_path)
            raise
        else:
            if not handle.is_bound:
                self._warn_kept(lease_path)

    @staticmethod
    def _warn_kept(lease_path: str) -> None:
        logger.warning(
            "Keeping provisional snapshot lease {}: a job may exist and still "
            "need this snapshot",
            lease_path,
        )
