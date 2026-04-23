"""Per-machine ownership marker for synced mounts (#137 part 4).

Without this marker srunx can't tell which workstation last pushed to
a mount, so the following sequence silently destroys work:

    laptop$  cd ~/proj && vim train.sbatch       # edit
    laptop$  srunx sbatch train.sbatch --profile X    # syncs new bytes
    desktop$ srunx sbatch train.sbatch --profile X    # different
                                                      # workstation, syncs
                                                      # *its own* old bytes
                                                      # over the laptop's

Both submissions succeed, but the desktop's run executes the laptop's
old code while overwriting the laptop's new code on the cluster. The
user has no signal that anything went wrong.

The marker is a tiny JSON file at ``<mount.remote>/.srunx-owner.json``
recording ``hostname`` + ``mount_name`` + ISO timestamp of the last
successful sync. Each sync:

1. Reads the marker (best-effort: missing / unparseable → no owner).
2. If the recorded hostname differs from this machine's
   :func:`current_machine_id` AND the user has not explicitly opted
   out (``--force-sync`` CLI flag or ``[sync] owner_check = false``
   config), aborts with :class:`OwnerMismatch`.
3. If the rsync succeeds, writes a fresh marker so the next sync sees
   the up-to-date owner.

The check is deliberately conservative — when the marker can't be
fetched (ssh failure, malformed JSON, …) we treat it as "no owner"
rather than blocking, so a transient network blip doesn't strand the
user. The protection it provides is "two known machines competing";
arbitrary corruption is out of scope.
"""

from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from datetime import UTC, datetime

from srunx.common.logging import get_logger
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.mount_helpers import build_rsync_client

logger = get_logger(__name__)


_MARKER_FILENAME = ".srunx-owner.json"


class OwnerMismatch(RuntimeError):
    """Raised when the remote marker shows a different owning machine.

    Carries the recorded hostname + mount + timestamp so the caller's
    error message can tell the user *which* workstation currently
    owns the mount and *when* it last touched it. Recovery is
    explicit: re-run with ``--force-sync`` after confirming the
    other workstation isn't mid-edit, or set
    ``[sync] owner_check = false`` to disable the check globally.
    """

    def __init__(
        self,
        *,
        mount_name: str,
        local_machine: str,
        recorded_machine: str,
        recorded_at: str | None,
    ) -> None:
        when = f" at {recorded_at}" if recorded_at else ""
        super().__init__(
            f"Mount '{mount_name}' was last synced from '{recorded_machine}'"
            f"{when}, not '{local_machine}'. Syncing now would overwrite "
            f"the other machine's edits. Re-run with --force-sync to "
            f"override, or disable the check via "
            f"``[sync] owner_check = false``."
        )
        self.mount_name = mount_name
        self.local_machine = local_machine
        self.recorded_machine = recorded_machine
        self.recorded_at = recorded_at


@dataclass(frozen=True)
class OwnerMarker:
    """In-memory representation of ``.srunx-owner.json``."""

    hostname: str
    mount_name: str
    last_sync_at: str  # ISO-8601 UTC

    def to_json(self) -> str:
        return json.dumps(
            {
                "hostname": self.hostname,
                "mount_name": self.mount_name,
                "last_sync_at": self.last_sync_at,
            },
            indent=2,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, raw: str) -> OwnerMarker | None:
        """Parse a marker payload; return ``None`` for any malformed input.

        Defensive on every field: a marker that's missing or wrong-shape
        is treated as "no owner" rather than as an error so a one-off
        corruption doesn't block legitimate syncs forever. The user's
        next successful sync overwrites it with a fresh marker anyway.
        """
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(data, dict):
            return None
        hostname = data.get("hostname")
        mount_name = data.get("mount_name")
        last = data.get("last_sync_at")
        if not isinstance(hostname, str) or not isinstance(mount_name, str):
            return None
        if not isinstance(last, str):
            return None
        return cls(hostname=hostname, mount_name=mount_name, last_sync_at=last)


def current_machine_id() -> str:
    """Return a stable identifier for *this* machine.

    Uses :func:`socket.gethostname` as the cheapest readily-available
    identifier — matches what most users would type to describe their
    workstation, and survives reboots / DHCP IP churn (unlike
    something MAC-address based). Per-user tests can monkeypatch
    this.
    """
    return socket.gethostname()


def _marker_remote_path(mount: MountConfig) -> str:
    """Return the absolute remote path of the marker for *mount*.

    Joining with ``/`` explicitly because the remote side is POSIX —
    ``Path`` would pick up the host separator on Windows workstations.
    """
    return f"{mount.remote.rstrip('/')}/{_MARKER_FILENAME}"


def read_owner_marker(profile: ServerProfile, mount: MountConfig) -> OwnerMarker | None:
    """Read the remote marker for *mount*; return ``None`` if missing/invalid.

    "Missing" is the legitimate first-sync case and must not raise.
    Genuine ssh failures DO raise via :class:`RuntimeError` from the
    underlying :meth:`RsyncClient.read_remote_file` so the caller can
    decide whether a network problem warrants blocking the sync.
    """
    client = build_rsync_client(profile)
    raw = client.read_remote_file(_marker_remote_path(mount))
    if raw is None:
        return None
    marker = OwnerMarker.from_json(raw)
    if marker is None:
        # Malformed marker — log so it's visible in --verbose, but
        # don't raise. The next successful sync overwrites it.
        logger.debug(
            "Owner marker at %s is malformed; ignoring",
            _marker_remote_path(mount),
        )
    return marker


def write_owner_marker(
    profile: ServerProfile, mount: MountConfig, *, hostname: str | None = None
) -> OwnerMarker:
    """Write a fresh marker for *mount*; return the value that was written.

    ``hostname`` defaults to :func:`current_machine_id`; tests pass
    an explicit value to avoid leaking the real hostname into
    fixtures.
    """
    machine = hostname or current_machine_id()
    marker = OwnerMarker(
        hostname=machine,
        mount_name=mount.name,
        last_sync_at=datetime.now(UTC).isoformat(),
    )
    client = build_rsync_client(profile)
    client.write_remote_file(_marker_remote_path(mount), marker.to_json())
    return marker


def check_owner(
    profile: ServerProfile,
    mount: MountConfig,
    *,
    enabled: bool,
    force: bool,
    hostname: str | None = None,
) -> None:
    """Raise :class:`OwnerMismatch` if the remote marker names a different host.

    No-ops when ``enabled=False`` (config opt-out) or ``force=True``
    (CLI ``--force-sync`` opt-out) so the caller can run the same
    function in every code path without branching at the call site.
    Marker read failures are logged at debug and treated as "no
    owner" — see module docstring for the rationale.
    """
    if not enabled or force:
        return
    try:
        marker = read_owner_marker(profile, mount)
    except RuntimeError as exc:
        # Network / ssh failure: don't block the sync; the user is
        # already in trouble and silently aborting would just hide it.
        # The rsync that follows will surface the same connection
        # problem with its own error.
        logger.debug("Owner marker read failed (treating as absent): %s", exc)
        return
    if marker is None:
        return
    me = hostname or current_machine_id()
    if marker.hostname == me:
        return
    raise OwnerMismatch(
        mount_name=mount.name,
        local_machine=me,
        recorded_machine=marker.hostname,
        recorded_at=marker.last_sync_at,
    )
