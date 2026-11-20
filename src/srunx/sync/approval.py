"""First-sync approval of (cluster, remote path) targets (design §3).

Project configs can point ``remote_dir`` anywhere on a configured
cluster. The first rsync into a given ``(cluster, remote)`` pair is
confirmed interactively and remembered in
``<state_dir>/approved-sync-targets.json``; non-interactive callers must
pass ``assume_yes`` (CLI ``--yes``).
"""

from __future__ import annotations

import sys

from srunx.common import paths
from srunx.common.statefile import read_json, update_json


class SyncNotApprovedError(RuntimeError):
    """The sync target has not been approved and no prompt was possible."""


def _path():
    return paths.state_dir() / "approved-sync-targets.json"


def _load() -> dict[str, list[str]]:
    return read_json(_path())


def is_approved(cluster: str, remote: str) -> bool:
    return remote in _load().get(cluster, [])


def approve(cluster: str, remote: str) -> None:
    def add(data: dict) -> None:
        targets = data.setdefault(cluster, [])
        if remote not in targets:
            targets.append(remote)

    update_json(_path(), add)


def ensure_approved(
    cluster: str, local: str, remote: str, *, assume_yes: bool = False
) -> None:
    """Return if the target is (now) approved; raise otherwise."""
    if is_approved(cluster, remote):
        return
    if assume_yes:
        approve(cluster, remote)
        return
    if not sys.stdin.isatty():
        raise SyncNotApprovedError(
            f"First sync to {cluster}:{remote} needs approval. "
            "Re-run interactively or pass --yes."
        )
    import typer

    if typer.confirm(
        f"Sync {local} to {cluster}:{remote} for the first time?", default=False
    ):
        approve(cluster, remote)
        return
    raise SyncNotApprovedError(f"Sync to {cluster}:{remote} was not approved.")
