"""Mount-aware rsync helpers, used by both CLI and Web sync paths.

Originally these lived in :mod:`srunx.web.sync_utils` because the Web
router was the only caller. Phase 1 of auto-sync (PR #134) made the
CLI ``srunx sbatch`` path depend on the same helpers, which left
``srunx.sync.service`` reaching into ``srunx.web``. That dependency
direction is wrong: ``srunx.sync`` is shared infrastructure, ``srunx.web``
is one of its consumers. Moved here so the layering reads correctly,
and the old ``srunx.web.sync_utils`` re-exports for backward
compatibility.

Two functions live here:

* :func:`build_rsync_client` — translates a :class:`ServerProfile` into
  an :class:`RsyncClient`, honouring ``ssh_host``-based ``~/.ssh/config``
  delegation when present.
* :func:`sync_mount_by_name` — runs ``rsync push`` for the named mount,
  with a configurable ``delete`` flag (default ``False`` for safety:
  Phase 1 auto-sync ran with ``delete=True`` and silently ate
  remote-side outputs/checkpoints inside mounts).

The ``delete`` default change is the user-visible behavioural fix for
Codex's blocker #4 on PR #134. It later became the default of
:meth:`RsyncClient.push` itself, so *no* caller inherits mirror
semantics by accident — the manual ``srunx ssh sync`` command opts in
via its ``--delete`` flag, and the Web file/template/job sync routes
pass ``delete=True`` explicitly.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from srunx.common.logging import get_logger

if TYPE_CHECKING:
    from srunx.ssh.core.config import MountConfig, ServerProfile
    from srunx.sync.manifest import SyncManifest

from srunx.sync.rsync import RsyncClient

logger = get_logger(__name__)


def build_rsync_client(profile: ServerProfile) -> RsyncClient:
    """Create RsyncClient from SSH profile, handling ssh_host vs hostname.

    When *ssh_host* is set, the client delegates all connection
    parameters (user, key, proxy, port) to ``~/.ssh/config``.
    Otherwise the explicit profile fields are used directly.
    """
    if profile.ssh_host:
        return RsyncClient(
            hostname=profile.ssh_host,
            username="",
            ssh_config_path=str(Path.home() / ".ssh" / "config"),
        )
    return RsyncClient(
        hostname=profile.hostname,
        username=profile.username,
        key_filename=profile.key_filename,
        port=profile.port,
        proxy_jump=profile.proxy_jump,
    )


def sync_mount_by_name(
    profile: ServerProfile,
    mount_name: str,
    *,
    delete: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    record_manifest: bool = True,
) -> str:
    """Sync a named mount's local directory to remote via rsync.

    ``delete`` is **False by default** so callers that don't opt in
    can't accidentally wipe remote-only outputs. Any explicit
    ""mirror this exactly"" caller passes ``delete=True`` (the Web
    file/template/job sync routes do). Auto-sync paths (PR #134
    Phase 1) leave the default.

    ``dry_run=True`` runs rsync with ``-n -i`` (no transfer + itemize)
    and returns rsync's stdout — the human-readable list of files
    that *would* be touched. The remote is not modified. Used by the
    CLI ``srunx sbatch --dry-run`` preview path (#137 part 2).

    ``verbose=True`` switches the underlying rsync invocation to
    streaming mode so per-file progress reaches the user's terminal
    live (#137 part 3). Mutually compatible with ``dry_run`` — the
    preview output streams the same way.

    ``record_manifest=False`` skips recording the upload, leaving it to the
    caller. Only for callers that verify the transfer afterwards: rsync can
    exit 0 while the remote copy differs, and recording before that check
    would assert every file arrived intact when the verification is about to
    prove otherwise. :func:`~srunx.sync.service.mount_sync_session` defers for
    that reason, exactly as it already defers the ownership marker.

    Returns:
        rsync stdout — empty for a non-dry-run sync, the itemize lines
        for a dry-run preview.

    Raises:
        ValueError: If *mount_name* does not exist in the profile.
        RuntimeError: If the rsync process exits with a non-zero code.
            The error message includes rsync's stderr so the CLI / API
            layer can surface the underlying cause unchanged.
    """
    mount = next((m for m in profile.mounts if m.name == mount_name), None)
    if mount is None:
        raise ValueError(f"Mount '{mount_name}' not found in profile")
    rsync = build_rsync_client(profile)

    before = None if dry_run or not record_manifest else snapshot_local(rsync, mount)

    result = rsync.push(
        mount.local,
        mount.remote,
        delete=delete,
        dry_run=dry_run,
        # ``itemize`` tracks ``dry_run`` — for an actual push we don't
        # want the per-file output spam in the success path, but for
        # a preview the itemize lines ARE the value.
        itemize=dry_run,
        verbose=verbose,
        exclude_patterns=mount.exclude_patterns or None,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rsync sync failed for mount '{mount_name}': {result.stderr}"
        )

    if not dry_run and record_manifest:
        record_upload(rsync, mount, mirrored=delete, before=before)
    return result.stdout


def snapshot_local(client: RsyncClient, mount: MountConfig) -> list[str] | None:
    """Inventory the mount *before* a transfer — the set :func:`record_upload` records.

    Scanning afterwards instead gets both edges of a concurrent change wrong,
    and in opposite directions:

    * A file **created** while the transfer ran, after rsync walked its parent,
      appears in an after-scan though rsync never sent it. Recorded as
      uploaded, deleted locally later, and — if a cluster job happens to write
      the same relative path — that job's output is reported as a stale upload.
      Naming live output for deletion is the one answer this must never give,
      and a shared ``dist/`` holding both build artifacts and job output is
      enough to reach it.
    * A file **deleted** locally while the transfer ran is missing from an
      after-scan although rsync did send it. It sits on the cluster unrecorded,
      and no later sync can record it — it is not local any more — so it stays
      invisible while the report still calls itself complete. That silent gap
      is the exact failure this feature exists to close.

    The before-scan is right on both counts: created-during is absent from it,
    deleted-during is present. It is also the honest set — what rsync was asked
    to send. A file created mid-sync is simply recorded by the next sync, which
    finds it sitting in the tree like any other.

    Known gap: this scan and rsync's own are separate walks, so a file deleted
    between them is recorded without having been sent. Stale paths are narrowed
    to what the remote actually holds, which discards it — unless a job wrote
    output at that same relative path, in which case the output is presented as
    a stale upload. Left open deliberately. The window is the gap between two
    scans rather than a whole transfer, and closing it is not cheap:

    * Binding rsync to this list (``--files-from``) needs the real filenames,
      and this list is deliberately kept in rsync's escaped form — control
      bytes are never converted back, since doing so is what makes a crafted
      name unparseable. Keeping a second, raw copy would reintroduce exactly
      that ambiguity.
    * Confirming against a scan taken afterwards costs a second full walk of
      the tree on every sync, and would drop a *new* file deleted mid-transfer
      — the gap described above, reopened for the sake of a much narrower one.

    What is left is the same shape as an ambiguity this cannot resolve anyway:
    a path that is both srunx-managed and job-written needs content identity to
    tell apart, which means a hash per file per sync. See
    :mod:`srunx.sync.manifest`.

    Returns ``None`` if the scan fails. :func:`record_upload` then records
    nothing and marks tracking unknown — there is no after-scan fallback, since
    publishing a record built the wrong way as authoritative is precisely what
    ends with a job's output named for deletion.
    """
    try:
        return client.list_local_files(
            mount.local, exclude_patterns=mount.exclude_patterns or None
        )
    except Exception as exc:  # noqa: BLE001 — tracking must not fail a sync
        logger.debug("Pre-transfer scan of '{}' failed: {}", mount.name, exc)
        return None


def record_upload(
    client: RsyncClient,
    mount: MountConfig,
    *,
    mirrored: bool = False,
    before: list[str] | None = None,
) -> None:
    """Record what a just-completed sync uploaded, for stale-file detection.

    ``mirrored`` must reflect whether the sync deleted remote-only files. An
    additive sync leaves everything previously uploaded on the cluster, so the
    record accumulates; a mirror removes them, so it replaces. Passing False for
    a mirror would report deleted files as still stale, and passing True for an
    additive sync would erase exactly the files that just became stale.

    Best-effort by design: a failure here is logged and swallowed, because the
    files did reach the cluster and failing the sync afterwards would be a
    worse outcome than losing one generation of tracking. Detection reports
    "unknown" rather than claiming the tree is clean, and the next successful
    sync recovers by unioning onto the baseline the invalidation preserved —
    see :mod:`srunx.sync.manifest`.

    A record with no such baseline (corrupt, or written by another account) is
    the one case an additive sync cannot repair, for the reason given at the
    branch below; the warning there names the non-destructive reset.

    Call only after a real, successful transfer. Recording a dry run, or a run
    that failed partway, would mark files as uploaded that are not there; once
    those are deleted locally they would be reported as stale on the cluster
    when they were never sent in the first place.

    ``before`` is the matching :func:`snapshot_local` result and becomes the
    recorded set. Without one there is nothing trustworthy to publish, so
    tracking is marked unknown instead: a scan taken *after* the transfer
    mis-handles both edges of a concurrent change (see :func:`snapshot_local`),
    and one of those directions ends with a job's output named as stale. A
    record that reads as authoritative must not be built that way — reporting
    "cannot tell" is the honest answer, and the next sync re-records.
    """
    from srunx.sync import manifest as manifest_mod

    if before is None:
        logger.warning(
            "Could not inventory mount '{}' before the transfer, so this sync "
            "cannot be recorded — stale-file detection reports 'unknown' until "
            "the next successful sync.",
            mount.name,
        )
        _mark_unknown(client, mount, "pre-transfer inventory failed")
        return

    try:
        previous = None
        try:
            previous = manifest_mod.read(client, mount)
        except manifest_mod.ManifestUnavailable as exc:
            if not mirrored:
                # An earlier failure preserved the last good path set alongside
                # the invalidation mark; union onto that and the record is
                # trustworthy again without anything being deleted.
                previous = manifest_mod.read_superseded(client, mount)
                if previous is None:
                    # Nothing to build on. The current inventory is not a valid
                    # baseline for an additive run: rsync left everything
                    # previously uploaded on the cluster, including files since
                    # deleted locally, and writing "this is everything" would
                    # claim those do not exist. Rather than a mirror — which
                    # deletes the cluster-only job output this feature exists to
                    # protect — the deliberate reset is removing the record.
                    logger.warning(
                        "Manifest for mount '{}' is unusable ({}) and an "
                        "additive sync cannot rebuild it, so stale-file "
                        "detection stays 'unknown'. To start over, delete "
                        "'{}' on the cluster and sync again — files uploaded "
                        "before that point are then untracked, which "
                        "under-reports rather than risking job output.",
                        mount.name,
                        exc,
                        manifest_mod.manifest_remote_path(mount),
                    )
                    return
                logger.info(
                    "Recovering the manifest for mount '{}' from the baseline "
                    "kept when it was invalidated ({}).",
                    mount.name,
                    exc,
                )
            else:
                logger.debug(
                    "Replacing unusable manifest for '{}': {}", mount.name, exc
                )

        excludes = client.effective_excludes(mount.exclude_patterns or None)
        # The before-scan *is* the record — see ``snapshot_local`` for why
        # scanning afterwards is wrong in both directions, and why there is no
        # after-scan fallback.
        manifest_mod.write(
            client, mount, before, excludes, previous=previous, mirrored=mirrored
        )
    except Exception as exc:  # noqa: BLE001 — tracking must not fail a sync
        logger.warning(
            "Could not record the upload manifest for mount '{}': {}. "
            "Stale-file detection will report 'unknown' until the next "
            "successful sync, which recovers from the baseline kept below.",
            mount.name,
            exc,
        )
        # Leaving the previous manifest would leave it *trusted* while it no
        # longer describes the remote — a file this sync uploaded and that is
        # later deleted locally would be reported as "nothing stale". Marking it
        # unusable is what makes the warning above true. The last good path set
        # rides along so the next successful sync can union onto it instead of
        # needing a mirror, which would delete cluster-only job output.
        _mark_unknown(client, mount, f"recording failed: {exc}", previous=previous)


def _mark_unknown(
    client: RsyncClient,
    mount: MountConfig,
    reason: str,
    previous: SyncManifest | None = None,
) -> None:
    """Stop trusting the record, without letting that failure fail the sync."""
    from srunx.sync import manifest as manifest_mod

    try:
        manifest_mod.invalidate(client, mount, reason, previous=previous)
    except Exception as inner:  # noqa: BLE001
        logger.warning(
            "Could not invalidate the stale manifest for mount '{}': {}. "
            "Its contents are now out of date and may under-report stale "
            "files until the next successful sync.",
            mount.name,
            inner,
        )
