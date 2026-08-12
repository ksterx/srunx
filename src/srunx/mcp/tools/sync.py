"""MCP tools: rsync-based file sync between local + remote SLURM cluster.

Two tools live here, split on purpose:

* :func:`inspect_mount` — read-only. Reports what a sync *would* change,
  including what exists only on the cluster.
* :func:`sync_files` — performs the sync.

They are separate rather than one tool with a flag because the inspection has
to be described as unconditionally safe to be usable. Folding it into
``sync_files`` meant one ``delete`` argument carrying both "show me what a
mirror would remove" (harmless) and "remove it" (destructive), so the argument's
documentation had to warn about data loss — and an agent reading that warning
avoids the argument altogether, losing the safe inspection with it.

The sync tool is reachable by an autonomous agent, which shapes three choices
that differ from the CLI's:

* **Mount-name only.** There is no free-form ``local_path`` / ``remote_path``
  pair. An agent that can name arbitrary source and destination paths can
  push anything the SSH credential can reach to anywhere it can write;
  restricting the surface to pre-registered mounts makes that structurally
  impossible instead of relying on a path check to catch it.
* **Additive by default.** ``delete`` is opt-in, and even when opted in it
  is capped by ``max_delete`` so a wrong or empty source tree cannot prune
  a whole remote directory.
* **Nothing is truncated.** The response reports exact transfer/deletion
  counts and enumerates every deleted path, because a caller that has to
  decide whether a sync was safe cannot do it from a clipped preview. The
  deletion list is bounded by ``max_delete``, which is what keeps "report
  everything" affordable.
"""

from __future__ import annotations

import io
import re
from dataclasses import replace
from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok
from srunx.sync.rsync import unescape_rsync_path

# rsync's ``--itemize-changes`` marks a removal with the literal pseudo-flag
# ``*deleting``; every other itemize line starts with a flag block whose first
# char is the update type.
#
# The path group is optional and permitted to be empty so that a deletion is
# still *recognised* when the filename is made purely of spaces. Requiring
# ``\s+(.+)`` there would fail to match such a line, and a missed deletion is
# not cosmetic: it under-counts the mirror preflight and lets a sync past the
# cap that should have stopped it.
_DELETING_RE = re.compile(r"^\*deleting(?:\s(.*))?$")

# Update types that mean file data actually crossed the wire: ``<`` (sent)
# and ``>`` (received). Everything else rsync can put here describes a change
# that moved no data, and counting any of them would inflate a number we
# promise is exact:
#   ``c`` — a *local* creation/change (a directory, a symlink's target)
#   ``h`` — a hard link, created rather than transferred
#   ``.`` — attributes only; contents already matched
# This is the same distinction rsync draws in its own "regular files
# transferred" statistic.
_TRANSFER_FLAGS = "<>"

# Only regular files (``f``) carry transferred data. Directories (``d``),
# symlinks (``L``), devices (``D``) and specials (``S``) are recreated on the
# far side instead. Requiring a valid item type here also rejects English
# chatter that happens to start with a flag letter.
_FILE_ITEM_TYPES = "f"

# Ceiling on deletions for a mirror sync, in rsync's own unit: filesystem
# *entries*, so a removed directory costs one on top of each file inside it.
# Bounds the damage a wrong source tree can cause on a real run.
DEFAULT_MAX_DELETE = 100

# Beyond this many deletions we report the count but omit the path list,
# flagging the omission explicitly (``deleted_paths_omitted``) rather than
# silently clipping it — a truncated list read as complete is worse than an
# absent one.
_MAX_REPORTED_DELETIONS = 1000

# rsync exits 25 (RERR_DEL_LIMIT) when --max-delete is exceeded.
_RERR_DEL_LIMIT = 25


def _display(paths: list[str]) -> list[str]:
    """Escaped rsync paths → the names as they exist on disk, for reporting."""
    return [unescape_rsync_path(p) for p in paths]


def _parse_itemized(
    stdout: str, *, max_paths: int | None = None
) -> tuple[list[str], int, int]:
    """Parse rsync ``-i`` output into (deleted paths, deletion count, transfers).

    ``max_paths`` bounds how many deletion *paths* are retained; the returned
    count is always exact. This split matters because an uncapped preview of a
    huge destination emits one line per deletable file, and retaining all of
    them would allocate millions of strings for a caller that only reports a
    number — or discards the list entirely above its reporting limit.
    ``max_paths=0`` collects no paths at all, which is what the mirror
    preflight wants since it only compares a count against the cap.

    The flag block is located by splitting on whitespace rather than by a
    fixed offset, because its width is version-dependent: GNU rsync 3.x emits
    11 characters (``YXcstpoguax``) while openrsync / rsync 2.6.9 — the stock
    binary on macOS — emit 9 (``YXcstpogz``). A fixed-offset predicate silently
    matches nothing on the shorter form and reports zero transfers.

    A line counts as a transfer only when its first token is shaped like a
    real flag block — a transfer update type followed by a file item type.
    Everything else is rsync chatter ("sending incremental file list", byte
    totals, ``created directory ...``, the ``Deletions stopped ...`` warning)
    and is ignored rather than miscounted. Directory entries are skipped for
    the same reason: creating a directory moves no file data.

    Trailing whitespace is never stripped from a line, because it can be part
    of the filename.
    """
    deleted: list[str] = []
    deleted_count = 0
    transferred = 0
    # Iterate the buffer instead of materialising splitlines() into a list, so
    # a mount with very many changed files doesn't cost a second full copy of
    # rsync's output. (The captured stdout itself still lives in memory —
    # bounding that would mean streaming inside RsyncClient.)
    for line in io.StringIO(stdout):
        line = line.rstrip("\r\n")
        if not line.strip():
            continue
        match = _DELETING_RE.match(line)
        if match:
            deleted_count += 1
            if max_paths is not None and len(deleted) >= max_paths:
                continue
            raw_path = match.group(1) or ""
            # The separator's width is version-dependent (one space on
            # openrsync, three on GNU rsync), so drop the indent — but keep
            # the raw value when stripping would empty it, which is how a
            # name consisting only of spaces survives.
            #
            # Known limit: a filename that legitimately *starts* with spaces
            # is indistinguishable from a wider separator in this format, so
            # its leading spaces are lost here. Preserving them instead would
            # prefix every GNU-rsync path with the separator's two extra
            # spaces — wrong in the common case to be right in a rare one.
            # The deletion is still counted either way, which is what the cap
            # and the refusal decision depend on.
            deleted.append(raw_path.lstrip() or raw_path)
            continue
        # The flag block is the first whitespace-delimited token. The filename
        # is whatever follows and is NOT required to be present or non-blank:
        # a name made only of spaces is legal, and demanding a non-empty
        # remainder would drop such a transfer from the count. What qualifies
        # the line is the flag block's shape, checked below.
        parts = line.split(None, 1)
        flags = parts[0] if parts else ""
        if len(flags) < 2:
            continue
        if flags[0] not in _TRANSFER_FLAGS or flags[1] not in _FILE_ITEM_TYPES:
            continue
        transferred += 1
    return deleted, deleted_count, transferred


def _resolve_mount(transport: str, mount: str) -> tuple[str, Any, Any]:
    """Resolve ``(profile_name, profile, mount_config)`` for a tool call.

    Raises :class:`ValueError` with a caller-facing message, which the tools
    turn into their error payload — shared so that both the read-only inspect
    tool and the sync tool reject the same inputs with the same wording.
    """
    from srunx.ssh.core.config import ConfigManager

    pname = transport.strip() if transport else ""
    if not pname or pname == "local":
        raise ValueError(
            "an SSH profile name is required (transport='<profile>'); "
            "there is no local sync"
        )

    cm = ConfigManager()
    profile = cm.get_profile(pname)
    if not profile:
        raise ValueError(f"SSH profile '{pname}' not found")

    mount_cfg = next((m for m in profile.mounts if m.name == mount), None)
    if not mount_cfg:
        available = [m.name for m in profile.mounts]
        raise ValueError(
            f"Mount '{mount}' not found in profile '{pname}'. Available: {available}"
        )
    return pname, profile, mount_cfg


def _candidates_among(stdout: str, wanted: set[str]) -> set[str]:
    """Return which of *wanted* appear as deletion candidates in *stdout*.

    Retains only members of *wanted* — a set the caller already holds — instead
    of materialising every candidate path. Parsing the full list to intersect
    afterwards would allocate a string per deletable file on a mount with many
    artifacts, which is the unbounded retention the parser's ``max_paths``
    exists to avoid.
    """
    if not wanted:
        return set()
    found: set[str] = set()
    for line in io.StringIO(stdout):
        match = _DELETING_RE.match(line.rstrip("\r\n"))
        if not match:
            continue
        raw = match.group(1) or ""
        path = raw.lstrip() or raw
        if path in wanted:
            found.add(path)
    return found


def _stale_report(rsync: Any, mount_cfg: Any) -> Any:
    """Compare srunx's upload record against the current local tree.

    Never raises: an inspection that fails wholesale because tracking is
    unavailable would be worse than one that reports the parts it does know.
    Any failure becomes an "unknown" report carrying the reason, which the
    caller must not read as "nothing is stale".
    """
    from srunx.sync import manifest as manifest_mod

    try:
        recorded = manifest_mod.read(rsync, mount_cfg)
    except manifest_mod.ManifestUnavailable as exc:
        return manifest_mod.StaleReport(known=False, reason=str(exc))
    except Exception as exc:  # noqa: BLE001 — inspection degrades, never fails
        return manifest_mod.StaleReport(
            known=False, reason=f"could not read the upload record: {exc}"
        )

    try:
        current = manifest_mod.local_inventory(
            rsync, mount_cfg.local, exclude_patterns=mount_cfg.exclude_patterns or None
        )
    except Exception as exc:  # noqa: BLE001
        return manifest_mod.StaleReport(
            known=False, reason=f"could not list the local tree: {exc}"
        )

    return manifest_mod.find_stale(
        recorded,
        current,
        rsync.effective_excludes(mount_cfg.exclude_patterns or None),
    )


@mcp.tool()
def inspect_mount(
    transport: str,
    mount: str,
    max_paths: int = _MAX_REPORTED_DELETIONS,
) -> dict[str, Any]:
    """Report what syncing a mount would change, without changing anything.

    Read-only: this never transfers, deletes, or creates anything on the
    cluster. Call it freely, including before a sync you are unsure about.

    Its main job is answering a question ``sync_files`` cannot: **what is on the
    cluster that no longer exists locally?** A sync is additive, so those files
    stay — including code deleted in a local refactor, which a job on the
    cluster can still import and run. They are listed here as
    ``mirror_delete_candidate_paths``.

    Those candidates mix two kinds of thing:

    * **produced by jobs** — checkpoints, logs, outputs. Must NOT be deleted.
    * **left over locally** — stale modules, renamed files. Usually should be.

    ``stale_upload_paths`` is the second group on its own: paths srunx recorded
    uploading that are no longer present locally. Job output was never uploaded,
    so it does not appear there — which holds even when the mount's exclude list
    misses an output directory, the case that otherwise buries a few stale
    scripts among dozens of artifacts.

    One exception: output pulled into the local tree with ``srunx ssh sync
    --pull`` becomes a file the next push manages, so it is recorded like any
    other and can be reported as stale once its local copy is removed. Excluding
    the output directories on the mount avoids that, and is worth doing anyway.

    **Check ``stale_uploads_known`` first.** When it is false the record could
    not answer (nothing uploaded with tracking yet, an unreadable record, or a
    changed exclude filter), and ``stale_uploads: 0`` means "could not tell",
    not "nothing is stale" — ``stale_uploads_unknown_reason`` says which. Fall
    back to reading the full candidate list yourself in that case.

    Args:
        transport: SSH profile name to inspect. Required — there is no local
            inspection, and (unlike the CLI) no implicit current-profile
            fallback. Call ``list_ssh_profiles`` for the available profiles and
            the mounts each defines.
        mount: Mount name from that SSH profile.
        max_paths: Cap on how many paths to list. Counts stay exact regardless;
            past the cap the list is omitted rather than shortened, and
            ``mirror_delete_candidate_paths_omitted`` says so.

    Returns:
        ``files_would_transfer``, ``mirror_delete_candidates`` (a count),
        ``mirror_delete_candidate_paths``, whether that list was omitted,
        ``effective_exclude_patterns``, and the stale-upload fields described
        above (``stale_uploads_known`` / ``stale_uploads`` /
        ``stale_upload_paths`` / ``stale_uploads_unknown_reason``).

        The exclude list matters for reading the result: excluded paths are
        invisible to this inspection *and* protected from a mirror's deletions,
        so something absent from the candidates may simply be excluded rather
        than in sync.
    """
    try:
        from srunx.sync.mount_helpers import build_rsync_client

        if max_paths < 0:
            raise ValueError(f"max_paths must be >= 0, got {max_paths}")

        pname, profile, mount_cfg = _resolve_mount(transport, mount)
        rsync = build_rsync_client(profile)

        # One pass with --delete --dry-run reports both halves at once: what
        # would transfer, and what a mirror would delete. A dry run changes
        # nothing on the remote — not even a directory — so asking about
        # deletions here carries no risk of performing any.
        #
        # No cap is passed: a cap exists to bound a real mirror's damage, and
        # capping an inspection would replace the very list it was asked for
        # with an error. No lock is taken either — this only reads, and making
        # callers queue behind a running sync (or time out) to look at a
        # snapshot is a worse trade than a snapshot that is a moment stale.
        result = rsync.push(
            mount_cfg.local,
            mount_cfg.remote,
            delete=True,
            dry_run=True,
            itemize=True,
            exclude_patterns=mount_cfg.exclude_patterns,
        )
        if not result.success:
            return err(
                f"Inspection failed (exit {result.returncode}): "
                f"{result.stderr[:500] if result.stderr else 'unknown error'}. "
                f"If '{mount_cfg.remote}' does not exist on the cluster yet, "
                f"there is nothing to inspect — sync once with "
                f"sync_files(delete=False) to create it."
            )

        paths, candidates, transferred = _parse_itemized(
            result.stdout or "", max_paths=max_paths + 1
        )
        omit = candidates > max_paths

        # Narrow the candidates to what srunx itself uploaded and is now gone
        # locally. That is the actionable subset: everything else on the cluster
        # was produced there, and no exclude list has to be complete for this to
        # hold. Reported separately rather than filtered in, so the caller still
        # sees the full picture.
        stale = _stale_report(rsync, mount_cfg)
        if stale.known and stale.paths:
            # Intersect with what rsync says is actually still there. The record
            # says what was uploaded, not what survived: a manual cleanup, or a
            # mirror whose recording failed, leaves entries naming files that no
            # longer exist. Reporting those contradicts the documented promise
            # that this is a subset of the deletion candidates.
            #
            # Scanned against the recorded set rather than by collecting every
            # candidate first, so the retained data stays bounded by the record
            # instead of by how much output the cluster holds.
            on_remote = _candidates_among(result.stdout or "", set(stale.paths))
            stale = replace(stale, paths=[p for p in stale.paths if p in on_remote])

        return ok(
            profile=pname,
            mount=mount_cfg.name,
            local=mount_cfg.local,
            remote=mount_cfg.remote,
            files_would_transfer=transferred,
            mirror_delete_candidates=candidates,
            # Unescaped only here, at the boundary. Paths are matched in
            # rsync's escaped form — that is what makes an inventory entry and
            # a deletion candidate the same string — but a caller acting on
            # this list needs the name as it exists on disk.
            mirror_delete_candidate_paths=[] if omit else _display(paths),
            mirror_delete_candidate_paths_omitted=omit,
            stale_uploads_known=stale.known,
            stale_uploads=stale.count,
            # Omitted wholesale past the cap, matching the mirror-candidate
            # contract. Returning a silently shortened list would let a caller
            # present it as the complete set.
            stale_upload_paths=[] if stale.count > max_paths else _display(stale.paths),
            stale_upload_paths_omitted=stale.count > max_paths,
            stale_uploads_unknown_reason=stale.reason,
            # The merged view, not ``rsync.exclude_patterns``: per-call patterns
            # are applied for the invocation without being stored, so the
            # attribute omits exactly the mount-level patterns the user
            # configured — the ones most likely to explain a missing candidate.
            effective_exclude_patterns=rsync.effective_excludes(
                mount_cfg.exclude_patterns
            ),
        )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def sync_files(
    transport: str,
    mount: str,
    dry_run: bool = False,
    delete: bool = False,
    max_delete: int = DEFAULT_MAX_DELETE,
) -> dict[str, Any]:
    """Sync a configured mount from this machine to a remote SLURM cluster.

    Copies new and changed files only. Files that exist on the cluster but not
    locally are left untouched unless ``delete=True``.

    That means a file deleted locally stays on the cluster, where a job can
    still pick it up. This tool does not report those — call ``inspect_mount``
    to see them. It is read-only, so it is safe to call before or after a sync;
    reach for it rather than setting ``delete=True`` to find out what is stale.

    Args:
        transport: SSH profile name to sync against. Required and must name
            an SSH profile — there is no local-to-local sync, and (unlike
            the CLI) no implicit current-profile fallback. ``"local"`` is
            rejected. Call ``list_ssh_profiles`` to see profiles and the
            mounts each one defines.
        mount: Mount name from that SSH profile. Only pre-registered mounts
            can be synced; arbitrary paths are not accepted.
        dry_run: Preview only. Reports exactly what would be transferred and
            deleted without touching the cluster. Prefer this first whenever
            you are unsure, and always before a ``delete=True`` run.
        delete: Mirror the mount — also DELETE cluster files that no longer
            exist locally. **This destroys remote-only data** such as
            training checkpoints, job logs, and outputs written by jobs on
            the cluster, which by definition do not exist locally. Leave it
            off unless the user explicitly asked for a mirror, and preview
            with ``dry_run=True`` before running it.
        max_delete: Refuse the mirror, without changing anything, if it would
            delete more than this many **entries**. Entries are files *and*
            directories, matching rsync's own ``--max-delete`` unit: removing
            a directory holding two files counts as three entries (both files
            plus the directory), so set this above the file count you have in
            mind. Guards against mirroring from a wrong or half-populated
            local directory. Must be >= 1; to sync without deleting, leave
            ``delete`` off. Only applies to a real ``delete=True`` run — a
            ``dry_run`` preview is never capped, so it can show the whole list.

    Returns:
        On success: ``files_transferred``, ``entries_deleted``, and the
        ``deleted_paths`` list. Past a very large number of deletions the list
        is omitted and ``deleted_paths_omitted`` is set — the count stays
        exact, and no list is ever silently shortened.

        The two counts use different units on purpose, because that is what
        rsync reports: ``entries_deleted`` includes removed directories, while
        ``files_transferred`` counts only regular files whose data actually
        crossed the wire — matching rsync's own "regular files transferred"
        statistic. Directory creations, symlinks, devices, hard links and
        attribute-only touch-ups move no data and are excluded, so a sync can
        legitimately change the remote while reporting zero transfers.

        Counts are reliable; path *strings* have one documented limit. rsync
        separates its flag block from the filename with whitespace whose width
        varies by version, so a filename that itself begins with spaces cannot
        be told apart from that separator, and those leading spaces are lost
        from the reported string. Such a deletion is still counted, so the
        cap and the refusal logic are unaffected.
    """
    try:
        from srunx.common.config import get_config
        from srunx.sync.lock import SyncLockTimeoutError, acquire_sync_lock
        from srunx.sync.mount_helpers import build_rsync_client

        if max_delete < 1:
            return err(
                f"max_delete must be >= 1, got {max_delete}. To sync without "
                "deleting anything, leave delete=False (the default) instead "
                "of capping deletions at zero."
            )

        pname, profile, mount_cfg = _resolve_mount(transport, mount)
        rsync = build_rsync_client(profile)
        timeout = get_config().sync.lock_timeout_seconds

        # Hold the per-(profile, mount) lock that every srunx writer of a mount
        # takes — submission auto-sync, workflow runs, `srunx ssh sync`, and the
        # Web sync route — so an agent-driven sync cannot interleave with any of
        # them and leave a half-written tree on the cluster. The mirror preflight
        # below runs inside the same lock as the push it guards, which is what
        # lets the refusal claim the remote is unchanged. Note the lock is
        # advisory and machine-local: it cannot stop a cluster-side job from
        # writing, nor a sync launched from another workstation.
        try:
            with acquire_sync_lock(pname, mount_cfg.name, timeout=timeout):
                # rsync's --max-delete is NOT an atomic precheck. It deletes up
                # to the cap, skips the remaining deletions, finishes
                # transferring, and only then exits 25 — verified against
                # openrsync: a capped run left the destination modified. So the
                # cap alone can never justify reporting "nothing changed".
                # Counting the deletions in a dry run first is what lets a
                # refusal actually mean the remote is untouched.
                if delete and not dry_run:
                    preview = rsync.push(
                        mount_cfg.local,
                        mount_cfg.remote,
                        delete=True,
                        dry_run=True,
                        itemize=True,
                        # The preflight carries the cap too. Without it, a
                        # mirror against a wrong or empty source enumerates
                        # every deletable path before we get to refuse it, so
                        # the cap would bound the damage but not the memory —
                        # the process could die counting deletions it was
                        # about to reject. With it, rsync stops counting at
                        # the cap and exits 25, which IS the refusal signal.
                        max_delete=max_delete,
                        exclude_patterns=mount_cfg.exclude_patterns,
                    )
                    if preview.returncode == _RERR_DEL_LIMIT:
                        return err(
                            f"Refused to mirror: it would delete more than "
                            f"{max_delete} entries under '{mount_cfg.remote}' "
                            f"(the max_delete cap; entries count directories "
                            f"as well as files). Nothing was changed. Re-run "
                            f"with dry_run=True to see the deletions, and only "
                            f"raise max_delete if they are expected."
                        )
                    if not preview.success:
                        return err(
                            f"Mirror preflight failed (exit "
                            f"{preview.returncode}): "
                            f"{preview.stderr[:500] if preview.stderr else 'unknown error'}. "
                            f"Nothing was changed. If '{mount_cfg.remote}' does "
                            f"not exist on the cluster yet, sync once with "
                            f"delete=False to create it — a preview cannot "
                            f"create the destination, because a dry run is not "
                            f"allowed to modify the remote."
                        )
                    # Count only: the preflight compares against the cap and
                    # never reports paths, so retaining none keeps this bounded
                    # regardless of how many deletions the remote holds.
                    _, would_delete_count, _ = _parse_itemized(
                        preview.stdout or "", max_paths=0
                    )
                    if would_delete_count > max_delete:
                        return err(
                            f"Refused to mirror: it would delete "
                            f"{would_delete_count} entries (files and "
                            f"directories) under '{mount_cfg.remote}', over the "
                            f"max_delete cap of {max_delete}. Nothing was "
                            f"changed. Re-run with dry_run=True to see the full "
                            f"list, and only raise max_delete if those "
                            f"deletions are expected."
                        )

                from srunx.sync.mount_helpers import record_upload, snapshot_local

                before = (
                    None if dry_run else snapshot_local(rsync, mount_cfg)
                )  # paired with record_upload below

                result = rsync.push(
                    mount_cfg.local,
                    mount_cfg.remote,
                    delete=delete,
                    dry_run=dry_run,
                    # Always itemize: the per-file lines ARE the report, for a
                    # real push as much as for a preview.
                    itemize=True,
                    # Kept as a backstop behind the preflight for a real
                    # mirror. A preview passes no cap: it changes nothing, and
                    # capping it would replace the very list the caller asked
                    # to see with an error.
                    max_delete=max_delete if (delete and not dry_run) else None,
                    exclude_patterns=mount_cfg.exclude_patterns,
                )

                if result.success and not dry_run:
                    # Inside the lock, deliberately. Recording after releasing it
                    # lets an overlapping sync finish its push and then have its
                    # record overwritten by this one's older path set, leaving
                    # the manifest describing neither tree.
                    record_upload(rsync, mount_cfg, mirrored=delete, before=before)
        except SyncLockTimeoutError as exc:
            return err(str(exc))

        if not result.success:
            if result.returncode == _RERR_DEL_LIMIT:
                return err(
                    f"Mirror hit the max_delete cap of {max_delete} even though "
                    f"the preflight check passed, so the remote changed in "
                    f"between. Files may already have been deleted or "
                    f"transferred — re-run with dry_run=True to see the "
                    f"current difference before retrying."
                )
            return err(
                f"rsync failed (exit {result.returncode}): "
                f"{result.stderr[:500] if result.stderr else 'unknown error'}"
            )

        # Retain one path beyond the reporting limit: that is all it takes to
        # know the limit was exceeded, without holding a list we would drop.
        deleted_paths, deleted_count, transferred = _parse_itemized(
            result.stdout or "", max_paths=_MAX_REPORTED_DELETIONS + 1
        )
        omit_paths = deleted_count > _MAX_REPORTED_DELETIONS
        return ok(
            profile=pname,
            mount=mount_cfg.name,
            local=mount_cfg.local,
            remote=mount_cfg.remote,
            dry_run=dry_run,
            delete=delete,
            files_transferred=transferred,
            # The exact count, not len(deleted_paths) — that list is capped.
            # Named "entries" because rsync emits (and caps) one deletion per
            # filesystem entry, directories included.
            entries_deleted=deleted_count,
            deleted_paths=[] if omit_paths else _display(deleted_paths),
            deleted_paths_omitted=omit_paths,
            # Distinguishes "nothing to delete" from "deletions were never
            # looked for". An additive sync does not ask rsync about them at
            # all, so entries_deleted=0 must not be read as "nothing is stale
            # on the cluster" — that question is what inspect_mount answers.
            mirror_candidates_inspected=delete,
        )

    except Exception as e:
        return err(str(e))
