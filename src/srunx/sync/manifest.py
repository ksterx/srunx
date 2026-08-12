"""Record of what srunx last uploaded to a mount, for stale-file detection.

Syncing is additive (see :mod:`srunx.sync.mount_helpers`), so a file deleted
locally stays on the cluster — where a job can still import it. Listing what
exists only on the remote is not enough to act on, because that set mixes two
opposite things:

* files a **job produced** — checkpoints, logs, outputs. Must not be deleted.
* files **deleted locally** — stale modules, renamed files. Usually should be.

Excluding output directories separates them only as well as the exclude list is
maintained, and a missed pattern silently buries the second group in the first:
in one real mount, four stale scripts sat among 39 job artifacts because
``dist/`` had never been excluded.

A manifest removes the guesswork. srunx records the paths it uploaded, so
"present in the manifest but no longer local" identifies stale files regardless
of the exclude list — output a job wrote at a path of its own was never
uploaded, so it is simply not in the record.

Design notes
------------

**The manifest lives on the remote**, at
``<mount.remote>/.srunx-manifest.json``, not in the local database. A single
file rather than a file inside a ``.srunx/`` directory, because creating that
directory would follow a symlink planted in its place. It describes the state
of the remote tree, so it
belongs with it: it survives a workstation being rebuilt, and every machine
syncing that mount reads the same record instead of each keeping a private
guess.

**It is separate from the owner marker** even though both are small JSON files
at the mount root. The marker is deliberately fail-open — an unreadable one
means "no owner recorded", and syncing proceeds. A manifest must fail *closed*:
if it cannot be read, srunx must report that it does not know rather than
report an empty set of stale files, which would read as "everything is clean".

**Paths are stored in rsync's escaped form** (``\\#012`` for a newline, and so
on), because they exist to be compared against a deletion preview, which is
printed that way. The escaping is also what makes the inventory parseable at
all: unescaped, a newline inside a filename splits one file across two lines,
and a name crafted to look like a listing line yields paths that do not exist.
:func:`~srunx.sync.rsync.unescape_rsync_path` converts back for display.

**Only paths are recorded.** That is enough to *detect* staleness, which is all
this layer does. Deciding to delete would need more (at minimum a content hash,
to prove the remote copy is still the one srunx uploaded and not something a
job rewrote), and that decision is deliberately left to a human here.

**The exclude set is fingerprinted.** Excluded files are never uploaded, so they
are never in the manifest; if the exclude list changes, paths can leave the
manifest for a reason that has nothing to do with being stale. When the
fingerprint differs from the recorded one, detection reports "unknown" rather
than presenting those paths as stale — the safe direction, since the cost of
missing a stale file is a warning that does not appear, while the cost of a
false positive is a user deleting something a job needs.

Known gap: pulled job output
----------------------------

The record is built from what a push *manages* — the local tree after filtering,
scanned on both sides of the transfer and narrowed to what both scans saw — not
from bytes rsync moved, because an unchanged file transfers nothing yet is still
srunx's to track. That makes one case wrong: ``srunx ssh sync --pull``
fetches checkpoints or logs into the local tree, and a later push then records
them as uploads. Deleting the local copy afterwards reports the remote artifact
as stale, which is exactly the confusion this module exists to remove.

The fix is a mount whose excludes cover the output directories, which is the
same fix that makes the raw candidate list readable, and which every configured
mount here already does. Pulling job output into a mount that syncs it back is
the misconfiguration; the record cannot detect that on its own, since at that
point the artifact genuinely is a file the push manages.

Known gap: concurrent workstations
----------------------------------

Recording is read-modify-write, and the sync lock is a machine-local ``flock``.
Two workstations syncing one mount at the same time can both read the same
record and each write their own union, so the later write drops paths the other
just added — detection stays "known" while being incomplete. Closing it needs
remote serialization or a compare-and-swap on the generation. It is left open
because the ownership marker already treats two machines writing one mount as
the thing to warn about, and the error runs the safe way: a missed warning.

Known gap: interrupted syncs
----------------------------

The record is written after a sync succeeds, so a push that transfers files and
then fails — rsync exiting 23 partway, hash verification raising, the process
being killed — leaves the previous record in place. Files that did reach the
cluster are missing from it, and deleting one of them locally before the next
successful sync makes it go unreported.

Recording *before* the transfer would close that, but not cheaply or cleanly: a
record marked unusable up front is, by the rule above, one an additive sync
refuses to rebuild, so it would need a second file or an embedded copy of the
previous state, plus two extra round-trips on every sync — paid by everyone, on
every sync, for a window that a single successful sync closes (the next
inventory sees those files and the union restores them). The error also runs in
the safe direction: a missed warning, never a wrong instruction to delete.
"""

from __future__ import annotations

import hashlib
import json
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from srunx.common.logging import get_logger

if TYPE_CHECKING:
    from srunx.ssh.core.config import MountConfig
    from srunx.sync.rsync import RsyncClient

logger = get_logger(__name__)

# Version 2 stores paths in rsync's escaped form (``\#012`` for a newline, and
# so on) so that a recorded path and a deletion candidate are literally the same
# string. A version 1 record holds literal names, which would silently fail to
# match, so it is refused: reported as "cannot tell" rather than "clean". Like
# any record this cannot build on, it is cleared by deleting the file and
# syncing again — version 1 never shipped, so nothing in the wild needs it.
SCHEMA_VERSION = 2

# A file at the mount root, not ``.srunx/manifest.json``. A directory would have
# to be created before writing, and ``mkdir -p`` follows an existing symlink —
# so another account able to write the mount root could pre-create ``.srunx`` as
# a link and have the manifest written into any directory they can reach. A
# plain file needs no directory creation, and the writer already refuses a
# symlinked target.
_MANIFEST_FILENAME = ".srunx-manifest.json"


class ManifestUnavailable(RuntimeError):
    """The manifest could not be read or parsed.

    Raised instead of returning "no stale files", so a caller cannot mistake an
    unreadable record for a clean tree.
    """


@dataclass(frozen=True)
class SyncManifest:
    """What srunx uploaded to one mount, and under which filter."""

    paths: frozenset[str]
    exclude_fingerprint: str
    generation: int = 1
    written_at: str = ""
    writer_host: str = ""
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> str:
        """Serialise for the remote file.

        Paths are sorted so an unchanged tree produces an unchanged file,
        which keeps diffs and manual inspection meaningful.
        """
        return json.dumps(
            {
                "schema_version": self.schema_version,
                "generation": self.generation,
                "written_at": self.written_at,
                "writer_host": self.writer_host,
                "exclude_fingerprint": self.exclude_fingerprint,
                "paths": sorted(self.paths),
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, raw: str) -> SyncManifest:
        """Parse a remote manifest.

        Raises:
            ManifestUnavailable: On anything unparseable or of an unknown
                schema version. A future version may record fields this one
                cannot interpret, and guessing at them risks reporting files as
                stale that the newer writer knew were not.
        """
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ManifestUnavailable(f"manifest is not valid JSON: {exc}") from exc

        if not isinstance(data, dict):
            raise ManifestUnavailable("manifest is not a JSON object")

        version = data.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ManifestUnavailable(
                f"manifest schema version {version!r} is not supported "
                f"(this srunx understands {SCHEMA_VERSION})"
            )

        if data.get("invalidated"):
            # Written deliberately when a sync could not be recorded. Without
            # it, the previous manifest would keep being trusted and changes
            # made since would be invisible — reported as "known, nothing
            # stale" rather than "cannot tell".
            raise ManifestUnavailable(
                f"manifest was invalidated: {data.get('reason', 'unknown reason')}"
            )

        paths = data.get("paths")
        fingerprint = data.get("exclude_fingerprint")
        if not isinstance(paths, list) or not isinstance(fingerprint, str):
            raise ManifestUnavailable("manifest is missing paths or fingerprint")

        if not all(isinstance(p, str) for p in paths):
            # Coercing with str() would turn null/numbers/objects into
            # plausible-looking paths and report them as stale. A record we
            # cannot read exactly is a record we must not act on.
            raise ManifestUnavailable("manifest contains a non-string path entry")

        try:
            generation = int(data.get("generation", 1))
        except (TypeError, ValueError) as exc:
            # Must surface as ManifestUnavailable, not a bare TypeError: the
            # recorder's "replace an unreadable manifest" path keys off this
            # exception type, and without it a corrupt file is left in place and
            # every later sync stays unable to restore tracking.
            raise ManifestUnavailable(
                f"manifest has a non-numeric generation: {data.get('generation')!r}"
            ) from exc

        return cls(
            paths=frozenset(str(p) for p in paths),
            exclude_fingerprint=fingerprint,
            generation=generation,
            written_at=str(data.get("written_at", "")),
            writer_host=str(data.get("writer_host", "")),
        )


@dataclass(frozen=True)
class StaleReport:
    """Result of comparing a manifest against the current local tree."""

    paths: list[str] = field(default_factory=list)
    known: bool = True
    reason: str = ""

    @property
    def count(self) -> int:
        return len(self.paths)


def exclude_fingerprint(patterns: Sequence[str]) -> str:
    """Fingerprint an exclude set.

    Order matters to rsync, so it is preserved rather than sorted: two lists
    with the same patterns in a different order can filter differently, and
    treating them as equal would let a changed filter pass unnoticed.

    Serialized as JSON rather than joined on a separator, so that no two
    different lists can hash alike: joining on a newline made ``["a\\nb"]`` and
    ``["a", "b"]`` identical, and a filter change that hashes the same is a
    filter change this cannot notice — the one thing the fingerprint is for.
    """
    serialized = json.dumps(list(patterns))
    return hashlib.sha256(serialized.encode()).hexdigest()


def manifest_remote_path(mount: MountConfig) -> str:
    """Return the manifest's absolute remote path.

    Joined with ``/`` explicitly because the remote is POSIX — ``Path`` would
    use the host separator on a Windows workstation.
    """
    return f"{mount.remote.rstrip('/')}/{_MANIFEST_FILENAME}"


def local_inventory(
    client: RsyncClient,
    local_path: str,
    exclude_patterns: Sequence[str] | None = None,
) -> list[str]:
    """List the files a sync of *local_path* would upload.

    Uses rsync itself, with the same binary and the same merged filter as the
    real transfer, so the manifest cannot disagree with what was actually sent.
    Re-implementing rsync's matching in Python would drift: anchored patterns,
    directory rules and ``**`` all have specific semantics.

    Directories are omitted — only files are recorded, since a directory that
    lingers because it still holds job output is not itself a stale file.
    """
    return client.list_local_files(local_path, exclude_patterns=exclude_patterns)


def read(client: RsyncClient, mount: MountConfig) -> SyncManifest | None:
    """Read the manifest for *mount*, or ``None`` if none has been written.

    Raises:
        ManifestUnavailable: If a manifest exists but cannot be understood.
            Distinct from ``None`` on purpose: "never recorded" is a normal
            first-run state, while "recorded but unreadable" must not be
            silently treated as an empty record.
    """
    # ``require_owned``: this record decides which paths a user is told are safe
    # to delete, so one written by another account on a shared mount must not be
    # believed. Refusing symlinks alone does not cover it — a peer able to write
    # the mount root can plant an ordinary, schema-valid file naming job output
    # as stale.
    raw = client.read_remote_file(manifest_remote_path(mount), require_owned=True)
    if raw is None:
        return None
    return SyncManifest.from_json(raw)


def write(
    client: RsyncClient,
    mount: MountConfig,
    paths: Sequence[str],
    exclude_patterns: Sequence[str],
    *,
    previous: SyncManifest | None = None,
    mirrored: bool = False,
) -> SyncManifest:
    """Record *paths* as what srunx has uploaded to *mount*.

    Call only after a successful sync. Recording paths that failed to transfer
    would mark files as uploaded that are not there, and they would then be
    reported as stale on the next run once deleted locally.

    ``mirrored`` says whether the sync deleted remote-only files. It decides
    whether the previous record is kept, and getting it wrong defeats the whole
    feature:

    * **Additive sync** (the default) leaves everything srunx ever uploaded on
      the cluster, including files since deleted locally — those are exactly
      what should be reported as stale. So the record is the union of what was
      there before and what was just sent. Replacing it with the current
      inventory instead makes the sync that *creates* a stale file also erase
      the evidence of it, and the next inspection reports nothing.
    * **Mirror** removes remote-only files, so afterwards the remote does match
      the local tree and the record is replaced outright — keeping the old
      paths would report files as stale that the mirror already deleted.
    """
    fingerprint = exclude_fingerprint(exclude_patterns)

    combined = set(paths)
    if not mirrored and previous is not None:
        if previous.exclude_fingerprint == fingerprint:
            combined |= previous.paths
        # Filter changed: the old paths were selected by a different filter, so
        # a file that is still present locally but newly excluded would be kept
        # in the record while vanishing from every later inventory — and then
        # reported as stale, which is the exact false positive the fingerprint
        # check exists to prevent. Starting from the current inventory instead
        # loses track of genuinely stale files uploaded under the old filter;
        # that is the safer direction, since a missed warning costs less than
        # telling someone to delete a file a job needs.

    manifest = SyncManifest(
        paths=frozenset(combined),
        exclude_fingerprint=fingerprint,
        generation=(previous.generation + 1) if previous else 1,
        written_at=datetime.now(UTC).isoformat(timespec="seconds"),
        writer_host=socket.gethostname(),
    )
    # 0600: this enumerates a project's file names and the workstation that
    # pushed them, and only its own writer ever reads it (reads already refuse a
    # foreign owner). A mount root is often traversable on a shared cluster even
    # when the directories under it are not.
    client.write_remote_file(
        manifest_remote_path(mount), manifest.to_json(), mode="600"
    )
    logger.debug(
        "Recorded {} paths for mount '{}' (generation {})",
        len(manifest.paths),
        mount.name,
        manifest.generation,
    )
    return manifest


def invalidate(
    client: RsyncClient,
    mount: MountConfig,
    reason: str,
    previous: SyncManifest | None = None,
) -> None:
    """Mark the manifest unusable, so detection reports unknown rather than clean.

    Used when a sync succeeded but could not be recorded. Leaving the previous
    manifest in place would keep it *trusted* while it no longer describes the
    remote: a file uploaded by that sync and later deleted locally would sit on
    the cluster while inspection reported "known, nothing stale". Marking it
    unusable turns that into an honest "cannot tell".

    *previous* is carried over as a **superseded** baseline. Without it the mark
    is a dead end: an additive sync will not rebuild a record from scratch (it
    cannot — see :func:`~srunx.sync.mount_helpers.record_upload`), so detection
    would stay unknown until someone ran a mirror, and a mirror deletes exactly
    the cluster-only job output this feature exists to protect. Keeping the last
    good path set lets the next successful sync union onto it and recover on its
    own. It is *not* read as a manifest: :meth:`SyncManifest.from_json` still
    refuses the record, so nothing is reported while it stands.

    Does nothing when there is no manifest yet and no baseline to keep. An
    absent record already means "cannot tell", and it is the one state an
    additive sync can still build on — so marking it would strand the mount at
    unknown rather than protect anything.
    """
    try:
        existing_raw = _read_raw(client, mount)
        # ``None`` from the reader means the file is genuinely missing — it
        # tells that apart from an error by exit code — so absence is *proven*
        # only on this path.
        proven_absent = existing_raw is None
    except Exception as exc:  # noqa: BLE001 — must not fail a landed sync
        # Unreadable is not absent. Skipping the mark here would leave an
        # outdated record trusted the moment ssh recovers, so files this sync
        # uploaded could go unrecorded while detection still reports "known".
        logger.debug("Could not read the manifest for '{}': {}", mount.name, exc)
        existing_raw = None
        proven_absent = False

    if previous is None:
        # Carry forward whatever last-known-good set is already there — a live
        # record's own paths, or a baseline an earlier invalidation kept. Look
        # at both: without the first, one transient read error erases a valid
        # record; without the second, a second failure erases what the first
        # preserved. Either way the recovery path is gone for good.
        previous = _last_known_good(existing_raw)

    if previous is None and proven_absent:
        # Nothing on the remote to distrust, and an absent record already reads
        # as "cannot tell". Writing the mark here would only remove the one
        # state an additive sync can still build on from scratch, stranding the
        # mount at unknown until someone deleted the file by hand.
        logger.debug(
            "No manifest to invalidate for mount '{}'; leaving it absent so the "
            "next successful sync can establish one ({}).",
            mount.name,
            reason,
        )
        return

    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "invalidated": True,
        "reason": reason,
        "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "writer_host": socket.gethostname(),
    }
    if previous is not None:
        payload["superseded"] = {
            "paths": sorted(previous.paths),
            "exclude_fingerprint": previous.exclude_fingerprint,
            "generation": previous.generation,
        }
    client.write_remote_file(
        manifest_remote_path(mount), json.dumps(payload, indent=2), mode="600"
    )


def _read_raw(client: RsyncClient, mount: MountConfig) -> str | None:
    """The manifest file's contents, or ``None`` if it is absent.

    Errors propagate rather than reading as ``None``: callers act differently on
    "there is no record" than on "I could not find out", and collapsing the two
    turns a transient ssh failure into a claim of absence.
    """
    return client.read_remote_file(manifest_remote_path(mount), require_owned=True)


def _last_known_good(raw: str | None) -> SyncManifest | None:
    """The newest trustworthy path set in *raw*: a live record, else a baseline."""
    if raw is None:
        return None
    try:
        return SyncManifest.from_json(raw)
    except ManifestUnavailable:
        return _superseded_from(raw)


def read_superseded(client: RsyncClient, mount: MountConfig) -> SyncManifest | None:
    """Return the baseline an :func:`invalidate` preserved, if there is one.

    Only the recorder uses this, to recover on the next successful sync. It
    deliberately does not go through :func:`read`, which must keep refusing an
    invalidated record so that detection reports "cannot tell".

    Returns ``None`` whenever there is nothing trustworthy to recover from — no
    record, an unreadable one, or one written before the baseline was carried
    over. Recovery is a bonus; failing to find one leaves the honest unknown.
    """
    try:
        return _superseded_from(_read_raw(client, mount))
    except Exception as exc:  # noqa: BLE001 — recovery must not raise
        logger.debug("Could not read a baseline for '{}': {}", mount.name, exc)
        return None


def _superseded_from(raw: str | None) -> SyncManifest | None:
    """Parse a preserved baseline out of an invalidated record."""
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        # Both guards matter. The version, because a future writer may record
        # paths this version cannot interpret — restoring them would rewrite a
        # confident record with semantics we do not understand, which is worse
        # than the honest unknown a downgrade otherwise gets. The flag, because
        # only an invalidation puts a baseline here; anything else carrying that
        # key is not a record this wrote.
        if data.get("schema_version") != SCHEMA_VERSION:
            return None
        if data.get("invalidated") is not True:
            return None
        superseded = data["superseded"]
        paths = superseded["paths"]
        fingerprint = superseded["exclude_fingerprint"]
        if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
            return None
        if not isinstance(fingerprint, str):
            return None
        return SyncManifest(
            paths=frozenset(paths),
            exclude_fingerprint=fingerprint,
            generation=int(superseded.get("generation", 1)),
        )
    except (json.JSONDecodeError, TypeError, ValueError, KeyError, AttributeError):
        return None


def find_stale(
    manifest: SyncManifest | None,
    current_paths: Sequence[str],
    exclude_patterns: Sequence[str],
) -> StaleReport:
    """Identify paths srunx uploaded that are no longer present locally.

    Returns a report whose ``known`` is False when no conclusion can be drawn.
    That is a distinct answer from "nothing is stale", and the two must not be
    collapsed: presenting "unknown" as "clean" is how a stale module keeps
    running unnoticed.
    """
    if manifest is None:
        return StaleReport(
            known=False,
            reason=(
                "no manifest recorded for this mount yet — sync once with this "
                "version of srunx to start tracking what it uploads"
            ),
        )

    current_fingerprint = exclude_fingerprint(exclude_patterns)
    if current_fingerprint != manifest.exclude_fingerprint:
        # Excluded files are never uploaded, so a changed filter moves paths in
        # and out of the manifest for reasons unrelated to staleness. Reporting
        # them anyway would invite deleting files that are merely newly
        # excluded.
        return StaleReport(
            known=False,
            reason=(
                "the exclude patterns changed since the manifest was written, "
                "so paths may be absent for that reason rather than being "
                "stale — sync once to re-record under the current filter"
            ),
        )

    stale = sorted(manifest.paths - set(current_paths))
    return StaleReport(paths=stale)
