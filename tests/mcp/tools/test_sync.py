"""Tests for srunx.mcp.tools.sync.

Both tools take ``transport`` (an SSH profile name — there is no local-to-local
sync and no implicit current-profile fallback) plus ``mount``, which is the
*only* way to name what is synced: the former free-form ``local_path`` /
``remote_path`` pair was removed so an agent cannot push an arbitrary source to
an arbitrary destination.

Properties covered: deletion is opt-in and capped, itemize is always requested
so the response can report exactly what moved, the per-mount sync lock is held
across a transfer — and ``inspect_mount`` reports cluster-only files while
holding no lock and changing nothing.
"""

import inspect
from unittest.mock import MagicMock, patch

from srunx.mcp.tools.sync import (
    DEFAULT_MAX_DELETE,
    _parse_itemized,
    inspect_mount,
    sync_files,
)


def _profile_with_mount(
    name: str = "ml",
    local: str = "/local/ml",
    remote: str = "/remote/ml",
    exclude_patterns: list[str] | None = None,
) -> MagicMock:
    """Build a profile mock exposing a single named mount."""
    mount = MagicMock()
    mount.name = name
    mount.local = local
    mount.remote = remote
    mount.exclude_patterns = exclude_patterns if exclude_patterns is not None else []

    profile = MagicMock()
    profile.mounts = [mount]
    return profile


def _rsync_returning(
    stdout: str = "",
    returncode: int = 0,
    stderr: str = "",
    exclude_patterns: list[str] | None = None,
) -> MagicMock:
    rsync = MagicMock()
    rsync.push.return_value = MagicMock(
        success=returncode == 0,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )
    rsync.exclude_patterns = (
        exclude_patterns if exclude_patterns is not None else [".git/"]
    )
    return rsync


class TestParseItemized:
    """rsync ``-i`` output → (deleted paths, deletion count, transfer count).

    The flag block's width differs between rsync builds, so both widths are
    covered with output captured from real binaries. The count is returned
    separately from the path list so the list can be bounded while the count
    stays exact.
    """

    def test_max_paths_bounds_the_list_but_not_the_count(self):
        """An uncapped preview must not materialise every deletable path.

        ``sync_files`` reports a number and, above its reporting limit, no
        list at all — so retaining every path would allocate strings purely
        to throw them away.
        """
        stdout = "".join(f"*deleting f{i}.pt\n" for i in range(500))
        deleted, deleted_count, _ = _parse_itemized(stdout, max_paths=10)
        assert deleted_count == 500  # exact
        assert len(deleted) == 10  # bounded
        assert deleted[0] == "f0.pt"

    def test_max_paths_zero_collects_nothing(self):
        """What the mirror preflight uses: a count, with no path retained."""
        stdout = "".join(f"*deleting f{i}.pt\n" for i in range(1000))
        deleted, deleted_count, _ = _parse_itemized(stdout, max_paths=0)
        assert deleted_count == 1000
        assert deleted == []

    def test_removed_directory_counts_its_own_entry(self):
        """Verbatim openrsync output for deleting a dir holding two files.

        Two *files* produce four deletion lines, because each removed
        directory gets its own. That is rsync's unit and the unit
        ``--max-delete`` caps, which is why the response says
        ``entries_deleted`` rather than a file count.
        """
        stdout = (
            "*deleting old/sub/f2.txt\n"
            "*deleting old/sub/\n"
            "*deleting old/f1.txt\n"
            "*deleting old/\n"
        )
        deleted, deleted_count, transferred = _parse_itemized(stdout)
        assert deleted_count == 4
        assert len(deleted) == 4
        assert transferred == 0

    def test_whitespace_only_transfer_name_is_counted(self):
        """A file named only of spaces still moved data.

        ``split(None, 1)`` yields just the flag token here, so requiring a
        non-blank filename would drop the transfer from the count and break
        the exact-count contract.
        """
        deleted, _, transferred = _parse_itemized(">f+++++++    \n")
        assert deleted == []
        assert transferred == 1

    def test_gnu_rsync_11_char_flags(self):
        """GNU rsync 3.x: ``YXcstpoguax`` — 11 characters."""
        stdout = (
            "sending incremental file list\n"
            "*deleting   old/checkpoint.pt\n"
            "*deleting   old/\n"
            ">f+++++++++ data/new.csv\n"
            ">f.st...... train.py\n"
            "\n"
        )
        deleted, deleted_count, transferred = _parse_itemized(stdout)
        assert deleted == ["old/checkpoint.pt", "old/"]
        assert transferred == 2

    def test_openrsync_9_char_flags(self):
        """openrsync / rsync 2.6.9 (stock on macOS): ``YXcstpogz`` — 9 chars.

        Verbatim output from ``rsync -az -n -i --delete``. A fixed 11-offset
        parser matches none of these and reports zero transfers.
        """
        stdout = (
            "*deleting gone3.txt\n"
            "*deleting gone2.txt\n"
            "*deleting gone1.txt\n"
            ">f+++++++ a.txt\n"
        )
        deleted, deleted_count, transferred = _parse_itemized(stdout)
        assert deleted == ["gone3.txt", "gone2.txt", "gone1.txt"]
        assert transferred == 1

    def test_directory_entries_are_not_counted_as_files(self):
        """``cd+++++++++`` creates a directory — no file data moves."""
        deleted, deleted_count, transferred = _parse_itemized(
            "cd+++++++++ data/\n>f+++++++++ data/a.csv\n"
        )
        assert deleted == []
        assert transferred == 1

    def test_attribute_only_changes_are_not_transfers(self):
        """``.f...p.....`` fixed up permissions; contents already matched."""
        deleted, deleted_count, transferred = _parse_itemized(
            ".f...p..... train.py\n.d..t...... data/\n>f+++++++++ new.csv\n"
        )
        assert deleted == []
        assert transferred == 1

    def test_chatter_is_ignored(self):
        """Stats/summary lines must not be miscounted as transfers."""
        stdout = (
            "sending incremental file list\n"
            "\n"
            "sent 1,234 bytes  received 56 bytes  2,580.00 bytes/sec\n"
            "total size is 7,890  speedup is 6.11\n"
            "Deletions stopped due to --max-delete limit (2 skipped)\n"
        )
        assert _parse_itemized(stdout) == ([], 0, 0)

    def test_created_directory_chatter_is_not_a_transfer(self):
        """GNU rsync prints this when it builds the destination (--mkpath).

        ``created`` starts with ``c``, a real update type, so a parser that
        only checks the first character counts one phantom file on every
        first sync. The item-type check is what rejects it.
        """
        stdout = "created directory /remote/ml\n>f+++++++++ a.txt\n"
        deleted, deleted_count, transferred = _parse_itemized(stdout)
        assert deleted == []
        assert transferred == 1

    def test_only_transferred_regular_files_are_counted(self):
        """Symlinks, hard links and local creations move no file data.

        rsync marks them ``cL`` / ``hf`` / ``c*`` — ``c`` being a *local*
        creation and ``h`` a link — and omits them from its own "regular
        files transferred" statistic. Counting them would overstate
        ``files_transferred``: here only ``plain.txt`` was transferred.
        """
        stdout = (
            "cL+++++++++ link -> target\n"
            "hf+++++++++ hardlink\n"
            "cD+++++++++ dev\n"
            ">f+++++++++ plain.txt\n"
        )
        assert _parse_itemized(stdout) == ([], 0, 1)

    def test_sent_direction_is_counted(self):
        """``<f`` (sent) counts the same as ``>f`` (received)."""
        assert _parse_itemized("<f+++++++++ a.txt\n>f+++++++++ b.txt\n") == ([], 0, 2)

    def test_paths_with_spaces(self):
        deleted, deleted_count, transferred = _parse_itemized(
            ">f+++++++ my data/file one.csv\n*deleting old dir/file two.pt\n"
        )
        assert deleted == ["old dir/file two.pt"]
        assert transferred == 1

    def test_trailing_whitespace_in_name_is_preserved(self):
        """Stripping the line would corrupt a name that ends in a space."""
        deleted, deleted_count, _ = _parse_itemized("*deleting trailing space .pt \n")
        assert deleted == ["trailing space .pt "]

    def test_whitespace_only_name_is_still_counted(self):
        """Verbatim openrsync output for a destination file named ``'   '``.

        The deletion must be *counted* even if the name is only spaces: a
        missed deletion under-counts the preflight and lets a mirror through
        the cap that should have refused it.
        """
        deleted, deleted_count, _ = _parse_itemized(
            "*deleting trailing space .pt\n*deleting    \n>f+++++++ keep.txt\n"
        )
        assert len(deleted) == 2
        assert deleted[0] == "trailing space .pt"
        assert deleted[1].strip() == ""  # name preserved as whitespace

    def test_crlf_line_endings(self):
        deleted, deleted_count, transferred = _parse_itemized(
            "*deleting a.pt\r\n>f+++++++ b.txt\r\n"
        )
        assert deleted == ["a.pt"]
        assert transferred == 1

    def test_empty_output(self):
        assert _parse_itemized("") == ([], 0, 0)


class TestSyncFilesGuards:
    """Argument / profile / mount validation before any rsync runs."""

    def test_transport_local_rejected(self):
        result = sync_files(transport="local", mount="ml")
        assert result["success"] is False
        assert "SSH profile" in result["error"]

    def test_transport_empty_rejected(self):
        result = sync_files(transport="   ", mount="ml")
        assert result["success"] is False
        assert "SSH profile" in result["error"]

    def test_only_registered_mounts_can_be_named(self):
        """No free-form source/destination: ``mount`` is required and the
        former ``local_path`` / ``remote_path`` pair is gone, so an agent
        cannot push an arbitrary tree to an arbitrary remote path."""
        params = inspect.signature(sync_files).parameters
        assert "local_path" not in params
        assert "remote_path" not in params
        assert params["mount"].default is inspect.Parameter.empty

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_negative_max_delete_rejected(self, mock_cm_cls):
        mock_cm_cls.return_value = MagicMock()
        result = sync_files(transport="prod", mount="ml", max_delete=-1)
        assert result["success"] is False
        assert "max_delete" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_zero_max_delete_rejected_with_guidance(self, mock_cm_cls):
        """0 is ambiguous: rsync 2.6.x reads --max-delete=0 as *unlimited*.

        Rather than guess, point the caller at ``delete=False``.
        """
        mock_cm_cls.return_value = MagicMock()
        result = sync_files(transport="prod", mount="ml", max_delete=0)
        assert result["success"] is False
        assert "max_delete must be >= 1" in result["error"]
        assert "delete=False" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_unknown_profile(self, mock_cm_cls):
        cm = MagicMock()
        cm.get_profile.return_value = None
        mock_cm_cls.return_value = cm

        result = sync_files(transport="missing", mount="ml")
        assert result["success"] is False
        assert "missing" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_unknown_mount_name_lists_available(self, mock_cm_cls):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount(name="data")
        mock_cm_cls.return_value = cm

        result = sync_files(transport="prod", mount="nope")
        assert result["success"] is False
        assert "nope" in result["error"]
        assert "data" in result["error"]  # available mounts surfaced

    def test_catches_exception(self):
        with patch(
            "srunx.ssh.core.config.ConfigManager",
            side_effect=RuntimeError("config broken"),
        ):
            result = sync_files(transport="prod", mount="ml")
            assert result["success"] is False
            assert "config broken" in result["error"]


@patch("srunx.sync.mount_helpers.build_rsync_client")
@patch("srunx.ssh.core.config.ConfigManager")
class TestSyncFilesTransfer:
    """The rsync invocation and the reported result.

    The per-mount lock is exercised for real (pytest's autouse XDG isolation
    puts the lock file under a temp config dir), so these also prove the
    lock is acquirable and released rather than mocked away.
    """

    def test_delete_is_off_by_default(self, mock_cm_cls, mock_build):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount(exclude_patterns=["*.log"])
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning(">f+++++++++ train.py\n")
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml")

        assert result["success"] is True
        assert result["delete"] is False
        assert result["files_transferred"] == 1
        assert result["entries_deleted"] == 0
        assert result["deleted_paths"] == []

        rsync.push.assert_called_once_with(
            "/local/ml",
            "/remote/ml",
            delete=False,
            dry_run=False,
            itemize=True,
            # No cap is passed when nothing can be deleted anyway.
            max_delete=None,
            exclude_patterns=["*.log"],
        )

    def test_delete_opt_in_preflights_then_pushes(self, mock_cm_cls, mock_build):
        """A mirror counts deletions in a dry run before touching the remote."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning("*deleting   stale.pt\n>f+++++++++ train.py\n")
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", delete=True)

        assert result["success"] is True
        assert result["delete"] is True
        assert result["entries_deleted"] == 1
        assert result["deleted_paths"] == ["stale.pt"]
        assert result["files_transferred"] == 1

        assert rsync.push.call_count == 2
        preflight = rsync.push.call_args_list[0].kwargs
        assert preflight["delete"] is True
        assert preflight["dry_run"] is True  # counts only, changes nothing
        assert preflight["itemize"] is True
        # The preflight is capped as well, so it cannot enumerate (and buffer)
        # unbounded deletions before we get the chance to refuse them.
        assert preflight["max_delete"] == DEFAULT_MAX_DELETE

        real = rsync.push.call_args_list[1].kwargs
        assert real["delete"] is True
        assert real["dry_run"] is False
        assert real["max_delete"] == DEFAULT_MAX_DELETE  # backstop behind preflight

    def test_explicit_max_delete_forwarded(self, mock_cm_cls, mock_build):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning()

        sync_files(transport="prod", mount="ml", delete=True, max_delete=3)

        assert mock_build.return_value.push.call_args.kwargs["max_delete"] == 3

    def test_dry_run_still_itemizes_and_reports(self, mock_cm_cls, mock_build):
        """A preview is only useful if it enumerates the deletions."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning("*deleting   a.pt\n*deleting   b.pt\n")
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", dry_run=True, delete=True)

        assert result["success"] is True
        assert result["dry_run"] is True
        assert result["entries_deleted"] == 2
        assert result["deleted_paths"] == ["a.pt", "b.pt"]
        # One pass only: a preview needs no preflight, since it is one.
        rsync.push.assert_called_once()
        kwargs = rsync.push.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["itemize"] is True
        # Capping a preview would replace the requested list with an error.
        assert kwargs["max_delete"] is None

    def test_deleted_paths_are_not_truncated(self, mock_cm_cls, mock_build):
        """Every deletion is reported — a clipped list can't be reviewed."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        stdout = "".join(f"*deleting   ckpt/{i:04d}.pt\n" for i in range(80))
        mock_build.return_value = _rsync_returning(stdout)

        result = sync_files(transport="prod", mount="ml", delete=True)

        assert result["entries_deleted"] == 80
        assert len(result["deleted_paths"]) == 80
        assert result["deleted_paths"][-1] == "ckpt/0079.pt"

    def test_preflight_refuses_before_touching_remote(self, mock_cm_cls, mock_build):
        """Over the cap → refuse, and never run the destructive push.

        This is why the preflight exists: rsync's own ``--max-delete`` deletes
        up to the cap and transfers before exiting 25, so a refusal based on it
        alone could not honestly claim the remote is untouched.
        """
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        stdout = "".join(f"*deleting ckpt/{i}.pt\n" for i in range(9))
        rsync = _rsync_returning(stdout)
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", delete=True, max_delete=5)

        assert result["success"] is False
        assert "9 entries" in result["error"]
        assert "max_delete cap of 5" in result["error"]
        assert "Nothing was changed" in result["error"]
        assert "dry_run=True" in result["error"]
        # Exactly one call — the preflight. The real mirror never ran.
        rsync.push.assert_called_once()
        assert rsync.push.call_args.kwargs["dry_run"] is True

    def test_preflight_exit_25_refuses_without_enumerating(
        self, mock_cm_cls, mock_build
    ):
        """rsync stops counting at the cap and exits 25 — that IS the refusal.

        This is the path that keeps memory bounded: the preflight never has to
        buffer every deletable path just to decide it is too many.
        """
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning(returncode=25, stderr="del limit")
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", delete=True, max_delete=5)

        assert result["success"] is False
        assert "more than 5 entries" in result["error"]
        assert "Nothing was changed" in result["error"]
        # Only the preflight ran; the destructive push never started.
        rsync.push.assert_called_once()
        assert rsync.push.call_args.kwargs["dry_run"] is True

    def test_preflight_failure_is_reported(self, mock_cm_cls, mock_build):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning(returncode=23, stderr="permission denied")
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", delete=True)

        assert result["success"] is False
        assert "preflight failed" in result["error"]
        assert "permission denied" in result["error"]
        assert "Nothing was changed" in result["error"]
        rsync.push.assert_called_once()

    def test_cap_hit_after_preflight_does_not_claim_no_change(
        self, mock_cm_cls, mock_build
    ):
        """Remote changed between preflight and push → say so honestly.

        rsync exit 25 on the real push means deletions and transfers may
        already have happened, so this must NOT report "nothing changed".
        """
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = MagicMock()
        rsync.push.side_effect = [
            # Preflight: within the cap, so the mirror proceeds.
            MagicMock(success=True, returncode=0, stdout="*deleting a.pt\n", stderr=""),
            # Real push: the cap trips anyway.
            MagicMock(success=False, returncode=25, stdout="", stderr="del limit"),
        ]
        mock_build.return_value = rsync

        result = sync_files(transport="prod", mount="ml", delete=True, max_delete=5)

        assert result["success"] is False
        assert "Nothing was changed" not in result["error"]
        assert "may already have been deleted" in result["error"]
        assert rsync.push.call_count == 2

    def test_huge_deletion_list_is_omitted_not_clipped(self, mock_cm_cls, mock_build):
        """Past the reporting limit: exact count, explicit omission, no clip."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        stdout = "".join(f"*deleting ckpt/{i}.pt\n" for i in range(1500))
        mock_build.return_value = _rsync_returning(stdout)

        result = sync_files(transport="prod", mount="ml", dry_run=True, delete=True)

        assert result["success"] is True
        assert result["entries_deleted"] == 1500
        assert result["deleted_paths"] == []
        assert result["deleted_paths_omitted"] is True

    def test_generic_rsync_failure_surfaces_stderr(self, mock_cm_cls, mock_build):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning(
            returncode=23, stderr="permission denied"
        )

        result = sync_files(transport="prod", mount="ml")
        assert result["success"] is False
        assert "exit 23" in result["error"]
        assert "permission denied" in result["error"]

    def test_lock_timeout_is_reported(self, mock_cm_cls, mock_build):
        """A mount already being synced elsewhere fails cleanly, no rsync."""
        from srunx.sync.lock import SyncLockTimeoutError

        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning()
        mock_build.return_value = rsync

        from pathlib import Path

        with patch(
            "srunx.sync.lock.acquire_sync_lock",
            side_effect=SyncLockTimeoutError(Path("/tmp/ml.lock"), 120.0),
        ):
            result = sync_files(transport="prod", mount="ml")

        assert result["success"] is False
        assert "sync lock" in result["error"]
        rsync.push.assert_not_called()


@patch("srunx.sync.mount_helpers.build_rsync_client")
@patch("srunx.ssh.core.config.ConfigManager")
class TestInspectMount:
    """The read-only counterpart to ``sync_files``.

    It exists because an additive sync leaves cluster-only files in place, and
    ``sync_files`` cannot report them: with ``delete=False`` rsync is never asked
    about deletions, so its result says nothing about what is stale. Asking via
    ``delete=True`` is a preview and harmless, but the ``delete`` argument has to
    be documented as destructive — so an agent reading that avoids it, and the
    safe inspection with it. Hence a separate tool.
    """

    def test_is_read_only(self, mock_cm_cls, mock_build):
        """A dry run with --delete: reports deletions, performs none."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount(exclude_patterns=["*.log"])
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning("*deleting stale.py\n")

        result = inspect_mount(transport="prod", mount="ml")

        assert result["success"] is True
        kwargs = mock_build.return_value.push.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["delete"] is True  # asks about deletions...
        assert kwargs["itemize"] is True
        # ...and passes no cap: a cap bounds a real mirror's damage, and capping
        # an inspection would replace the requested list with an error.
        assert "max_delete" not in kwargs or kwargs["max_delete"] is None
        assert kwargs["exclude_patterns"] == ["*.log"]

    def test_reports_cluster_only_paths(self, mock_cm_cls, mock_build):
        """The answer to "what did I delete locally that is still up there?"."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning(
            "*deleting old_train.py\n*deleting ckpt/500.pt\n>f+++++++ train.py\n"
        )

        result = inspect_mount(transport="prod", mount="ml")

        assert result["mirror_delete_candidates"] == 2
        assert result["mirror_delete_candidate_paths"] == [
            "old_train.py",
            "ckpt/500.pt",
        ]
        assert result["mirror_delete_candidate_paths_omitted"] is False
        assert result["files_would_transfer"] == 1

    def test_reports_effective_excludes(self, mock_cm_cls, mock_build):
        """Excluded paths are invisible here *and* safe from a mirror.

        Without the list, an absent candidate is ambiguous: in sync, or merely
        excluded?
        """
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning()
        rsync.effective_excludes.return_value = [".git/", "*.pyc", "data/"]
        mock_build.return_value = rsync

        result = inspect_mount(transport="prod", mount="ml")
        assert result["effective_exclude_patterns"] == [".git/", "*.pyc", "data/"]

    def test_effective_excludes_include_the_mount_patterns(
        self, mock_cm_cls, mock_build
    ):
        """The merged view, not the client attribute.

        Per-call patterns are applied for the invocation without being stored,
        so ``rsync.exclude_patterns`` omits exactly the mount-level patterns the
        user configured — the ones most likely to explain a missing candidate.
        Reporting those would make an excluded path read as "in sync".
        """
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount(
            exclude_patterns=["data/raw/", "*.h5"]
        )
        mock_cm_cls.return_value = cm
        rsync = _rsync_returning()
        mock_build.return_value = rsync

        inspect_mount(transport="prod", mount="ml")

        rsync.effective_excludes.assert_called_once_with(["data/raw/", "*.h5"])

    def test_takes_no_lock(self, mock_cm_cls, mock_build):
        """Reading should not queue behind a running sync, or time out on one."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning()

        with patch("srunx.sync.lock.acquire_sync_lock") as mock_lock:
            result = inspect_mount(transport="prod", mount="ml")

        assert result["success"] is True
        mock_lock.assert_not_called()

    def test_large_candidate_list_is_omitted_not_shortened(
        self, mock_cm_cls, mock_build
    ):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        stdout = "".join(f"*deleting f{i}.pt\n" for i in range(50))
        mock_build.return_value = _rsync_returning(stdout)

        result = inspect_mount(transport="prod", mount="ml", max_paths=10)

        assert result["mirror_delete_candidates"] == 50  # exact
        assert result["mirror_delete_candidate_paths"] == []
        assert result["mirror_delete_candidate_paths_omitted"] is True

    def test_negative_max_paths_rejected(self, mock_cm_cls, mock_build):
        mock_cm_cls.return_value = MagicMock()
        result = inspect_mount(transport="prod", mount="ml", max_paths=-1)
        assert result["success"] is False
        assert "max_paths" in result["error"]

    def test_missing_destination_explains_itself(self, mock_cm_cls, mock_build):
        """rsync cannot diff against a destination that does not exist yet."""
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount()
        mock_cm_cls.return_value = cm
        mock_build.return_value = _rsync_returning(
            returncode=23, stderr="No such file or directory"
        )

        result = inspect_mount(transport="prod", mount="ml")

        assert result["success"] is False
        assert "nothing to inspect" in result["error"]
        assert "delete=False" in result["error"]

    def test_rejects_local_transport(self, mock_cm_cls, mock_build):
        result = inspect_mount(transport="local", mount="ml")
        assert result["success"] is False
        assert "SSH profile" in result["error"]

    def test_unknown_mount_lists_available(self, mock_cm_cls, mock_build):
        cm = MagicMock()
        cm.get_profile.return_value = _profile_with_mount(name="data")
        mock_cm_cls.return_value = cm

        result = inspect_mount(transport="prod", mount="nope")
        assert result["success"] is False
        assert "nope" in result["error"]
        assert "data" in result["error"]
