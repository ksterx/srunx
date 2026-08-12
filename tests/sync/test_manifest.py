"""Tests for srunx.sync.manifest.

The manifest exists because listing remote-only files is not actionable on its
own: that set mixes job output (must not be deleted) with locally deleted files
(usually should be). Excluding output directories separates them only as well as
the exclude list is maintained — in a real mount, four stale scripts sat among 39
job artifacts because ``dist/`` had never been excluded.

Recording what srunx uploaded removes the guesswork, so the properties pinned
here are: a job artifact never appears (it was never uploaded), and "cannot
tell" is never reported as "clean".
"""

from __future__ import annotations

import json
import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from srunx.sync import manifest as M
from srunx.sync.rsync import RsyncClient, unescape_rsync_path


def _mount(local="/local/proj", remote="/remote/proj", excludes=None):
    return SimpleNamespace(
        name="proj",
        local=local,
        remote=remote,
        exclude_patterns=excludes if excludes is not None else [],
    )


def _client_with_remote(files: dict[str, str] | None = None) -> MagicMock:
    """A client whose remote side is a dict of path -> contents."""
    store = files if files is not None else {}
    client = MagicMock()
    client.read_remote_file.side_effect = lambda p, **kw: store.get(p)
    client.write_remote_file.side_effect = lambda p, c, **kw: store.__setitem__(p, c)
    client.effective_excludes.side_effect = lambda extra=None: [".git/", *(extra or [])]
    client._store = store
    return client


class TestFingerprint:
    def test_order_matters(self):
        """rsync applies patterns in order, so two orderings can filter
        differently. Treating them as equal would let a changed filter pass
        unnoticed and paths would be called stale for the wrong reason."""
        assert M.exclude_fingerprint(["a", "b"]) != M.exclude_fingerprint(["b", "a"])

    def test_no_two_pattern_lists_hash_alike(self):
        """Joining on a separator made ``["a\\nb"]`` and ``["a", "b"]`` identical.

        Those filter differently, and a filter change that hashes the same is a
        change this cannot notice — which is the one thing the fingerprint is
        for. Newly excluded paths would then be presented as stale rather than
        reported unknown.
        """
        assert M.exclude_fingerprint(["a\nb"]) != M.exclude_fingerprint(["a", "b"])

    def test_same_list_same_fingerprint(self):
        assert M.exclude_fingerprint(["a", "b"]) == M.exclude_fingerprint(["a", "b"])


class TestSerialisation:
    def test_round_trip(self):
        m = M.SyncManifest(paths=frozenset({"b.py", "a.py"}), exclude_fingerprint="fp")
        assert M.SyncManifest.from_json(m.to_json()).paths == m.paths

    def test_paths_are_sorted_on_disk(self):
        """An unchanged tree should produce an unchanged file."""
        m = M.SyncManifest(paths=frozenset({"c", "a", "b"}), exclude_fingerprint="fp")
        assert json.loads(m.to_json())["paths"] == ["a", "b", "c"]

    def test_unparseable_raises_rather_than_returning_empty(self):
        """An empty manifest would read as "nothing was ever uploaded", which
        in turn reads as "nothing is stale" — the opposite of unknown."""
        with pytest.raises(M.ManifestUnavailable):
            M.SyncManifest.from_json("{not json")

    def test_unknown_schema_version_raises(self):
        """A newer writer may record fields this version cannot interpret;
        guessing risks reporting files it knew were not stale."""
        raw = json.dumps({"schema_version": 99, "paths": [], "exclude_fingerprint": ""})
        with pytest.raises(M.ManifestUnavailable, match="schema version"):
            M.SyncManifest.from_json(raw)

    def test_missing_fields_raise(self):
        raw = json.dumps({"schema_version": M.SCHEMA_VERSION, "paths": ["a"]})
        with pytest.raises(M.ManifestUnavailable):
            M.SyncManifest.from_json(raw)


class TestReadWrite:
    def test_absent_manifest_reads_as_none(self):
        """Distinct from unreadable: never-recorded is a normal first run."""
        assert M.read(_client_with_remote(), _mount()) is None

    def test_write_then_read(self):
        client = _client_with_remote()
        mount = _mount()
        M.write(client, mount, ["a.py", "sub/b.py"], [".git/"])

        got = M.read(client, mount)
        assert got is not None
        assert got.paths == frozenset({"a.py", "sub/b.py"})
        assert got.generation == 1

    def test_generation_increments(self):
        client = _client_with_remote()
        mount = _mount()
        first = M.write(client, mount, ["a.py"], [".git/"])
        second = M.write(client, mount, ["a.py"], [".git/"], previous=first)
        assert (first.generation, second.generation) == (1, 2)

    def test_lives_beside_the_owner_marker_but_separately(self):
        """Same root, different file: the marker is fail-open by design and the
        manifest must fail closed, so their lifecycles must not be shared."""
        assert M.manifest_remote_path(_mount()) == "/remote/proj/.srunx-manifest.json"

    def test_is_a_root_file_not_a_directory(self):
        """A directory would need creating first, and ``mkdir -p`` follows an
        existing symlink — so a peer able to write the mount root could redirect
        the manifest write into any directory they can reach."""
        path = M.manifest_remote_path(_mount())
        assert path.count("/") == "/remote/proj/x".count("/")

    def test_write_needs_no_directory_creation(self):
        client = _client_with_remote()
        M.write(client, _mount(), ["a.py"], [".git/"])
        client.ensure_remote_dir.assert_not_called()


class TestAdditiveRetention:
    """An additive sync must not forget what it previously uploaded.

    Rsync leaves a locally deleted file on the cluster, so that file is exactly
    what should be reported. Replacing the record with the current inventory
    makes the sync that *creates* a stale file also erase the evidence of it,
    and the next inspection reports nothing — the feature silently does nothing.
    """

    def test_additive_sync_keeps_previously_uploaded_paths(self):
        client = _client_with_remote()
        mount = _mount()
        first = M.write(client, mount, ["train.py", "tools/probe.py"], [".git/"])

        # probe.py deleted locally; an ordinary sync follows.
        second = M.write(
            client, mount, ["train.py"], [".git/"], previous=first, mirrored=False
        )

        assert second.paths == frozenset({"train.py", "tools/probe.py"})

    def test_mirror_replaces_the_record(self):
        """A mirror removed the remote-only files, so keeping them would report
        as stale exactly what was just deleted."""
        client = _client_with_remote()
        mount = _mount()
        first = M.write(client, mount, ["train.py", "tools/probe.py"], [".git/"])

        second = M.write(
            client, mount, ["train.py"], [".git/"], previous=first, mirrored=True
        )

        assert second.paths == frozenset({"train.py"})

    def test_detection_survives_a_resync(self):
        """End to end: delete locally, sync again, still detected."""
        client = _client_with_remote()
        mount = _mount()
        first = M.write(client, mount, ["train.py", "tools/probe.py"], [".git/"])
        M.write(client, mount, ["train.py"], [".git/"], previous=first)

        report = M.find_stale(M.read(client, mount), ["train.py"], [".git/"])
        assert report.paths == ["tools/probe.py"]


class TestFindStale:
    def test_identifies_uploaded_then_locally_deleted(self):
        recorded = M.SyncManifest(
            paths=frozenset({"train.py", "tools/probe.py"}),
            exclude_fingerprint=M.exclude_fingerprint([".git/"]),
        )
        report = M.find_stale(recorded, ["train.py"], [".git/"])

        assert report.known is True
        assert report.paths == ["tools/probe.py"]

    def test_job_output_never_appears(self):
        """The whole point: artifacts were never uploaded, so they cannot be in
        the record — and this holds with no exclude pattern for them at all."""
        recorded = M.SyncManifest(
            paths=frozenset({"train.py"}),
            exclude_fingerprint=M.exclude_fingerprint([]),
        )
        report = M.find_stale(recorded, ["train.py"], [])

        assert report.known is True
        assert report.paths == []

    def test_no_manifest_is_unknown_not_clean(self):
        report = M.find_stale(None, ["a.py"], [])
        assert report.known is False
        assert report.count == 0
        assert "no manifest" in report.reason

    def test_changed_excludes_make_it_unknown(self):
        """Excluded files are never uploaded, so a changed filter moves paths
        out of the manifest for reasons unrelated to staleness. Reporting them
        would invite deleting files that are merely newly excluded."""
        recorded = M.SyncManifest(
            paths=frozenset({"data/big.bin", "train.py"}),
            exclude_fingerprint=M.exclude_fingerprint([".git/"]),
        )
        report = M.find_stale(recorded, ["train.py"], [".git/", "data/"])

        assert report.known is False
        assert "exclude patterns changed" in report.reason
        assert report.paths == []  # not presented as stale

    def test_result_is_sorted(self):
        recorded = M.SyncManifest(
            paths=frozenset({"c.py", "a.py", "b.py"}),
            exclude_fingerprint=M.exclude_fingerprint([]),
        )
        assert M.find_stale(recorded, [], []).paths == ["a.py", "b.py", "c.py"]


class TestListLocalFiles:
    """Inventory parsing, against real itemize output.

    The inventory is an itemize run against a throwaway empty directory, not
    ``--list-only``: that listing prints a newline inside a filename literally,
    so a crafted name yields paths that do not exist. Itemize escapes it.
    """

    def _client(self) -> RsyncClient:
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            return RsyncClient(hostname="h", username="u")

    def test_parses_real_output(self):
        """Verbatim ``rsync -a -n -i`` output against an empty destination."""
        stdout = "cd+++++++ ./\n>f+++++++ a.py\ncd+++++++ sub/\n>f+++++++ sub/b.py\n"
        assert self._client()._parse_inventory(stdout) == ["a.py", "sub/b.py"]

    def test_keeps_zero_byte_files(self):
        """No size column to mis-parse: ``__init__.py`` is an ordinary entry.

        It is present in essentially every Python package, and the previous
        listing format dropped it — so it could never be reported as stale.
        """
        stdout = ">f+++++++ pkg/__init__.py\n>f+++++++ pkg/mod.py\n"
        assert self._client()._parse_inventory(stdout) == [
            "pkg/__init__.py",
            "pkg/mod.py",
        ]

    def test_parses_gnu_style_flag_width(self):
        """GNU rsync uses an 11-char flag block, openrsync 9. Matching the
        block as a space-free run handles both."""
        stdout = ">f+++++++++ with space/a b.py\n"
        assert self._client()._parse_inventory(stdout) == ["with space/a b.py"]

    def test_keeps_paths_containing_spaces(self):
        stdout = ">f....... with space/c.py\n"
        assert self._client()._parse_inventory(stdout) == ["with space/c.py"]

    def test_skips_directories(self):
        """A directory lingering because it holds job output is not itself a
        stale upload."""
        stdout = "cd+++++++ sub/\n>f+++++++ a.py\n"
        assert self._client()._parse_inventory(stdout) == ["a.py"]

    def test_tracks_symlinks_by_name(self):
        """``rsync -a`` uploads links too, so one deleted locally lingers on the
        cluster exactly like a file and has to be trackable. Only the name is
        the path — the ``-> target`` half is not."""
        stdout = "cL+++++++ latest.py -> train.py\n>f+++++++ train.py\n"
        assert self._client()._parse_inventory(stdout) == ["latest.py", "train.py"]

    def test_failure_raises_rather_than_returning_partial(self):
        """A partial listing recorded as complete would mark the missing files
        as never-uploaded, and they would never be reported as stale."""
        client = self._client()
        with patch("srunx.sync.rsync.subprocess.run") as run:
            run.return_value = MagicMock(returncode=23, stdout="", stderr="denied")
            with pytest.raises(RuntimeError, match="inventory failed"):
                client.list_local_files("/local/proj")

    def test_does_not_pass_protect_args(self):
        """It guards a *remote* shell re-splitting arguments; this listing is
        local, and openrsync does not have the flag at all."""
        client = self._client()
        with patch("srunx.sync.rsync.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.list_local_files("/local/proj")

        assert "--protect-args" not in run.call_args[0][0]

    def test_inventory_destination_is_a_throwaway_empty_directory(self):
        """Against a non-empty destination rsync omits files already up to
        date, which would silently under-record. It must also be a dry run."""
        client = self._client()
        with patch("srunx.sync.rsync.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.list_local_files("/local/proj")

        cmd = run.call_args[0][0]
        assert "-n" in cmd and "-i" in cmd
        dest = cmd[-1]
        assert "srunx-inventory-" in dest
        # Removed once the scan is done, so nothing accumulates.
        assert not pathlib.Path(dest).exists()


class TestFailClosedLifecycle:
    """Every path that cannot produce a trustworthy record must end in unknown.

    Reporting "known, nothing stale" when tracking is broken is the failure mode
    that matters: it looks identical to a clean tree, so nobody investigates.
    """

    def test_corrupt_manifest_is_not_rebuilt_by_an_additive_sync(self):
        """The current inventory is not a valid baseline for an additive run.

        Rsync left everything previously uploaded on the cluster, including
        files since deleted locally; writing "this is everything" would claim
        those do not exist. Only a mirror actually makes the remote match.
        """
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote({"/remote/proj/.srunx-manifest.json": "{bad"})
        record_upload(client, _mount(), mirrored=False, before=["train.py"])

        # Nothing written: the corrupt file is left, so reads keep failing and
        # detection keeps reporting unknown.
        assert client._store["/remote/proj/.srunx-manifest.json"] == "{bad"

    def test_an_additive_sync_recovers_from_the_preserved_baseline(self):
        """Invalidation is otherwise a dead end.

        An additive sync will not rebuild from scratch, so without a preserved
        baseline detection stays unknown until someone runs a mirror — and a
        mirror deletes exactly the cluster-only job output this protects.
        """
        client = _client_with_remote()
        mount = _mount()
        good = M.write(client, mount, ["train.py", "tools/probe.py"], [".git/"])
        M.invalidate(client, mount, "recording failed: disk full", previous=good)

        # While invalidated, detection must still report "cannot tell".
        with pytest.raises(M.ManifestUnavailable):
            M.read(client, mount)

        from srunx.sync.mount_helpers import record_upload

        record_upload(client, mount, mirrored=False, before=["train.py"])

        recovered = M.read(client, mount)
        assert recovered is not None
        # probe.py was deleted locally after the failure; it is still stale.
        assert recovered.paths == frozenset({"train.py", "tools/probe.py"})

    def test_a_second_failure_does_not_erase_the_preserved_baseline(self):
        """Otherwise one more hiccup destroys the only recovery path."""
        client = _client_with_remote()
        mount = _mount()
        good = M.write(client, mount, ["train.py"], [".git/"])
        M.invalidate(client, mount, "first failure", previous=good)
        M.invalidate(client, mount, "second failure")

        assert M.read_superseded(client, mount).paths == frozenset({"train.py"})

    def test_no_baseline_means_no_rebuild(self):
        """A corrupt record has nothing to union onto, and inventing one would
        claim files uploaded earlier were never sent."""
        client = _client_with_remote({"/remote/proj/.srunx-manifest.json": "{bad"})
        assert M.read_superseded(client, _mount()) is None

    def test_manifest_is_written_privately(self):
        """It enumerates a project's file names and the pushing workstation,
        and only its own writer reads it — a mount root is often traversable
        on a shared cluster even when the directories under it are not."""
        client = _client_with_remote()
        M.write(client, _mount(), ["train.py"], [".git/"])
        assert client.write_remote_file.call_args.kwargs["mode"] == "600"

        M.invalidate(client, _mount(), "boom")
        assert client.write_remote_file.call_args.kwargs["mode"] == "600"

    def test_mirror_may_rebuild_a_corrupt_manifest(self):
        """A mirror does make the remote match local, so it can re-baseline."""
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote({"/remote/proj/.srunx-manifest.json": "{bad"})
        record_upload(client, _mount(), mirrored=True, before=["train.py"])

        assert M.read(client, _mount()).paths == frozenset({"train.py"})

    def test_files_appearing_mid_transfer_are_not_recorded(self):
        """The post-transfer scan can see a file rsync never sent.

        A concurrent build writing into the tree after rsync walked its parent
        would otherwise be recorded as uploaded. Delete it locally later and,
        if a job wrote the same relative path on the cluster, that job's output
        gets named as a stale upload.
        """
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote()
        client.list_local_files.return_value = ["train.py", "dist/out.bin"]

        record_upload(client, _mount(), mirrored=False, before=["train.py"])

        assert M.read(client, _mount()).paths == frozenset({"train.py"})

    def test_files_deleted_mid_transfer_are_still_recorded(self):
        """rsync sent them, so they are on the cluster and genuinely stale.

        Dropping them leaves a file no later sync can ever record — it is not
        local any more — while the report still calls itself complete. That
        silent gap is the exact failure this feature exists to close.
        """
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote()
        # Present before the transfer, gone by the time it finished.
        client.list_local_files.return_value = ["train.py"]

        record_upload(
            client, _mount(), mirrored=False, before=["train.py", "tools/probe.py"]
        )

        assert M.read(client, _mount()).paths == frozenset(
            {"train.py", "tools/probe.py"}
        )

    def test_a_failed_pre_scan_publishes_nothing(self):
        """There is no after-scan fallback, and that is deliberate.

        A scan taken after the transfer records files rsync never sent, and if
        a job already holds one of those paths the report ends up naming live
        output for deletion. A record that reads as authoritative must not be
        built that way — an existing one stops being trusted instead.
        """
        from srunx.sync.mount_helpers import record_upload, snapshot_local

        client = _client_with_remote()
        client.list_local_files.side_effect = RuntimeError("scan blew up")
        assert snapshot_local(client, _mount()) is None

        mount = _mount()
        M.write(client, mount, ["train.py"], [".git/"])
        client.list_local_files.side_effect = None
        client.list_local_files.return_value = ["train.py", "dist/out.bin"]

        record_upload(client, mount, mirrored=False, before=None)

        with pytest.raises(M.ManifestUnavailable):
            M.read(client, mount)
        # The scan it refused to trust is nowhere in the record.
        assert "dist/out.bin" not in client._store[M.manifest_remote_path(mount)]

    def test_a_first_sync_failure_leaves_the_manifest_absent(self):
        """Marking it would strand the mount at unknown.

        Absent already reads as "cannot tell", and it is the one state an
        additive sync can still build on — so writing the mark here removes the
        only recovery path without protecting anything.
        """
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote()

        record_upload(client, _mount(), mirrored=False, before=None)

        assert client._store == {}
        assert M.read(client, _mount()) is None

        # And the next successful sync establishes one.
        record_upload(client, _mount(), mirrored=False, before=["train.py"])
        assert M.read(client, _mount()).paths == frozenset({"train.py"})

    def test_a_valid_record_survives_a_transient_read_failure(self):
        """Invalidating must carry the live record's own paths forward, not
        only a previously preserved baseline — otherwise one hiccup erases it."""
        client = _client_with_remote()
        mount = _mount()
        M.write(client, mount, ["train.py"], [".git/"])

        M.invalidate(client, mount, "transient ssh error")

        assert M.read_superseded(client, mount).paths == frozenset({"train.py"})

    def test_an_unreadable_manifest_is_not_treated_as_absent(self):
        """Skipping the mark on a read error leaves an outdated record trusted
        the moment ssh recovers, so files this sync uploaded go unrecorded
        while detection still answers "known"."""
        client = _client_with_remote()
        mount = _mount()
        M.write(client, mount, ["train.py"], [".git/"])

        store = client._store
        client.read_remote_file.side_effect = RuntimeError("ssh went away")
        M.invalidate(client, mount, "recording failed")

        # Written despite the read failing — absence was never proven.
        client.read_remote_file.side_effect = lambda p, **kw: store.get(p)
        with pytest.raises(M.ManifestUnavailable, match="recording failed"):
            M.read(client, mount)

    def test_a_future_schema_baseline_is_not_restored(self):
        """Its path semantics are exactly what this version cannot interpret;
        rewriting a confident record from it is worse than staying unknown."""
        client = _client_with_remote(
            {
                "/remote/proj/.srunx-manifest.json": json.dumps(
                    {
                        "schema_version": M.SCHEMA_VERSION + 1,
                        "invalidated": True,
                        "superseded": {
                            "paths": ["train.py"],
                            "exclude_fingerprint": "fp",
                        },
                    }
                )
            }
        )
        assert M.read_superseded(client, _mount()) is None

    def test_a_baseline_outside_an_invalidation_is_not_restored(self):
        """Only an invalidation puts one there; anything else carrying the key
        is not a record this wrote."""
        client = _client_with_remote(
            {
                "/remote/proj/.srunx-manifest.json": json.dumps(
                    {
                        "schema_version": M.SCHEMA_VERSION,
                        "paths": [],
                        "exclude_fingerprint": "fp",
                        "superseded": {
                            "paths": ["planted.py"],
                            "exclude_fingerprint": "fp",
                        },
                    }
                )
            }
        )
        assert M.read_superseded(client, _mount()) is None

    def test_failed_recording_invalidates_the_previous_manifest(self):
        """Leaving it in place leaves it *trusted* while it no longer describes
        the remote — a file this sync uploaded and later deleted locally would
        be reported as "nothing stale"."""
        from srunx.sync.mount_helpers import record_upload

        client = _client_with_remote()
        mount = _mount()
        M.write(client, mount, ["train.py"], [".git/"])
        client.effective_excludes.side_effect = RuntimeError("filter blew up")

        record_upload(client, mount, mirrored=False, before=["train.py"])

        with pytest.raises(M.ManifestUnavailable, match="invalidated"):
            M.read(client, mount)

    def test_invalidated_manifest_reports_unknown(self):
        """A record that exists must stop being trusted the moment a sync it
        does not describe has landed."""
        client = _client_with_remote()
        mount = _mount()
        M.write(client, mount, ["train.py"], [".git/"])

        M.invalidate(client, mount, "recording failed: disk full")

        with pytest.raises(M.ManifestUnavailable, match="disk full"):
            M.read(client, mount)


class TestExcludeChangeDoesNotCreateFalsePositives:
    def test_changed_filter_replaces_rather_than_merges(self):
        """A file that is still present locally but newly excluded would be kept
        in the record while vanishing from every later inventory — and then
        reported as stale, the exact false positive the fingerprint guards."""
        client = _client_with_remote()
        mount = _mount()
        first = M.write(client, mount, ["train.py", "data/big.bin"], [".git/"])

        # ``data/`` added to the excludes; big.bin is still on disk locally.
        second = M.write(
            client, mount, ["train.py"], [".git/", "data/"], previous=first
        )

        assert second.paths == frozenset({"train.py"})
        report = M.find_stale(second, ["train.py"], [".git/", "data/"])
        assert report.paths == []  # not reported as stale


class TestPathValidation:
    def test_non_string_paths_are_rejected(self):
        """Coercing null/numbers into strings would report them as stale."""
        raw = json.dumps(
            {
                "schema_version": M.SCHEMA_VERSION,
                "paths": ["ok.py", None],
                "exclude_fingerprint": "fp",
            }
        )
        with pytest.raises(M.ManifestUnavailable, match="non-string"):
            M.SyncManifest.from_json(raw)

    def test_non_numeric_generation_is_a_manifest_error(self):
        """It must surface as ManifestUnavailable so the recorder's
        replace-corrupt path can key off it; a bare TypeError escapes and the
        corrupt file is left in place forever."""
        raw = json.dumps(
            {
                "schema_version": M.SCHEMA_VERSION,
                "paths": [],
                "exclude_fingerprint": "fp",
                "generation": None,
            }
        )
        with pytest.raises(M.ManifestUnavailable, match="generation"):
            M.SyncManifest.from_json(raw)


class TestVerificationOrdering:
    """Recording must not outrun the check that the transfer was intact.

    rsync can exit 0 while the remote copy differs — the silent failure the
    hash check exists to catch. A record written first asserts every file
    arrived just as verification is about to prove otherwise. The ownership
    marker already defers for this reason; the record follows it.
    """

    def test_session_defers_recording_until_after_verification(self):
        from srunx.sync import service

        assert "record_manifest=False" in service.__doc__ or True  # doc-free check
        src = __import__("inspect").getsource(service.mount_sync_session)
        # The sync is told not to record...
        assert "record_manifest=False" in src
        # ...and the record is written after the verification call.
        assert src.index("verify_paths_match(") < src.index("record_upload(")


class TestEscapedPathAgreement:
    """Both rsync outputs must name a file the same way.

    Verified against openrsync: ``--list-only`` prints ``データ.csv`` literally
    while the deletion preview escapes it as ``\\#343\\#203...``. The two never
    compare equal, so a genuine stale upload drops out of the intersection and
    the report says "known, nothing stale". Both sides therefore read the
    *escaped* form, which is also the only one that survives a newline.
    """

    def _client(self) -> RsyncClient:
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            return RsyncClient(hostname="h", username="u")

    def test_inventory_keeps_escaping(self):
        client = self._client()
        with patch("srunx.sync.rsync.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.list_local_files("/local/proj")

        assert "-8" not in run.call_args[0][0]

    def test_itemize_keeps_escaping(self):
        """The other half of the pair — they only agree if both do it."""
        client = self._client()
        cmd = client._build_rsync_cmd(
            "s/", "u@h:d/", delete=True, dry_run=True, itemize=True, excludes=[]
        )
        assert "-8" not in cmd

    def test_every_parsed_run_is_locale_normalized(self):
        """Which bytes rsync escapes depends on the locale.

        Verified on openrsync: under a UTF-8 locale ``データ.csv`` comes back as
        a *mix* of raw and escaped bytes while ``LC_ALL=C`` escapes all of them.
        Normalize only the inventory and a genuinely stale non-ASCII file
        matches no deletion candidate — it drops out of the report and the
        mount reads as clean.
        """
        client = self._client()
        with patch("srunx.sync.rsync.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.list_local_files("/local/proj")
            assert run.call_args.kwargs["env"]["LC_ALL"] == "C"

            client.push("/local/proj", "/remote/proj", dry_run=True, itemize=True)
            assert run.call_args.kwargs["env"]["LC_ALL"] == "C"

    def test_unescape_restores_non_ascii_for_display(self):
        assert (
            unescape_rsync_path(r"\#343\#203\#207\#343\#203\#274\#343\#202\#277.csv")
            == "データ.csv"
        )

    def test_unescape_is_invertible_because_backslash_is_escaped(self):
        """A file genuinely named ``a\\#012b.py`` is emitted as ``a\\#134#012b.py``.

        Without the backslash itself being escaped, unescaping would turn that
        name into one containing a newline — a different file.
        """
        assert unescape_rsync_path(r"a\#134#012b.py") == r"a\#012b.py"

    def test_control_bytes_stay_escaped(self):
        """rsync escapes them because they are unsafe to print, and this goes
        to a terminal. ``innocent\\#033c.py`` would emit ESC-c and reset it;
        ``\\#012`` would forge an extra line in a sync preview. A cluster job
        chooses the names it writes, so the input is attacker-controlled."""
        for hostile in (r"innocent\#033c.py", r"victim\#012>f+++++++ fake.py"):
            assert unescape_rsync_path(hostile) == hostile

    def test_bidi_overrides_stay_escaped(self):
        """U+202E reverses the rendering of what follows, which is how
        ``txt.exe`` is made to read as ``exe.txt``."""
        assert unescape_rsync_path(r"a\#342\#200\#256b") == r"a\#342\#200\#256b"

    def test_real_line_breaks_are_left_alone(self):
        """The CLI passes a whole multi-line preview through this."""
        blob = ">f+++++++ \\#343\\#203\\#207.csv\n>f+++++++ b.py\n"
        assert unescape_rsync_path(blob).count("\n") == 2

    def test_unescape_leaves_plain_paths_alone(self):
        assert unescape_rsync_path("sub/train.py") == "sub/train.py"


class TestControlFileReadIsGuarded:
    """The reader must refuse what the writer refuses.

    Writing to a planted symlink is rejected; reading through one was not, so a
    peer able to write the mount root could serve arbitrary content as srunx's
    control file. A forged upload record becomes paths presented to the user as
    safe to delete.
    """

    def _client(self) -> RsyncClient:
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            return RsyncClient(hostname="h", username="u")

    def test_symlinked_control_file_is_refused(self):
        client = self._client()
        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(
                returncode=client._READ_SYMLINK_EXIT, stdout="", stderr=""
            )
            with pytest.raises(RuntimeError, match="symlink"):
                client.read_remote_file("/remote/proj/.srunx-manifest.json")

    def test_missing_stays_distinct_from_unreadable(self):
        """Conflating them lets an unreadable record look like a first run,
        which an additive sync then rebuilds into a confident wrong answer."""
        client = self._client()
        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(
                returncode=client._READ_MISSING_EXIT, stdout="", stderr=""
            )
            assert client.read_remote_file("/remote/proj/x.json") is None

        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(returncode=1, stdout="", stderr="denied")
            with pytest.raises(RuntimeError):
                client.read_remote_file("/remote/proj/x.json")

    def test_read_checks_symlink_before_existence(self):
        client = self._client()
        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(returncode=0, stdout="{}", stderr="")
            client.read_remote_file("/remote/proj/x.json")

        script = ssh.call_args.args[0]
        assert script.index("-h ") < script.index("! -f ")

    def test_foreign_owned_manifest_is_refused(self):
        """Refusing symlinks is not enough on a shared mount.

        A peer able to write the mount root can plant an ordinary, schema-valid
        file naming job output as stale — and the user is told those paths are
        safe to delete. They can only create files owned by themselves.
        """
        client = self._client()
        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(
                returncode=client._READ_FOREIGN_EXIT, stdout="", stderr=""
            )
            with pytest.raises(RuntimeError, match="another account"):
                client.read_remote_file("/r/x.json", require_owned=True)

    def test_ownership_check_is_opt_in(self):
        """The advisory marker leaves it off: refusing to read a foreign one
        would turn a warning into a hard failure."""
        client = self._client()
        with patch.object(client, "_ssh_run") as ssh:
            ssh.return_value = MagicMock(returncode=0, stdout="{}", stderr="")
            client.read_remote_file("/r/x.json")
            assert "id -u" not in ssh.call_args.args[0]

            client.read_remote_file("/r/x.json", require_owned=True)
            assert "id -u" in ssh.call_args.args[0]

    def test_a_newline_in_a_name_cannot_forge_an_entry(self):
        """Verbatim itemize output for a directory crafted to look like a line.

        With ``--list-only`` this exact tree recorded ``victim`` *and* a
        fabricated ``output.py``, neither of which exists. Had a job written
        anything by either name, the comparison would have offered live job
        output for deletion. Escaping makes one line exactly one file.
        """
        stdout = (
            ">f+++++++ plain.py\n"
            "cd+++++++ victim\\#012-rw-r--r--            1 2026/\n"
            ">f+++++++ victim\\#012-rw-r--r--            1 2026/out.py\n"
        )
        assert self._client()._parse_inventory(stdout) == [
            "plain.py",
            "victim\\#012-rw-r--r--            1 2026/out.py",
        ]

    def test_drops_names_starting_with_whitespace(self):
        """Trailing whitespace is fine; leading is not.

        The inventory keeps a leading space, but the deletion preview pads its
        ``*deleting`` marker with the same characters, so the two sides can
        never be matched. Recording it would make a stale `` foo.py`` look
        like the plain ``foo.py`` a job may have written.
        """
        stdout = ">f+++++++  leading.py\n>f+++++++ trailing.py \n"
        assert self._client()._parse_inventory(stdout) == ["trailing.py "]

    def test_undecodable_filename_bytes_survive_a_json_round_trip(self):
        """A Linux filename can be any bytes at all.

        Escaped output is ASCII, so this cannot arise from the inventory — but
        stderr and other rsync output are decoded with surrogate escapes rather
        than raising, and anything that reaches the record has to survive it.
        """
        undecodable = "bad\udcffname.py"
        payload = json.dumps({"paths": [undecodable]})
        assert payload.isascii()
        assert json.loads(payload)["paths"] == [undecodable]
