"""Unit tests for srunx.snapshot.remote (script builders; store tests appended below)."""

from __future__ import annotations

import pytest

from srunx.snapshot import remote
from srunx.snapshot.remote import (
    build_bind_script,
    build_commit_script,
    build_delete_lease_script,
    build_probe_script,
    literal_root_conflict,
    root_expr,
)

SHA = "b" * 40
TOKEN = "c" * 32
LEASE_ID = "d" * 32
OWNER = {"client_host": "host1", "client_pid": 123, "token": TOKEN}


class TestRootExpr:
    def test_default_is_home_relative(self) -> None:
        assert root_expr(None) == '"$HOME"/.srunx/snapshots'

    def test_home_only(self) -> None:
        assert root_expr("~") == '"$HOME"'

    def test_home_subdir(self) -> None:
        assert root_expr("~/snap/x") == '"$HOME"/snap/x'

    def test_absolute(self) -> None:
        assert root_expr("/scratch/me/snap") == "/scratch/me/snap"

    def test_normalises_slashes(self) -> None:
        assert root_expr("/a//b/./c/") == "/a/b/c"

    @pytest.mark.parametrize(
        "bad",
        [
            "relative/path",
            "/a b",
            "/a'b",
            "/a$HOME",
            "/a;b",
            "/a/../b",
            "~/../x",
            "~user/x",
            "/",
            "/a\nb",
        ],
    )
    def test_rejects(self, bad: str) -> None:
        with pytest.raises(ValueError):
            root_expr(bad)


class TestLiteralConflict:
    def test_root_inside_mount(self) -> None:
        assert literal_root_conflict("/data/proj/snap", ["/data/proj"]) is not None

    def test_mount_inside_root(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap/work"]) is not None

    def test_equal(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap/"]) is not None

    def test_sibling_prefix_is_not_a_conflict(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap2"]) is None

    def test_home_root_vs_absolute_mount_unknown(self) -> None:
        assert literal_root_conflict(None, ["/home/me/proj"]) is None

    def test_home_vs_home(self) -> None:
        assert literal_root_conflict("~/work/snap", ["~/work"]) is not None


class TestBuilders:
    def _probe(self, **kw: object) -> str:
        args: dict[str, object] = {
            "root_expr": root_expr("/r/snap"),
            "repo_id": "repo-0123456789ab",
            "sha": SHA,
            "mounts": [],
            **OWNER,
        }
        args.update(kw)
        return build_probe_script(**args)  # type: ignore[arg-type]

    def test_probe_rejects_bad_repo_id_and_sha(self) -> None:
        for bad in ("a b", "a/b", "a;b", "", "../x"):
            with pytest.raises(ValueError):
                self._probe(repo_id=bad)
        with pytest.raises(ValueError):
            self._probe(sha="xyz")
        with pytest.raises(ValueError):
            self._probe(sha="B" * 40)

    def test_probe_rejects_bad_token(self) -> None:
        with pytest.raises(ValueError):
            self._probe(token="not-hex")

    def test_hostile_mount_path_is_quoted(self) -> None:
        script = self._probe(mounts=["/m/$(touch pwned);`id`'x"])
        assert "'/m/$(touch pwned);`id`'\"'\"'x'" in script

    def test_home_mount_uses_home_expr(self) -> None:
        assert '"$HOME"/work' in self._probe(mounts=["~/work"])

    def test_lock_wait_and_traps_embedded(self) -> None:
        script = self._probe(lock_wait=7)
        assert "-ge 7 ]" in script
        for trap in ("trap release EXIT", "exit 129", "exit 130", "exit 143"):
            assert trap in script
        assert "rm -rf" not in script.split("acquire()")[0]

    def test_no_stale_lock_breaking(self) -> None:
        for script in (
            self._probe(),
            build_delete_lease_script(repo_dir="/r/snap/x", lease_id=LEASE_ID, **OWNER),
        ):
            assert "stale" not in script.lower()
            assert 'rm -rf "$LOCKDIR"' not in script

    def test_commit_rejects_bad_inputs(self) -> None:
        base = {
            "repo_dir": "/r/snap/x",
            "git": "/usr/bin/git",
            "sha": SHA,
            "lease_json": None,
            "lease_id": None,
            **OWNER,
        }
        build_commit_script(**base)  # type: ignore[arg-type]
        for key, bad in (
            ("repo_dir", "/r/a b"),
            ("repo_dir", "relative"),
            ("git", "git"),
            ("sha", "abc"),
            ("lease_id", "zz"),
        ):
            args = {**base, key: bad}
            if key == "lease_id":
                args["lease_json"] = "{}"
            with pytest.raises(ValueError):
                build_commit_script(**args)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            build_commit_script(**{**base, "lease_json": "{}"})  # type: ignore[arg-type]

    def test_lease_json_is_quoted(self) -> None:
        script = build_bind_script(
            repo_dir="/r/snap/x",
            sha=SHA,
            lease_json='{"a":"it\'s"}',
            lease_id=LEASE_ID,
            **OWNER,
        )
        assert 'LEASE_JSON=\'{"a":"it\'"\'"\'s"}\'' in script

    def test_bind_and_delete_reject_bad_ids(self) -> None:
        with pytest.raises(ValueError):
            build_bind_script(
                repo_dir="/r/x", sha="1", lease_json="{}", lease_id=LEASE_ID, **OWNER
            )
        with pytest.raises(ValueError):
            build_delete_lease_script(repo_dir="/r/x", lease_id="../x", **OWNER)

    def test_constants(self) -> None:
        assert remote.MIN_REMOTE_GIT == (2, 17)
