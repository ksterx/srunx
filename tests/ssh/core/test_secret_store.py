"""Tests for srunx.ssh.core.secret_store.RemoteSecretStore.

Mocks ``SSHConnection.execute_command`` and ``RemoteFileManager`` so we can
capture the emitted shell commands and the bytes written to the temp file
without touching a real cluster. The security-critical invariants are:

* the secret value is written only via SFTP (``write_remote_file``) and never
  appears in a command string;
* ``list_keys`` returns key names only;
* KEY / value validation matches ``JobEnvironment`` parity + single-line guard;
* writes are atomic (temp file + ``mv``), chmod 0600 file / 0700 dir;
* a symlink target is refused.
"""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from srunx.ssh.core.connection import SSHConnection
from srunx.ssh.core.secret_store import RemoteSecretStore

_HOME = "/home/tester"
# Account-scoped: no ``secrets/`` subdir, no profile name in the path.
_SECRET_PATH = f"{_HOME}/.config/srunx/secrets.env"
_SECRET_DIR = f"{_HOME}/.config/srunx"


class _FakeConn:
    """Records every executed command and replays scripted responses."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        # Response for a `cat <path>` — the current file contents.
        self.file_contents = ""
        # Exit code the `cat <path>` read returns (non-zero => read failure
        # on a file that exists, e.g. chmod 000 / ACL / transient error).
        self.cat_exit_code = 0
        # `id -u` / owner uid used by the symlink/owner guard.
        self.uid = "1000"
        self.owner_result = "ABSENT"  # what the guard probe returns
        # Non-zero exit codes to inject for a command prefix, e.g.
        # {"chmod 700": 1} makes any `chmod 700 ...` fail.
        self.fail_prefixes: dict[str, int] = {}

    def execute_command(self, command: str) -> tuple[str, str, int]:
        self.commands.append(command)
        if command == 'echo "$HOME"':
            return (_HOME + "\n", "", 0)
        if command.startswith("cat "):
            if self.cat_exit_code != 0:
                return ("", "cat: Permission denied", self.cat_exit_code)
            return (self.file_contents, "", 0)
        if command == "id -u":
            return (self.uid + "\n", "", 0)
        # symlink/owner guard probe
        if command.startswith("if [ -h "):
            return (self.owner_result + "\n", "", 0)
        for prefix, code in self.fail_prefixes.items():
            if command.startswith(prefix):
                return ("", f"{prefix} failed", code)
        # mkdir/chmod/mv all succeed
        return ("", "", 0)


@pytest.fixture
def conn() -> _FakeConn:
    return _FakeConn()


@pytest.fixture
def files() -> MagicMock:
    fm = MagicMock()
    fm.file_exists.return_value = False
    return fm


@pytest.fixture
def store(conn: _FakeConn, files: MagicMock) -> RemoteSecretStore:
    return RemoteSecretStore(cast(SSHConnection, conn), files)


class TestSecretPath:
    def test_secret_path_is_deterministic_and_home_resolved(self, store, conn):
        assert store.secret_path() == _SECRET_PATH
        # Cached — resolved once.
        store.secret_path()
        assert conn.commands.count('echo "$HOME"') == 1

    def test_secret_path_is_account_scoped_no_profile_name(self, store):
        """Path is flat + account-scoped: no ``secrets/`` subdir, no profile."""
        path = store.secret_path()
        assert path == _SECRET_PATH
        assert "/secrets/" not in path
        assert "gmo" not in path

    def test_exists_delegates_to_file_manager(self, store, files):
        files.file_exists.return_value = True
        assert store.exists() is True
        files.file_exists.assert_called_with(_SECRET_PATH)


class TestSetSecret:
    def test_set_writes_export_line_via_sftp_not_command(self, store, conn, files):
        store.set_secret("API_KEY", "sk-secret-123")

        # Value written via SFTP only.
        assert files.write_remote_file.call_count == 1
        temp_path, content = files.write_remote_file.call_args[0]
        assert "export API_KEY='sk-secret-123'" in content
        # Value never appears in any executed command string.
        for cmd in conn.commands:
            assert "sk-secret-123" not in cmd

    def test_set_chmods_dir_0700_and_file_0600(self, store, conn, files):
        store.set_secret("API_KEY", "v")
        temp_path, _content = files.write_remote_file.call_args[0]
        # The 0700 parent dir is ``~/.config/srunx`` (secures the temp-file
        # permission window), not a ``secrets/`` subdir.
        assert any("chmod 700" in c and _SECRET_DIR in c for c in conn.commands)
        assert any("chmod 600" in c and temp_path in c for c in conn.commands)

    def test_set_is_atomic_temp_then_mv(self, store, conn, files):
        store.set_secret("API_KEY", "v")
        temp_path, _content = files.write_remote_file.call_args[0]
        # temp file lives in the same (account) dir as the target and is mv'd
        # over it — no ``secrets/`` subdir, no profile name in the temp name.
        assert temp_path.startswith(f"{_SECRET_DIR}/.secrets.")
        assert "gmo" not in temp_path
        assert any(c.startswith("mv -f ") and _SECRET_PATH in c for c in conn.commands)

    def test_set_upsert_replaces_existing_key(self, store, conn, files):
        conn.file_contents = "export API_KEY='old'\nexport OTHER='keep'\n"
        files.file_exists.return_value = True
        store.set_secret("API_KEY", "new")
        _temp, content = files.write_remote_file.call_args[0]
        assert "export API_KEY='new'" in content
        assert "export API_KEY='old'" not in content
        # Unrelated key preserved.
        assert "export OTHER='keep'" in content
        assert content.count("export API_KEY=") == 1

    def test_set_single_quote_escaped(self, store, files):
        store.set_secret("MSG", "it's ok")
        _temp, content = files.write_remote_file.call_args[0]
        assert "export MSG='it'\\''s ok'" in content

    def test_set_refuses_symlink_target(self, store, conn, files):
        conn.owner_result = "SYMLINK"
        with pytest.raises(RuntimeError, match="symlink"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_set_refuses_foreign_owner(self, store, conn, files):
        conn.owner_result = "4242"  # a uid different from `id -u` (1000)
        with pytest.raises(RuntimeError, match="another user"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()


class TestSetExitCodeChecks:
    """Hardening: mkdir/chmod/mv failures must abort before publishing."""

    def test_mkdir_failure_aborts_before_write(self, store, conn, files):
        conn.fail_prefixes = {"mkdir -p ": 1}
        with pytest.raises(RuntimeError, match="create secrets directory"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_dir_chmod_failure_aborts_before_write(self, store, conn, files):
        conn.fail_prefixes = {"chmod 700 ": 1}
        with pytest.raises(RuntimeError, match="chmod 0700"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_temp_chmod_failure_aborts_before_mv(self, store, conn, files):
        conn.fail_prefixes = {"chmod 600 ": 1}
        with pytest.raises(RuntimeError, match="chmod 0600"):
            store.set_secret("API_KEY", "v")
        # File was written to temp, but no mv happened.
        files.write_remote_file.assert_called_once()
        assert not any(c.startswith("mv -f ") for c in conn.commands)
        # Temp is cleaned up on the aborted write.
        files.cleanup_file.assert_called_once()

    def test_mv_failure_raises_and_cleans_temp(self, store, conn, files):
        conn.fail_prefixes = {"mv -f ": 1}
        with pytest.raises(RuntimeError, match="store secret"):
            store.set_secret("API_KEY", "v")
        files.cleanup_file.assert_called_once()


class TestDirectoryGuard:
    """Hardening: the secrets directory itself is symlink/owner-guarded."""

    def test_refuses_symlink_directory(self, store, conn, files):
        conn.owner_result = "SYMLINK"
        with pytest.raises(RuntimeError, match="symlink"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_refuses_foreign_owned_directory(self, store, conn, files):
        conn.owner_result = "4242"
        with pytest.raises(RuntimeError, match="another user"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_refuses_when_owner_undeterminable(self, store, conn, files):
        # `[ -e ]` true but no stat form yields an owner uid -> fail-closed.
        conn.owner_result = ""  # empty, but the path exists (not ABSENT)
        with pytest.raises(RuntimeError, match="cannot determine owner"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()

    def test_refuses_when_connecting_user_undeterminable(self, store, conn, files):
        conn.owner_result = "1000"  # a real owner uid
        conn.uid = ""  # `id -u` returns nothing
        with pytest.raises(RuntimeError, match="cannot determine connecting user"):
            store.set_secret("API_KEY", "v")
        files.write_remote_file.assert_not_called()


class TestTamperingRejection:
    """Hardening: sourced file is re-rendered from validated records only."""

    def test_set_rejects_unrecognised_line(self, store, conn, files):
        conn.file_contents = "export API_KEY='ok'\nrm -rf ~\n"
        files.file_exists.return_value = True
        with pytest.raises(RuntimeError, match="did not write"):
            store.set_secret("NEW", "v")
        files.write_remote_file.assert_not_called()

    def test_set_rejects_comment_line(self, store, conn, files):
        conn.file_contents = "# injected\nexport API_KEY='ok'\n"
        files.file_exists.return_value = True
        with pytest.raises(RuntimeError, match="did not write"):
            store.set_secret("NEW", "v")

    def test_unset_rejects_unrecognised_line(self, store, conn, files):
        conn.file_contents = "export API_KEY='ok'\nmalicious code\n"
        files.file_exists.return_value = True
        with pytest.raises(RuntimeError, match="did not write"):
            store.unset_secret("API_KEY")

    def test_rerender_keeps_only_known_records(self, store, conn, files):
        conn.file_contents = "export A='1'\nexport B='2'\n"
        files.file_exists.return_value = True
        store.set_secret("C", "3")
        _temp, content = files.write_remote_file.call_args[0]
        lines = [ln for ln in content.splitlines() if ln]
        assert lines == ["export A='1'", "export B='2'", "export C='3'"]

    def test_value_round_trips_through_rerender(self, store, conn, files):
        # A value with an embedded single quote must survive parse -> re-render.
        conn.file_contents = "export MSG='it'\\''s ok'\n"
        files.file_exists.return_value = True
        store.set_secret("OTHER", "x")
        _temp, content = files.write_remote_file.call_args[0]
        # Original record preserved byte-for-byte after the round-trip.
        assert "export MSG='it'\\''s ok'" in content

    def test_blank_lines_ignored_not_rejected(self, store, conn, files):
        conn.file_contents = "export A='1'\n\n\nexport B='2'\n"
        files.file_exists.return_value = True
        store.set_secret("C", "3")
        _temp, content = files.write_remote_file.call_args[0]
        assert "export A='1'" in content
        assert "export B='2'" in content
        assert "export C='3'" in content


class TestReadFailureNotAbsent:
    """A read failure on an existing file must not be treated as "empty".

    Otherwise ``set`` re-renders from an empty record set and atomically
    overwrites the file, wiping every existing secret (data loss).
    """

    def test_read_failure_raises_not_empty_string(self, store, conn, files):
        files.file_exists.return_value = True
        conn.cat_exit_code = 1
        conn.file_contents = "export API_KEY='keep-me'\n"
        with pytest.raises(RuntimeError, match="could not be read"):
            store._read()

    def test_set_aborts_and_does_not_overwrite_on_read_failure(
        self, store, conn, files
    ):
        files.file_exists.return_value = True
        conn.cat_exit_code = 1  # existing file, but unreadable
        conn.file_contents = "export API_KEY='keep-me'\n"
        with pytest.raises(RuntimeError, match="could not be read"):
            store.set_secret("NEW", "v")
        # Existing file is never re-rendered/overwritten.
        files.write_remote_file.assert_not_called()

    def test_unset_aborts_and_does_not_overwrite_on_read_failure(
        self, store, conn, files
    ):
        files.file_exists.return_value = True
        conn.cat_exit_code = 1
        conn.file_contents = "export API_KEY='keep-me'\n"
        with pytest.raises(RuntimeError, match="could not be read"):
            store.unset_secret("API_KEY")
        files.write_remote_file.assert_not_called()
        # The file is not deleted either — the aborted read protects it.
        files.cleanup_file.assert_not_called()

    def test_absent_file_still_treated_as_empty_new_file(self, store, conn, files):
        """Regression guard: genuine absence still yields empty + new file."""
        files.file_exists.return_value = False
        # cat is never reached for an absent file.
        store.set_secret("API_KEY", "v")
        _temp, content = files.write_remote_file.call_args[0]
        assert "export API_KEY='v'" in content


class TestKeyValidation:
    @pytest.mark.parametrize("bad_key", ["1BAD", "has space", "with-dash", ""])
    def test_reject_invalid_identifier(self, store, bad_key):
        with pytest.raises(ValueError, match="valid identifier"):
            store.set_secret(bad_key, "v")

    @pytest.mark.parametrize("bad_key", ["SLURM_FOO", "SBATCH_BAR"])
    def test_reject_reserved_prefix(self, store, bad_key):
        with pytest.raises(ValueError, match="reserved"):
            store.set_secret(bad_key, "v")

    @pytest.mark.parametrize(
        "bad_value", ["line1\nline2", "carriage\rreturn", "a\x00b"]
    )
    def test_reject_multiline_or_control_value(self, store, bad_value):
        with pytest.raises(ValueError, match="single line"):
            store.set_secret("API_KEY", bad_value)


class TestListKeys:
    def test_list_returns_names_only_never_values(self, store, conn, files):
        conn.file_contents = (
            "export API_KEY='sk-secret'\nexport WANDB='wandb-token-xyz'\n"
        )
        files.file_exists.return_value = True
        keys = store.list_keys()
        assert keys == ["API_KEY", "WANDB"]
        # No value leaks into the returned data.
        assert "sk-secret" not in keys
        assert "wandb-token-xyz" not in keys

    def test_list_empty_when_no_file(self, store, files):
        files.file_exists.return_value = False
        assert store.list_keys() == []


class TestUnsetSecret:
    def test_unset_removes_line_preserving_others(self, store, conn, files):
        conn.file_contents = "export API_KEY='x'\nexport OTHER='y'\n"
        files.file_exists.return_value = True
        store.unset_secret("API_KEY")
        _temp, content = files.write_remote_file.call_args[0]
        assert "export API_KEY=" not in content
        assert "export OTHER='y'" in content
        assert any(c.startswith("mv -f ") for c in conn.commands)

    def test_unset_deletes_file_when_empty(self, store, conn, files):
        conn.file_contents = "export API_KEY='x'\n"
        files.file_exists.return_value = True
        store.unset_secret("API_KEY")
        # File becomes empty -> removed via cleanup_file, no temp write.
        files.cleanup_file.assert_called_once_with(_SECRET_PATH)
        files.write_remote_file.assert_not_called()

    def test_unset_noop_when_file_absent(self, store, files):
        files.file_exists.return_value = False
        store.unset_secret("API_KEY")
        files.write_remote_file.assert_not_called()
        files.cleanup_file.assert_not_called()
