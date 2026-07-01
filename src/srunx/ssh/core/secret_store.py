"""Per-account remote secret file manager (SSH-layer / transport concern).

Owns ``$HOME/.config/srunx/secrets.env`` on the remote cluster with a ``0700``
``~/.config/srunx`` directory and a ``0600`` file. The file is keyed by remote
account (``$HOME``) rather than by local profile name: secrets are only ever
needed per remote account, and encoding the local profile name in the path
would orphan the file on rename and produce "not found" surprises when two
machines use different profile names for the same account. Secrets are stored
as ``export KEY='<single-quote-escaped>'`` lines (one per key) and delivered by
sourcing the file in the SSH submission shell (see
:meth:`SlurmRemoteClient._get_slurm_env_setup`).

Design constraints (see ``.golem/specs/ssh-secret-store/spec.md``):

* Secrets never touch local ``config.json``, command-line arguments, or logs —
  only SFTP writes and the submit-shell ``source`` carry the value.
* Writes are atomic: the full new file is re-rendered from validated records
  to a temp path in the same directory, ``chmod 0600`` (exit-code-checked,
  must succeed before publishing), then ``mv`` over the target (torn-file
  prevention). The store refuses to write if the target, temp path, or the
  ``~/.config/srunx`` directory itself is a symlink, owned by another user, or
  of an owner it cannot determine (fail-closed). ``mkdir`` / ``chmod`` / ``mv``
  exit codes are all checked; any failure aborts before the next step.
* The file is a *sourced* shell script, so it is re-rendered from validated
  ``export KEY='...'`` records only — an unrecognised line (manual edit /
  corruption) is rejected rather than silently preserved, so tampering is
  surfaced instead of being carried into an executed shell.
* Single-user cluster assumption — no remote exclusive lock (temp+rename
  atomicity is sufficient; a rare lost write under simultaneous edits is
  accepted per spec).
"""

from __future__ import annotations

import re
import shlex
import uuid
from typing import TYPE_CHECKING

from srunx.common.logging import get_logger

if TYPE_CHECKING:
    from .connection import SSHConnection
    from .file_manager import RemoteFileManager

_logger = get_logger(__name__)

# KEY validation — parity with ``JobEnvironment.validate_env_var_keys``
# (``domain/jobs.py``): valid shell identifier, no reserved scheduler prefix.
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Parse ``export KEY=...`` lines to recover key names (values never surfaced).
_EXPORT_LINE_RE = re.compile(r"^export ([A-Za-z_][A-Za-z0-9_]*)=")

# Full recognised record: ``export KEY='<single-quote-escaped value>'``. The
# value body is any run of non-single-quote chars interspersed with the
# ``'\''`` escape sequence — the exact shape :meth:`_render` emits. Anything a
# line does not match is treated as tampering (see :meth:`_parse_records`).
_RECORD_RE = re.compile(r"^export ([A-Za-z_][A-Za-z0-9_]*)='((?:[^']|'\\'')*)'$")


def _validate_key(key: str) -> None:
    """Validate a secret KEY (identifier + reserved-prefix rules)."""
    if not _KEY_RE.match(key):
        raise ValueError(
            f"Invalid secret name: {key!r}. "
            "Must be a valid identifier (letters, digits, underscores; "
            "no leading digit, no whitespace/newline/NUL)."
        )
    if key.startswith(("SLURM_", "SBATCH_")):
        raise ValueError(
            f"Invalid secret name: {key!r}. "
            "The 'SLURM_' and 'SBATCH_' prefixes are reserved by the "
            "scheduler and cannot be set as secrets."
        )


def _validate_value(value: str) -> None:
    """Reject multi-line / control-character values (single-line guard)."""
    if any(c in value for c in ("\n", "\r")) or any(
        ord(c) < 32 and c != "\t" for c in value
    ):
        raise ValueError(
            "Secret value must be a single line without control characters."
        )


def _sq_escape(value: str) -> str:
    """Escape a value for embedding inside a single-quoted shell string."""
    return value.replace("'", "'\\''")


def _sq_unescape(escaped: str) -> str:
    """Inverse of :func:`_sq_escape` — recover the raw value from a record."""
    return escaped.replace("'\\''", "'")


class RemoteSecretStore:
    """Manages one remote account's secret file over an SSH connection.

    The file path is account-scoped (``$HOME/.config/srunx/secrets.env``) and
    carries no local profile name — the connection selects the account, so no
    profile identity is threaded in here.
    """

    def __init__(
        self,
        connection: SSHConnection,
        files: RemoteFileManager,
    ) -> None:
        self._conn = connection
        self._files = files
        self.logger = _logger
        self._resolved_path: str | None = None

    # ------------------------------------------------------------------
    # Path derivation
    # ------------------------------------------------------------------

    def secret_path(self) -> str:
        """Return the deterministic, ``$HOME``-resolved remote secret path.

        ``~/.config/srunx/secrets.env`` with ``$HOME`` expanded to a concrete
        absolute path (via ``echo $HOME``) so the result can be embedded in
        single-quoted shell commands without further expansion. Cached after
        the first resolution.
        """
        if self._resolved_path is None:
            stdout, _stderr, exit_code = self._conn.execute_command('echo "$HOME"')
            home = stdout.strip()
            if exit_code != 0 or not home:
                raise RuntimeError("Failed to resolve remote $HOME for secret path")
            self._resolved_path = f"{home}/.config/srunx/secrets.env"
        return self._resolved_path

    def _secret_dir(self) -> str:
        return self.secret_path().rsplit("/", 1)[0]

    def _run_checked(self, command: str, error_context: str) -> None:
        """Run a remote command and raise if its exit code is non-zero.

        Used for the setup steps (``mkdir`` / ``chmod`` / ``mv``) whose failure
        must abort the write rather than silently proceed to the next step.
        """
        _stdout, stderr, exit_code = self._conn.execute_command(command)
        if exit_code != 0:
            raise RuntimeError(f"{error_context}: {stderr.strip()}")

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def exists(self) -> bool:
        """True if the secret file is present on the remote."""
        return self._files.file_exists(self.secret_path())

    def _read(self) -> str:
        """Return the current file contents.

        Empty string *only* when the file is absent. A file that exists but
        cannot be read (chmod 000, ACL, transient read error) raises rather
        than being conflated with "absent" — otherwise ``set``/``unset`` would
        re-render the file from an empty record set and wipe every existing
        secret. The error message never includes the file contents.
        """
        if not self.exists():
            return ""
        quoted = shlex.quote(self.secret_path())
        stdout, _stderr, exit_code = self._conn.execute_command(f"cat {quoted}")
        if exit_code != 0:
            raise RuntimeError(
                f"Refusing to modify secret file: {self.secret_path()} exists "
                "but could not be read. Fix its permissions/ownership before "
                "setting/unsetting secrets."
            )
        return stdout

    @staticmethod
    def _parse_keys(content: str) -> list[str]:
        keys: list[str] = []
        for line in content.splitlines():
            match = _EXPORT_LINE_RE.match(line.strip())
            if match:
                keys.append(match.group(1))
        return keys

    @staticmethod
    def _parse_records(content: str) -> dict[str, str]:
        """Parse the file into an ordered KEY -> raw-value mapping.

        Only fully recognised ``export KEY='...'`` records are accepted; any
        line the store did not itself emit (a manual edit, a stray comment, a
        malformed record) is rejected so the tampering surfaces instead of
        being silently dropped. Blank lines are ignored. Values are recovered
        from their single-quote escaping so a round-trip preserves them.
        """
        records: dict[str, str] = {}
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            match = _RECORD_RE.match(line)
            if match is None:
                raise RuntimeError(
                    "Refusing to modify secret file: it contains a line this "
                    "store did not write (manual edit or corruption). Fix or "
                    "remove the file before setting/unsetting secrets."
                )
            records[match.group(1)] = _sq_unescape(match.group(2))
        return records

    @staticmethod
    def _render(records: dict[str, str]) -> str:
        """Re-emit the file from validated records only (source-safe)."""
        lines = [
            f"export {key}='{_sq_escape(value)}'" for key, value in records.items()
        ]
        return "\n".join(lines) + "\n" if lines else ""

    def list_keys(self) -> list[str]:
        """Return the stored KEY names only — never the values."""
        return self._parse_keys(self._read())

    # ------------------------------------------------------------------
    # Guard
    # ------------------------------------------------------------------

    def _assert_safe_target(self, path: str) -> None:
        """Refuse to write when *path* is a symlink or owned by another user.

        ``stat`` gives us the owner uid; ``-h`` detects a symlink. A missing
        path is fine (we're about to create it). Any owner other than the
        connecting user — or an owner we cannot determine — is rejected
        (fail-closed).
        """
        quoted = shlex.quote(path)
        # ``%u`` = owner uid; guard the whole probe so a missing file yields
        # a clean "ABSENT" rather than a stat error.
        cmd = (
            f"if [ -h {quoted} ]; then echo SYMLINK; "
            f"elif [ -e {quoted} ]; then "
            f"stat -c %u {quoted} 2>/dev/null || stat -f %u {quoted} 2>/dev/null; "
            f"else echo ABSENT; fi"
        )
        stdout, _stderr, _exit_code = self._conn.execute_command(cmd)
        result = stdout.strip()
        if result == "SYMLINK":
            raise RuntimeError(f"Refusing to write secret: {path} is a symlink.")
        if result == "ABSENT":
            return
        if result == "":
            # ``[ -e ]`` was true but neither stat form yielded an owner uid —
            # we cannot prove ownership, so refuse (fail-closed).
            raise RuntimeError(
                f"Refusing to write secret: cannot determine owner of {path}."
            )
        # ``result`` is the owner uid — compare against the connecting user's uid.
        me, _stderr, _exit_code = self._conn.execute_command("id -u")
        me = me.strip()
        if not me:
            raise RuntimeError(
                f"Refusing to write secret: cannot determine connecting user "
                f"to verify ownership of {path}."
            )
        if me != result:
            raise RuntimeError(
                f"Refusing to write secret: {path} is owned by another user."
            )

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def _ensure_secure_dir(self) -> None:
        """Create ``~/.config/srunx`` as ``0700`` and verify it is safe to use.

        This is the secret file's parent directory. Its ``0700`` mode is what
        secures the brief window during :meth:`_atomic_write` where the SFTP
        temp file may momentarily be ``0644`` before the explicit
        ``chmod 600``. The directory is guarded (symlink / foreign-owner /
        undetermined owner rejected) *before* and *after* ``mkdir`` so a
        pre-existing directory that is a symlink or owned by someone else is
        refused, and the ``chmod 700`` exit code is checked.
        """
        secret_dir = self._secret_dir()
        # Refuse if a pre-existing entry at the dir path is unsafe.
        self._assert_safe_target(secret_dir)
        quoted_dir = shlex.quote(secret_dir)
        self._run_checked(
            f"mkdir -p {quoted_dir}",
            f"Failed to create secrets directory {secret_dir}",
        )
        self._run_checked(
            f"chmod 700 {quoted_dir}",
            f"Failed to chmod 0700 secrets directory {secret_dir}",
        )
        # Re-check after mkdir in case the path was created as/into a symlink.
        self._assert_safe_target(secret_dir)

    def _atomic_write(self, new_content: str, error_context: str) -> None:
        """Write *new_content* to the target atomically (temp + rename).

        The temp file lives inside the ``0700`` ``~/.config/srunx`` directory
        (whose mode and ownership are verified in :meth:`_ensure_secure_dir`),
        so no other
        user can traverse into it and read the temp during the brief window
        between the SFTP create (which may momentarily be ``0644``) and the
        explicit ``chmod 600``. That ``chmod 600`` is exit-code-checked and
        must succeed *before* the ``mv`` — a chmod failure raises and never
        proceeds to rename, so a wrongly-permissioned file is never published
        to the target path. Every setup command's exit code is checked.
        """
        target = self.secret_path()
        secret_dir = self._secret_dir()

        self._ensure_secure_dir()

        temp_path = f"{secret_dir}/.secrets.{uuid.uuid4().hex[:8]}.tmp"
        self._assert_safe_target(temp_path)
        self._assert_safe_target(target)

        quoted_temp = shlex.quote(temp_path)
        self._files.write_remote_file(temp_path, new_content)
        # Confirm 0600 before publishing; failure aborts before ``mv``.
        try:
            self._run_checked(
                f"chmod 600 {quoted_temp}",
                f"Failed to chmod 0600 temp secret file {temp_path}",
            )
        except RuntimeError:
            self._files.cleanup_file(temp_path)
            raise
        quoted_target = shlex.quote(target)
        # Atomic replace — mv within the same directory is a rename(2).
        _stdout, stderr, exit_code = self._conn.execute_command(
            f"mv -f {quoted_temp} {quoted_target}"
        )
        if exit_code != 0:
            # Best-effort temp cleanup so a failed rename doesn't litter.
            self._files.cleanup_file(temp_path)
            raise RuntimeError(f"{error_context}: {stderr.strip()}")

    def set_secret(self, key: str, value: str) -> None:
        """Upsert one KEY into the secret file (atomic temp+rename).

        Validates the KEY (identifier + reserved-prefix) and the single-line
        value, parses the current file into validated records (rejecting any
        unrecognised line), upserts this KEY, re-renders the file from records
        only, then writes it atomically under a tight umask.
        """
        _validate_key(key)
        _validate_value(value)

        records = self._parse_records(self._read())
        records[key] = value
        new_content = self._render(records)

        self._atomic_write(new_content, f"Failed to store secret {key!r}")

    def unset_secret(self, key: str) -> None:
        """Remove KEY via temp+rename; delete the file if it empties."""
        if not self.exists():
            return
        records = self._parse_records(self._read())
        records.pop(key, None)

        target = self.secret_path()
        # File empties out -> remove it entirely (REQ-6 / REQ-N1).
        if not records:
            self._files.cleanup_file(target)
            return

        new_content = self._render(records)
        self._atomic_write(new_content, f"Failed to remove secret {key!r}")
