"""The only writer for srunx TOML config files (design §10).

Every CLI / Web / MCP write goes through :class:`ConfigStore`:

1. take an exclusive lock on ``<file>.lock``;
2. re-read the file (``tomlkit`` keeps comments and formatting);
3. apply a targeted edit (set one key, delete one key, ...);
4. validate the resulting document;
5. write a temp file in the same directory and ``os.replace`` it in.

A file that fails to parse is never treated as empty: reads and writes
raise :class:`ConfigFileError` so a broken hand-edit cannot be silently
clobbered.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import tempfile
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import tomlkit
from tomlkit.exceptions import TOMLKitError
from tomlkit.items import Table
from tomlkit.toml_document import TOMLDocument

KeyPath = Sequence[str]


class ConfigFileError(RuntimeError):
    """A config file exists but cannot be read or parsed."""


class ConfigConflictError(RuntimeError):
    """The file changed since the caller read it (optimistic concurrency)."""


def _plain(value: Any) -> Any:
    """Convert tomlkit containers to plain Python objects."""
    if hasattr(value, "unwrap"):
        return value.unwrap()
    return value


class ConfigStore:
    """Read and edit one TOML config file safely."""

    def __init__(self, path: Path, *, private: bool = False) -> None:
        self.path = path
        # User config holds SSH host details: keep it 0600 in a 0700 dir.
        self.private = private

    # ── reading ─────────────────────────────────────────────────────

    def _read_bytes(self) -> bytes | None:
        try:
            return self.path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ConfigFileError(f"Cannot read {self.path}: {exc}") from exc

    def _parse(self, data: bytes | None) -> TOMLDocument:
        if data is None:
            return tomlkit.document()
        try:
            return tomlkit.parse(data.decode("utf-8"))
        except (TOMLKitError, UnicodeDecodeError) as exc:
            raise ConfigFileError(f"Invalid TOML in {self.path}: {exc}") from exc

    def read(self) -> dict[str, Any]:
        """Return the file contents as plain dicts (``{}`` when missing)."""
        return _plain(self._parse(self._read_bytes()))

    def read_with_version(self) -> tuple[dict[str, Any], str]:
        """Contents and version token computed from the same bytes."""
        data = self._read_bytes()
        return _plain(self._parse(data)), hashlib.sha256(data or b"").hexdigest()

    def version(self) -> str:
        """Opaque token identifying the current file contents."""
        data = self._read_bytes() or b""
        return hashlib.sha256(data).hexdigest()

    # ── writing ─────────────────────────────────────────────────────

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.private:
            with contextlib.suppress(OSError):
                self.path.parent.chmod(0o700)
        # Lock files live in the state dir so nothing appears next to a
        # committed project config.
        from srunx.common import paths

        digest = hashlib.sha256(str(self.path.resolve()).encode()).hexdigest()[:16]
        lock_path = paths.locks_dir() / f"config-{digest}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a+") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def _write_atomic(self, doc: TOMLDocument) -> None:
        text = tomlkit.dumps(doc)
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            if self.private:
                os.chmod(tmp, 0o600)
            elif self.path.exists():
                os.chmod(tmp, self.path.stat().st_mode & 0o777)
            else:
                os.chmod(tmp, 0o644)
            os.replace(tmp, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise

    def edit(
        self,
        mutate: Callable[[TOMLDocument], None],
        *,
        validate: Callable[[dict[str, Any]], None] | None = None,
        expected_version: str | None = None,
    ) -> None:
        """Apply *mutate* to the freshly re-read document under the lock.

        Args:
            mutate: Edits the ``tomlkit`` document in place.
            validate: Called with the edited contents as plain dicts;
                raise to abort the write.
            expected_version: When given, abort with
                :class:`ConfigConflictError` if the file changed since
                the caller obtained this token from :meth:`version`.
        """
        with self._locked():
            data = self._read_bytes()
            if expected_version is not None:
                current = hashlib.sha256(data or b"").hexdigest()
                if current != expected_version:
                    raise ConfigConflictError(
                        f"{self.path} was modified by someone else; reload and retry"
                    )
            doc = self._parse(data)
            mutate(doc)
            if validate is not None:
                validate(_plain(doc))
            self._write_atomic(doc)

    def set(self, key: KeyPath, value: Any, **kwargs: Any) -> None:
        """Set ``key`` (a path of table names + leaf) to *value*."""
        self.edit(lambda doc: set_in(doc, key, value), **kwargs)

    def delete(self, key: KeyPath, **kwargs: Any) -> None:
        """Remove ``key`` so the value is inherited from lower layers again."""
        self.edit(lambda doc: delete_in(doc, key), **kwargs)


def _container(doc: Any, parents: KeyPath, *, create: bool) -> Any | None:
    node = doc
    for part in parents:
        if part not in node:
            if not create:
                return None
            node.add(part, tomlkit.table())
        node = node[part]
        if not isinstance(node, (Table, dict)) and not hasattr(node, "add"):
            raise ValueError(f"'{'.'.join(parents)}' is not a table")
    return node


def set_in(doc: TOMLDocument, key: KeyPath, value: Any) -> None:
    if not key:
        raise ValueError("empty key")
    parent = _container(doc, key[:-1], create=True)
    assert parent is not None
    if isinstance(value, dict):
        table = tomlkit.table()
        for k, v in value.items():
            if v is not None:
                table.add(k, v)
        parent[key[-1]] = table
    else:
        parent[key[-1]] = value


def delete_in(doc: TOMLDocument, key: KeyPath) -> None:
    if not key:
        raise ValueError("empty key")
    parent = _container(doc, key[:-1], create=False)
    if parent is not None and key[-1] in parent:
        del parent[key[-1]]


def inherit_in(doc: TOMLDocument, key: KeyPath) -> None:
    """Make *key* inherit again: remove it and drop it from its table's ``unset``."""
    delete_in(doc, key)
    parent = _container(doc, key[:-1], create=False)
    if parent is None or "unset" not in parent:
        return
    remaining = [k for k in parent["unset"] if k != key[-1]]
    if remaining:
        parent["unset"] = remaining
    else:
        del parent["unset"]
