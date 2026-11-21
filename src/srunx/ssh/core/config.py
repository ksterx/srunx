import os
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Shared profile-name regex. ``scheduler_key`` uses a colon-delimited
# grammar (``ssh:<profile>``) that has to round-trip through SQLite
# target_refs (``job:ssh:<profile>:<id>``), so profile names must not
# contain ``:``. Length is capped at 64 so oversized names cannot
# wedge UI tables or log lines.
_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


def validate_profile_name(name: str) -> None:
    """Validate *name* against the shared profile-name regex.

    Raises:
        ValueError: When *name* is empty, too long, or contains any
            character outside the allowed alphabet. ``':'`` is
            explicitly called out because it's the scheduler_key
            delimiter and the most likely accidental violation.
    """
    if not isinstance(name, str) or not _PROFILE_NAME_RE.match(name):
        raise ValueError(
            f"Invalid profile name {name!r}: must match "
            f"{_PROFILE_NAME_RE.pattern}. Profile names cannot contain "
            "':' (reserved for scheduler_key grammar) or exceed 64 characters."
        )


from srunx.common.validation import (  # noqa: E402  (re-exported)
    validate_snapshot_root,
)


class MountConfig(BaseModel):
    """Local-to-remote path mapping for a project directory.

    Deeply immutable: ``frozen=True`` locks fields and ``exclude_patterns``
    is a tuple so nested state cannot be mutated either. This lets the
    model be safely embedded in ``frozen`` dataclasses (e.g. the SSH
    adapter pool spec) without a ``list[str]`` escape hatch.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    local: str  # local path, e.g. "~/projects/ml-project" (resolved absolute after validation)
    remote: str  # remote path, must be absolute
    exclude_patterns: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Additional rsync exclude patterns for this mount",
    )
    sync: bool = Field(
        default=True,
        description="Rsync this mount before submission (false on shared storage)",
    )

    @field_validator("local", mode="before")
    @classmethod
    def _expand_local(cls, v: Any) -> Any:
        """Expand ``~`` and resolve to an absolute path."""
        if not isinstance(v, str):
            return v
        return str(Path(v).expanduser().resolve())

    @field_validator("remote")
    @classmethod
    def _validate_remote(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError(f"Mount remote path must be absolute: {v}")
        return v

    @field_validator("exclude_patterns", mode="before")
    @classmethod
    def _coerce_patterns_to_tuple(cls, v: Any) -> Any:
        # JSON / YAML round-trip produces list[str]; normalize so the stored
        # field is always a tuple and the model stays hashable-compatible.
        if isinstance(v, list):
            return tuple(v)
        return v


class ServerProfile(BaseModel):
    # ``extra="ignore"`` (the Pydantic v2 default, made explicit here) drops
    # any stray keys — notably the removed
    # ``env_vars`` block — so old configs still load without error (REQ-11).
    model_config = ConfigDict(extra="ignore")

    hostname: str = Field(..., description="The hostname of the server")
    username: str = Field(..., description="The username of the server")
    key_filename: str = Field(..., description="The key filename of the server")
    port: int = Field(22, description="The port of the server")
    description: str | None = Field(None, description="The description of the server")
    ssh_host: str | None = Field(
        None, description="The SSH config host name if using SSH config"
    )
    proxy_jump: str | None = Field(
        None, description="The ProxyJump host name if using ProxyJump"
    )
    mounts: list[MountConfig] = Field(
        default=[], description="Local-to-remote path mappings for project directories"
    )

    snapshot_root: str | None = Field(
        None,
        description="Remote directory holding read-only per-commit snapshots "
        "(default ~/.local/share/srunx/snapshots)",
    )

    @field_validator("snapshot_root")
    @classmethod
    def _validate_snapshot_root(cls, v: str | None) -> str | None:
        return validate_snapshot_root(v)

    @model_validator(mode="after")
    def expand_key_filename(self) -> "ServerProfile":
        """Expand ~ in key_filename so paramiko receives a concrete path."""
        if self.key_filename:
            self.key_filename = str(Path(self.key_filename).expanduser())
        return self


def connection_params(profile: ServerProfile) -> dict[str, Any]:
    """Resolve concrete SSH connection parameters for *profile*.

    ``ssh_host`` aliases are looked up in ``~/.ssh/config``; direct
    profiles still pick up IdentityFile / Port / ProxyJump from a matching
    ``Host`` block.
    """
    from .ssh_config import get_ssh_config_host

    if profile.ssh_host:
        resolved = get_ssh_config_host(profile.ssh_host, None)
        if resolved is None:
            raise ValueError(
                f"SSH host '{profile.ssh_host}' not found in ~/.ssh/config"
            )
        return {
            "hostname": resolved.hostname or profile.ssh_host,
            "username": resolved.user or "",
            "key_filename": resolved.identity_file,
            "port": resolved.port or 22,
            "proxy_jump": resolved.proxy_jump,
        }
    hostname = profile.hostname
    key_filename = profile.key_filename or None
    port = profile.port
    proxy_jump = profile.proxy_jump
    resolved = get_ssh_config_host(profile.hostname, None)
    if resolved is not None and resolved.hostname:
        hostname = resolved.hostname
        key_filename = key_filename or resolved.identity_file
        if resolved.port and port == 22:
            port = resolved.port
        proxy_jump = proxy_jump or resolved.proxy_jump
    return {
        "hostname": hostname,
        "username": profile.username,
        "key_filename": key_filename,
        "port": port,
        "proxy_jump": proxy_jump,
    }


def _profile_from_cluster(
    name: str, cluster: Any, mounts: list[MountConfig]
) -> ServerProfile:
    return ServerProfile(
        hostname=cluster.hostname or cluster.host or "",
        username=cluster.username or "",
        key_filename=cluster.key_filename or "",
        port=cluster.port,
        description=cluster.description,
        ssh_host=cluster.host,
        proxy_jump=cluster.proxy_jump,
        mounts=mounts,
        snapshot_root=cluster.snapshot_root,
    )


class ConfigManager:
    """SSH view of the configured clusters.

    Every ``transport = "ssh"`` cluster in the user/system config is exposed
    as a :class:`ServerProfile` whose ``mounts`` are the current project's
    mounts for that cluster. Writes go through the single TOML writer in
    :mod:`srunx.common.config_store`.
    """

    def __init__(self, project_root: Path | None = None):
        self.project_root = project_root

    def _settings(self) -> Any:
        from srunx.common.config import load_settings

        return load_settings(self.project_root, reload=True)

    @property
    def config_path(self) -> Path:
        from srunx.common import paths

        return paths.user_config_path()

    def _mounts(self, settings: Any, name: str) -> list[MountConfig]:
        return [
            MountConfig(
                name=m.name,
                local=m.local,
                remote=m.remote,
                exclude_patterns=m.exclude,
                sync=m.sync,
            )
            for m in settings.mounts(name)
        ]

    def get_profile(
        self, name: str, *, with_mounts: bool = True
    ) -> ServerProfile | None:
        settings = self._settings()
        cluster = settings.clusters.get(name)
        if cluster is None or cluster.transport != "ssh":
            return None
        mounts = self._mounts(settings, name) if with_mounts else []
        return _profile_from_cluster(name, cluster, mounts)

    def list_profiles(self, *, with_mounts: bool = False) -> dict[str, ServerProfile]:
        settings = self._settings()
        return {
            name: _profile_from_cluster(
                name, c, self._mounts(settings, name) if with_mounts else []
            )
            for name, c in settings.clusters.items()
            if c.transport == "ssh"
        }

    def add_profile(self, name: str, profile: ServerProfile) -> None:
        from srunx.common.config import user_store, validate_user_file

        validate_profile_name(name)
        table: dict[str, Any] = {"transport": "ssh"}
        if profile.ssh_host:
            table["host"] = profile.ssh_host
        else:
            table["hostname"] = profile.hostname
            if profile.username:
                table["username"] = profile.username
            if profile.key_filename:
                table["key_filename"] = profile.key_filename
            if profile.port != 22:
                table["port"] = profile.port
        if profile.proxy_jump:
            table["proxy_jump"] = profile.proxy_jump
        if profile.description:
            table["description"] = profile.description
        if profile.snapshot_root:
            table["snapshot_root"] = profile.snapshot_root
        user_store().set(("clusters", name), table, validate=validate_user_file)

    def remove_profile(self, name: str) -> bool:
        from srunx.common.config import user_store

        store = user_store()
        if name not in store.read().get("clusters", {}):
            return False

        def mutate(doc: Any) -> None:
            del doc["clusters"][name]
            if doc.get("default_cluster") == name:
                del doc["default_cluster"]

        store.edit(mutate)
        return True

    def update_profile(self, name: str, **kwargs: Any) -> bool:
        from srunx.common.config import user_store, validate_user_file

        store = user_store()
        if name not in store.read().get("clusters", {}):
            return False
        rename = {"ssh_host": "host"}

        def mutate(doc: Any) -> None:
            table = doc["clusters"][name]
            for key, value in kwargs.items():
                key = rename.get(key, key)
                if value is None:
                    if key == "snapshot_root" and key in table:
                        del table[key]
                    continue
                table[key] = value

        store.edit(mutate, validate=validate_user_file)
        return True

    def expand_path(self, path: str) -> str:
        return os.path.expanduser(path)
