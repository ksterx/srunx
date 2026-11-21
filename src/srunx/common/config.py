"""srunx configuration: TOML layers, clusters, projects and resolution.

See ``docs/design/clusters.md``. Layers, lowest precedence first:

1. built-in defaults (the pydantic field defaults below)
2. system ``defaults``          (``/etc/srunx/config.toml``)
3. user ``defaults``            (``$XDG_CONFIG_HOME/srunx/config.toml``)
4. user ``clusters.<c>.defaults``
5. project ``defaults``         (``<root>/.srunx/config.toml``)
6. project ``clusters.<c>.defaults``

Job-specific values (script ``#SBATCH``, workflow job fields, CLI flags)
are layered on top by the callers. Any table may carry ``unset = [...]``
to drop keys inherited from lower layers.

The domain models never read these files. Entry points build a
:class:`SrunxConfig` with :meth:`Settings.effective` and activate it
with :func:`activate_config`; :func:`active_config` is what the domain
models consult for defaults.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import os
import re
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from srunx.common import paths
from srunx.common.config_store import ConfigFileError, ConfigStore
from srunx.common.logging import get_logger
from srunx.domain.jobs import ContainerResource

logger = get_logger(__name__)

CLUSTER_ENV_VAR = "SRUNX_CLUSTER"

# Cluster names end up in scheduler keys (``ssh:<name>``) and target refs
# (``job:ssh:<name>:<id>``) so ``:`` is excluded.
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class ConfigError(ValueError):
    """Invalid or inconsistent srunx configuration."""


def validate_name(kind: str, name: str) -> str:
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError(
            f"Invalid {kind} name {name!r}: must match {NAME_RE.pattern} "
            "(letters, digits, '_' and '-', at most 64 characters)"
        )
    return name


# ── job defaults (the content of every ``defaults`` table) ──────────


class ResourceDefaults(BaseModel):
    """Default resource configuration."""

    model_config = ConfigDict(extra="forbid")

    nodes: int = Field(default=1, ge=1, description="Default number of compute nodes")
    gpus_per_node: int = Field(
        default=0, ge=0, description="Default number of GPUs per node"
    )
    ntasks_per_node: int = Field(
        default=1, ge=1, description="Default number of tasks per node"
    )
    cpus_per_task: int = Field(
        default=1, ge=1, description="Default number of CPUs per task"
    )
    memory_per_node: str | None = Field(
        default=None, description="Default memory per node"
    )
    time_limit: str | None = Field(default=None, description="Default time limit")
    nodelist: str | None = Field(default=None, description="Default nodelist")
    partition: str | None = Field(default=None, description="Default partition")


class EnvironmentDefaults(BaseModel):
    """Default environment configuration."""

    model_config = ConfigDict(extra="forbid")

    conda: str | None = Field(default=None, description="Default conda environment")
    venv: str | None = Field(
        default=None, description="Default virtual environment path"
    )
    container: ContainerResource | None = Field(
        default=None, description="Default container resource"
    )
    env_vars: dict[str, str] = Field(
        default_factory=dict, description="Default environment variables"
    )


class NotificationConfig(BaseModel):
    """Notification preferences (user/system level only)."""

    model_config = ConfigDict(extra="forbid")

    default_endpoint_name: str | None = Field(
        default=None,
        description=(
            "Default endpoint name pre-selected in the Web submit dialog. "
            "Must match an existing endpoint's name."
        ),
    )
    default_preset: str = Field(
        default="terminal",
        description=(
            "Default subscription preset for new submissions. One of "
            "'terminal', 'running_and_terminal', 'all', 'digest'."
        ),
    )


class SyncDefaults(BaseModel):
    """Workspace sync behaviour for job submission (user/system level)."""

    model_config = ConfigDict(extra="forbid")

    auto: bool = Field(
        default=True,
        description="Rsync the script's enclosing mount before sbatch.",
    )
    lock_timeout_seconds: int = Field(
        default=120,
        ge=1,
        description="Seconds to wait for the per-(cluster, mount) sync lock.",
    )
    warn_dirty: bool = Field(
        default=True,
        description="Warn before sync when the mount has uncommitted git changes.",
    )
    require_clean: bool = Field(
        default=False,
        description="Refuse to sync when the mount has uncommitted git changes.",
    )
    owner_check: bool = Field(
        default=True,
        description=(
            "Refuse to sync when the remote ``.srunx-owner.json`` marker shows "
            "a different machine last touched the mount."
        ),
    )
    verify_remote_hash: bool = Field(
        default=False,
        description="SHA-256 the submitted script on both ends after sync.",
    )


class SrunxConfig(BaseModel):
    """Effective configuration for one (cluster, project) context."""

    resources: ResourceDefaults = Field(default_factory=ResourceDefaults)
    environment: EnvironmentDefaults = Field(default_factory=EnvironmentDefaults)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    sync: SyncDefaults = Field(default_factory=SyncDefaults)
    log_dir: str = Field(default="logs", description="Default log directory")
    work_dir: str | None = Field(default=None, description="Default working directory")

    # Resolution context — not configuration values.
    cluster: str | None = Field(default=None, exclude=True)
    project_root: str | None = Field(default=None, exclude=True)
    sources: dict[str, str] = Field(default_factory=dict, exclude=True)


DEFAULTS_KEYS = ("resources", "environment", "log_dir", "work_dir")


# ── file schemas ────────────────────────────────────────────────────


def _check_defaults(value: dict[str, Any]) -> dict[str, Any]:
    """Validate a partial ``defaults`` table (``unset`` allowed anywhere)."""
    if not isinstance(value, dict):
        raise ValueError("defaults must be a table")
    unknown = set(value) - set(DEFAULTS_KEYS) - {"unset"}
    if unknown:
        raise ValueError(f"unknown keys in defaults: {sorted(unknown)}")
    stripped = {k: v for k, v in _strip_unset(value).items() if k in DEFAULTS_KEYS}
    # A layer may set only part of a container (e.g. ``nv = true`` on top of
    # a lower layer's apptainer image), so container fields are checked one
    # by one here; cross-field rules run on the merged config.
    environment = stripped.get("environment")
    if isinstance(environment, dict) and isinstance(environment.get("container"), dict):
        _check_container_fields(environment["container"])
        stripped = {
            **stripped,
            "environment": {k: v for k, v in environment.items() if k != "container"},
        }
    try:
        SrunxConfig.model_validate(stripped)
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc
    return value


def _check_container_fields(container: dict[str, Any]) -> None:
    from pydantic import TypeAdapter

    fields = ContainerResource.model_fields
    unknown = set(container) - set(fields)
    if unknown:
        raise ValueError(f"unknown container keys: {sorted(unknown)}")
    for key, item in container.items():
        try:
            TypeAdapter(fields[key].annotation).validate_python(item)
        except ValidationError as exc:
            raise ValueError(f"container.{key}: {exc}") from exc


def _strip_unset(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _strip_unset(v) for k, v in value.items() if k != "unset"}
    return value


class ClusterConfig(BaseModel):
    """A job target: connection settings plus cluster-specific defaults."""

    model_config = ConfigDict(extra="forbid")

    transport: Literal["local", "ssh"]
    host: str | None = Field(default=None, description="Host alias from ~/.ssh/config")
    hostname: str | None = None
    username: str | None = None
    key_filename: str | None = None
    port: int = 22
    proxy_jump: str | None = None
    description: str | None = None
    snapshot_root: str | None = None
    defaults: dict[str, Any] = Field(default_factory=dict)

    @field_validator("defaults")
    @classmethod
    def _v_defaults(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _check_defaults(v)

    @field_validator("snapshot_root")
    @classmethod
    def _v_snapshot_root(cls, v: str | None) -> str | None:
        from srunx.common.validation import validate_snapshot_root

        return validate_snapshot_root(v)

    @model_validator(mode="after")
    def _v_connection(self) -> ClusterConfig:
        connection = (self.host, self.hostname, self.username, self.key_filename)
        if self.transport == "ssh" and not (self.host or self.hostname):
            raise ValueError(
                "ssh cluster needs 'host' (ssh config alias) or 'hostname'"
            )
        if self.transport == "local" and any(connection):
            raise ValueError("local cluster cannot set host/hostname/username/key")
        return self


class MountSpec(BaseModel):
    """An extra mount declared in a project config."""

    model_config = ConfigDict(extra="forbid")

    name: str
    local: str
    remote: str
    sync: bool = True
    exclude: list[str] = Field(default_factory=list)

    @field_validator("remote")
    @classmethod
    def _v_remote(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError(f"mount remote path must be absolute: {v}")
        return v


class ProjectClusterOverride(BaseModel):
    """Project × cluster settings (sync target, mounts)."""

    model_config = ConfigDict(extra="forbid")

    remote_dir: str | None = None
    sync: bool | None = None
    exclude: list[str] | None = None
    mounts: list[MountSpec] | None = None

    @field_validator("remote_dir")
    @classmethod
    def _v_remote_dir(cls, v: str | None) -> str | None:
        if v is not None and not v.startswith("/"):
            raise ValueError(f"remote_dir must be absolute: {v}")
        return v


class ProjectClusterConfig(ProjectClusterOverride):
    defaults: dict[str, Any] = Field(default_factory=dict)

    @field_validator("defaults")
    @classmethod
    def _v_defaults(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _check_defaults(v)


class UserProjectConfig(BaseModel):
    """``[projects.<name>]`` in the user config — personal overrides."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ProjectClusterOverride] = Field(default_factory=dict)


class UserConfigFile(BaseModel):
    """Schema of the user and system ``config.toml``."""

    model_config = ConfigDict(extra="forbid")

    default_cluster: str | None = None
    state_dir: str | None = None
    defaults: dict[str, Any] = Field(default_factory=dict)
    clusters: dict[str, ClusterConfig] = Field(default_factory=dict)
    projects: dict[str, UserProjectConfig] = Field(default_factory=dict)
    notifications: dict[str, Any] = Field(default_factory=dict)
    sync: dict[str, Any] = Field(default_factory=dict)

    @field_validator("defaults")
    @classmethod
    def _v_defaults(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _check_defaults(v)

    @field_validator("clusters")
    @classmethod
    def _v_clusters(cls, v: dict[str, ClusterConfig]) -> dict[str, ClusterConfig]:
        for name in v:
            validate_name("cluster", name)
        return v

    @field_validator("notifications")
    @classmethod
    def _v_notifications(cls, v: dict[str, Any]) -> dict[str, Any]:
        NotificationConfig.model_validate(_strip_unset(v))
        return v

    @field_validator("sync")
    @classmethod
    def _v_sync(cls, v: dict[str, Any]) -> dict[str, Any]:
        SyncDefaults.model_validate(_strip_unset(v))
        return v


class ProjectConfigFile(BaseModel):
    """Schema of ``<root>/.srunx/config.toml``.

    Connection settings (``transport``, ``host``, ...) are deliberately
    not accepted: cloning a repository must never make srunx connect to
    a host the user has not configured.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    cluster: str | None = None
    defaults: dict[str, Any] = Field(default_factory=dict)
    clusters: dict[str, ProjectClusterConfig] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _v_name(cls, v: str) -> str:
        return validate_name("project", v)

    @field_validator("defaults")
    @classmethod
    def _v_defaults(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _check_defaults(v)


# ── merging with provenance ─────────────────────────────────────────


def merge_layer(
    base: dict[str, Any],
    override: dict[str, Any],
    sources: dict[str, str],
    label: str,
    prefix: str = "",
) -> dict[str, Any]:
    """Merge *override* into *base* (returns a new dict), recording sources.

    ``unset = ["key", ...]`` inside a table removes those keys (and their
    recorded sources) from the result before the table's own values apply.
    """
    result = dict(base)
    for key in override.get("unset", []) or []:
        result.pop(key, None)
        dotted = f"{prefix}{key}"
        for src_key in [
            k for k in sources if k == dotted or k.startswith(dotted + ".")
        ]:
            del sources[src_key]
    for key, value in override.items():
        if key == "unset":
            continue
        dotted = f"{prefix}{key}"
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_layer(result[key], value, sources, label, dotted + ".")
        elif isinstance(value, dict):
            result[key] = merge_layer({}, value, sources, label, dotted + ".")
        else:
            result[key] = value
            sources[dotted] = label
    return result


# ── settings ────────────────────────────────────────────────────────


class Mount(BaseModel):
    """A resolved local ↔ remote mapping for one cluster."""

    name: str
    local: str
    remote: str
    sync: bool = True
    exclude: tuple[str, ...] = ()


def _load_file(path: Path, model: type[BaseModel]) -> tuple[dict[str, Any], Any]:
    try:
        raw = ConfigStore(path).read()
    except ConfigFileError as exc:
        raise ConfigError(str(exc)) from exc
    if not raw:
        return {}, None
    try:
        return raw, model.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Invalid config in {path}:\n{exc}") from exc


class Settings:
    """All config files relevant to one project context, parsed and validated."""

    def __init__(self, project_root: Path | None = None, *, discover: bool = True):
        if project_root is None and discover:
            project_root = paths.find_project_root()
        self.project_root = project_root.resolve() if project_root else None

        self.system_path = paths.system_config_path()
        self.user_path = paths.user_config_path()
        self.project_path = (
            paths.project_config_path(self.project_root) if self.project_root else None
        )

        self.system_raw, system = _load_file(self.system_path, UserConfigFile)
        self.user_raw, user = _load_file(self.user_path, UserConfigFile)
        self.system: UserConfigFile = system or UserConfigFile()
        self.user: UserConfigFile = user or UserConfigFile()

        self.project: ProjectConfigFile | None = None
        self.project_raw: dict[str, Any] = {}
        if self.project_path is not None:
            self.project_raw, self.project = _load_file(
                self.project_path, ProjectConfigFile
            )
            if self.project is None:
                raise ConfigError(f"{self.project_path} is empty; 'name' is required")

        locals_ = [n for n, c in self.clusters.items() if c.transport == "local"]
        if len(locals_) > 1:
            raise ConfigError(
                f"At most one cluster may use transport = 'local' (found {locals_})"
            )
        # A shared project may mention clusters this user has not
        # configured; those sections are simply unused. Selecting such a
        # cluster fails in :meth:`cluster` with a clear message.

    # ── clusters ────────────────────────────────────────────────────

    @property
    def clusters(self) -> dict[str, ClusterConfig]:
        merged = dict(self.system.clusters)
        merged.update(self.user.clusters)
        return merged

    def cluster(self, name: str) -> ClusterConfig:
        try:
            return self.clusters[name]
        except KeyError:
            known = ", ".join(sorted(self.clusters)) or "none"
            raise ConfigError(
                f"Unknown cluster '{name}' (configured: {known}). "
                "Add it with 'srunx cluster add'."
            ) from None

    def cluster_source(self, explicit: str | None = None) -> tuple[str | None, str]:
        """Return ``(cluster name, where it came from)`` (design §5)."""
        if explicit:
            return explicit, "--cluster"
        env = os.environ.get(CLUSTER_ENV_VAR)
        if env:
            return env, f"${CLUSTER_ENV_VAR}"
        if self.project is not None and self.project.cluster:
            return self.project.cluster, str(self.project_path)
        if self.user.default_cluster:
            return self.user.default_cluster, str(self.user_path)
        if self.system.default_cluster:
            return self.system.default_cluster, str(self.system_path)
        return None, ""

    def resolve_cluster(self, explicit: str | None = None) -> str:
        name, _ = self.cluster_source(explicit)
        if name is None:
            raise ConfigError(
                "No cluster selected. Pass --cluster, set $SRUNX_CLUSTER, set "
                "'cluster' in .srunx/config.toml or 'default_cluster' in "
                f"{self.user_path}."
            )
        self.cluster(name)
        return name

    # ── effective defaults ──────────────────────────────────────────

    def layers(self, cluster: str | None) -> list[tuple[str, dict[str, Any]]]:
        """``(label, partial SrunxConfig dict)`` for layers 2–6."""
        sys_label, user_label = str(self.system_path), str(self.user_path)
        out: list[tuple[str, dict[str, Any]]] = []

        def top(raw: dict[str, Any]) -> dict[str, Any]:
            data = dict(raw.get("defaults", {}))
            for key in ("notifications", "sync"):
                if key in raw:
                    data[key] = raw[key]
            return data

        out.append((sys_label, top(self.system_raw)))
        out.append((user_label, top(self.user_raw)))
        if cluster is not None:
            for label, raw in (
                (sys_label, self.system_raw),
                (user_label, self.user_raw),
            ):
                c = raw.get("clusters", {}).get(cluster, {})
                if c.get("defaults"):
                    out.append((f"{label} [clusters.{cluster}]", c["defaults"]))
        if self.project is not None:
            proj_label = str(self.project_path)
            out.append((proj_label, self.project_raw.get("defaults", {})))
            if cluster is not None:
                c = self.project_raw.get("clusters", {}).get(cluster, {})
                if c.get("defaults"):
                    out.append((f"{proj_label} [clusters.{cluster}]", c["defaults"]))
        return [(label, data) for label, data in out if data]

    def effective(self, cluster: str | None = None) -> SrunxConfig:
        sources: dict[str, str] = {}
        merged: dict[str, Any] = {}
        for label, data in self.layers(cluster):
            merged = merge_layer(merged, data, sources, label)
        try:
            config = SrunxConfig.model_validate(merged)
        except ValidationError as exc:
            raise ConfigError(f"Invalid merged configuration:\n{exc}") from exc
        config.cluster = cluster
        config.project_root = str(self.project_root) if self.project_root else None
        config.sources = sources
        return config

    # ── project × cluster ───────────────────────────────────────────

    def project_cluster(self, cluster: str) -> ProjectClusterOverride:
        """Merged sync settings: project file, then the user's personal override."""
        merged: dict[str, Any] = {}
        if self.project is not None:
            pc = self.project.clusters.get(cluster)
            if pc is not None:
                merged.update(pc.model_dump(exclude={"defaults"}, exclude_none=True))
            user_proj = self.user.projects.get(self.project.name)
            if user_proj is not None and cluster in user_proj.clusters:
                merged.update(user_proj.clusters[cluster].model_dump(exclude_none=True))
        return ProjectClusterOverride.model_validate(merged)

    def mounts(self, cluster: str, *, fetch_remote: bool = True) -> list[Mount]:
        """Resolved mounts of the current project on *cluster*.

        The project root maps to ``remote_dir``; without ``remote_dir`` the
        root maps to the remote default workspace
        (``$XDG_DATA_HOME/srunx/workspaces/<name>-<hash>``), which needs
        the remote environment and is resolved by :func:`default_workspace`.
        """
        if self.project is None or self.project_root is None:
            return []
        if self.cluster(cluster).transport != "ssh":
            return []
        pc = self.project_cluster(cluster)
        remote_dir = pc.remote_dir or default_workspace(
            cluster, self.project.name, self.project_root, fetch=fetch_remote
        )
        out: list[Mount] = []
        if remote_dir:
            out.append(
                Mount(
                    name=self.project.name,
                    local=str(self.project_root),
                    remote=remote_dir,
                    sync=pc.sync if pc.sync is not None else True,
                    exclude=tuple(pc.exclude or ()),
                )
            )
        for m in pc.mounts or []:
            local = Path(m.local).expanduser()
            if not local.is_absolute():
                local = self.project_root / local
            out.append(
                Mount(
                    name=m.name,
                    local=str(local.resolve()),
                    remote=m.remote,
                    sync=m.sync,
                    exclude=tuple(m.exclude),
                )
            )
        names = [m.name for m in out]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        locals_ = [m.local for m in out]
        same_root = sorted({p for p in locals_ if locals_.count(p) > 1})
        if same_root:
            raise ConfigError(
                f"{self.project_path}: two mounts on cluster '{cluster}' share "
                f"the local directory {same_root} (the project root is "
                "already mounted via remote_dir)"
            )
        if duplicates:
            raise ConfigError(
                f"{self.project_path}: mount names must be unique on cluster "
                f"'{cluster}' (the project root mount is named "
                f"'{self.project.name}'); duplicated: {duplicates}"
            )
        return out


def workspace_dirname(project_name: str, project_root: Path) -> str:
    digest = hashlib.sha256(str(project_root.resolve()).encode()).hexdigest()[:8]
    return f"{project_name}-{digest}"


def default_workspace(
    cluster: str, project_name: str, project_root: Path, *, fetch: bool = True
) -> str | None:
    """Remote default sync target, or ``None`` if the remote env is unknown."""
    from srunx.common.remote_env import get_remote_env

    env = get_remote_env(cluster, fetch=fetch)
    if env is None:
        return None
    name = workspace_dirname(project_name, project_root)
    return f"{env.data_home}/srunx/workspaces/{name}"


# ── writing ─────────────────────────────────────────────────────────


def user_store() -> ConfigStore:
    return ConfigStore(paths.user_config_path(), private=True)


def project_store(root: Path) -> ConfigStore:
    return ConfigStore(paths.project_config_path(root))


def validate_user_file(data: dict[str, Any]) -> None:
    try:
        user = UserConfigFile.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc
    # Cross-file invariant checked before saving, so a write can never
    # leave a configuration every later command rejects.
    try:
        system_clusters = (
            ConfigStore(paths.system_config_path()).read().get("clusters", {})
        )
    except ConfigFileError:
        system_clusters = {}
    transports = {name: c.get("transport") for name, c in system_clusters.items()} | {
        name: c.transport for name, c in user.clusters.items()
    }
    locals_ = sorted(n for n, t in transports.items() if t == "local")
    if len(locals_) > 1:
        raise ConfigError(
            f"At most one cluster may use transport = 'local' (found {locals_})"
        )


def validate_project_file(data: dict[str, Any]) -> None:
    try:
        ProjectConfigFile.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc


# ── process / request context ───────────────────────────────────────

_active: contextvars.ContextVar[SrunxConfig | None] = contextvars.ContextVar(
    "srunx_active_config", default=None
)


def active_config() -> SrunxConfig | None:
    """The config activated for the current CLI command / request, if any."""
    return _active.get()


@contextlib.contextmanager
def activate_config(config: SrunxConfig) -> Iterator[SrunxConfig]:
    token = _active.set(config)
    try:
        yield config
    finally:
        _active.reset(token)


def set_active_config(config: SrunxConfig | None) -> None:
    """Activate *config* for the rest of this context (CLI process scope)."""
    _active.set(config)


_project_override: contextvars.ContextVar[Path | None] = contextvars.ContextVar(
    "srunx_project_override", default=None
)


@contextlib.contextmanager
def use_project(root: Path | None) -> Iterator[None]:
    """Use *root* instead of cwd discovery for :func:`load_settings` (``--project``)."""
    if root is None:
        yield
        return
    root = root.expanduser().resolve()
    if not paths.project_config_path(root).is_file():
        raise ConfigError(f"{paths.project_config_path(root)} does not exist")
    token = _project_override.set(root)
    try:
        yield
    finally:
        _project_override.reset(token)


_NO_PROJECT = Path("/\0no-project")


@contextlib.contextmanager
def no_project() -> Iterator[None]:
    """Disable project discovery (API surfaces must not guess from cwd)."""
    token = _project_override.set(_NO_PROJECT)
    try:
        yield
    finally:
        _project_override.reset(token)


def project_scope(project: str | Path | None) -> Any:
    """``use_project(project)`` when given, else :func:`no_project`."""
    if project:
        return use_project(Path(project))
    return no_project()


# Short names accepted by ``--unset`` → dotted config keys.
UNSET_ALIASES = {
    "nodes": "resources.nodes",
    "gpus_per_node": "resources.gpus_per_node",
    "ntasks_per_node": "resources.ntasks_per_node",
    "cpus_per_task": "resources.cpus_per_task",
    "memory_per_node": "resources.memory_per_node",
    "mem": "resources.memory_per_node",
    "time_limit": "resources.time_limit",
    "time": "resources.time_limit",
    "nodelist": "resources.nodelist",
    "partition": "resources.partition",
    "conda": "environment.conda",
    "venv": "environment.venv",
    "container": "environment.container",
    "env_vars": "environment.env_vars",
}


def apply_unset(config: SrunxConfig, keys: list[str]) -> SrunxConfig:
    """Return a copy of *config* with *keys* reset to their built-in defaults."""
    if not keys:
        return config
    data = config.model_dump()
    sources = dict(config.sources)
    defaults = SrunxConfig().model_dump()
    for raw in keys:
        dotted = UNSET_ALIASES.get(raw.replace("-", "_"), raw.replace("-", "_"))
        parts = dotted.split(".")
        if parts[0] not in DEFAULTS_KEYS:
            raise ConfigError(f"Cannot unset {raw!r}: not a job default")
        node, default = data, defaults
        try:
            for part in parts[:-1]:
                node, default = node[part], default[part]
            if parts[-1] not in default:
                raise KeyError(parts[-1])
        except (KeyError, TypeError):
            raise ConfigError(f"Cannot unset {raw!r}: unknown key") from None
        node[parts[-1]] = default[parts[-1]]
        for src_key in [
            k for k in sources if k == dotted or k.startswith(dotted + ".")
        ]:
            del sources[src_key]
    result = SrunxConfig.model_validate(data)
    result.cluster = config.cluster
    result.project_root = config.project_root
    result.sources = sources
    return result


_settings_cache: dict[str | None, Settings] = {}
_settings_lock = threading.Lock()


def load_settings(
    project_root: Path | None = None, *, reload: bool = False
) -> Settings:
    """Parse all config files for *project_root* (auto-discovered when ``None``)."""
    if project_root is None:
        project_root = _project_override.get()
    if project_root == _NO_PROJECT:
        root: Path | None = None
    else:
        root = project_root if project_root is not None else paths.find_project_root()
    key = str(root.resolve()) if root else None
    with _settings_lock:
        if reload or key not in _settings_cache:
            _settings_cache[key] = Settings(root, discover=False)
        return _settings_cache[key]


def clear_settings_cache() -> None:
    with _settings_lock:
        _settings_cache.clear()


def get_config(reload: bool = False) -> SrunxConfig:
    """Effective config for the current context.

    Returns the activated config when an entry point set one; otherwise
    resolves the cwd project and the default cluster (no error when no
    cluster is selected — cluster layers are then simply skipped).
    """
    active = _active.get()
    if active is not None and not reload:
        return active
    settings = load_settings(reload=reload)
    name, _ = settings.cluster_source()
    if name is not None and name not in settings.clusters:
        name = None
    return settings.effective(name)


def example_user_config() -> str:
    return """\
# srunx user configuration — see docs/design/clusters.md
default_cluster = "local"

[defaults]
log_dir = "logs"

[defaults.resources]
time_limit = "01:00:00"

[clusters.local]
transport = "local"

# [clusters.gpu]
# transport = "ssh"
# host = "gpu-login"          # alias from ~/.ssh/config
#
# [clusters.gpu.defaults.resources]
# partition = "gpu"
"""


def example_project_config(name: str) -> str:
    return f"""\
# srunx project configuration — commit this file
name = "{name}"
# cluster = "gpu"             # default target for this project

# [defaults.resources]
# time_limit = "04:00:00"

# [clusters.gpu]
# remote_dir = "/home/me/work/{name}"
# exclude = ["data/"]
#
# [clusters.gpu.defaults.resources]
# partition = "gpu"
"""
