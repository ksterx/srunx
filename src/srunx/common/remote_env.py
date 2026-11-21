"""Remote environment discovery (``$HOME`` / ``$XDG_*`` on a cluster).

Remote paths are computed from the *remote* environment, never from the
local ``Path.home()`` (design §2). Results are cached per cluster in
``<state_dir>/remote-env.json``; ``srunx cluster test`` refreshes them.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable
from dataclasses import asdict, dataclass

from srunx.common import paths
from srunx.common.logging import get_logger
from srunx.common.statefile import read_json, update_json

logger = get_logger(__name__)

_PROBE = (
    'printf "%s\\n%s\\n%s\\n" "$HOME" '
    '"${XDG_DATA_HOME:-$HOME/.local/share}" '
    '"${XDG_CONFIG_HOME:-$HOME/.config}"'
)


@dataclass(frozen=True)
class RemoteEnv:
    home: str
    data_home: str
    config_home: str

    @property
    def secrets_path(self) -> str:
        return f"{self.config_home}/srunx/secrets.env"

    @property
    def snapshots_dir(self) -> str:
        return f"{self.data_home}/srunx/snapshots"


def _cache_path():
    return paths.state_dir() / "remote-env.json"


def _read_cache() -> dict[str, dict[str, str]]:
    return read_json(_cache_path())


def parse_probe(stdout: str) -> RemoteEnv | None:
    lines = stdout.splitlines()
    if len(lines) < 3 or not all(line.startswith("/") for line in lines[:3]):
        return None
    return RemoteEnv(home=lines[0], data_home=lines[1], config_home=lines[2])


def probe_command() -> str:
    return f"sh -c {shlex.quote(_PROBE)}"


def store_remote_env(cluster: str, env: RemoteEnv) -> None:
    update_json(_cache_path(), lambda data: data.__setitem__(cluster, asdict(env)))


def forget_remote_env(cluster: str) -> None:
    """Drop the cached environment (e.g. after the connection changed)."""
    update_json(_cache_path(), lambda data: data.pop(cluster, None))


# Set by ``srunx.ssh.core.remote_probe`` (the SSH layer owns connections;
# ``common`` must not import it).
_fetcher: Callable[[str], RemoteEnv | None] | None = None


def register_fetcher(fetcher: Callable[[str], RemoteEnv | None]) -> None:
    global _fetcher
    _fetcher = fetcher


def fetch_remote_env(cluster: str) -> RemoteEnv | None:
    """Connect to *cluster*, probe its environment and cache the result."""
    if _fetcher is None:
        import importlib

        importlib.import_module("srunx.ssh.core.remote_probe")
    assert _fetcher is not None
    env = _fetcher(cluster)
    if env is not None:
        store_remote_env(cluster, env)
    return env


def get_remote_env(cluster: str, *, fetch: bool = True) -> RemoteEnv | None:
    cached = _read_cache().get(cluster)
    if cached:
        try:
            return RemoteEnv(**cached)
        except TypeError:
            pass
    if not fetch:
        return None
    try:
        return fetch_remote_env(cluster)
    except Exception as exc:  # noqa: BLE001 — discovery must not crash callers
        logger.warning(f"Remote environment probe for '{cluster}' failed: {exc}")
        return None


def connection_changed(before: dict, after: dict) -> list[str]:
    """Cluster names whose connection settings differ between two user configs.

    Only the connection part counts (``defaults`` edits do not); added,
    removed and modified clusters are all reported.
    """

    def conn(data: dict) -> dict[str, dict]:
        return {
            name: {k: v for k, v in (table or {}).items() if k != "defaults"}
            for name, table in (data.get("clusters") or {}).items()
        }

    a, b = conn(before), conn(after)
    return sorted(n for n in set(a) | set(b) if a.get(n) != b.get(n))
