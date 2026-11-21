"""Remember which cluster / project a sweep was submitted for.

Recorded when a sweep is materialized (from the active config of the CLI
command, Web request or MCP call) so crash recovery can resume it against
the same target instead of a default one.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from srunx.common import paths
from srunx.common.statefile import read_json, update_json


@dataclass(frozen=True)
class SweepContext:
    cluster: str
    project_root: str | None


def _path() -> Path:
    return paths.state_dir() / "sweep-context.json"


def record_sweep_context(sweep_run_id: int) -> None:
    from srunx.common.config import active_config

    config = active_config()
    if config is None or config.cluster is None:
        return
    entry = {"cluster": config.cluster, "project_root": config.project_root}
    update_json(_path(), lambda data: data.__setitem__(str(sweep_run_id), entry))


def load_sweep_context(sweep_run_id: int) -> SweepContext | None:
    entry = read_json(_path()).get(str(sweep_run_id))
    if not isinstance(entry, dict) or not entry.get("cluster"):
        return None
    return SweepContext(
        cluster=entry["cluster"], project_root=entry.get("project_root")
    )
