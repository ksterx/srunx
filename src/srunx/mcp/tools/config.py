"""MCP tools: effective srunx configuration + cluster listing."""

from __future__ import annotations

from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok


@mcp.tool()
def get_config(
    cluster: str | None = None, project: str | None = None
) -> dict[str, Any]:
    """Get the effective job defaults for a cluster (and optionally a project).

    Every value is reported with the config file it came from (``built-in``
    when no file sets it).

    Args:
        cluster: Cluster whose cluster-specific defaults apply. Omit to see
            only the global defaults.
        project: Project root (directory holding ``.srunx/config.toml``).
    """
    try:
        from srunx.common.config import load_settings, project_scope

        with project_scope(project):
            settings = load_settings(project_root=None, reload=True)
            if cluster is not None:
                settings.cluster(cluster)
            config = settings.effective(cluster)
        return ok(
            cluster=cluster,
            project=str(settings.project_root) if settings.project_root else None,
            resources=config.resources.model_dump(),
            environment={
                "conda": config.environment.conda,
                "venv": config.environment.venv,
                "env_vars": config.environment.env_vars,
            },
            log_dir=config.log_dir,
            work_dir=config.work_dir,
            sources=config.sources,
        )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_clusters(project: str | None = None) -> dict[str, Any]:
    """List configured clusters (job targets).

    With ``project``, each SSH cluster also lists that project's mounts on it
    (the names ``sync_files`` / ``inspect_mount`` / ``run_workflow`` accept).
    """
    try:
        from srunx.common.config import load_settings, project_scope

        with project_scope(project):
            settings = load_settings(reload=True)
            result = []
            for name, cfg in sorted(settings.clusters.items()):
                entry: dict[str, Any] = {
                    "name": name,
                    "transport": cfg.transport,
                    "host": cfg.host or cfg.hostname,
                    "description": cfg.description,
                }
                if project and cfg.transport == "ssh":
                    entry["mounts"] = [
                        {"name": m.name, "local": m.local, "remote": m.remote}
                        for m in settings.mounts(name)
                    ]
                result.append(entry)
        return ok(clusters=result, count=len(result))
    except Exception as e:
        return err(str(e))
