"""YAML loading + notification endpoint lookup for CLI workflow runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from srunx.common.exceptions import WorkflowValidationError
from srunx.runtime.sweep import SweepSpec


def _load_yaml_sweep(yaml_file: Path) -> tuple[dict[str, Any], SweepSpec | None]:
    """Return ``(raw_yaml_dict, yaml_sweep_spec)``.

    ``yaml_sweep_spec`` is ``None`` when the YAML has no ``sweep:`` block.
    Invalid sweep blocks surface via ``WorkflowValidationError`` so the
    CLI path reports them consistently with the orchestrator.
    """
    with open(yaml_file, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}, None
    raw_sweep = data.get("sweep")
    if raw_sweep is None:
        return data, None
    if not isinstance(raw_sweep, dict):
        raise WorkflowValidationError(
            "YAML sweep: must be a mapping with matrix / fail_fast / max_parallel"
        )
    try:
        spec = SweepSpec.model_validate(raw_sweep)
    except ValueError as exc:
        # Pydantic v2 ``ValidationError`` extends ``ValueError`` — covers
        # bad matrix entries, missing max_parallel, etc.
        raise WorkflowValidationError(f"invalid YAML sweep block: {exc}") from exc
    return data, spec


def _resolve_endpoint_id(endpoint: str | None) -> int | None:
    """Look up a notification endpoint by name, returning its id or ``None``.

    Matches the attach logic used by :class:`NotificationWatchCallback`
    (lookup by name, skip when missing or disabled) so sweep runs honour
    ``--endpoint`` the same way non-sweep runs do.
    """
    if not endpoint:
        return None
    try:
        from srunx.observability.storage.connection import open_connection
        from srunx.observability.storage.repositories.endpoints import (
            EndpointRepository,
        )
    except ImportError:  # pragma: no cover — DB module unavailable
        return None
    conn = open_connection()
    try:
        row = EndpointRepository(conn).get_by_name("slack_webhook", endpoint)
    finally:
        conn.close()
    if row is None or row.disabled_at is not None or row.id is None:
        return None
    return row.id
