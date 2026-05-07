"""Callback assembly for CLI workflow runs."""

from __future__ import annotations

import os

from srunx.callbacks import Callback, NotificationWatchCallback
from srunx.common.logging import get_logger
from srunx.observability.notifications.legacy_slack import SlackCallback

logger = get_logger(__name__)


def _build_workflow_callbacks(
    *,
    endpoint: str | None,
    effective_preset: str,
    is_sweep: bool,
    slack: bool,
    debug: bool,
    scheduler_key: str,
) -> list[Callback]:
    """Assemble the callback list for a CLI workflow invocation.

    ``NotificationWatchCallback`` is omitted for sweep runs because the
    orchestrator manages a sweep-level watch + subscription; attaching a
    per-job callback there would spam one notification per cell. The
    ``scheduler_key`` is threaded in so the watch the callback creates
    targets the transport the workflow will actually submit against.

    ``SlackCallback`` is legacy in-process delivery and is still attached
    in both modes for backward compatibility.
    """
    callbacks: list[Callback] = []
    if endpoint and not is_sweep:
        callbacks.append(
            NotificationWatchCallback(
                endpoint_name=endpoint,
                preset=effective_preset,
                scheduler_key=scheduler_key,
            )
        )
    if slack:
        logger.warning(
            "`--slack` is deprecated; configure an endpoint via "
            "Settings → Notifications and pass `--endpoint <name>`."
        )
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL environment variable is not set")
        callbacks.append(SlackCallback(webhook_url=webhook_url))

    if debug:
        from srunx.cli._helpers.debug_callback import DebugCallback

        callbacks.append(DebugCallback())

    return callbacks
