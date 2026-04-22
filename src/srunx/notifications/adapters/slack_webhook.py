"""Slack incoming-webhook delivery adapter.

Uses :class:`slack_sdk.WebhookClient` (already a project dependency) to
POST mrkdwn-formatted blocks to an Incoming Webhook URL. Every user-
supplied identifier is sanitized through
:func:`srunx.notifications.sanitize.sanitize_slack_text`.
"""

from __future__ import annotations

from typing import Any

from slack_sdk import WebhookClient

from srunx.db.models import Event
from srunx.notifications.adapters.base import DeliveryError
from srunx.notifications.sanitize import sanitize_slack_text


class SlackWebhookDeliveryAdapter:
    """Deliver events via a Slack Incoming Webhook URL."""

    kind: str = "slack_webhook"

    def send(self, event: Event, endpoint_config: dict) -> None:
        """Render ``event`` as Slack mrkdwn blocks and POST them.

        Args:
            event: Event to deliver. ``event.kind`` selects the block
                template; ``event.payload`` provides the substitution
                values (status, identifiers, etc.).
            endpoint_config: Must contain ``"webhook_url"`` pointing
                at a valid Slack Incoming Webhook.

        Raises:
            DeliveryError: When the webhook URL is missing, or when
                Slack returns a non-OK response.
        """
        webhook_url = endpoint_config.get("webhook_url")
        if not webhook_url or not isinstance(webhook_url, str):
            raise DeliveryError(
                "slack_webhook endpoint config is missing 'webhook_url'"
            )

        text, blocks = self._build_message(event)

        client = WebhookClient(webhook_url)
        try:
            response = client.send(text=text, blocks=blocks)
        except Exception as exc:  # network / SSL / etc.
            raise DeliveryError(f"Slack webhook send raised: {exc!r}") from exc

        status_code = getattr(response, "status_code", None)
        body = getattr(response, "body", "")
        if status_code != 200 or body != "ok":
            raise DeliveryError(
                f"Slack webhook returned non-OK response: "
                f"status_code={status_code!r} body={body!r}"
            )

    # -- block templating --------------------------------------------------

    @staticmethod
    def _id_from_source_ref(source_ref: str, expected_prefix: str) -> str | None:
        """Extract the numeric id from ``"<prefix>:<id>"`` shaped source_ref."""
        if not source_ref.startswith(f"{expected_prefix}:"):
            return None
        suffix = source_ref[len(expected_prefix) + 1 :]
        return suffix or None

    @staticmethod
    def _build_message(event: Event) -> tuple[str, list[dict[str, Any]]]:
        """Return ``(text, blocks)`` for the given event.

        ``text`` is the fallback message for notifications that don't
        render blocks; ``blocks`` is the rich mrkdwn block list.

        Identifier fallback: when ``payload`` does not carry ``job_id`` /
        ``run_id``, we parse them from ``source_ref`` (which is guaranteed
        to be present in the canonical ``"<kind>:<id>"`` form) so
        notifications produced by ``ActiveWatchPoller`` — whose payloads
        carry only status fields — still show a useful identifier.
        """
        payload = event.payload or {}
        kind = event.kind
        source_ref = event.source_ref

        if kind == "job.submitted":
            fallback_id = SlackWebhookDeliveryAdapter._id_from_source_ref(
                source_ref, "job"
            )
            job_id = sanitize_slack_text(
                str(payload.get("job_id") or fallback_id or "?")
            )
            name = sanitize_slack_text(
                str(payload.get("name") or payload.get("job_name", "?"))
            )
            text = "Job submitted"
            body = f"*Job submitted*\n• ID: `{job_id}`\n• Name: `{name}`"
            return text, [_section(body)]

        if kind == "job.status_changed":
            fallback_id = SlackWebhookDeliveryAdapter._id_from_source_ref(
                source_ref, "job"
            )
            job_id = sanitize_slack_text(
                str(payload.get("job_id") or fallback_id or "?")
            )
            name = sanitize_slack_text(
                str(payload.get("name") or payload.get("job_name", "?"))
            )
            from_status = sanitize_slack_text(str(payload.get("from_status", "?")))
            to_status = sanitize_slack_text(str(payload.get("to_status", "?")))
            text = f"Job {to_status}"
            body = (
                f"*Job status changed*\n"
                f"• ID: `{job_id}`\n"
                f"• Name: `{name}`\n"
                f"• {from_status} → *{to_status}*"
            )
            return text, [_section(body)]

        if kind == "workflow_run.status_changed":
            fallback_id = SlackWebhookDeliveryAdapter._id_from_source_ref(
                source_ref, "workflow_run"
            )
            run_id = sanitize_slack_text(
                str(
                    payload.get("workflow_run_id")
                    or payload.get("run_id")
                    or fallback_id
                    or "?"
                )
            )
            name = sanitize_slack_text(str(payload.get("workflow_name", "?")))
            from_status = sanitize_slack_text(str(payload.get("from_status", "?")))
            to_status = sanitize_slack_text(str(payload.get("to_status", "?")))
            text = f"Workflow {to_status}"
            body = (
                f"*Workflow run status changed*\n"
                f"• Run: `{run_id}`\n"
                f"• Name: `{name}`\n"
                f"• {from_status} → *{to_status}*"
            )
            return text, [_section(body)]

        if kind == "sweep_run.status_changed":
            return SlackWebhookDeliveryAdapter._format_sweep_run_event(event)

        if kind == "resource.threshold_crossed":
            partition = sanitize_slack_text(str(payload.get("partition", "all")))
            available = sanitize_slack_text(str(payload.get("gpus_available", "?")))
            threshold = sanitize_slack_text(str(payload.get("threshold", "?")))
            text = "Resource threshold crossed"
            body = (
                f"*Resource threshold crossed*\n"
                f"• Partition: `{partition}`\n"
                f"• GPUs available: `{available}` (threshold: `{threshold}`)"
            )
            return text, [_section(body)]

        if kind == "scheduled_report.due":
            schedule_id = sanitize_slack_text(str(payload.get("schedule_id", "?")))
            scheduled_at = sanitize_slack_text(
                str(payload.get("scheduled_run_at_iso", "?"))
            )
            text = "Scheduled report"
            body = (
                f"*Scheduled report due*\n"
                f"• Schedule: `{schedule_id}`\n"
                f"• Scheduled for: `{scheduled_at}`"
            )
            return text, [_section(body)]

        # Fallback for unknown kinds: stringify the whole event.
        safe_kind = sanitize_slack_text(kind)
        safe_ref = sanitize_slack_text(event.source_ref)
        text = f"srunx event: {safe_kind}"
        body = f"*{safe_kind}*\n• source_ref: `{safe_ref}`"
        return text, [_section(body)]

    @staticmethod
    def _format_sweep_run_event(event: Event) -> tuple[str, list[dict[str, Any]]]:
        """Render a ``sweep_run.status_changed`` event.

        Uses the aggregator payload schema: ``to_status``, ``from_status``,
        ``cell_count``, ``cells_completed``, ``cells_failed``,
        ``cells_cancelled``, ``cells_running``, ``cells_pending``,
        ``representative_error``, ``sweep_run_id``, ``name``.
        """
        payload = event.payload or {}
        fallback_id = SlackWebhookDeliveryAdapter._id_from_source_ref(
            event.source_ref, "sweep_run"
        )
        sweep_id = sanitize_slack_text(
            str(payload.get("sweep_run_id") or fallback_id or "?")
        )
        name = sanitize_slack_text(str(payload.get("name", "?")))
        from_status = sanitize_slack_text(str(payload.get("from_status", "?")))
        to_status = sanitize_slack_text(str(payload.get("to_status", "?")))

        cell_count = int(payload.get("cell_count") or 0)
        cells_completed = int(payload.get("cells_completed") or 0)
        cells_failed = int(payload.get("cells_failed") or 0)
        cells_cancelled = int(payload.get("cells_cancelled") or 0)

        progress = (
            f"{cells_completed}/{cell_count} completed "
            f"({cells_failed} failed, {cells_cancelled} cancelled)"
        )
        progress_safe = sanitize_slack_text(progress)

        text = f"Sweep {to_status}"
        body_lines = [
            "*Sweep status changed*",
            f"• Sweep: `{sweep_id}`",
            f"• Name: `{name}`",
            f"• {from_status} → *{to_status}*",
            f"• Progress: {progress_safe}",
        ]

        representative_error = payload.get("representative_error")
        if representative_error:
            error_safe = sanitize_slack_text(str(representative_error))
            body_lines.append(f"• Error: `{error_safe}`")

        return text, [_section("\n".join(body_lines))]


def _section(markdown: str) -> dict[str, Any]:
    """Return a Slack ``section`` block wrapping ``markdown``."""
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": markdown},
    }
