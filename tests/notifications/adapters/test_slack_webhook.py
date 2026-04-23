"""Tests for :class:`srunx.notifications.adapters.slack_webhook.SlackWebhookDeliveryAdapter`.

``slack_sdk.WebhookClient.send`` is mocked via
``unittest.mock.patch`` — no live HTTP calls are made.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from srunx.db.models import Event

from srunx.notifications.adapters.base import DeliveryError
from srunx.notifications.adapters.slack_webhook import SlackWebhookDeliveryAdapter


def _make_event(
    kind: str,
    source_ref: str,
    payload: dict | None = None,
) -> Event:
    return Event(
        id=1,
        kind=kind,  # type: ignore[arg-type]  # literal checked in tests
        source_ref=source_ref,
        payload=payload or {},
        payload_hash="dummy-hash",
        observed_at=datetime.now(UTC),
    )


def _ok_response() -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.body = "ok"
    return response


def _error_response(status_code: int = 500, body: str = "server_error") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.body = body
    return response


class TestSend:
    """High-level ``send()`` behaviour."""

    def test_missing_webhook_url_raises(self) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event("job.submitted", "job:1", {"job_id": 1, "name": "x"})
        with pytest.raises(DeliveryError, match="webhook_url"):
            adapter.send(event, {})

    def test_non_ok_response_raises(self) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event("job.submitted", "job:1", {"job_id": 1, "name": "x"})
        endpoint_config = {"webhook_url": "https://hooks.slack.com/services/A/B/C"}

        mock_client = MagicMock()
        mock_client.send.return_value = _error_response()

        with patch(
            "srunx.notifications.adapters.slack_webhook.WebhookClient",
            return_value=mock_client,
        ):
            with pytest.raises(DeliveryError, match="non-OK"):
                adapter.send(event, endpoint_config)

    def test_exception_wrapped(self) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event("job.submitted", "job:1", {"job_id": 1, "name": "x"})
        endpoint_config = {"webhook_url": "https://hooks.slack.com/services/A/B/C"}

        mock_client = MagicMock()
        mock_client.send.side_effect = RuntimeError("connection refused")

        with patch(
            "srunx.notifications.adapters.slack_webhook.WebhookClient",
            return_value=mock_client,
        ):
            with pytest.raises(DeliveryError, match="raised"):
                adapter.send(event, endpoint_config)

    def test_ok_response_does_not_raise(self) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event("job.submitted", "job:1", {"job_id": 1, "name": "x"})
        endpoint_config = {"webhook_url": "https://hooks.slack.com/services/A/B/C"}

        mock_client = MagicMock()
        mock_client.send.return_value = _ok_response()

        with patch(
            "srunx.notifications.adapters.slack_webhook.WebhookClient",
            return_value=mock_client,
        ):
            adapter.send(event, endpoint_config)
            mock_client.send.assert_called_once()


class TestSanitization:
    """Every user-supplied identifier should flow through ``sanitize_slack_text``."""

    def test_job_name_sanitized_in_blocks(self) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event(
            "job.submitted",
            "job:1",
            {"job_id": 1, "name": "<script>&bad`"},
        )
        endpoint_config = {"webhook_url": "https://hooks.slack.com/services/A/B/C"}

        mock_client = MagicMock()
        mock_client.send.return_value = _ok_response()

        with patch(
            "srunx.notifications.adapters.slack_webhook.WebhookClient",
            return_value=mock_client,
        ):
            adapter.send(event, endpoint_config)

        kwargs = mock_client.send.call_args.kwargs
        blocks = kwargs["blocks"]
        rendered = " ".join(block["text"]["text"] for block in blocks)
        # Raw HTML brackets from the input should be entity-escaped.
        assert "<script>" not in rendered
        assert "&lt;" in rendered and "&gt;" in rendered
        # Raw ampersand from the input should be entity-escaped.
        assert "&amp;" in rendered
        # Backticks from the *input* value are replaced with apostrophes
        # (template-level backticks around identifiers are preserved).
        assert "bad`" not in rendered


class TestEventKinds:
    """Each supported event kind produces a non-empty ``blocks`` payload."""

    @pytest.mark.parametrize(
        "kind,source_ref,payload",
        [
            (
                "job.submitted",
                "job:42",
                {"job_id": 42, "name": "train"},
            ),
            (
                "job.status_changed",
                "job:42",
                {
                    "job_id": 42,
                    "name": "train",
                    "from_status": "PENDING",
                    "to_status": "RUNNING",
                },
            ),
            (
                "workflow_run.status_changed",
                "workflow_run:7",
                {
                    "run_id": 7,
                    "workflow_name": "pipeline",
                    "from_status": "pending",
                    "to_status": "running",
                },
            ),
            (
                "resource.threshold_crossed",
                "resource:gpu",
                {
                    "partition": "gpu",
                    "gpus_available": 4,
                    "threshold": 4,
                },
            ),
            (
                "scheduled_report.due",
                "scheduled_report:daily",
                {
                    "schedule_id": "daily",
                    "scheduled_run_at_iso": "2026-04-18T00:00:00Z",
                },
            ),
        ],
    )
    def test_produces_blocks(
        self,
        kind: str,
        source_ref: str,
        payload: dict[str, Any],
    ) -> None:
        adapter = SlackWebhookDeliveryAdapter()
        event = _make_event(kind, source_ref, payload)
        endpoint_config = {"webhook_url": "https://hooks.slack.com/services/A/B/C"}

        mock_client = MagicMock()
        mock_client.send.return_value = _ok_response()

        with patch(
            "srunx.notifications.adapters.slack_webhook.WebhookClient",
            return_value=mock_client,
        ):
            adapter.send(event, endpoint_config)

        kwargs = mock_client.send.call_args.kwargs
        assert "text" in kwargs
        assert isinstance(kwargs["text"], str) and kwargs["text"] != ""
        blocks = kwargs["blocks"]
        assert isinstance(blocks, list) and len(blocks) >= 1
        for block in blocks:
            assert block["type"] == "section"
            assert block["text"]["type"] == "mrkdwn"
            assert isinstance(block["text"]["text"], str)
            assert block["text"]["text"] != ""
