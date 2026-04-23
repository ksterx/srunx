"""Tests for sweep-level notification integration.

Covers:
  * :func:`srunx.notifications.presets.should_deliver` truth table for
    the new ``sweep_run.status_changed`` event kind.
  * :meth:`SlackWebhookAdapter._format_sweep_run_event` block
    rendering + sanitization.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from srunx.db.models import Event
from srunx.notifications.adapters.slack_webhook import SlackWebhookAdapter
from srunx.notifications.presets import should_deliver

# ---------------------------------------------------------------------------
# should_deliver truth table
# ---------------------------------------------------------------------------

TERMINAL_SWEEP_STATUSES = ["completed", "failed", "cancelled"]
NON_TERMINAL_SWEEP_STATUSES = ["pending", "running", "draining"]


class TestShouldDeliverSweep:
    """``sweep_run.status_changed`` preset truth table."""

    @pytest.mark.parametrize("status", TERMINAL_SWEEP_STATUSES)
    def test_terminal_preset_terminal_status_fires(self, status: str) -> None:
        assert should_deliver("terminal", "sweep_run.status_changed", status) is True

    @pytest.mark.parametrize("status", NON_TERMINAL_SWEEP_STATUSES)
    def test_terminal_preset_non_terminal_status_skipped(self, status: str) -> None:
        assert should_deliver("terminal", "sweep_run.status_changed", status) is False

    def test_running_and_terminal_preset_running_fires(self) -> None:
        assert (
            should_deliver(
                "running_and_terminal", "sweep_run.status_changed", "running"
            )
            is True
        )

    @pytest.mark.parametrize("status", TERMINAL_SWEEP_STATUSES)
    def test_running_and_terminal_preset_terminal_fires(self, status: str) -> None:
        assert (
            should_deliver("running_and_terminal", "sweep_run.status_changed", status)
            is True
        )

    def test_running_and_terminal_preset_pending_skipped(self) -> None:
        assert (
            should_deliver(
                "running_and_terminal", "sweep_run.status_changed", "pending"
            )
            is False
        )

    def test_running_and_terminal_preset_draining_skipped(self) -> None:
        # 'draining' is an internal transient state and not interesting
        # to subscribers — only running + terminal qualify.
        assert (
            should_deliver(
                "running_and_terminal", "sweep_run.status_changed", "draining"
            )
            is False
        )

    @pytest.mark.parametrize(
        "status", TERMINAL_SWEEP_STATUSES + NON_TERMINAL_SWEEP_STATUSES
    )
    def test_all_preset_always_fires(self, status: str) -> None:
        assert should_deliver("all", "sweep_run.status_changed", status) is True

    @pytest.mark.parametrize(
        "status", TERMINAL_SWEEP_STATUSES + NON_TERMINAL_SWEEP_STATUSES
    )
    def test_digest_preset_never_fires_phase_one(self, status: str) -> None:
        # Digest is a Phase-2 placeholder; returns False for every kind
        # including sweep_run.status_changed.
        assert should_deliver("digest", "sweep_run.status_changed", status) is False


# ---------------------------------------------------------------------------
# _format_sweep_run_event
# ---------------------------------------------------------------------------


def _make_event(payload: dict) -> Event:
    return Event(
        id=1,
        kind="sweep_run.status_changed",
        source_ref=f"sweep_run:{payload.get('sweep_run_id', 1)}",
        payload=payload,
        payload_hash="deadbeef",
        observed_at=datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC),
    )


class TestFormatSweepRunEvent:
    """Slack block rendering for sweep events."""

    def test_terminal_completed_renders_progress(self) -> None:
        event = _make_event(
            {
                "sweep_run_id": 42,
                "name": "hyperparam_search",
                "from_status": "running",
                "to_status": "completed",
                "cell_count": 4,
                "cells_completed": 4,
                "cells_failed": 0,
                "cells_cancelled": 0,
                "cells_running": 0,
                "cells_pending": 0,
                "representative_error": None,
            }
        )

        text, blocks = SlackWebhookAdapter._build_message(event)

        assert text == "Sweep completed"
        assert len(blocks) == 1
        body = blocks[0]["text"]["text"]
        assert "*Sweep status changed*" in body
        assert "`42`" in body
        # Underscores in the sweep name are escaped by sanitize_slack_text.
        assert "`hyperparam\\_search`" in body
        assert "running → *completed*" in body
        assert "4/4 completed (0 failed, 0 cancelled)" in body
        # No error line when representative_error is None.
        assert "Error:" not in body

    def test_failed_sweep_includes_representative_error(self) -> None:
        event = _make_event(
            {
                "sweep_run_id": 7,
                "name": "sweep7",
                "from_status": "running",
                "to_status": "failed",
                "cell_count": 3,
                "cells_completed": 1,
                "cells_failed": 2,
                "cells_cancelled": 0,
                "cells_running": 0,
                "cells_pending": 0,
                "representative_error": "CUDA out of memory",
            }
        )

        _text, blocks = SlackWebhookAdapter._build_message(event)

        body = blocks[0]["text"]["text"]
        assert "1/3 completed (2 failed, 0 cancelled)" in body
        # Sanitization converts backticks to apostrophes before wrapping
        # the value in backticks — ensure the raw error text is still
        # recognizable.
        assert "CUDA out of memory" in body

    def test_sanitization_escapes_markdown_and_control_chars(self) -> None:
        # Injection attempt: angle brackets, markdown bold, control chars.
        # All must be escaped before reaching the Slack block payload.
        event = _make_event(
            {
                "sweep_run_id": 1,
                "name": "<script>*bold*_italic_",
                "from_status": "pending",
                "to_status": "running",
                "cell_count": 2,
                "cells_completed": 0,
                "cells_failed": 0,
                "cells_cancelled": 0,
                "cells_running": 1,
                "cells_pending": 1,
                "representative_error": "bad `code` here",
            }
        )

        _text, blocks = SlackWebhookAdapter._build_message(event)

        body = blocks[0]["text"]["text"]
        # Angle brackets HTML-escaped.
        assert "<script>" not in body
        assert "&lt;script&gt;" in body
        # Bold / italic markers escaped.
        assert "\\*bold\\*" in body
        assert "\\_italic\\_" in body
        # Backticks in error replaced with apostrophes before wrapping.
        assert "bad 'code' here" in body

    def test_missing_payload_fields_fall_back_gracefully(self) -> None:
        # Aggregator is expected to supply every field, but adapters
        # must not crash if a payload is missing keys (e.g. during a
        # partial rollout). All integer counters default to 0.
        event = _make_event(
            {
                "sweep_run_id": 99,
                "name": "sparse",
                "from_status": "pending",
                "to_status": "running",
            }
        )

        text, blocks = SlackWebhookAdapter._build_message(event)
        assert text == "Sweep running"
        body = blocks[0]["text"]["text"]
        assert "0/0 completed (0 failed, 0 cancelled)" in body

    def test_fallback_id_parsed_from_source_ref(self) -> None:
        # sweep_run_id missing from payload → adapter falls back to
        # the numeric id in source_ref.
        event = Event(
            id=1,
            kind="sweep_run.status_changed",
            source_ref="sweep_run:123",
            payload={"name": "x", "from_status": "running", "to_status": "completed"},
            payload_hash="deadbeef",
            observed_at=datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC),
        )

        _text, blocks = SlackWebhookAdapter._build_message(event)
        assert "`123`" in blocks[0]["text"]["text"]
